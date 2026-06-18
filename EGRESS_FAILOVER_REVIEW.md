# Egress Failover Review — Rust/Python client vs Java reference & server contract

**Date:** 2026-06-18
**Branch:** `jh_conn_pool_refactor` (submodule `c-questdb-client` @ `6fd1989`)
**Scope:** the QWP/WebSocket **egress (query/read)** failover path — multi-endpoint
walk, host-health tracking, retry budget, role-mismatch taxonomy, connect/handshake/TLS,
and test coverage — compared against:

- **Java reference client:** `/home/jara/devel/oss/java-questdb-client` (`QwpQueryClient`, `QwpHostHealthTracker`)
- **Server wire contract:** `/home/jara/devel/oss/questdb-arrays` (`core/src/main/.../cutlass/qwp/server`)
- **Enterprise role/zone/auth/TLS:** `/home/jara/devel/oss/questdb-enterprise` (`questdb-ent/.../cutlass/qwp`)

> Note on the standalone vs embedded Java client: `QwpQueryClient.java` and
> `QwpHostHealthTracker.java` in `/home/jara/devel/oss/java-questdb-client` are
> **byte-identical** to the copies embedded in `questdb-arrays/java-questdb-client`,
> so the implementation comparison is valid; only the *test sets* differ.

---

## 1. Verdict

The egress failover is a **faithful, high-quality port of the Java reference, with no
critical correctness gap against the server wire contract.** In several areas it is
*stronger* than Java (mid-query replay correctness, a `FailoverWouldDuplicate`
anti-duplication guard, bounded TLS-handshake reads, exact dial-budget assertions).

The real issues are:

1. **Small behavioral divergences** on the egress path worth a deliberate decision.
2. **A meaningful e2e test-coverage gap** — the Rust/py failover walk is only
   unit-tested; the live multi-endpoint role/zone e2e harness drives the *Java* client.

---

## 2. Architecture grounding (read this first)

There are **two role-signaling mechanisms on two different endpoints.** Conflating
them is the main source of confusion in this area.

| | Ingress `/write/v4` (line sender) | **Egress `/read/v1` (query reader — focus)** |
|---|---|---|
| Role rejection | HTTP **`421 Misdirected Request` + `X-QuestDB-Role`** (pre-upgrade) | **Always 101**, then an unsolicited binary **`SERVER_INFO` (`0x18`)** frame carrying `role/epoch/capabilities/zone`; client applies its `target=` filter and skips on mismatch |
| Java code | `QwpIngressUpgradeProcessor` / `QwpUpgradeFailures` | `QwpQueryClient.connect()` → `matchesTarget` |
| Rust code | `ingress/sender/qwp_ws*.rs` | `egress/{reader,transport,server_event}.rs` |

The egress reader handles **both** surfaces (the 421 path defensively, for proxies /
mixed deploys) and matches Java exactly.

- **Roles:** `STANDALONE=0x00, PRIMARY=0x01, REPLICA=0x02, PRIMARY_CATCHUP=0x03`.
- **Transient vs topological:** only `PRIMARY_CATCHUP` is *transient* (promotion in
  flight); every other role, including unrecognized tokens, is *topological*.
  **Confirmed identical** in Java (`QwpIngressRoleRejectedException.isTransient`,
  lines 84-86) and Rust (`egress/error.rs:202-206` `UpgradeReject::is_transient`).
- **No mid-stream resume contract.** On a dropped read the client must re-issue the
  whole query (fresh `request_id`, replay from `batch_seq=0`). The server provides no
  ACK/offset/resume token; its only "rollback" is internal connection-scoped
  symbol-dict cleanup (`QwpEgressResumeRollbackTest`).

---

## 3. Parity confirmed (what matches)

- **Host-health tracker** (`egress/tracker.rs` vs `QwpHostHealthTracker.java`): near 1:1
  port — identical `(state, zone_tier)` priority lattice
  (`HEALTHY < UNKNOWN < TRANSIENT_REJECT < TRANSPORT_ERROR < TOPOLOGY_REJECT`,
  `SAME < UNKNOWN < OTHER`), sticky-healthy semantics, round-based recovery (no timed
  expiry / half-open). Backoff constants match to the millisecond:
  **8 attempts / 50 ms→1 s full-jitter / 30 s deadline / 15 s auth-timeout /
  5 s server-info-timeout.**
- **Retry budget** (`reader.rs`/`transport.rs` vs `QwpQueryClient`): the
  `545f8a6` *"align failover budget with execute attempts"* fix is **correct** —
  per-Execute attempt+time budget, initial attempt counted
  (`reconnect_rounds = max_attempts - 1`), deadline checked before sleeping, budget
  shared across successive mid-query failovers (not reset), and the connect-walk (role
  election) kept *out* of the per-query budget. No off-by-one.
- **`RoleMismatch` error-code plumbing is ABI-stable and consistent end-to-end:**
  `ErrorCode::RoleMismatch → line_sender_error_role_mismatch = 18` (appended) across
  `error.rs`, FFI `lib.rs:294/343`, the C header (`:156`), the ABI tripwire test, and
  `line_sender.pxd`/`ingress.pyx`. The reader's pre-existing
  `line_reader_error_role_mismatch = 8` (separate enum) also folds into
  py `IngressErrorCode.RoleMismatch`. The Python-only sentinel relocation to the
  `0x10000` band (`BadDataFrame`, `Cancelled`, `FailoverWouldDuplicate`) genuinely
  prevents aliasing of the appended FFI code, and the enum-guard test
  (`test_python_only_error_codes_do_not_overlap_ffi_codes`) really catches collisions.
- **Connect/handshake/TLS classification: zero mismatches.** 401/403 → terminal
  cluster-wide; refused / TLS / 421 / 426 / 5xx / 404 / malformed / version-mismatch /
  timeout → retry-next. Rust is **safer** on one axis: its `auth_timeout` bounds the
  TLS-handshake read (lazy rustls during the upgrade), whereas Java does TLS eagerly
  with only OS timeouts — so a TLS-layer blackhole is bounded in Rust, unbounded in
  Java. Rust connect cleanup is RAII; no FD/native leak found under repeated failover.
- **Enterprise role/zone model is fully expressible** in Rust/py:
  `target=any|primary|replica`, `zone=<id>`, `CAP_ZONE=0x0000_0001`, identical wire
  bytes, case-insensitive/trimmed zone comparison, `target=primary` collapses zone
  tiers to `Same`. Python reaches every knob via the pass-through conf string
  (`line_reader_from_conf`).
- **Rust/py is *ahead* of Java** on coverage in: read-side mid-query replay +
  schema re-read, the `FailoverWouldDuplicate` streaming guard, distinct
  deadline-vs-attempts exhaustion messages, on-wire auth-header byte pinning, and
  progress-callback lifecycle assertions.

---

## 4. Findings (prioritized)

Legend — **Path:** which connect path the finding is on. **Sev:** severity.

| # | Path | Sev | Finding | Evidence | Recommendation |
|---|------|-----|---------|----------|----------------|
| 1 | Egress | Low | **Terminal-code set is wider in Rust:** `ConfigError` / `UnsupportedServer` / `AuthError` abort the walk; Java aborts only on auth and retries the rest across all hosts. Unclear whether `UnsupportedServer` ever fires on the *connect* path (content-encoding rejection maps to failover-eligible `HandshakeError`). | `reader.rs:305-309,514-518` vs `QwpQueryClient.java:866-868`; `transport.rs:540-545` | Confirm which connect-time condition yields terminal `UnsupportedServer`; align or document. Verdict is identical against a uniformly-bad cluster; only latency/log pressure differs. |
| 2 | Egress | Low | **401 and 403 collapse into one `AuthError`.** Behavior (terminate) is correct, but enterprise tests assert a 401 (bad credential) vs 403 (no grant / disabled) distinction, plus an in-band SQL `SECURITY_ERROR` — diagnostic granularity is lost. | `transport.rs:642`; enterprise `QwpEgressAuthTest`, `QwpWebSocketTlsAclTest:193` | Optional: keep the HTTP status on `AuthError` for diagnostics. |
| 3 | Egress | Low / by-design | **Replay-from-zero with `FailoverWouldDuplicate` guard.** Rust refuses post-data-delivery replay unless an `on_failover_reset` callback is registered; Java replays unconditionally. Rust is *safer*, and this **matches the server contract** (no resume token). Portability gotcha when comparing the two clients. | `reader.rs:434,900-968`; server: no resume (`QwpEgressResumeRollbackTest`) | Keep. Document the "restart-from-zero" expectation for reader callbacks. |
| 4 | Egress | Cosmetic | A query-path role mismatch surfaces under **`IngressErrorCode.RoleMismatch`** — shared enum name, right category, but the `Ingress` prefix reads oddly in a query traceback. | `egress.pxi:29-30` | Consider an alias / rename if the public surface allows. |
| 5 | Ingress | Low | Sender has **no post-handshake `SERVER_INFO` `target=` re-check** (detects role only via 421). Benign **given the contract** — the ingress server 421s a role mismatch *before* upgrading — but it's an unstated asymmetry vs the reader/Java. | `qwp_ws.rs` (absent); reader `reader.rs:361-408` | Confirm the "ingress always 421s first" assumption holds for all deployments/proxies; otherwise add the re-check. |

---

## 5. Test coverage — gaps (ranked)

1. **No live multi-endpoint e2e for the Rust/py egress walk.** Enterprise
   `QwpEgressServerInfoRoleTest` / `QwpEgressServerInfoZoneTest` and the
   `QwpEgressSidecarMain` harness stand up real primary+replica nodes — but the sidecar
   drives the **Java** `QwpQueryClient`. The Rust/py failover walk is only *unit*-tested
   (tracker, decoders, config). **Highest-value gap.**
2. **No live mid-query failover-to-replica replay test for Rust/py.** The mechanism
   exists (`on_failover_reset` trampoline, replay, schema re-read) but no integration
   test forces a real mid-stream disconnect against a second live endpoint and asserts a
   complete, strictly-ascending result. Java has
   `QwpEgressServerInfoRoleTest::testFailoverToReplicaReplaysAfterMidStreamDisconnect`
   via a server debug hook.
3. **403 / 404 / 426 classification not pinned in Rust tests** (only 401/421/version).
   The *code* is correct (403→`AuthError` terminal; 404/426→failover-eligible), so these
   are missing *tests*, not bugs — but the 401-vs-404 "is this terminal?" boundary
   deserves a guard. Java pins all of these in `QwpQueryClientWalkTrackerTest`.
4. **Connect-failure resource-leak assertion absent.** Java has `QueryClientPoolLeakTest`
   (native scratch on connect failure). Rust connect is RAII (low risk), but the **py
   eager-pool `from_conf` connect-walk-failure** path has no FD / native-memory leak
   assertion.
5. **No TLS-failover test anywhere** (Rust suite is `ws://` only) and **no
   concurrent-query-during-failover test anywhere** — the
   `reader_migrates_to_worker_thread_with_concurrent_stats_polling` test runs queries
   *sequentially* while polling atomic stats; it validates `Send`/`Sync`, not concurrent
   failover.
6. **Python fakes are HTTP-status stubs** (`_FakeStatusServer`) that never complete the
   WS upgrade or emit a `SERVER_INFO` frame. So the Python role-negotiation tests exercise
   the 421/401 *upgrade-reject* path but **not the SERVER_INFO-frame role filter** — the
   *primary* egress mechanism. The reader-side `line_reader_error_role_mismatch=8 →
   py RoleMismatch` mapping is likewise only covered via the sender path.

---

## 6. Test-quality concerns

- **Weak budget assertions in Python/system tests.** `test_*_exhausts_budget` checks only
  that the error *code* is in `{SocketError, ProtocolError, FailoverWouldDuplicate}`, not
  the dial count. A double-walk regression would pass. The exact-count contract
  (13 dials = 1 + 3×4) lives only in Rust's `attempts_exhausted_surfaces_error`.
- **Timing/sleep flakiness:**
  - Python streaming `test_iter_*_surfaces_failover_would_duplicate` relies on a 100M-row
    query still producing after the first batch when the server is bounced (comments admit
    it "can finish before the bounce"). Most flake-prone Python test.
  - Enterprise `test_kill9_primary_failover_no_data_loss` uses `time.sleep(0.5)` before
    SIGKILL; 60 s/180 s helper timeouts inflate CI cost.
  - Rust `backoff_bounded_by_jitter_ceiling` (<640 ms) and
    `failover_callback_runs_before_replayed_read` (100 ms park) are wall-clock asserts
    sensitive to loaded CI.
- **Fakes don't model the wire.** Both the Java `FakeStatusServer` and Python
  `_FakeStatusServer` answer a fixed HTTP status; they cannot drive the SERVER_INFO frame
  path. Exotic wire faults (stalled upgrade, malformed SERVER_INFO, version mismatch)
  remain Rust-mock-only.
- **Intentional multi-outcome tolerance** in `add_credit_failover_post_conditions_are_consistent`
  (accepts `resets ∈ {0,1}`) is documented and unavoidable, but means that branch is only
  deterministically pinned by the `would_silently_duplicate_truth_table` unit test.

---

## 7. Recommended next actions

1. **Add a live two-endpoint egress e2e** for Rust/py: differing roles
   (skip-replica → bind-primary via SERVER_INFO) and a forced mid-stream disconnect →
   replay (covers test gaps #1 + #2). Biggest coverage win.
2. **Add cheap classification tests:** 403-terminal, 404/426-walk-past, and a py-side
   leak assertion on connect-walk failure.
3. **Resolve the open questions** below before they bite in Enterprise multi-node.

---

## 8. Open questions

- Does the connect path ever produce a terminal **`UnsupportedServer`**, or only
  mid-stream from the zstd decoder? (Finding #1)
- **`epoch` is parsed but unused.** The contract notes clients "tracking a specific
  primary use epoch to refuse a stale reconnection." Fine for OSS (epoch always 0) — is
  there an intended Enterprise stale-primary refusal not yet wired?
- Does the reader have an **async/retrying initial-connect** mode (Java
  `InitialConnectAsyncTest`)? If purely synchronous-connect, those Java tests are
  correctly N/A.
- Does the Rust egress upgrade actually **send `X-QWP-Max-Version` / `X-QWP-Client-Id`**?
  (Server clamps if absent — not a failover risk, but worth a one-line check in request
  construction.)
- Is the **post-connect mutation guard** (`QwpQueryClientPostConnectGuardTest`)
  inapplicable because `ReaderConfig` is immutable after `from_conf`? Confirm no
  post-connect-mutable knob is exposed via FFI.

---

## Appendix — methodology

Review fanned out across seven parallel agents, each comparing the Rust implementation
(`c-questdb-client/questdb-rs/src/egress/`) against the Java reference and/or the server
contract:

| Agent | Area | Primary sources |
|-------|------|-----------------|
| A1 | Host-health tracking & endpoint selection | `egress/tracker.rs` vs `QwpHostHealthTracker.java` (+ test) |
| A2 | Retry loop & retry budget | `egress/{transport,reader,config}.rs` vs `QwpQueryClient.java`; submodule `545f8a6` |
| A3 | Error taxonomy & role-mismatch end-to-end | `error.rs` / FFI / `egress.pxi` vs `QwpRoleMismatchException` & friends |
| A4 | Connect / handshake / auth / TLS | `egress/{ws/client,auth,tls,transport}.rs` vs `WebSocketClient.java` |
| A5 | Test coverage & scenario matrix | `tests/egress_failover.rs`, `test/system_test.py`, `failover_clients/` vs Java failover tests |
| A6 | Server wire contract & e2e | `questdb-arrays/core/.../cutlass/qwp/server` + server-side e2e tests |
| A7 | Enterprise role/zone/auth/TLS | `questdb-enterprise/questdb-ent/.../cutlass/qwp` + enterprise e2e tests |

Cross-checks performed directly (not via agents): role-transience parity
(`QwpIngressRoleRejectedException.isTransient` vs `UpgradeReject::is_transient`),
standalone-vs-embedded Java client byte-identity, and the failover commit inventory.
