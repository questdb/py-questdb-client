# PR #130 Review Findings Tracker

PR: https://github.com/questdb/py-questdb-client/pull/130
Branch: `jh_experiment_new_ilp`
Last refreshed from GitHub CLI: 2026-05-26

## Tracking Legend

Use the checkboxes as the source of truth:

- `[ ]` Open
- `[x]` Fixed locally
- `[~]` Not applicable / intentionally skipped

For each item, fill in `Resolution` with the commit, test command, or reason for skipping.

## Summary

| ID | Status | Severity | Area | Finding |
| --- | --- | --- | --- | --- |
| CR-001 | [~] | Major | `setup.py` | Gate `insecure-skip-verify` behind an explicit opt-in env var. |
| CR-002 | [x] | Minor | `src/questdb/ingress.pyx` | Exclude `bool` from `retry_max_backoff` integer handling. |
| CR-003 | [ ] | Major | `test/system_test.py` | Reserve failover TCP ports instead of probing and releasing them. |
| CR-004 | [ ] | Low | `examples/qwp_udp.py` | Note that `max_datagram_size=1400` is the default, or omit it. |
| CR-005 | [ ] | Minor | `src/questdb/ingress.pyx` | Reject unknown `qwp_ws_progress` values explicitly. |
| CR-006 | [ ] | Major | `src/questdb/ingress.pyx` | Make `Sender.establish()` rollback-safe if buffer reservation fails. |
| CR-007 | [ ] | Minor | `src/questdb/ingress.pyi`, `docs/sender.rst` | Document `Sender.new_buffer()` lifecycle preconditions. |
| CR-008 | [ ] | Major | `test/system_test.py` | Avoid false positives in UDP auto-flush tests caused by context-manager close. |

## Findings

### CR-001: Gate `insecure-skip-verify` Behind Opt-In

- Status: [~]
- Source: https://github.com/questdb/py-questdb-client/pull/130#discussion_r3297951332
- Location: `setup.py:149`
- Severity: Major
- Finding: `setup.py` unconditionally enables Cargo feature `confstr-ffi,insecure-skip-verify`, which may weaken TLS certificate verification in default release builds.
- Expected fix: Build with only `confstr-ffi` by default. Add `insecure-skip-verify` only when an explicit opt-in environment variable is set, and document that variable in packaging docs.
- Verification:
  - Search Rust workspace for `insecure-skip-verify`.
  - Confirm default build args omit the feature.
  - Run the relevant packaging/build smoke test.
- Resolution: Intentionally skipped. The Python package should expose
  `tls_verify=False` / `tls_verify='unsafe_off'` as a runtime opt-in escape
  hatch for testing and controlled environments. Verification remains enabled by
  default; maintaining separate Python builds that differ only by whether this
  unsafe option exists would make the public API environment-dependent without
  improving the default security posture.

### CR-002: Exclude `bool` from `retry_max_backoff`

- Status: [x]
- Source: https://github.com/questdb/py-questdb-client/pull/130#discussion_r3297951340
- Location: `src/questdb/ingress.pyx:2352-2366`
- Severity: Minor
- Finding: Python treats `bool` as an `int`, so `True` and `False` are accepted as `1` and `0` ms for `retry_max_backoff`.
- Expected fix: Make the integer branch reject `bool`, so boolean values fall through to the existing `TypeError` path.
- Verification:
  - Add or update a test for `retry_max_backoff=True` and `retry_max_backoff=False`.
  - Run the focused Python test.
- Resolution: Fixed locally by rejecting `bool` in the shared
  int-or-`timedelta` duration handling for `auth_timeout`, `retry_timeout`,
  `retry_max_backoff`, and `request_timeout`, with focused coverage for both
  `False` and `True`. Verified with:
  `venv/bin/python test/test.py -v TestQwpWebSocketApi.test_duration_options_reject_bool TestQwpWebSocketApi.test_retry_max_backoff_rejects_non_http_protocol TestQwpWebSocketApi.test_from_conf_preserves_http_retry_max_backoff`.

### CR-003: Reserve Failover TCP Ports

- Status: [ ]
- Source: https://github.com/questdb/py-questdb-client/pull/130#discussion_r3298430224
- Location: `test/system_test.py:165-168`, also `test/system_test.py:384-386`
- Severity: Major
- Finding: `_unused_tcp_port()` returns a port after closing the socket, creating a TOCTOU race before the failover path uses it.
- Expected fix: Keep the socket bound while configuring the sender/receiver, then close it only once the failover setup no longer depends on the reservation.
- Verification:
  - Run the affected failover system tests.
- Resolution:

### CR-004: Clarify Default `max_datagram_size`

- Status: [ ]
- Source: https://github.com/questdb/py-questdb-client/pull/130#pullrequestreview-4356383933
- Location: `examples/qwp_udp.py:10-14`
- Severity: Low
- Finding: The example explicitly passes `max_datagram_size=1400`, which is the default, without saying it can be omitted or tuned.
- Expected fix: Either remove the argument or add a short inline comment that `1400` is the default.
- Verification:
  - Run `python -m py_compile examples/qwp_udp.py`.
- Resolution:

### CR-005: Reject Unknown `qwp_ws_progress` Values Explicitly

- Status: [ ]
- Source: https://github.com/questdb/py-questdb-client/pull/130#pullrequestreview-4356424837
- Location: `src/questdb/ingress.pyx:2214-2218`
- Severity: Minor
- Finding: `QwpWsProgress.parse()` can return `None`, causing an `AttributeError` on `.c_value` instead of a clear config error.
- Expected fix: Check the parse result. If it is `None`, raise a clear `IngressError` config/validation error before calling `line_sender_opts_qwpws_progress`.
- Verification:
  - Add or update a test for an unknown `qwp_ws_progress` value.
  - Run the focused Python test.
- Resolution:

### CR-006: Make `Sender.establish()` Rollback-Safe

- Status: [ ]
- Source: https://github.com/questdb/py-questdb-client/pull/130#pullrequestreview-4356424837
- Location: `src/questdb/ingress.pyx:2755-2760`
- Severity: Major
- Finding: If `_new_buffer_for_sender()` raises after `line_sender_build()` succeeds, `establish()` can leave native sender/options state live on the same `Sender`.
- Expected fix: Free options and close/reset native sender state on buffer reservation failure before re-raising.
- Verification:
  - Add or update a test that forces buffer reservation failure after sender build, if practical.
  - Run the focused Python test.
- Resolution:

### CR-007: Document `Sender.new_buffer()` Lifecycle Preconditions

- Status: [ ]
- Source: https://github.com/questdb/py-questdb-client/pull/130#pullrequestreview-4356981275
- Location: `test/system_test.py:796-814`, public contract in `src/questdb/ingress.pyi` and `docs/sender.rst`
- Severity: Minor
- Finding: Tests show `Sender.new_buffer()` raises `IngressError` before `Sender.establish()` and after `Sender.close()`, but the public stubs/docs do not state those preconditions.
- Expected fix: Document that `Sender.new_buffer()` requires an established, open sender and can raise `IngressError` for pre-establish or closed sender states.
- Verification:
  - Build or lint docs if available.
  - Run any stub/type-check smoke test used by the project.
- Resolution:

### CR-008: Avoid Auto-Flush Test False Positives

- Status: [ ]
- Source: https://github.com/questdb/py-questdb-client/pull/130#pullrequestreview-4356981275
- Location: `test/system_test.py:1043-1058`, also `1060-1073`, `1344-1361`, `1592-1607`
- Severity: Major
- Finding: QWP/UDP auto-flush tests use the sender context manager, whose normal exit calls `close(flush=True)` and can publish rows even if the tested auto-flush trigger never fired.
- Expected fix: Use manual sender lifetime for these tests: create sender, call `establish()`, and close in `finally` with `sender.close(flush=False)`.
- Verification:
  - Run the affected QWP/UDP auto-flush system tests.
- Resolution:
