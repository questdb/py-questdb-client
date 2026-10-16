# QWP ingestion failures around 2 MiB: Python client investigation

## Summary

**The reported symptom is reproducible with released software. There are five
separable client issues, not just one server limit.** Individually valid rows can
fail when the Python client combines them into a message larger than the server
will accept. Some client paths cannot split the message sufficiently; one error
hint incorrectly attributes that failure to an oversized individual row.

The starting customer report was only that QWP **ingestion** could not be used
because of “2MB limits.” The customer's exact error, schema, API calls and client
version remain unknown. Enterprise 4.0.0 or 4.0.1 was suspected. The experiments
below reproduce plausible causes; they do not establish which one the customer
encountered. Query/egress limits and historical egress fixes are outside scope.

### Issue breakdown

| ID | Component | Problem | Independent evidence |
|---|---|---|---|
| A | Rust columnar sender | Automatic splitting stops at eight rows, even when a smaller group would fit. | Eight 256 KiB strings fail; two explicitly submitted four-row groups succeed. |
| B | Python Pandas chunk planner | Small requested chunk sizes can be rounded **up** to eight for object/validity-bitmap paths. | `max_rows_per_batch=4` still fails on an eight-row object-string DataFrame. |
| C | Python standalone `Sender` | Default QWP auto-flush is row/time-based, with byte auto-flush disabled. | 600 rows of only 4 KiB each fail at the default auto-flush. |
| D | Python pooled row sender | The byte threshold is checked after appending a row; a batch can overshoot the hard cap and is not split. | Eight 256 KiB rows fail with defaults despite a 90%-of-cap byte trigger. |
| E | Python/native diagnostics | A multi-row splitting failure is reported as an oversized single row; some messages also obscure which cap binds. | Five 500,000-byte Arrow strings produce the single-row hint, but `max_rows_per_batch=2` succeeds. |

The server's finite per-message cap is a separate, legitimate constraint. An
individual encoded row larger than that cap cannot be fixed just by choosing
fewer rows per batch. Issues A–D concern **multi-row batches whose individual
rows do fit**. Raising a server buffer can mitigate these issues but does not
correct the client behavior.

## Python integration follow-up (2026-10-16)

[Native PR #219](https://github.com/questdb/c-questdb-client/pull/219) was merged
as `14fba05b2744b87bcac9616f82532d3e913cb45f`. The Python working-tree integration
pins that commit and adds regressions in `test/test_qwp_batch_splitting.py`.
This is **not a released Python fix**; the release evidence below is unchanged.

Against the same Enterprise 4.0.1 runtime and default advertised cap:

- Eight 256 KiB object strings with `max_rows_per_batch=4` now succeed.
- Five 500,000-byte Arrow strings now succeed without manual slicing.
- Both cases pass through `QuestDB.dataframe()` and standalone
  `Sender.dataframe()`, with stored row counts and string lengths verified.
- A smaller client cap forces singleton splits of 17-row Pandas and Arrow
  inputs; Boolean values, null positions, numbers, and strings round-trip.
- Mock-server tests cover two through seventeen rows, preserved small-batch
  packing, and an oversized row at the beginning, middle, or end. Failed direct
  loads do not commit or retry the uncommitted prefix in these small cases.

**A is fixed in this integration. B's rounding still exists, but no longer
causes these failures. C and D remain open; row-based batching is unchanged.**
The misleading multi-row single-row hint no longer applies to these columnar
reproductions; clearer reporting of the binding cap remains a separate task.
The live tests can be run with `QDB_HTTP_ADDR=host:port` against a QWP-capable
server; mock-server tests need no QuestDB installation.

## Versions and experimental scope

The investigation ran on 2026-10-15 using the **PyPI wheel**, not an editable
installation or the working-tree extension.

| Component | Tested version / provenance |
|---|---|
| Python client | `questdb==5.0.0`, the only QWP-capable Python release found in release history at investigation time |
| Python release source | `v5.0.0`, commit `64c10255d657c7d32cb22c57b3d80b8b2676f23e` |
| Release's C/Rust submodule | `0f74f5ee316b320afed4d2533dcd4594fd0a44f4` |
| Server executed | Locally built Enterprise **4.0.1**, commit `537be7d5cdfbd9aab7861d91287afb2ab50fc213` |
| Server's OSS submodule | `12a33d651e51e2682e7a448c8db5168fc72dfad3` |
| Python / dependencies | Python 3.11.11; NumPy 2.4.6; Pandas 3.0.6; PyArrow 25.0.1 |

Enterprise and OSS JAR manifests matched the release commits above. Runtime
`SELECT build()` reported Enterprise 4.0.1 and the same Enterprise commit.
This was a local build of the release source, not a downloaded vendor binary.

**Enterprise 4.0.0 was not executed.** Its release commit is
`c14b2bfe6bee1c405940c48e9ee7e0faa4298a43`, pinning OSS
`9559b8d490c63bf6edad1054f3af9b2231761ad7`. The following files have identical Git
blobs between the 4.0.0 and 4.0.1 source trees:

- OSS `core/src/main/java/io/questdb/PropServerConfiguration.java`
- OSS `core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressUpgradeProcessor.java`
- OSS `core/src/main/java/io/questdb/cutlass/qwp/protocol/QwpConstants.java`
- Enterprise `questdb-ent/src/main/java/com/questdb/EntPropServerConfiguration.java`

Thus the relevant server cap/configuration behavior is expected to be the same,
but that is source evidence, not a second runtime result.

Tests used fresh local databases, loopback listeners, ACL disabled, no TLS or
replication, and bounded worker counts. No customer systems or data were used.
A transparent local TCP relay recorded WebSocket payload lengths and handshake
headers without changing their contents. Successful calls were followed by SQL
row-count checks, polling for WAL apply. String tests also checked minimum and
maximum stored lengths. These checks are not a full value-by-value integrity
oracle, especially for arrays.

## The actual server constraint

With defaults, the ingress handshake advertised:

```text
X-QWP-Max-Batch-Size: 2097138
```

The server computes this as:

```text
min(http.recv.buffer.size - 14, 16 MiB)
```

`http.recv.buffer.size` defaults to 2 MiB, or 2,097,152 bytes. Fourteen bytes are
reserved for the worst-case WebSocket header. The remaining budget must contain
the QWP message, including its own headers, schema, values and dictionary data.
It is **not** a 2 MB limit on a whole DataFrame, session or dataset.

The header is present on the tested Enterprise version. A missing-cap-header
fallback is therefore **not** the explanation for these reproductions.

All 21 reproduced client failures in the final matrix occurred before publishing
any QWP data frame. A separate raw-WebSocket probe bypassed client validation and
sent a frame header declaring a 3 MiB payload. The server returned:

```text
CLOSE 1009: frame payload exceeds maximum size
```

This distinguishes two failure surfaces: a client-side `BatchTooLarge` /
`InvalidApiCall`, and a server-side WebSocket close if an oversized frame reaches
it. These are not necessarily the same customer-visible error.

[Version-pinned server calculation][server-cap].

## Findings and mechanisms

### A. Columnar splitting has an eight-row floor

The released Rust core contains:

```rust
fn split_mid(row_count: usize) -> Option<usize> {
    if row_count <= 8 {
        return None;
    }
    let mid = (row_count / 2) & !7;
    Some(if mid == 0 { 8 } else { mid })
}
```

The surrounding comment attributes the alignment to bit-packed Boolean columns
and validity bitmaps, which the slicing implementation addresses in bytes.
However, this splitting floor is also applied to all-string chunks. A five-row
or eight-row batch can be rejected even though every individual row fits.

Observed with default server buffers:

- Seven strings of 256 KiB each: success.
- Eight strings of 256 KiB each: `BatchTooLarge`.
- Sixty-four strings of 256 KiB each: recursive splitting reaches an eight-row
  group and fails.
- Five strings of 500,000 bytes each: `BatchTooLarge`.
- Eight float64 arrays of 256 KiB each: `BatchTooLarge`.

For a full eight-row group, this creates an approximate **256 KiB average
encoded-row budget**, reduced by metadata. That is an implementation consequence,
not a universal per-row or per-value protocol limit. Explicit smaller inputs
succeed.

The same floor remains in the inspected working-tree core commit
`9bbb00a18cd97373b22b0153f5ca5607525d3e3f`. The release is the runtime baseline;
this observation is not a separate full test of the development version.

[Version-pinned splitting implementation][split-floor].

### B. Pandas can undo the requested small batch size

`_dataframe_columnar_plan_has_validity()` conservatively requires eight-row
alignment for any PyObject source, even if the actual values contain no nulls.
It also requires alignment for Arrow-backed columns with nulls.

`_dataframe_columnar_rows_per_chunk()` raises a requested chunk size below eight
to eight when it is also smaller than the DataFrame's row count.

For an object-dtype string DataFrame containing eight 256 KiB values:

```python
db.dataframe(df, ..., max_rows_per_batch=4)  # still fails
```

Submitting `df.iloc[:4]` and `df.iloc[4:]` in separate calls succeeds. Five
500,000-byte strings also fail with `max_rows_per_batch=2`, but succeed when
submitted as explicit two-row slices.

This behavior is **path-dependent**, not true of every DataFrame-like input:

- A `pyarrow.Table` containing the same strings honors the tested small batch
  requests and succeeds.
- The tested Pandas column of NumPy arrays also succeeds with
  `max_rows_per_batch=4`.
- Null/Boolean alignment branches are identified from source; the experiments
  did not exhaust every dtype/null combination.

[Version-pinned Pandas planning code][pandas-alignment].

### C. Standalone QWP defaults can build an oversized batch

`Sender.from_conf('ws::...')` defaults to 600 rows or one second before
publication; byte auto-flush defaults off. A sufficiently fast loop adding 600
4 KiB strings produces a batch of about 2.35 MiB. The 600th `row()` triggers
publication and raises `InvalidApiCall`:

```text
Could not flush buffer: QWP/WebSocket encoded message size of 2460047
exceeds maximum configured allowed size of 2097138 bytes.
```

Setting `auto_flush_bytes=1048576` made the same input succeed in three frames.
The timing qualification matters: a slow loop can flush earlier on the interval
and conceal this reproduction.

[Version-pinned standalone defaults][standalone-defaults].

### D. Pooled byte auto-flush can overshoot the cap

`questdb.connect(...).sender()` does have a byte trigger, normally 90% of the
current frame cap. But `PooledSender.row()` appends a row **before** checking it.

Seven 256 KiB values remain below that soft threshold. Adding the eighth takes
the encoded frame past the hard cap, at which point the client attempts to
publish the entire buffer and fails:

```text
QWP frame (2097226 bytes) exceeds the store-and-forward per-frame cap
(2097138 bytes, the smaller of max_buf_size and the sf_max_segment_bytes
segment payload capacity)
```

The eight-row batch remains buffered; it is not automatically split. Setting a
1 MiB byte trigger made the tested input succeed in two frames. A longer test
with 2,000 such rows ingested **500 MiB total** through 500 frames successfully.

This is distinct from C: enabling a byte trigger is not by itself a complete fix
for checking only after append. A sufficiently large next row can overshoot even
a conservative trigger.

[Version-pinned threshold logic][pooled-threshold] and
[append/flush ordering][pooled-row].

### E. Diagnostics misclassify a multi-row failure

For a `pyarrow.Table` of five 500,000-byte strings, the error included:

```text
QWP frame (2500061 bytes) exceeds max_buf_size (2097138 bytes)
Hint: a single row exceeds the server per-batch cap
(max_buf_size / the server X-QWP-Max-Batch-Size); reduce the size of
individual values or raise the cap. `max_rows_per_batch` does not bound
a single row.
```

Each row fits. Repeating the Arrow call with `max_rows_per_batch=2` succeeds in
three frames. The hint incorrectly assumes native splitting has reached one row,
whereas it can stop at any group of eight or fewer rows.

There is a second diagnostic problem: native messages can call the effective
negotiated cap `max_buf_size`, or describe a store-and-forward cap only in terms
of local configuration/segment size. In these tests the **server's advertised
receive cap** was binding. Raising only client `max_buf_size` to 8 MiB did not
allow a 3 MiB row through the default server.

[Version-pinned Python hint][error-hint] and
[native effective-cap error wording][native-error].

## Experiment results and controls

The final matrix contained **50 cases: 29 successful ingestions with verified row
counts and 21 intentional failure reproductions**. No case timed out. It included
repeat cases and changed configurations; this does not mean 50 independent bugs.
Exact frame sizes depend partly on table/column names.

| Input or control | Default 2 MiB receive buffer | Tested change and outcome |
|---|---|---|
| Pandas: 100,000 rows × 16 float64 columns (12.8 MB raw) | Success, 13 frames | Shows there is no dataset-size ceiling at 2 MiB |
| Pandas / Arrow: 4,096 strings × 2 KiB (8 MiB raw) | Success, 8 frames each | Shows automatic splitting works above its floor |
| Arrow: 4,096 distinct 1 KiB symbols | Success, 4 frames | Dictionary traffic larger than 2 MiB in total is supported |
| Standalone: 600 strings × 4 KiB | Failure, issue C | 1 MiB byte trigger: success |
| Pooled: eight strings × 256 KiB | Failure, issue D | 1 MiB byte trigger: success |
| Pandas: eight strings × 256 KiB | Failure, issue A | Four-row explicit slices: success |
| Same Pandas input with `max_rows_per_batch=4` | Failure, issue B | Explicit slices still succeed |
| Arrow: five strings × 500,000 bytes | Failure, issues A/E | `max_rows_per_batch=2`: success |
| Pandas: eight arrays × 256 KiB | Failure, issue A | `max_rows_per_batch=4`: success on this path |
| Single string of 2 MiB minus 1 KiB | Success | Boundary control |
| Single string of exactly 2 MiB | Failure | Values alone exhaust the budget before encoding overhead |
| Single float64 array: 250,000 elements (2,000,000 bytes) | Success | Distinguishes decimal MB from binary MiB |
| Single float64 array: 262,144 elements (2 MiB) | Failure | Boundary control |
| Single 3 MiB string or array | Failure | Expected genuine single-row constraint |
| Single 3 MiB string, client `max_buf_size=8388608` | Still fails | Client setting cannot override server cap |
| Same 3 MiB string over ILP/HTTP | Success | The QWP transport constraint is not a storage-value limit |

With **server** `http.recv.buffer.size=8m`, the advertised cap became
`8388594`. Nine previously failing scenarios were retested successfully,
covering standalone rows, pooled rows, Pandas strings/arrays, and 3 MiB individual
values. A 16 MiB DataFrame of 64 × 256 KiB strings succeeded in four approximately
4 MiB frames.

## Self-contained reproduction

This section does not depend on the original `/tmp` experiment harness.

### Prerequisites

1. Use an isolated Enterprise 4.0.1 instance. **Do not run against production:**
   the script creates uniquely named tables and leaves them for inspection.
2. Bind HTTP to loopback, retain the default 2 MiB receive buffer, and disable
   authentication only on this isolated test instance. For example:

   ```properties
   http.bind.to=127.0.0.1:9000
   http.recv.buffer.size=2m
   acl.enabled=false
   line.tcp.acl.enabled=false
   telemetry.enabled=false
   ```

   Start it with the distribution's normal launcher. `SELECT build()` should
   identify the Enterprise release. The script verifies the advertised cap.
3. Create an environment outside the source checkout, so the wheel is not
   shadowed by local files:

   ```sh
   mkdir -p /tmp/qwp-independent-review
   cd /tmp/qwp-independent-review
   python3.11 -m venv venv
   venv/bin/python -m pip install \
     questdb==5.0.0 numpy==2.4.6 pandas==3.0.6 pyarrow==25.0.1 \
     python-dateutil==2.9.0.post0 six==1.17.0
   ```

Save the following as `repro.py` and run `venv/bin/python repro.py 9000`.
The script expects the broken behavior in 5.0.0 and also verifies working
controls. Once fixes land, its failure expectations must change.

```python
import importlib.metadata
import json
import socket
import sys
import time
import urllib.parse
import urllib.request
import uuid

import pandas as pd
import pyarrow as pa
import questdb as q

port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
conf = f"ws::addr=127.0.0.1:{port};"
prefix = "qwp_review_" + uuid.uuid4().hex[:8]
print("client:", importlib.metadata.version("questdb"), q.__file__)


def sql(query):
    url = f"http://127.0.0.1:{port}/exec?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


print("server:", sql("select build()")['dataset'])
with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
    sock.sendall((
        f"GET /write/v4 HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
        "Upgrade: websocket\r\nConnection: Upgrade\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        "Sec-WebSocket-Version: 13\r\nX-QWP-Version: 1\r\n\r\n"
    ).encode())
    response = b""
    while b"\r\n\r\n" not in response:
        part = sock.recv(4096)
        if not part:
            raise RuntimeError("Upgrade ended before headers arrived")
        response += part
    headers = response.split(b"\r\n\r\n", 1)[0].decode()
    print(headers)
    assert "X-QWP-Max-Batch-Size: 2097138" in headers


def verify_rows(table, expected):
    deadline = time.monotonic() + 10
    while True:
        count = sql(f"select count() from {table}")['dataset'][0][0]
        if count == expected:
            print(f"PASS: {table}: {count} stored rows")
            return
        if time.monotonic() >= deadline:
            raise AssertionError((table, expected, count))
        time.sleep(0.05)


def expect_error(label, operation, code):
    try:
        operation()
    except q.QuestDBError as exc:
        assert exc.code == code, (label, exc.code, str(exc))
        print(f"EXPECTED FAILURE [{label}]: {exc.code}: {exc}")
    else:
        raise AssertionError(f"{label}: expected an error on the affected version")


def dataframe(label, frame, *, manual_step=None, **kwargs):
    table = prefix + "_" + label
    with q.connect(conf) as db:
        if manual_step is None:
            db.dataframe(frame, table_name=table, at=q.ServerTimestamp, **kwargs)
        else:
            for offset in range(0, len(frame), manual_step):
                db.dataframe(frame.iloc[offset:offset + manual_step],
                             table_name=table, at=q.ServerTimestamp)
    verify_rows(table, len(frame))


def rows(label, *, pooled, n, width, byte_trigger=False):
    table = prefix + "_" + label
    config = conf + ("auto_flush_bytes=1048576;" if byte_trigger else "")
    db = q.connect(config) if pooled else None
    sender = db.sender() if pooled else q.Sender.from_conf(config)
    try:
        if not pooled:
            sender.establish()
        value = "x" * width
        for _ in range(n):
            sender.row(table, columns={"v": value}, at=q.ServerTimestamp)
        if pooled:
            sender.flush(wait=True)
        else:
            sender.flush()
            fsn = sender.published_fsn()
            if fsn is not None:
                assert sender.await_acked_fsn(fsn, 5000), "ACK timed out"
    finally:
        try:
            sender.close(flush=False)
        finally:
            if db is not None:
                db.close()
    verify_rows(table, n)


large = q.QuestDBErrorCode.BatchTooLarge
# Explicit object dtype is important: other Pandas paths can batch differently.
df = pd.DataFrame({"v": pd.Series(["x" * (256 * 1024)] * 8, dtype=object)})
expect_error("A: eight-row floor", lambda: dataframe("df_default", df), large)
expect_error("B: requested four rows", lambda: dataframe(
    "df_chunk4", df, max_rows_per_batch=4), large)
dataframe("df_manual", df, manual_step=4)

arrow = pa.table({"v": ["x" * 500000] * 5})
expect_error("E: incorrect single-row hint", lambda: dataframe("arrow_bad", arrow), large)
dataframe("arrow_chunk2", arrow, max_rows_per_batch=2)

expect_error("C: standalone defaults", lambda: rows(
    "standalone_bad", pooled=False, n=600, width=4096), q.QuestDBErrorCode.InvalidApiCall)
rows("standalone_ok", pooled=False, n=600, width=4096, byte_trigger=True)
expect_error("D: pooled overshoot", lambda: rows(
    "pooled_bad", pooled=True, n=8, width=256 * 1024), large)
rows("pooled_ok", pooled=True, n=8, width=256 * 1024, byte_trigger=True)
```

Expected outcome: five reproduced errors (covering A–E), and four successful
controls whose row counts are verified. The exact embedded script was extracted
from this document and run successfully against a fresh Enterprise 4.0.1 instance
with the PyPI 5.0.0 wheel; all nine expectations passed.

The standalone default failure assumes
600 appends complete before the one-second interval triggers an earlier flush;
if needed, disable only that interval with `auto_flush_interval=off` to isolate
the row-count behavior. Exact message byte counts vary with generated table names.

## Workarounds and their limitations

- **Server receive buffer:** `http.recv.buffer.size=8m` worked for the retested
  cases. Apply through the deployment's configuration procedure and reconnect
  clients; confirm the new advertised cap. Budget for additional per-connection
  memory. The protocol ceiling remains 16 MiB. This is not
  `http.send.buffer.size`, an OS socket buffer, or a client-only setting.
- **Row sender byte trigger:** `auto_flush_bytes=1048576` worked for the tested
  widths. It is not universally safe: leave headroom for the next row and all
  encoding overhead. Large individual rows require a different remedy.
- **Pandas:** explicitly slice into smaller DataFrames whose encoded contents
  fit. Do not rely solely on `max_rows_per_batch < 8` on affected paths.
- **Arrow Table:** the tested small `max_rows_per_batch` values worked. This does
  not establish the same behavior for one-shot Arrow streams; their batch
  boundaries may be controlled by the producer.
- **ILP/HTTP:** the tested 3 MiB string succeeded through this alternative
  transport. Transport changes can affect features and delivery semantics.

Explicit slicing/more frequent flushes may change transaction boundaries.
These are fresh-call workarounds, **not instructions to blindly retry a partially
successful ingestion**. The failure reproductions here published no data, but a
larger workload or a failure after an earlier commit can leave a committed prefix.

## Proposed fix scope and review criteria

These are recommendations, not implemented fixes:

1. **A — Native splitting:** support smaller row ranges when needed. Simply
   deleting the eight-row floor is unsafe if Boolean/validity bitmap offsets
   still assume byte alignment. Handle bit offsets or repack/copy as needed;
   retain a fast aligned path.
2. **B — Python planning:** ensure a requested smaller batch can actually bound
   the submitted group, or provide a safe byte-aware fallback rather than
   rounding into a batch that cannot fit. Fixing A alone may let the oversized
   group succeed but does not resolve the surprising parameter behavior.
3. **C — Standalone defaults:** make default QWP batching aware of the negotiated
   byte cap, not only rows and elapsed time.
4. **D — Row-buffer overshoot:** flush the preceding rows before the new row
   crosses the hard limit, or split safely on row boundaries. Preserve row
   ownership, error/retry behavior and acknowledgement semantics.
5. **E — Diagnostics:** distinguish an oversized individual row from a multi-row
   group the implementation cannot split. Name the binding server/client/queue
   cap. Do not recommend a setting that cannot address that cap.

Regression coverage should include 1–9-row groups; odd row counts; non-byte-aligned
Boolean and validity slices; nulls; strings, dictionaries and arrays; exact-cap
boundaries; multiple tables; changing caps after reconnect; and failures after
partial publication/commit. Verify stored values and null positions, not just
absence of exceptions or row counts. A row genuinely too large must still fail
clearly without repeated reconnect/replay attempts.

The source limitations are established, but the safest implementation strategy
and compatibility implications of changing chunk semantics need review.

## Evidence retention and limitations

The essential evidence, source links and reproduction are embedded above so this
document can be reviewed without access to the original machine. Supplementary
artifacts remain locally under `/tmp/qwp-2m-experiments/`:

- `results.jsonl`: final 50 cases, exact errors, headers and frame lengths.
- `matrix-final.log`, `followup-final.log`, `remedies-final.log`: final runs.
- `matrix.py`, `followup.py`, `remedies.py`, `case.py`, `tap.py`, `server.py`:
  full matrix and frame-observation harness.
- `raw-probe.json`: independent server-side rejection.
- Per-run server directories: `conf/server.conf` and `stdout.log`.

These `/tmp` artifacts are not part of the repository and may be removed by the
host. `preliminary-results.jsonl` is an exploratory run, not the evidence baseline:
its first symbol-count query preceded WAL apply. The final run polls visibility
and verifies all 4,096 symbol rows.

No production-code changes were made during investigation. The 50-case matrix
does not test TLS, replication/failover, every dtype, every dependency version,
or the customer's unknown workload. The confirmed result is that the suspected
released client/server combination has multiple reproducible routes to the
reported ingestion symptom.

[server-cap]: https://github.com/questdb/questdb/blob/12a33d651e51e2682e7a448c8db5168fc72dfad3/core/src/main/java/io/questdb/cutlass/qwp/server/QwpIngressUpgradeProcessor.java#L200-L218
[split-floor]: https://github.com/questdb/c-questdb-client/blob/0f74f5ee316b320afed4d2533dcd4594fd0a44f4/questdb-rs/src/ingress/column_sender/sender.rs#L90-L100
[pandas-alignment]: https://github.com/questdb/py-questdb-client/blob/64c10255d657c7d32cb22c57b3d80b8b2676f23e/src/questdb/_client.pyx#L2611-L2655
[standalone-defaults]: https://github.com/questdb/py-questdb-client/blob/64c10255d657c7d32cb22c57b3d80b8b2676f23e/src/questdb/_client.pyx#L1853-L1880
[pooled-threshold]: https://github.com/questdb/py-questdb-client/blob/64c10255d657c7d32cb22c57b3d80b8b2676f23e/src/questdb/_client.pyx#L8424-L8467
[pooled-row]: https://github.com/questdb/py-questdb-client/blob/64c10255d657c7d32cb22c57b3d80b8b2676f23e/src/questdb/_client.pyx#L8531-L8593
[error-hint]: https://github.com/questdb/py-questdb-client/blob/64c10255d657c7d32cb22c57b3d80b8b2676f23e/src/questdb/_client.pyx#L5932-L5955
[native-error]: https://github.com/questdb/c-questdb-client/blob/0f74f5ee316b320afed4d2533dcd4594fd0a44f4/questdb-rs/src/ingress/column_sender/sender.rs#L135-L143
