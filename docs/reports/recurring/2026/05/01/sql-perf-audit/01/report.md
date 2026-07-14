# SQL Perf Audit

Date: 2026-05-01

## Scope

This report stores the full SQL perf harness run from the current worktree
after adding the `PerfAuditBlob` payload scenarios. The new rows measure
byte-count-only blob projections against thumbnail, chunk, and full-payload
SELECTs.

## Command

```bash
POCKET_IC_BIN=/home/adam/projects/icydb/.cache/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_perf_audit sql_perf_audit_harness_reports_instruction_samples -- --nocapture
```

## Artifacts

- `artifacts/sql-perf-audit/sql-perf-audit-output.txt`
- `artifacts/sql-perf-audit/sql-perf-audit-output-rerun.txt`
- `artifacts/sql-perf-audit/sql-perf-audit-octet-fast-path-output.txt`
- `artifacts/sql-perf-audit/sql-perf-audit-octet-data-row-output.txt`

## Blob Rows

| Scenario | Runs | Avg Compile | Avg Execute | Avg Instructions | Avg store.get() | SQL Compile Hits | SQL Compile Misses | Shared Hits | Shared Misses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `blob.bucket.lengths.asc.limit3` | 1 | 205,630 | 1,257,198 | 1,462,828 | 6 | 0 | 1 | 0 | 1 |
| `blob.bucket.thumbnail_payload.asc.limit3` | 1 | 142,419 | 1,722,014 | 1,864,433 | 6 | 0 | 1 | 0 | 1 |
| `blob.bucket.chunk_payload.asc.limit2` | 1 | 142,362 | 6,349,086 | 6,491,448 | 6 | 0 | 1 | 0 | 1 |
| `blob.bucket.full_payload.asc.limit2` | 1 | 148,073 | 6,727,957 | 6,876,030 | 6 | 0 | 1 | 0 | 1 |
| `repeat.blob.bucket.lengths.asc.limit3.runs10` | 10 | 35,192 | 1,048,083 | 1,083,276 | 6 | 9 | 1 | 9 | 1 |

## Readout

- Returning three thumbnails costs about `1.86M` instructions total.
- Returning two chunks costs about `6.49M` instructions total.
- Returning two thumbnails plus chunks costs about `6.88M` instructions total.
- Byte-count-only projection avoids returning payload bytes, but still reads the
  six rows needed by the current route and costs about `1.46M` instructions on
  a cold query.
- Repeating the byte-count query ten times moves compile work onto the cache
  path and lowers average total cost to about `1.08M` instructions.

## Octet Length Optimization Probe

Two optimization probes were measured against the blob length scenario:

| Variant | Scenario | Avg Execute | Avg Instructions | Result |
|---|---|---:|---:|---|
| Baseline | `blob.bucket.lengths.asc.limit3` | 1,257,198 | 1,462,828 | retained-slot path |
| Borrowed raw-row helper only | `blob.bucket.lengths.asc.limit3` | 1,258,698 | 1,463,768 | noise/no win |
| Forced data-row routing | `blob.bucket.lengths.asc.limit3` | 1,301,877 | 1,506,947 | regression, not kept |
| Baseline | `repeat.blob.bucket.lengths.asc.limit3.runs10` | 1,048,083 | 1,083,276 | retained-slot path |
| Borrowed raw-row helper only | `repeat.blob.bucket.lengths.asc.limit3.runs10` | 1,048,593 | 1,083,729 | noise/no win |
| Forced data-row routing | `repeat.blob.bucket.lengths.asc.limit3.runs10` | 1,093,652 | 1,128,788 | regression, not kept |

The current query naturally uses retained-slot projection. Forcing it through
the data-row path lets raw scalar payload borrowing run, but the extra row
validation cost is higher than the blob materialization it avoids for this
scenario. The forced routing change was reverted.

## 2026-05-01 Rerun

The wasm-backed PocketIC SQL perf audit was rerun after the `0.144.9` release
commit. The full output is stored in
`artifacts/sql-perf-audit/sql-perf-audit-output-rerun.txt`.

| Scenario | Runs | Avg Compile | Avg Execute | Avg Instructions | Avg store.get() |
|---|---:|---:|---:|---:|---:|
| `blob.bucket.lengths.asc.limit3` | 1 | 205,684 | 1,258,708 | 1,464,392 | 6 |
| `blob.bucket.thumbnail_payload.asc.limit3` | 1 | 142,318 | 1,723,496 | 1,865,814 | 6 |
| `blob.bucket.chunk_payload.asc.limit2` | 1 | 142,261 | 6,350,484 | 6,492,745 | 6 |
| `blob.bucket.full_payload.asc.limit2` | 1 | 147,936 | 6,729,411 | 6,877,347 | 6 |
| `repeat.blob.bucket.lengths.asc.limit3.runs10` | 10 | 35,198 | 1,048,467 | 1,083,665 | 6 |

Readout:

- The rerun is stable relative to the earlier saved blob rows.
- Returning payload bytes still dominates the blob scenarios.
- `OCTET_LENGTH(...)` remains much cheaper than returning chunk/full payloads,
  but it still scans/loads the six matching rows for this route shape.

## Validation

The full sampler passed:

```text
test sql_perf_audit_harness_reports_instruction_samples ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 7 filtered out
```

The rerun also passed:

```text
test sql_perf_audit_harness_reports_instruction_samples ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 7 filtered out
```
