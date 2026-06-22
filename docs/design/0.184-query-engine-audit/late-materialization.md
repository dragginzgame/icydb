# Late Materialization

## Purpose

H7 tracks scalar paths that still open row storage after access planning because
residual filters, ordering, cursors, or projection require values that are not
fully covered by the chosen access route.

The first slice is observability-only. It should prove which executor lane ran
and how much retained-slot materialization it required before any runtime
rewrite.

## Current Metrics

`ScalarMaterializationLaneMetrics` already reports whether scalar execution used:

- direct raw `DataRow` lanes;
- direct filtered raw `DataRow` lanes;
- kernel data-row envelopes;
- retained full-row kernel envelopes;
- slot-only kernel rows.

The H7 proof slice adds retained-slot footprint counters:

- retained layout executions;
- retained value count;
- byte-length-only retained values.

These counters are execution-owned. They are recorded at the kernel retained
scan dispatch point, not during plan construction, so they describe the lane
that actually ran.

The second proof slice carries the same footprint into diagnostics attribution
and SQL perf-matrix reports through `KernelRowAttribution`. Matrix reports can
now rank kernel-row scenarios by retained-slot values without wrapping each
query in a test-only metrics capture.

## 2026-06-21 Matrix Readout

The first full deterministic matrix after adding retained-slot footprint
reporting did not justify a new runtime materialization lane yet.

Top retained-slot cases were bounded:

- `user.select.wide.field_compare.age_desc.limit3`: 18 retained values across
  three retained layouts, about 2.1M total instructions.
- `user.select.wide.field_compare.lower_name_asc.limit3`: 18 retained values
  across three retained layouts, about 2.1M total instructions.
- `blob.select.lengths.bucket_range.bucket_label_asc.limit1`: 16 retained
  values, eight of them byte-length-only values, about 2.3M total instructions.

The matrix's dominant cost remained outside H7:

- `token.collection_id.sparse_in.page_only.limit50`: about 1.77B total
  instructions, with about 1.67B in compile work.
- `token.collection_id.sparse_in.count`: about 848M total instructions, with
  about 846M in compile work.

This keeps late-materialization runtime specialization gated. The retained-slot
metrics are now useful evidence, but the next broad win from this matrix is
large literal `IN` compile/planning cost rather than a row materialization
rewrite.

A follow-up compact-membership slice fixed the compile/lowering side by keeping
SQL membership as one `IN_LIST` expression through lowering and predicate
recovery. The sparse token page/count scenarios now compile in low millions of
instructions; the remaining page hotspot is route/planner/executor work, not
late materialization.

A second sparse-membership follow-up admitted index multi-lookup routes into
the key-only / index-covered covering path. The filtered token page rerun still
reads 256 index entries because the available `(collection_id, stage, id)` index
does not prove global `ORDER BY id` for a collection-only lookup, but
`data_store.get` is now zero for `SELECT id ...`. That keeps this case outside
the late-materialization backlog unless a future projection reintroduces
avoidable row reads.

A third sparse-membership follow-up removed a residual-proof quadratic check
for identical canonical `IN` value sets and skipped unused preparation
predicate compilation when access proves the whole filter. The same page shape
now runs at about 40.6M total instructions with the same zero row-store reads,
confirming the remaining cost is branch/order routing and bounded index
traversal rather than late materialization.

## 2026-06-22 Byte-Length Retained Slots

The next H7 slice found a small but real retained-slot cleanup rather than a
new materialization lane. Retained layouts that mix normal slots with
byte-length-only text/blob slots now decode through one opened structural row
reader. The low-level value-mode decoder no longer validates normal slots
through the sparse direct decoder and then opens the row again to read scalar
byte lengths.

The live SQL `OCTET_LENGTH(blob)` projection path remains slot-only:

- it retains label, primary-key tie-break, and byte-length values for the blob
  fields;
- it does not retain full blob rows;
- it opens each projected row once;
- diagnostics still report byte-length-only retained slots.

The SQL perf matrix now ranks retained layout hits, retained slot values, and
retained byte-length values separately. A focused rerun can use
`ICYDB_SQL_PERF_MATRIX_KEYS=blob.select.lengths.bucket_range.bucket_label_asc.limit1`
with the ignored `sql_perf_generated_matrix_reports_hotspots` test when
PocketIC evidence is needed.

## 2026-06-22 Focused Retained-Slot Matrix

A focused 54-scenario matrix rerun covered the documented user retained-slot
cases and every deterministic blob `OCTET_LENGTH` projection shape. It passed
with zero failures.

Top byte-length retained cases were still bounded and slot-only:

- `blob.select.lengths.bucket_range.bucket_label_asc.limit1`: 8 retained
  byte-length values, 16 retained values total, 4 retained layouts, 4 row-store
  reads, 16 index-entry reads, about 2.37M total instructions.
- `blob.select.lengths.bucket_range.bucket_label_asc.limit3`: 6 retained
  byte-length values, 12 retained values total, 3 retained layouts, 9 row-store
  reads, 15 index-entry reads, about 2.73M total instructions.

Top non-byte retained-slot cases were:

- `user.select.wide.field_compare.age_desc.limit3`: 18 retained values,
  3 retained layouts, 16 row-store reads, about 2.15M total instructions.
- `user.select.wide.field_compare.lower_name_asc.limit3`: 18 retained values,
  3 retained layouts, 16 row-store reads, about 2.09M total instructions.

Those user cases are field-comparison scans that need row facts to evaluate
the predicate/order shape; they do not indicate an avoidable blob/text
materialization lane. The blob length cases continue to avoid retaining full
blob payload values. This keeps H7 in evidence-gathering mode rather than
runtime redesign mode.

## First Proof Points

- Ordinary scalar projections over direct fields and expressions should use the
  slot-only kernel row lane and retain only the source slots needed for filter,
  order, and output.
- `OCTET_LENGTH(text/blob)` projections should retain byte-length values for
  eligible text/blob fields instead of full payload values.

## Next Gate

Do not add another materialization path until the metrics identify one repeated
shape that either:

- falls back to retained full rows when slot-only rows would be sufficient;
- retains substantially more slots than filter/order/projection require;
- performs row-store reads for a projection that planner/executor coverage can
  prove from index entries.
