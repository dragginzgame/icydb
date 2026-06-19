# Branch-Set Validation Closeout

## Scope

This closeout covers the first 0.183 branch-aware query optimization slice:

```sql
WHERE collection_id = X
  AND stage IN ('Draft', 'Review')
ORDER BY id ASC
LIMIT N
```

against:

```text
(collection_id, stage, id)
```

## SQL Blob Test Unblock

The attribution-only `sql_blob` expectations now compile only under the
`diagnostics` feature, matching the current attribution API surface. This keeps
non-diagnostics SQL blob tests runnable and preserves the attribution assertions
where the attribution payload exists.

## Route Representation

The selected route is:

```text
AccessPath::IndexBranchSet { index, fixed_values, branch_values }
```

For this slice the compact representation is sufficient because the remaining
route proof is derived from accepted schema/index contracts:

- Branch slot identity is `fixed_values.len()` and the key item at that index
  position.
- Ordered suffix identity is derived from the same accepted index contract after
  consuming `fixed_values.len() + 1` prefix slots.
- Primary-key direction is admitted only for `ORDER BY primary_key ASC`.
- All branch streams share the same ordered suffix because every lowered branch
  is the same accepted index with the same consumed prefix length.
- Branch count is capped by `MAX_INDEX_BRANCH_SET_VALUES = 8`.
- Schema/index identity is carried by the semantic index contract and is
  exposed through EXPLAIN, fingerprint hashing, structural cache identity,
  execution trace variant, and plan metrics.

The focused tests assert that this route is not treated as generic unordered
union, multi-lookup, broad ordered scan, or full scan.

## Runtime Behavior

The scalar executor route is lazy: it opens one prefix stream per branch and
merges the ordered branch heads with the existing duplicate-suppressing ordered
key merge.

The SQL covering projection shortcut is bounded rather than fully one-row
pull-lazy: it reads at most the page/lookahead budget per branch, sorts the
small candidate set by primary key, deduplicates, and truncates. It no longer
materializes all matching rows and it preserves zero row-store reads for
covered/key-only projections.

## Perf Attribution

Before this closeout slice, the non-covered target projection was still
hydrating all matching branch rows in the covering path. The observed
diagnostics for a `LIMIT 3` target fixture were:

```text
data_store.get: 12
index_store.entries: 12
```

After the closeout fixes, diagnostics tests enforce:

```text
key-only SELECT id LIMIT 3:
  data_store.get == 0
  index_store.entries <= 8
  scalar_aggregate == None

non-covered SELECT id, title LIMIT 3:
  data_store.get in 3..=4
  index_store.entries <= 8
  scalar_aggregate == None
```

The default page-shaped SQL path does not invoke the count/aggregate path.

## Core SQL Surface Audit

Inspected generated/core SQL surface:

- `crates/icydb-build/src/db/sql.rs`
  - `SqlSurfaceTokens::readonly_dispatch_tokens`
  - `sql_surface_endpoint_exports`
  - `sql_surface_query_dispatch_arm`
- `crates/icydb-core/src/db/session/sql/mod.rs`
  - `DbSession::execute_sql_query`
  - `DbSession::execute_sql_query_with_attribution`
  - diagnostics-gated attribution wrapping around the same query execution

The generated core SQL surface exports a single readonly query endpoint,
`__icydb_query(sql: String)`, which dispatches exactly one SQL statement through
`__icydb_query_dispatch` and per-entity
`execute_sql_query_with_perf_attribution::<Entity>(sql)`.

A generator regression test now asserts that the readonly SQL surface does not
emit `__icydb_list`, `__icydb_page`, `__icydb_count`, or dedicated count
executor calls.

The recurring SQL perf audit now includes a `PerfAuditToken` surrogate for the
target branch-set query. The audit row exercises the same
`collection_id = X AND stage IN ('Draft', 'Review') ORDER BY id ASC LIMIT 3`
shape against `(collection_id, stage, id)`. The focused PocketIC test asserts
`IndexBranchSet`, no materialized sort, covered id-only projection with zero
row-store reads, non-covered projection with hydration bounded to returned rows
plus lookahead, bounded index-entry reads, and no grouped/count work on the page
path.

## Exact EXPLAIN Output

Captured from the diagnostics branch-set test for the target `SELECT id`
projection:

```text
phases:
  c=compile: parse, lower, and compile the SQL surface
  p=planner: resolve visible indexes and build the structural access plan
  s=store: traverse physical index/data storage and decode physical access payloads
  e=executor: run residual filter, order, group, aggregate, and projection logic
  d=decode: package the public SQL result payload for the shell
execution:
  IndexBranchSet execution_mode=Streaming
    node_id=0
    layer=scan
    execution_mode_detail=streaming
    predicate_pushdown_mode=none
    access_strategy=IndexBranchSet(collection_stage_id)
    covering_scan=true
    node_properties:
      acc_alts=List([])
      acc_choice=Text("IndexBranchSet(collection_stage_id)")
      acc_reason=Text("selected_index_not_projected")
      acc_reject=List([])
      cont_mode=Text("initial")
      cov_read_route=Text("covering_read")
      cov_scan_reason=Text("cover_read_route")
      fast_path=Text("secondary_prefix")
      fast_reason=Text("sec_order_ok")
      fast_reject=List([Text("primary_key=pk_fast_no"), Text("index_range=idx_limit_no")])
      fetch=Nat64(4)
      ord_route_mode=Text("direct_streaming")
      ord_route_reason=Text("none")
      pred_idx_cap=Text("fully_indexable")
      prefix_len=Nat64(2)
      prefix_values=List([Text("01KV5N439P0000000000000000"), Text("Draft"), Text("Review")])
      proj_fields=List([Text("id")])
      proj_materialization=Text("covering_read")
      proj_pushdown=Bool(true)
      resume_from=Text("none")
      scan_dir=Text("asc")
    IndexPredicatePrefilter execution_mode=Streaming
      node_id=1
      layer=pipeline
      execution_mode_detail=streaming
      predicate_pushdown_mode=full
      predicate_pushdown=strict_all_or_none
      filter_expr=collection_id = '01KV5N439P0000000000000000' AND (stage = 'Draft' OR stage = 'Review')
      node_properties:
        pushdown=Text("collection_id=Text(\"01KV5N439P0000000000000000\") AND stage IN [Text(\"Draft\"), Text(\"Review\")]")
    SecondaryOrderPushdown execution_mode=Streaming
      node_id=2
      layer=pipeline
      execution_mode_detail=streaming
      predicate_pushdown_mode=none
      node_properties:
        index=Text("collection_stage_id")
        prefix_len=Nat64(2)
    TopNSeek execution_mode=Streaming
      node_id=3
      layer=pipeline
      execution_mode_detail=streaming
      predicate_pushdown_mode=none
      node_properties:
        fetch=Nat64(4)
    OrderByAccessSatisfied execution_mode=Streaming
      node_id=4
      layer=pipeline
      execution_mode_detail=streaming
      predicate_pushdown_mode=none
      node_properties:
        order_by_idx=Bool(true)
    CoveringRead execution_mode=Streaming
      node_id=5
      layer=terminal
      execution_mode_detail=streaming
      predicate_pushdown_mode=none
      projection=covering_read
      covering_scan=true
      node_properties:
        covering_fields=List([Text("id")])
        covering_order=Text("primary_key_asc")
        covering_sources=List([Text("primary_key")])
        existing_row_mode=Text("planner_proven")
    LimitOffset execution_mode=Streaming
      node_id=6
      layer=terminal
      execution_mode_detail=streaming
      predicate_pushdown_mode=none
      limit=3
      cursor=false
      node_properties:
        offset=Nat64(0)
```
