# 0.185 Status

Status: active.

## Focus

Branch-aware query routing revisit after the 0.184 query-engine audit cleanup.
The first goal is to prove and document that SQL and fluent equivalent
branch-heavy shapes converge on the same explicit route contract before
expanding the optimizer.

## Current Slice

- 0.185.1 is hardening continuation proof for the shared merged-prefix stream
  helper.
- Branch-set continuation already proves page-two resume after the global
  primary-key cursor boundary.
- The active proof gap is sparse `IN` child-prefix expansion, because that path
  now shares the same merged-prefix helper but used to have only first-page
  coverage.
- The new proof compares SQL and fluent sparse-expanded page/resume behavior
  using each surface's own continuation token. Byte-identical cursor signatures
  for SQL `SELECT *` versus fluent full-entity sparse routes remain a separate
  projection-identity question and are not changed in this slice.
- This slice stays deliberately below a cursor-format hard-cut: it proves the
  current global primary-key boundary model for admitted primary-key suffix
  streams, without adding per-branch cursor payloads.

## Completed 185.0 Baseline

- Promoted the reminder note into an actionable 0.185 design/status baseline.
- Added a focused SQL/fluent convergence guard for the original 0.183 target
  shape.
- Started branch-tree cleanup by sharing physical merged-prefix stream
  construction across branch-set, streaming multi-lookup, and child-prefix
  expansion routes.
- Kept broader adaptive routing and cursor-format redesign queued behind the
  convergence and shared-runtime proof.

## First Representation Audit

- No runtime `fixed_values.len() + 1` branch reconstruction was found in the
  current branch-set path.
- Branch-prefix lowering goes through `IndexBranchSetSpec::branch_prefix_values`.
- Runtime execution consumes lowered prefix specs and the spec-derived branch
  count.
- Cache identity includes branch-set index name, ordered suffix, fixed values,
  and branch values.
- EXPLAIN access DTOs now carry branch field and ordered suffix explicitly
  instead of leaving those facts only to detail-mode JSON projection.
- Planner admission still computes the candidate shape from fixed-prefix length;
  that remains acceptable as planner-owned proof construction.

## Branch-Tree Inventory

- `IndexBranchSet` remains the strict branch-aware route: fixed prefix plus
  exact branch slot values with a primary-key ordered suffix proof.
- `IndexMultiLookup` remains the looser multi-prefix route: multiple exact
  prefix scans that may stream when final ordering is proven elsewhere.
- Child-prefix expansion currently feeds the same physical stream shape after
  prefix-cardinality pruning expands a parent prefix into exact child prefixes.
- Union and intersection already use `OrderedKeyStreamBox::merge_all` and
  `intersect_all` over child streams, so they are structurally close to the
  future branch-tree target.
- The first cleanup extracted merged-prefix stream construction so empty-prefix
  pruning, fair chunk sizing, primary-key suffix resume anchors, and merge
  construction have one runtime owner.
- Route admission, diagnostics, and cache identity remain separate so branch-set
  cannot silently collapse into generic unordered multi-lookup or union.
- Sparse `IN` routes now expose whether index-prefix child expansion is active,
  the expanded target prefix length, and the expansion cap in verbose EXPLAIN.
  This makes the current adaptive branch choice auditable before changing any
  thresholds or cost policy.

## Source Inputs

- `docs/design/0.183-branch-aware-routing/0.183-design.md`
- `docs/design/0.183-branch-aware-routing/branch-set-closeout.md`
- `docs/design/0.183-branch-aware-routing/follow-up-reminders.md`
- `docs/design/0.185-branch-aware-revisit/branch-aware-query-revisited-reminders.md`
- `docs/design/0.185-branch-aware-revisit/adaptive-routing.md`
- `docs/design/0.185-branch-aware-revisit/continuation.md`

## Deferred From 185.0

- Full per-branch cursor continuation hard-cut.
- Adaptive large-`IN` cost model.
- Shared branch-tree replacement for every special-case branch or `IN` flow.
- Wider downstream-specific query tuning.
