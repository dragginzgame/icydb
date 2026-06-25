# 0.185 Status

Status: active.

## Focus

Branch-aware query routing revisit after the 0.184 query-engine audit cleanup.
The first goal is to prove and document that SQL and fluent equivalent
branch-heavy shapes converge on the same explicit route contract before
expanding the optimizer.

## Current Slice

- No active implementation slice is selected after the physical prefix-stream
  cleanup.
- The remaining major follow-ups are broader branch-tree replacement and
  cursor-format design; choose one explicitly before changing runtime behavior
  again.

## Major Follow-Up Queue

- Adaptive routing: started in `0.185.5` with bounded-page child-prefix cap
  adjustment and continued with reverse child-prefix expansion. Deferred
  remainder is a real cost/estimate model.
- Branch-tree replacement: started with physical prefix-stream consolidation.
  Full branch-tree replacement remains deferred.
- Cursor-format design: not started.

## Completed Physical Prefix-Stream Cleanup Slice

- Single-prefix index streaming now consumes the same merged-prefix stream spec
  as branch-set, streaming multi-lookup, and sparse child-prefix expansion.
- Materialized multi-lookup and merged-prefix streaming now share one local
  active-prefix pruning helper, so empty-prefix metadata is interpreted once in
  the physical stream owner.
- This does not add a new route shape, change branch-set admission, or change
  cursor format. It is a branch-tree cleanup step under the existing route
  families.

## Completed Reverse Expansion Slice

- Sparse child-prefix expansion now admits primary-key descending order when
  the expanded prefix leaves exactly the primary-key suffix.
- The route remains `IndexMultiLookup` with a child-prefix expansion hint; this
  is not a new logical access path.
- Covering projection prep uses the requested primary-key scan direction when
  the route proves the prefix set is primary-key-order safe, so DESC key-only
  pages can avoid materialized sorting.
- DESC sparse child-prefix expansion has cursor continuation proof for the
  current global primary-key boundary model, including bounded resumed
  child-prefix scans and bounded row hydration.

## Completed Adaptive Cap Slice

- Sparse child-prefix expansion now keeps the default conservative cap for
  unbounded loads and grows the cap with bounded page fetch windows up to a
  small hard ceiling.
- Route planning and covering projection prep consume the same cap calculation,
  so EXPLAIN diagnostics and covering execution do not diverge.
- Runtime expansion treats the cap as a limit on non-empty child prefixes, not
  on parent literals inspected: if the cap is exactly filled, trailing parent
  prefixes must be metadata-proven empty before expansion remains admitted.
- This is still not a broad cost-based optimizer: it does not change branch-set
  admission, DESC expansion, prefix-cardinality metadata, or cursor format.

## Completed Merged-Prefix Contract Slice

- The physical merged-prefix runtime now carries selected index shape, lowered
  prefix specs, continuation, fetch hint, and primary-key suffix resume policy
  through one local spec instead of loose helper arguments.
- Branch-set routes still consume the spec-derived branch count before entering
  the stream merger, while sparse child-prefix expansion still enters only
  after metadata-backed expansion produces exact child prefixes.
- Route proof now asserts that branch-set primary-key ordering does not depend
  on sparse child-prefix expansion hints.

## Completed Terminal Metadata Slice

- The terminal metadata slice extended sparse `IN` proof into COUNT/EXISTS
  terminals.
- SQL sparse `IN` COUNT already uses direct prefix-cardinality metadata; the
  slice proved the equivalent fluent sparse terminal path does the
  same for non-empty and empty-prefix count/existence checks.
- This stayed proof-oriented. It did not change terminal semantics, route
  admission, branch caps, or prefix-cardinality metadata.

## Completed Projection Boundary Slice

- The projection boundary slice widened over-cap sparse `IN` fallback proof
  across SQL covering projection lanes and fluent full-entity loads.
- Combined child-prefix over-cap fixtures now exercise key-only primary-key
  projection, decoded index-component projection, and hybrid row-backed
  projection, plus fluent full-entity hydration, against the same parent-prefix
  fallback boundary.
- The goal is to prove the shared covering resolver fix is not a key-only
  special case: unsafe parent-prefix sets must sort before projection/windowing,
  index-backed fields must stay row-store-free, and hybrid projections must
  report both index-field decode and final-page row-backed hydration while the
  fluent surface preserves the same primary-key order and bounded row-store
  hydration.
- The same slice also locks the ASC-only boundary for child-prefix expansion:
  sparse `IN ... ORDER BY id DESC` must stay materialized/fallback and must not
  report the ASC child-prefix expansion hint.
- The slice stayed proof-oriented. It did not add a cost model or a denser
  over-cap streaming strategy.

## Completed Adaptive Boundary Slice

- The adaptive boundary slice hardened sparse `IN` child-prefix expansion
  boundaries.
- When exact child-prefix metadata can expand a sparse parent prefix within the
  cap, the route may stream the expanded child prefixes through the shared
  merged-prefix helper.
- When exact child-prefix metadata exceeds the cap, runtime must fail open to a
  safe parent-prefix fallback instead of pretending capped expansion produced a
  complete ordered branch set.
- The over-cap fallback must preserve primary-key order, avoid row-store
  hydration for key-only projections, and avoid accidental count execution on
  default page queries.
- Key-only covering fallback must not lazily merge parent-prefix streams unless
  the route proves each stream is already ordered by the final merge key; unsafe
  parent-prefix sets materialize, deduplicate, sort, and then apply the normal
  page window.
- The slice stayed below a cost-based optimizer: it proved the current
  cap boundary and fallback behavior, without changing thresholds or adding
  prefix cardinality estimates.

## Completed Continuation Slice

- Branch-set continuation proves page-two resume after the global primary-key
  cursor boundary.
- Sparse `IN` child-prefix expansion continuation now has page/resume coverage,
  because that path shares the same merged-prefix helper but previously had only
  first-page coverage.
- The proof compares SQL and fluent sparse-expanded page/resume behavior using
  each surface's own continuation token. Byte-identical cursor signatures for
  SQL `SELECT *` versus fluent full-entity sparse routes remain a separate
  projection-identity question and were not changed.
- The slice stayed deliberately below a cursor-format hard-cut: it proved the
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
