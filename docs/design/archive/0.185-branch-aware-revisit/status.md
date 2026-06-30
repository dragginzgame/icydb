# 0.185 Status

Status: closed after 0.185.22. Keep this line in guard mode unless a concrete
branch-aware regression appears.

## Focus

Branch-aware query routing revisit after the 0.184 query-engine audit cleanup.
The 0.185 line has proved and documented that SQL and fluent equivalent
branch-heavy shapes converge on the same explicit route contracts, and has
cleaned up the executor handoff boundaries that could be tightened without
changing cursor format or adding a broad optimizer.

## Final State

- General branch-tree replacement has been evaluated against the current
  access-shape and stream-runtime boundaries.
- 0.185 keeps the specialized branch-aware access families distinct because
  they carry different planner proofs, diagnostics, cache identity, prefix
  arity, and cursor semantics.
- The final closeout slice records the validation boundary and closes the
  0.185 branch-aware queue.

## Closed 0.185 Branch-Aware Queue

- No branch-aware work remains queued for 0.185.
- Future ideas stay outside the 0.185 closeout unless a concrete regression
  requires a targeted guard fix.

## Future Work Outside 0.185 Branch-Aware Closeout

- Wider downstream-specific query tuning and performance benchmarking.
- Arbitrary non-primary-key branch merge ordering unless a concrete 0.185
  correctness issue requires it.
- Broad cost-based optimization that threads generation-bound runtime metadata
  into route selection.
- Generalized branch-tree algebra if a future route needs to merge branch
  families beyond the current primary-key suffix contract.

## Completed Final Closeout Slice

- The 0.185 branch-aware status surface is now closed after the convergence,
  continuation, adaptive-budget, route-cost, and branch-tree decision slices.
- A final design sanity audit maps each original work-plan item to landed route,
  test, or documentation evidence.
- The design, adaptive-routing, and reminder notes now point future cost,
  cursor, benchmarking, and generalized branch-tree ideas outside the 0.185
  line instead of treating them as open closeout scope.
- No runtime route behavior changed in the closeout slice.

## Completed Branch-Tree Replacement Decision Slice

- Access-shape proof now keeps `IndexMultiLookup`, `IndexBranchSet`, and
  general union/intersection composites distinct.
- The proof locks the important semantic differences: leading-slot
  multi-lookup consumes one exact prefix per lookup value, branch-set consumes
  fixed-prefix-plus-branch-slot prefixes with its ordered suffix proof, and
  general set composites do not masquerade as one selected index path.
- Runtime already shares the useful mechanics below that representation:
  prefix-family stream construction, empty-prefix pruning, fair chunk sizing,
  primary-key suffix resume anchors, and ordered merge/intersection reducers.
- 0.185 therefore rules out a broad replacement of every branch or `IN` flow
  with one generalized branch tree. That would be representational churn unless
  a future route admits broader branch merging semantics.

## Completed Adaptive Expansion Budget Decision Slice

- Sparse child-prefix expansion now carries a named route budget instead of an
  anonymous cap helper.
- Route proof covers the three budget cases: small bounded pages keep the
  default expansion floor, normal bounded pages follow the page lookahead
  window, and large bounded pages stop at the hard child-prefix ceiling.
- The 0.185 cost/selectivity question is closed at the route boundary:
  synchronized prefix-cardinality metadata remains an execution-time proof and
  terminal preflight input, not a compile-time route-cache scoring input.
- Existing over-cap sparse `IN` runtime coverage remains the fallback proof:
  capped child-prefix expansion must fail open to complete parent-prefix
  materialization before primary-key ordering/windowing.

## Completed Branch Continuation Hard-Cut Decision Slice

- Route-level proof now asserts that branch-set, sparse child-prefix ASC, and
  sparse child-prefix DESC routes all resume through the existing global
  `CursorBoundary` mode.
- The proof keeps child-prefix expansion explicit on sparse multi-lookup routes
  and confirms branch-set routes rely on their own ordered suffix proof.
- The 0.185 cursor hard-cut question is closed for currently admitted
  primary-key suffix branch routes: no per-branch cursor payload is needed
  unless a later route broadens branch merging beyond global primary-key suffix
  continuation.

## Completed Validation Matrix Baseline Slice

- The validation baseline keeps the 0.185 boundary clear: route convergence,
  sparse child-prefix expansion, covering fallback proof, terminal metadata
  proof, stream-policy cleanup, access-shape cleanup, and docs are complete.
- Cost-based large-`IN` routing and full branch-tree replacement were kept
  visible as 0.185 queue items at that point; later decision slices closed
  both questions.
- Final validation rechecked invariants, feature combinations, full Clippy,
  and workspace/unit test coverage.

## Completed Indexed-IN Prefix-Cardinality Admission Slice

- Route capability facts now own the shape predicate for indexed `IN` EXISTS
  prefix-cardinality preflight admission.
- The predicate admits only single-path `IndexMultiLookup` access with more
  than one exact prefix, matching the previous aggregate-terminal behavior.
- Aggregate EXISTS preflight admission now consumes `AccessShapeFacts` and the
  shared route helper instead of borrowing the raw multi-lookup access payload.
- Exact prefix materialization and cardinality lookup still live with the
  prefix-cardinality owner because those steps require concrete prefix bytes
  and index identity.
- Existing aggregate prefix-cardinality coverage remains the semantic proof;
  route capability coverage now guards the shared shape predicate directly.

## Completed Index-Range Cursor Retained-Slot Shape Slice

- `AccessShapeFacts` now owns a dedicated single-path index-range predicate
  for consumers that only need the access shape.
- Retained-slot layout derivation uses that predicate before retaining the
  index key-item slots required for cursor anchor reconstruction.
- Continuation-token minting uses that predicate before requiring and
  validating the raw index-range anchor carried by the materialized cursor row.
- The page cursor anchor builder still reads the raw range spec because anchor
  construction needs the range payload, not just access shape.
- Existing access-shape regression coverage now asserts the new predicate for
  pure index-range plans.

## Completed Covering Index-Shape Admission Slice

- `AccessShapeFacts` now owns the selected-secondary-index access predicate for
  single-path prefix-family and range access.
- `AccessPlan::has_selected_index_access_path()` delegates to that access-shape
  predicate instead of recomputing prefix/range facts.
- Covering planner admission and executor covering scan resolution consume the
  shared predicate instead of re-matching raw index access variants.
- Count/existence existing-row route capability also consumes the same
  predicate, keeping terminal and covering gates aligned on one access-shape
  authority.
- Covering detail extraction remains variant-specific in the covering owner,
  because it still needs prefix constants, branch-set metadata, and range
  bounds.

## Completed Index-Prefix-Set Page Shape Slice

- Route capability facts now own the shape predicates for index-prefix-set
  page fetch hints and branch-set page keep caps.
- Scalar materialized fallback no longer peeks directly at raw
  `IndexMultiLookup`/`IndexBranchSet` access variants to decide whether a
  bounded page fetch hint can be applied.
- Terminal page materialization no longer peeks directly at raw
  `IndexBranchSet` access variants to decide whether the merged lookahead row
  cap can be applied.
- The existing non-shape gates still live with their runtime owners because
  they depend on residual filters, distinct handling, ordered-load route mode,
  post-access strategy, page limits, and continuation state.

## Completed Access Stream Execution Policy Slice

- Executor fallback stream resolution now passes one
  `AccessStreamExecutionPolicy` instead of separate physical fetch-hint and
  leaf-order arguments.
- The policy pairs `physical_fetch_hint` with `IndexLeafOrderPolicy`, keeping
  route-owned stream mechanics together after scan-hint derivation.
- Verified fast-stream and aggregate direct traversal callers pass canonical
  key-order access policies explicitly.
- Executable access bindings, traversal inputs, structural physical requests,
  and physical stream bindings carry the grouped policy across their handoff
  boundaries.
- Composite traversal children still preserve the fetch hint override behavior
  while resetting the leaf-order member to canonical key order before
  merge/intersection reducers consume child streams.
- The obsolete `StreamExecutionHints` wrapper was removed once policy grouping
  made it redundant.

## Completed Index Leaf Order Policy Slice

- Route planning now exposes ordered secondary-index leaf preservation as an
  `IndexLeafOrderPolicy` instead of a raw boolean.
- Fallback stream runtime, executable access bindings, traversal inputs, and
  physical stream bindings carry the same policy type across their handoff
  boundaries.
- Composite traversal children still reset the policy to canonical key order
  before merge/intersection reducers consume child streams.
- Aggregate fast-path and verified fast-stream execution pass canonical
  key-order policy explicitly because those paths do not preserve ordered index
  leaf streams.
- Physical single-prefix and multi-lookup stream setup converts the policy to
  its local prefix-merge resume policy only at the point where resume anchors
  are constructed.
- Route tests now assert ordered leaf-stream preservation through the policy
  contract.

## Completed Prefix-Set Shape Slice

- Physical key prefix streams and covering component prefix streams now share
  one payload-agnostic prefix-set shape classifier after each path has already
  pruned inactive prefixes.
- The shared classifier owns only the empty, single-prefix, materialized
  fallback, and ordered-merge split. Store handles, component decoding, resume
  anchors, predicate pruning, and route admission remain with their existing
  owners.
- The classifier takes `OrderedMergeSafe` versus `RequiresMaterialization`
  explicitly instead of receiving a raw boolean at shared helper boundaries.
- The physical path uses the shared split before constructing direct
  single-prefix streams or ordered sibling merges; the covering path uses it
  before choosing direct bounded scans, materialized fallback, or ordered
  component-stream merge.
- Covering child-prefix expansion now derives its bounded expansion cap input
  from the planner-owned scalar access-window projection, matching route
  planning's offset, limit, and lookahead semantics without a private helper.
- Covering child-prefix expansion now calls the route helper with the
  route-owned access-window contract, leaving raw fetch-limit adaptation
  private to route pushdown.
- Metadata-backed child-prefix expansion now returns one expanded prefix-family
  bundle, so the physical stream path no longer reconstructs the expanded
  index slot arity beside the shared metadata enumeration.
- Physical single-prefix, multi-lookup, and branch-set streaming now share the
  same prefix-family stream helper for expected-prefix-count validation and
  merged stream request construction.
- Shared physical prefix-family streaming now receives the route's exact
  primary-key suffix resume policy at the helper boundary.
- Sparse child-prefix expansion now validates lowered prefix count through the
  same physical multi-lookup guard before opening metadata-backed expansion.

## Completed Shared Flat Merge Slice

- Scalar key streams and covering component streams now use one generic
  payload-agnostic flat merge driver for larger sibling sets.
- Payload-specific child adapters still own polling and monotonicity state:
  scalar streams emit decoded keys, while covering streams emit decoded
  component rows.
- The shared flat-merge owner now also owns the zero/one/two/many sibling-set
  shape split, so scalar and covering routes cannot drift on when they keep a
  simple pair merge versus when they use a flat merge.
- The shared driver owns sibling head selection, duplicate-head clearing, and
  defensive duplicate output suppression.

## Completed Covering Prefix Preparation Slice

- Single-prefix and multi-prefix covering projections now share the same
  active-prefix preparation helper for empty-prefix metadata, index predicate
  rejection, scan-contract recovery, and store-handle resolution.
- Covering execution consumes the prepared active-prefix set before choosing
  direct bounded prefix scan, materialized fallback, or lazy merged prefix
  streams.
- If pruning leaves exactly one active covering prefix, the covering lane can
  use the single-prefix bounded scan instead of materializing an unsafe
  multi-prefix fallback.
- Scalar key streams and covering component streams now share the raw-index
  chunk helpers that cap each pull and advance the resume anchor, remaining
  output budget, and exhaustion flag.

## Completed Materialized Covering Window Slice

- Covering fallback for unsafe prefix sets no longer owns a hand-written
  decoded-key sort, dedup, direction, and limit block.
- The new executor utility owns only in-memory row mechanics; callers still
  decide whether a full materialized fallback is required.
- Focused utility tests prove ascending, descending, deduplication, and limit
  behavior over arbitrary row payloads.

## Completed Covering Branch Chunk Contract Slice

- Covering branch and multi-prefix projections now call the same
  branch-specific chunk sizing wrapper as scalar branch execution.
- The wrapper still delegates to the shared prefix-stream sizing formula; this
  slice is about preserving the active-branch-count contract at both runtime
  surfaces.
- The index-range invariant check now guards covering branch projections
  against sizing pulls from the original prefix count after empty-prefix
  pruning.
- Covering component-stream merge and key-stream intersection now share a small
  payload-agnostic pairwise stream reducer, avoiding a duplicate merge-tree
  loop without forcing component-row streams into the key-stream abstraction.

## Completed Exact-Prefix Predicate Rejection Slice

- Index-only predicates can now be evaluated against known exact prefix bytes
  through one scan-owned helper.
- Covering projections no longer own a private copy of prefix-predicate
  rejection logic.
- Scalar physical prefix streams and covering prefix-component streams now
  share the same active-prefix selection helper for empty-prefix metadata and
  prefix-predicate rejection.
- Materialized scalar prefix access now skips raw index range traversal when a
  single exact prefix, or every exact prefix in a multi-prefix set, proves the
  index predicate false before any key reads.
- Lowered range specs now retain exact prefix bytes, so structural range scans
  can reject impossible prefix predicates before opening the raw index range.
- Covered and non-covered SQL projection tests prove rejected-prefix query
  shapes return without row-store reads, index-entry reads, or range-scan
  calls.
- A direct executor range-spec test protects the range-prefix case because SQL
  can now fold some contradictory range predicates before route selection.

## Completed Covering Empty-Prefix Slice

- Single-prefix covering projections now consult synchronized empty-prefix
  metadata before opening a raw index range scan.
- This aligns the direct covering lane with scalar prefix streams and
  multi-prefix covering streams: metadata-proven empty exact prefixes return an
  empty page without row-store reads, index-entry reads, or range-scan calls.
- Scalar prefix streams and covering prefix-component streams now share one
  scan-owned prefix chunk sizing helper, avoiding separate tuning formulas for
  key streams versus key-only/covered projection streams.
- The helper stays fail-open. If metadata cannot prove the prefix is empty, the
  existing covering scan path is still used.

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
- The later route-level hard-cut decision closed this question for 0.185 by
  proving resumed branch-set, sparse child-prefix ASC, and sparse child-prefix
  DESC routes all stay on global `CursorBoundary` continuation.

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

## Carried Forward From 185.0 Baseline

- Shared branch-tree replacement for every special-case branch or `IN` flow was
  evaluated and ruled out for 0.185 after the stream mechanics had already
  been shared at their narrower runtime owners.

## Future Tuning Outside 0.185 Branch-Aware Closeout

- Wider downstream-specific query tuning.
- Broad cost-based route optimization over runtime prefix-cardinality
  metadata.
- Generalized branch-tree algebra for future non-primary-key branch merge
  semantics.
