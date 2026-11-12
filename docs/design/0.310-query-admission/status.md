# Query admission consolidation

Authorized 2026-11-12 after the focused-ready startup-testing closeout.
The unpublished startup and query-admission work share database 0.310.0.
The initial query-admission working label 0.311 was corrected before release;
0.309.0 remains the latest published database release.

## Landing slices

1. Replace FilterValue with InputValue end to end: typed literals, serialized
   inputs, bounded admission/cleanup, accepted enum resolution and callers.
2. Establish one accepted-schema-bound filter authority and retire duplicated
   frontend/intent/planner reconstruction, retaining pre-fold validation.
3. Bind SQL parameters into semantic admission without syntax writeback.
4. Close grouped, aggregate FILTER, mutation and exact-count consumer coverage;
   remove transitional routes and qualify the full selected boundary family.
5. Hard-cut positional access binding: make prepared leaves identify their
   physical specs directly, propagate traversal, covering and metadata consumers,
   and remove the shared spec-consumption cursor. Authorized separately after
   the four-slice native closeout; implemented and native-focused-ready.

6. Carry bound leaves through the physical scan entrypoint; remove redundant
   handoff wrappers and repeated prefix-count validation. Authorized after
   slice 5 closeout; implemented and native-focused-ready.

Each slice includes direct tests, diagnostics and documentation. No slice may
ship a legacy fallback. See the [programme](../../audits/mega-audit/query-admission-design.md)
and [literal contract](../../audits/mega-audit/query-admission-first-slice.md).

## Current work

Slice 1 is complete and focused-ready. Existing schema-aware text conversions remain explicit;
typed values retain their original kind. InputValue/PublicValue owns the sole
public literal shape. Maps and enum payloads join bounded traversal and cleanup.

505 query tests and 52 schema-info tests pass; strict Core Clippy passes.
See the [slice evidence](../../audits/mega-audit/query-admission-first-slice.md#implementation-outcome)
for the literal-contract code and complexity deltas. Slice 2 is now implemented
and focused-ready as described below. Direct parameter binding (slice 3) is now
implemented and focused-ready. The native consumer closeout (slice 4) is now
focused-ready as recorded below; this is not release qualification or measured
Wasm/IC acceptance.

The maintainer subsequently authorized the MA-04 access-binding hard cut as
slice 5 in this same pending line. The four-slice verdict above remains evidence
for query admission only; it does not qualify this extension. See the
[binding investigation](../../audits/mega-audit/query-admission-removal-map.md#authorized-access-binding-hard-cut)
for the concrete ownership and acceptance constraints.

## Continuation evidence

Slice 2 boundary investigation identified unreachable predicate-only intent kept
alive by impossible SQL wrapper states and historical fixtures. The smaller SQL
mirror-only experiment was rejected because it adds repeat derivation. See the
[boundary findings](../../audits/mega-audit/query-admission-design.md#slice-2-boundary-investigation--2026-11-12).
The next implementation must remove that route and propagate the maintained test
contracts together. The wider bound-filter consolidation remains unfinished.
Toko's current v0.258.0 dependency is unaffected; its future migration has
[explicit consumer impact](../../audits/mega-audit/toko-filter-impact.md).

The [removal map](../../audits/mega-audit/query-admission-removal-map.md) now
separates retired intent tests from live residual guarantees, records membership
writeback construction, and identifies exact-count's pre-planning consumer. The
shared admission result must remain usable without requiring full scalar planning.
Fourteen focused baseline tests passed; no additional runtime edits landed.

## First slice-2 removal

The unreachable predicate-only intent mode and impossible SQL expression states
are removed. Behavioral fixtures now use maintained expression-backed inputs;
strict covering-contract qualification retains explicit matching expression and
predicate facts. Production delta: -79 code lines; tests: +37; net: -42.
The wider bound-filter consolidation and SQL parameter writeback remain pending.

Focused qualification passed: 503 query tests, 298 cardinality/covering session
tests, 99 ordering/cache session tests and 66 SQL-lowering tests (966 total).
Strict Core Clippy with SQL/migration/metrics and documentation checks pass.
Broad/release gates remain skipped user-owned validation; no package versions
or deployment changed. This qualifies the intent removal, not the whole programme.

## SQL binding writeback removal

Both canonical-value writeback sites are removed. Parameter substitution remains;
semantic admission is read-only afterward. Membership lowers its target once and
uses the shared semantic comparison admission helper instead of per-item SQL AST
construction. Complex comparisons retain inference and pre-fold refusal.

Delta: +22 production / +48 test code lines (net +70). This removes a backward
representation conversion and repeated target lowering, with no new data type.
The broader slice-3 direct semantic binding outcome is not complete: templates
still substitute into SQL syntax, and whole-query lowering remains. The accepted
schema filter authority and cross-consumer closeout also remain pending.

Final focused qualification passed: 67 SQL-lowering tests, 99 parameter/cache
ordering tests and 298 cardinality/covering/residual session tests (464 total).
Strict Core Clippy with SQL/migration/metrics, make fmt-check, documentation links
and diff hygiene pass. Broad/release gates remain skipped user-owned validation.
No package versions or deployment changed.

## Admitted WHERE handoff

Parameter admission now returns the semantic expression already produced for its
truth-shape check. Scalar/grouped template binding and global-aggregate binding
consume it directly through shared boolean normalization, removing one repeated
whole-WHERE lowering pass. Cold bound SELECTs use the same checked-template
compiler as cached parameterized calls; the separate bound-statement preparation
function is removed. No current-schema or pre-fold gate is intentionally skipped.

Delta: +2 production / -23 test Rust code lines (net -21). Tests now inspect the
maintained semantic output. There is no additional semantic representation or
legacy mode. This does not eliminate the execution-local SQL copy for substitution,
nor all leaf admission lowering; wider direct binding and bound-filter authority
consolidation remain pending. Cold compilation now constructs a template as well;
Wasm module/code-section bytes and IC cycles/instructions remain unmeasured.

Qualification for this handoff: 67 SQL-lowering, 99 parameter/cache ordering and
298 cardinality/covering/residual session tests pass (464 total), together with
strict Core Clippy under SQL/migration/metrics and documentation/diff checks.
Broad/release gates remain skipped user-owned validation. No package versions
or deployment changed; this is not a complete programme closeout.


## Accepted-schema filter owner

Slice 2 now replaces the unbound filter handoff end to end. One admission owner
constructs expression/predicate agreement, coverage and original key-input facts,
and retains accepted store/schema authority plus entity identity. Planning
borrows that result instead of reconstructing it; exact-count and metadata-only
aggregate shortcuts validate the same authority without full scalar planning.
SQL can no longer supply an independently asserted expression/predicate pair.
Its literal policy and required-predicate capability gates remain explicit.

A same-contract heap-store rebuild can renew store-local authority while keeping
the durable database root identity. Compiled SQL retention now checks both and
recompiles bound commands on renewal. Strict covering fixtures qualify planner
assembly directly rather than bypassing the authored filter-admission boundary.
UPDATE uses its required-predicate expression consistently, preserving rejection
of incompatible arithmetic before mutation.

Qualification: 505 query, 298 cardinality/covering/residual, 100 ordering/cache,
67 SQL-lowering and 52 schema-info tests pass (1,022 total across focused runs).
The corrected heap-rebuild regression was rerun separately after its fixture was
changed to recreate stores, rather than merely discard the runtime projection.
Strict Core Clippy passes with SQL/migration/metrics and with default features.

This change's cloc delta against its starting worktree is +60 production / +215
test Rust code lines (net +275). This is an ownership simplification, not a net
line deletion: planner reconstruction and caller-supplied mirror authority are
retired, while schema guards and qualification add code. No new generic executor
or fallback mode was introduced. SQL pre-fold parameter checks and execution-local
syntax substitution remain for slice 3. Wasm total/code-section bytes and IC
cycles/instructions are unmeasured. Broad and release gates remain skipped
user-owned validation; package versions and deployment are unchanged.

## Slice 3 implementation boundary review

The [direct-binding removal map](../../audits/mega-audit/query-admission-removal-map.md#slice-3-audit-remove-syntax-substitution-preserve-literal-rules)
now identifies the execution-local syntax clone/substitution traversal and two
syntax-sensitive rules that direct semantic binding must preserve: field
arithmetic literal admission and numeric-scale literal canonicalization. ILIKE's
in-lowering normalization also prevents postponing all validation until after
lowering. Keep the existing lowerer as the single translation owner and qualify
scalar/grouped/global WHERE callers together; do not add another expression IR.
This is source-inspection evidence, not implementation or a performance result.

## Direct semantic parameter binding

Slice 3 removes the execution-local WHERE clone and SQL parameter/membership
substitution walker. The existing expression lowerer resolves slots directly,
passes child semantic expressions upward, and validates bound subtrees before
parent simplification. Its invocation-local context tracks slot visits without
retaining bound values or adding an expression representation. Scalar, grouped
and global-aggregate templates share this path for cold and cached execution.

Arithmetic literals and numeric scales retain typed rejection. The added scale
control uses programmatically authored WHERE syntax: SQL text's existing parser
requires literal scale arguments and remains unchanged. The first new test
incorrectly assumed a text scale slot; that fixture was corrected, not the parser.
An initial lint correction and a formatter-exposed semicolon warning were fixed;
final strict Core library/test Clippy with SQL/migration/metrics passes.

All 466 selected tests pass: 68 SQL lowering, 100 ordering/cache/binding, and 298
cardinality/covering/residual controls. These include exact construction limits,
cache retention and failed admission, changed parameters across SELECT lanes,
accepted-schema renewal, nested CASE, NULL and identity operands, and dead-branch
rejection. Canonical formatting and documentation/diff checks pass.

Task-relative cloc delta: +49 production / +65 test Rust code lines, net +114.
This is a traversal/representation reduction, not a net code-size reduction.
Wasm total/code-section sizes and IC cycles/instructions remain unmeasured;
the maintained base fixture is blocked by the deferred Canic executor graph.
Broad/release gates remain skipped user-owned validation. No package versions,
deployment or network lifecycle changed. Slice 4 remains the programme closeout.

## Native consumer closeout and aggregate template contraction

Slice 4 found and removed the complete unfiltered execution command retained
inside a global-aggregate template. Templates now keep reusable base shape,
strategies, projection and HAVING. One consuming constructor builds the actual
structural query, extrema ordering and metadata eligibility after current WHERE
binding; ordinary compilation uses the same owner. The redundant command clone,
query/fact overwrite and forwarding constructor are removed without adding a
cache or execution mode.

The extended cache matrix exercises aggregate FILTER, HAVING, LIMIT 0 and OFFSET
1 with current parameter values, literal-query controls, exact retained-capacity
boundaries and failed binding. Pagination controls use explicit primary-key
ordering: the first fixture omitted ordering and correctly received the existing
UnorderedPagination refusal. That fixture was corrected; admission was not relaxed.

Final focused evidence covers 466 distinct tests: 99 initially passing ordering
controls plus the corrected cache matrix, 68 SQL-lowering controls, and 298
cardinality/covering/residual controls. The latter use the current Wire 0.21.3
binary; an accidental invocation of an older retained binary is excluded from
this evidence. Strict Core library/test Clippy under SQL/migration/metrics,
canonical formatting, selected documentation and diff checks pass. The incoming
Wire update was preserved, fetched with the lock fixed, and the lock remained
stable through these checks.

Task-relative cloc: -4 production / +4 test lines, net zero. Complexity reduction
is one retained full command and one throwaway query/fact construction stage;
no memory-byte or performance saving is inferred from the source change.
The accepted four-slice programme is native-focused-ready. Required-predicate
SQL capability checks, pre-fold parameter refusal, accepted-schema authority and
full-expression versus derived-predicate semantics remain intentional boundaries.
Wasm total/code-section and IC instruction/cycle effects remain unmeasured; the
maintained measurement fixture is blocked by the deferred Canic executor graph.
Broad/release gates and installed qualification remain unrun. No package version,
publication, deployment or local network lifecycle action occurred.


## Access-binding hard cut closeout

Slice 5 is implemented and native-focused-ready. The existing access tree owns
its index leaves' shared physical bindings. Traversal borrows them directly;
the positional cursor, parallel prepared/grouped spec arrays, repeated alignment
protocol and redundant grouped handoff wrapper are removed. Covering and exact
counts use the same owner; page-suppressed clones share bounds. Semantic equality
and continuation identity remain independent of physical residents.

813 distinct focused tests pass: 11 lowering/ownership cases, 21 access tests,
338 planner tests, 40 access-stream tests, 298 cardinality/covering session tests,
100 ordering/cache tests and 5 prepared-plan retention tests. The initial two new
rejection fixtures accidentally reused already-prepared candidates; constructing
semantic-only leaves corrected the fixtures, and both now pass. No production
refusal was relaxed. Strict Core Clippy under SQL/migration/metrics, canonical
formatting and selected documentation/diff checks pass.

Task-relative cloc: -123 production / +122 test code lines, net -1. The meaningful
reduction is removal of the tree/array synchronization protocol and separate
handoff owners. Per-index-leaf allocation granularity is a tradeoff; memory,
Wasm total/code-section bytes and IC cycles/instructions remain unmeasured.
Broad/release gates and macOS/installed qualification remain skipped user-owned
validation. Package versions and deployment are unchanged.


## Physical handoff closeout

Slice 6 carries the executable leaf into physical dispatch. The physical owner
borrows its admitted specs and checks prefix arity once. Two handoff wrappers,
one forwarding method and redundant count parameters/checks are removed;
private scan inputs, prefix expansion, storage decoding, hard budgets and
continuation policy remain local to their existing owners. No new retained
state or generic abstraction was introduced.

439 focused tests pass: 41 access-stream tests, 298 cardinality/covering session
tests and 100 ordering/cache tests. Primary resume controls now use the physical
entrypoint; an added case rejects composite plans at that leaf-only boundary.
Strict Core Clippy with SQL/migration/metrics, canonical formatting and selected
documentation/diff checks pass. Validation preserves the incoming Platform Types
0.19.5 and Wire 0.21.4 lock selections; the initially missing locked crate was
fetched, and manifest/lock hashes remained stable through qualification.

Task-relative cloc: -55 production / +12 test code lines, net -43. Wasm total and
code-section bytes, IC cycles and instructions remain unmeasured. Broad/release
gates and macOS/installed qualification remain skipped user-owned validation.
Per-leaf storage representation is a separate follow-up; package versions and
deployment are unchanged.
