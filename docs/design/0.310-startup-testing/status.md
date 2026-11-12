# Dependency-independent startup testing

Status: startup isolation is ready; the maintainer authorized the audit reduction
programme on 2026-11-11. Its bounded slices are now in progress.
The maintainer authorized this minor on 2026-11-11.
Owner: [#634 — Decouple native testing dependencies](https://github.com/dragginzgame/icydb/issues/634).

## Current closeout

Database 0.309.0 and CLI 0.296.10 are delivered. Their final published-source
product CI passed, including both macOS hosts. The compatible pending Host Tools
0.91 and Testkit 0.100.2 adoption is ready under focused checks: caller lint and
tests, the managed startup fixture, Wasm compilation/graph exclusion, tooling,
dependency admission, formatting and documentation. That pending work remains
local and unpublished; broad gates were not selected for it.

## Boundary and contract

The public `icydb::testing::drive_startup` wrapper currently binds the facade to
Testkit's `PocketIcInstance` and `TickUntilError`. Its optional native dependency
still participates in root Cargo resolution. Moving only its test or importing
PocketIC through Testkit cannot remove the product lock's exact thiserror pin.

Expose the existing typed readiness projection as
`icydb::testing::startup_poll(Result<DatabaseStartupState, E>) -> Poll<Result<(), E>>`.
Keep the current native-only `testing` feature, with no infrastructure dependency.
`Ready` yields successful completion, `Recovering` yields pending, and every
observation error yields immediate failure with its original typed value. The
function does not inspect diagnostic strings or progress a simulator.

Consumers compose this predicate with Testkit's existing `pic::tick_until`.
Testkit continues to own the initial observation, one-second advance/tick rounds,
zero-round and last-round boundaries, progress exhaustion, transport behavior,
server custody and lifecycle. Document the full composition and the existing
prohibition on retrying application mutations through readiness observations.

Remove the superseded `drive_startup` wrapper and Testkit type re-exports in one
public API hard cut. Do not introduce an IcyDB polling loop, driver trait,
compatibility shim or separately published adapter. This changes Rust authoring,
while preserving the supported typed readiness and bounded polling capability.

## Pre-cut caller inventory

- The only maintained in-repository runtime caller is the facade's
  `tests/startup_polling.rs`; the facade README documents the same call.
- Two facade unit tests already exercise the private readiness projection.
  Keep those at their semantic owner when the projection becomes public.
- Root Make/CI feature and lint selections already enable `icydb/testing`.
  Preserve their coverage of the dependency-free public predicate.
- No current qualification member calls the helper or depends on Testkit.
  Root Testkit serves the public wrapper and its native startup fixture; root
  Tokio serves that fixture alone.
- The deferred Canic integration uses its older, independent framework/Testkit
  graph. Its private lifecycle helpers stay under #373/#596.
- #315's delivered producer polling acceptance is retained. Actual composed
  install/upgrade and downstream retry-helper deletion remain with their existing
  owners; moving this fixture does not claim those outcomes complete.

## Landing slice

1. Complete #634's dependency boundary end to end: expose and document the typed
   predicate; remove the Testkit-bound public wrapper and unused root dependency
   declarations; move the real simulator fixture into a private startup
   qualification package in the existing `qualification/` workspace; align
   membership, lockfiles and CI/release discovery; qualify both graphs and
   update the owning pending group/detail notes.

The `qualification/startup` package owns actual runtime composition
tests, not dependency selection alone. It remains unpublished and introduces no
product edge into qualification. Its independent lock is the existing approved
isolation boundary. A new shipped package or workspace is unnecessary.

## Acceptance

- Product metadata and normal/build/test selections have no Testkit or PocketIC
  edge. Resolve current compatible thiserror in the product lock through Cargo;
  preserve unrelated selections and all published package versions.
- Qualification resolves its selected published Testkit/PocketIC cohort in its
  own lock. Shared types, actual singleton checks and graph policies remain intact.
- Keep typed ready/recovering/terminal/error unit assertions. The moved actual
  managed-PocketIC test must preserve initial readiness, zero-round exhaustion,
  finite exhaustion, last-round readiness, immediate failure and failure after
  progression, including observation counts and simulated-time assertions.
- Compile and strictly lint the changed facade/helper and actual qualification
  fixture. Verify native and Wasm graphs, tooling/membership admission, focused
  roster fixtures, formatting and documentation. Broad gates need their normal
  explicit selection; native Linux evidence does not qualify macOS.
- Preserve pending adoption notes when moving the database draft to the next
  minor; published notes, CLI draft numbering and deferred Canic source remain
  intact. No package version change, commit, push or deployment is authorized by
  this implementation request.
- Report IC instructions/cycles and total/code-section Wasm changes as unmeasured
  unless actually measured. No timing benchmark is required.

## Focused closeout

The facade now publishes the dependency-free predicate. The root all-feature
metadata and native testing graph contain no Testkit, PocketIC or Tokio, and the
product lock selects thiserror 2.0.21. Qualification independently resolves
Testkit 0.100.2, upstream PocketIC 16.1 and thiserror 2.0.18; its Types identity
aligns to 0.19.3 as required by Testkit. No override, fork or weakened guard is used.

Strict affected facade and qualification lint pass. Both typed semantic tests
and the moved managed-simulator test pass, retaining all six polling scenarios.
Testkit admitted the existing server and owned instance startup and teardown.
Wasm compilation and graph exclusion pass, as do tooling/pin/boundary admission,
current-source Cargo fixtures, actual Make dispatch fixtures and release-plan
fixtures. The fixture's former assumption that root always selected both SHA-2
families was corrected to test the actual selected families; simulator package
rejection controls are included. The first failure evidence is retained.

The pending group/detail notes and release metadata select 0.310. The reviewed
Make comment and detail-file selection hashes are readmitted without changing
stage rosters or renewing selected/native qualification certificates. Published
notes, existing package versions, CLI draft and deferred Canic manifest/lock are
preserved. Work is local, uncommitted, unpushed and unpublished. Broad gates and
new macOS/Canic qualification were not selected; IC cycles/instructions and
total/code-section Wasm-size changes are unmeasured. Actual composed install and
upgrade acceptance remains at its existing consumer owners.

## Authorized simplification extension

The maintainer requested working through the [mega-audit proposals](../../audits/mega-audit/programme.md). Keep the completed startup slice intact. Additional independent landing slices in this line are:

2. Derive the ordinary schema application head once, retaining future/published head distinctions.
3. Retire the parallel executor diagnostic detail representation after consumer tracing.
4. Borrow accepted-after topology for schema-domain preflight.
5. Reuse canonical predicate proof for hashing where caller evidence permits.
6. Reuse authoritative field-slot order without adding a redundant cache.
7. Investigate and simplify executable access representations only with semantic proof.
8. Investigate filter representation convergence without losing SQL semantics.
9. Investigate overlay consolidation while preserving exact recovery provenance.
10. Measure generated adapters before changing specialization.
11. Consolidate fixture input setup only when independent measurement subjects remain qualified.

Conditional findings may conclude that the current mechanism should remain. Each implementation includes its focused qualification, direct propagation, pending notes, and test/non-test code and complexity deltas. No broad gate, deployment, new persistence format or feature removal is selected.

Slices 2–4 are implemented locally: one head derivation, canonical executor
diagnostics and borrowed preflight topology. Strict affected Core library/test
lint and 95 focused diagnostic/schema tests pass. The batch removes 71 non-test
code lines and adds 12 test code lines. See the
[implementation evidence](../../audits/mega-audit/implementation.md) for remaining
conditional findings and qualification limits. The full simplification programme
is not yet complete; no release closeout or deployment is claimed for it.

Slice 5 is also complete under focused qualification: canonical predicate hashing
reuses the planner's normalized tree, with value encoding and refusal semantics
preserved. Current production constructors and rewrites were traced before
removing the repeated copy/normalization pass. Strict affected lint and 79
focused fingerprint/budget/idempotence cases pass. This slice removes nine
non-test code lines and adds 30 test code lines; its Wasm/IC effects are unmeasured.

Slice 7's owned-tree simplification is complete: executable access now borrows
composite children instead of recursively allocating another tree. The private
child view retains the semantic/mechanical boundary; positional prefix/range
specification binding remains unchanged. Strict affected lint and 52 focused
traversal, ordering, continuation and corruption cases pass. This removes 13
non-test code lines without changing test code. No IC/Wasm speedup is claimed;
the remaining binding-protocol proposal still requires separate evidence.

Slice 8's coverage-bookkeeping simplification is complete: a completeness flag
replaces the redundant three-state enum, while expression authority and optional
predicate subsets retain their distinct semantic roles. Strict affected lint and
22 focused intent/planner/session cases pass, including partial-filter SQL writes.
This removes 41 non-test and 13 test code lines. Merging the underlying semantic
representations remains unproven; no feature or runtime stage was removed.

Slice 9's investigation found an unused classification layer: recovery discarded
both classifiers' results before its real exhaustive dispatch. That enum, its
two classifiers and mapping-only tests are now removed. Provenance and live
containers remain unchanged; the proposed container merger is not established.
Strict affected lint and 22 focused provenance/recovery/fold tests pass. This
removes 48 non-test and 94 test code lines without adding a replacement mechanism.
IC/Wasm impact remains unmeasured; broad gates and delivery were not selected.

Slice 7's further cursor simplification retains positional spec binding while
removing its two redundant offsets. Cursors carry only remaining slices, preserve
failed-read state and remain independently copyable for speculative traversal.
One exhaustion check replaces the unused range count-comparison helper. Strict
affected lint and 50 access-stream/intersection/range/continuation tests pass.
This removes 18 non-test code lines with no test-code change; IC/Wasm effects
remain unmeasured. No new representation or binding protocol was introduced.

## Simplification batch closeout

The proven cleanup batch is ready under its recorded focused checks. The initial
ten findings now have [reviewed dispositions](../../audits/mega-audit/implementation.md#reviewed-disposition-of-the-initial-ten-findings):
slice 6 retains current slot sorting, and slices 10–11 defer generated-adapter
and fixture changes pending the existing Canic-dependent measurements. Remaining
representation/container mergers in slices 7–9 are not established as beneficial.
This is not whole-repository audit completion or broad release qualification.
The six implemented batches remove 200 non-test and 65 test code lines in total.
Package versions, published notes and existing dependency edits remain untouched;
no next minor, push or deployment is selected by this closeout.

## Authorized follow-on reductions

After reviewing the [follow-on investigation](../../audits/mega-audit/follow-on.md),
the maintainer authorized the first two recommended outcomes on 2026-11-11:

12. Consume completed migration working catalogues when forming fallback candidates;
    retain reconciliation copies, candidate precedence and existing errors.
13. Converge owned/borrowed aggregate ingestion on the existing Cow boundary;
    preserve NULL/numeric policy and move-or-clone selected extrema correctly.

These extend the same pending line. Each outcome receives direct caller checks,
release notes and code/complexity deltas. Grouped-page policy consolidation and
broader specialization changes are not part of this implementation batch.

Slices 12–13 are complete under focused checks. Migration candidate assembly
moves four catalogues after reconciliation; aggregate ingestion uses one semantic
match and removes its unused owned adapter. Strict affected lint and 30 selected
migration/reducer/caller tests pass, as do formatting and documentation checks.
The combined delta is -29 non-test and +39 test code lines (net +10); no new
representation or behavior axis is added. All audit implementations now total
-229 non-test and -26 test code lines. IC/Wasm effects remain unmeasured. This
restores the focused-ready verdict for the approved batch without selecting a
new minor or delivery; the third follow-on proposal remains unimplemented.

## Authorized DISTINCT lookup cleanup

The maintainer authorized follow-on candidate 4: remove the second grouped
DISTINCT membership preflight after the existing borrowed probe proves absence.
Landing slice 14 preserves fallible reservation, final HashSet equality, budget
ordering and the caller's insertion invariant. No new insertion API or collection
is introduced; focused key-set and DISTINCT-admission checks qualify the outcome.

Slice 14 is complete: one repeated membership lookup is removed; final insertion
still enforces equality and reserves before mutation. Strict affected lint and
20 focused key-set/admission tests pass. Delta: -3 non-test and 0 test code lines.
Formatting and documentation pass; IC/Wasm effects remain unmeasured. The approved
batch remains focused-ready, with grouped-page consolidation still conditional.

## Authorized schema lifecycle cleanup

The maintainer authorized both concrete schema-lifecycle candidates on 2026-11-11:

15. Remove the initial per-store entity sort: source-key map traversal already
    orders each store's subsequence. Retain the existing-head sort.
16. Borrow immutable enum/composite catalogues in the shared new-entity context;
    retain the source-binding snapshot and its complete attachment validation.

These extend the current pending line without changing persisted contracts.
Qualification covers installation ordering and additive entity construction.

Slices 15–16 are complete under focused checks: strict affected lint, 76 selected
proposal/additive/relation tests and formatting/documentation checks pass. Delta:
-24 non-test and +68 test code lines, net +44; cumulative audit delta is -256
non-test and +42 test, net -214. The added regression qualifies multi-store
identity ordering; no new runtime mechanism is introduced. IC/Wasm effects remain
unmeasured. The approved batch is focused-ready; no delivery or next minor is
selected. Provisional binding redesign remains conditional.

## Authorized recovery cleanup

The maintainer authorized both recovery candidates on 2026-11-11:

17. Borrow call-local cardinality coalescing inputs, preserving last-write-wins
    behavior and checkpoint/prefix validation.
18. Extract schema candidates from the admitted first record, retaining recovery
    store, revision and source-binding validation.

These remain in the current pending line and do not change persisted bytes.

Slices 17–18 are complete under strict affected lint, 87 focused tests, formatting
and documentation checks. Delta: -18 non-test, +38 test, net +20 code lines.
Cumulative audit delta: -274 non-test, +80 test, net -194. The approved batch
is focused-ready. Wasm/IC effects remain unmeasured; no release or next minor
is selected.

## Authorized relation preparation cleanup

19. Reuse one resolved component-specification vector and replace the immediate
    descriptor wrapping/unwrapping with borrowed tuple validation. Preserve
    validation order, diagnostics and specialized scalar/nested execution.

The maintainer authorized this bounded follow-on in the current pending line.

Slice 19 is complete under strict affected lint, 50 focused tests, formatting
and documentation checks. Delta: -10 non-test, 0 test code lines. Cumulative
audit delta: -284 non-test, +80 test, net -204. The approved batch remains
focused-ready; no next minor or release is selected. Wasm/IC effects are unmeasured.

## Authorized parameterized cache cleanup

20. Share final prepared-plan construction and cache publication between template
    rebinding and first compilation. Preserve exact-bound early reuse, miss-only
    compilation charging, template-allocation order and failure-before-publication.

The maintainer authorized this outcome within the current pending line.

Slice 20 is complete under strict affected lint, 18 focused tests, formatting
and documentation checks. Delta: -9 non-test, 0 test code lines. Cumulative
audit delta: -293 non-test, +80 test, net -213. The approved batch is
focused-ready. IC/Wasm effects remain unmeasured; no release or next minor is
selected.
