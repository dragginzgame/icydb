# Flow Convergence And Duplication — Broad Baseline

## Preamble And Comparability

- Method: `FCD-1.0`; run `2026-10-01/01`; auditor `Codex`.
- Definition: [flow-convergence and duplication](../../../../../../../audits/recurring/crosscutting/crosscutting-flow-convergence-and-duplication.md).
- Snapshot: HEAD `45ff7e8270514a505260460a8db1f7f0ed96c1a6`, tree
  `3cbc0202d8dad27a2e477893a14db918c0b5d951`, released `v0.262.2`, plus the
  pre-existing dirty worktree described below.
- Tracked diff SHA-256 at entry:
  `2e6e35137238eb56176b16ef9a8b40878f5e0c127e068f554e5034369025b0f5`.
- Cargo.lock SHA-256 at entry:
  `6abd52233d9de42eed2740b6eb1d5b4ccd7e69a80ed1247ba9b75ba7ed2a2e5f`.
- Dirty worktree: eight schema package manifests, Cargo.lock, root/detailed
  changelogs and the 0.262 status tracker already contain dependency-cleanup
  work. These twelve files are excluded from this audit's edits. No Cargo
  version changes were made. The source snapshot remained unchanged.
- Baseline: [2026-08-26 affected-owner run](../../../../08/26/flow-convergence-and-duplication/01/report.md).
  Scope comparison is **N/A**: this explicitly requested broad baseline covers
  more behavior families. Only named stable owner anchors are compared below.
- Output: this report and [structured findings](findings.json). An audit is
  evidence; it does not authorize implementing the findings or creating a debt
  ledger. No source, release metadata, design or older report was edited.

## Scope And Discovery Evidence

Locked offline Cargo metadata identifies 49 workspace packages and 120 targets.
Rust discovery covered `crates/`, `testing/`, `schema/`, `canisters/` and
`scripts/`: 1,659 Rust files and 21,809 lexical function-body candidates. These
are scanner candidates, including macro/fixture bodies, rather than
compiler-proven function or reachability counts.

Exact-body screening found 23 groups of substantial identical bodies at a
60-token threshold. A second test/helper screening at 15 tokens found 32
exact-body groups. Structural screening with renamed local identifiers also
exposed the equivalent named-type walkers. These scans select inspection
candidates; counts do not establish equivalence, deadness or severity.

Manual inspection traced SQL, typed/dynamic session APIs, prepared execution,
exact aggregation, physical streams, catalog application, startup/recovery,
write publication, diagnostics, scalar capability registries, generated/model
consumers, replay evidence, migration tests and canister fixtures. Invariant
scripts and focused tests qualify the selected boundaries. Native elapsed
results are not used as performance evidence.

This is a broad maintained-flow audit, not a compiler-backed proof that every
symbol is reachable or every duplicate is redundant. Generated build outputs,
external consumers, historical prose and the full workspace correctness suite
are outside its proof scope. No new unused-code deletion is recommended solely
from a zero reference count.

## Verdict

**PASS WITH FINDINGS**

Five consolidation opportunities remain: a shared physical seek-control
contract, direct named-type dependency enumeration, a parser diagnostic map,
registry-derived numeric capabilities, and shared budgeted-request test setup.
Three have `MEDIUM` maintenance risk and two have `LOW` risk. No inspected
finding establishes a present correctness failure or a release blocker.

The inspected runtime flows retain accepted-catalog authority, common prepared
execution and journal publication owners. The proposed changes can remove
handwritten switch sites without adding modes, caches, compatibility routes or
persisted forms. Whole-engine replacement and a generic traversal framework
are not needed.

## Behavior And Owner Map

| Behavior | Canonical owner | Inputs | Carried contract | Consumers |
| --- | --- | --- | --- | --- |
| Read planning and execution | accepted catalog context and prepared query-plan owner | SQL/dynamic/typed query, request scope | accepted entity authority, structural query, prepared plan | scalar/grouped execution, projection, EXPLAIN |
| Exact aggregate execution | exact terminal and cardinality metadata | pinned accepted index identity, immutable exact target | generation-matched metadata result or unavailable outcome | global aggregate projection or prepared fallback |
| Held-head seek control | physical stream owner; common control remains duplicated | direction, target, seek work | held/exhausted/page-stop outcome | primary and secondary-index streams |
| Named-type direct dependencies | schema fragment representation; enumeration remains duplicated | NamedTypeFragment and nested FieldType | direct TypeSourceKey sequence | source digest and catalog application lowering |
| Catalog mutation | schema application/lowering | submitted proposal and accepted snapshot | catalog-native candidate and durable operation | publication and dedicated recovery driver |
| Write publication | accepted structural mutation batch and journal commit | typed/dynamic/SQL writes | accepted row changes and optional durable progress | atomic commit/fold and derived indexes |
| Startup readiness | startup observation and dedicated driver | durable recovery evidence | observed readiness or bounded recovery page | ordinary admission guard and background driver |
| Parse diagnostics | existing parse-error boundary; map currently duplicated | trailing token rejected by its parser | SqlFeatureCode | standalone predicate and feature-gated SQL parser |
| Scalar capabilities | schema scalar registry | Value variant | distinct numeric/coercion flags | comparison/coercion consumers |
| Test request construction | RequestExecutionRoot test support | resource and limit | uniform test budget and request scope | 59 identical local factories |

## Flow Trace

| Entry surface | Frontend-only work | Convergence point | Runtime path | Result projection |
| --- | --- | --- | --- | --- |
| SQL SELECT / dynamic or typed reads | parsing, binding or public input construction | accepted authority and shared prepared plan | scalar/grouped executor | accepted row/value projection |
| SQL global aggregates / session exact count | aggregate admission or count request construction | immutable exact target and exact terminal | shared metadata envelope or canonical prepared fallback | aggregate-specific typed result |
| Query-only SQL entry | reject write command kinds | same read execution owner | ordinary SELECT/aggregate execution | ordinary SQL result |
| SQL / dynamic / typed writes | statement lowering or row admission | AcceptedStructuralMutation and batch inner | one journal publication window | write-specific diagnostics/RETURNING |
| Initial schema / existing-entity changes / entity creation | authority-specific proposal preflights | catalog-native candidate and publication | durable application/recovery | application status and accepted catalog |
| Ordinary access during startup | observe admission state | ensure_recovery_admitted | guard only; dedicated driver executes recovery pages | readiness or typed refusal |
| Primary / secondary-index held-head seek | backend key lowering/refill | equivalent control loop currently duplicated | account, compare, consume, configure backend seek | held/exhausted/page-stop outcome |
| Standalone predicate / SQL statement parsing | separate grammars and rejection decisions | existing token/error types; diagnostic map duplicated | parser-specific validation | shared diagnostic codes |

Separate outer validation or result projection is not an independent engine
when it converges through the maintained authority and execution contract.

## Findings

| ID | Class | Risk | Owner and evidence | Present friction | Disposition and action trigger |
| --- | --- | --- | --- | --- | --- |
| FCD-004 | DuplicateFlow | MEDIUM | [physical streams](../../../../../../../../crates/icydb-core/src/db/executor/stream/access/physical.rs), ensure_physical_head at 1207/1655 and seek_head_at_or_after at 1300/1688 | identical held-head, pull admission, comparison charging and page-stop control must be changed twice | CONSOLIDATE before another backend or a shared accounting/resume change; share only the control contract within the existing owner |
| FCD-005 | DuplicateFlow | MEDIUM | [source digest](../../../../../../../../crates/icydb-schema/src/source_digest.rs):223/254 and [application lowering](../../../../../../../../crates/icydb-core/src/db/schema/application_lowering.rs):281/315 | the same named-type and nested field-type dependency branches are handwritten twice | CONSOLIDATE during the next authorized schema cleanup; put direct dependency enumeration with schema fragments |
| FCD-006 | DuplicateFlow | LOW | [predicate parser](../../../../../../../../crates/icydb-core/src/db/predicate/parser/mod.rs):74 and [statement parser](../../../../../../../../crates/icydb-core/src/db/sql/parser/mod.rs):307 | the same trailing-token diagnostic map has two copies | LOCALIZE during a parser diagnostic edit; share pure diagnostic projection through an ungated parse-error owner |
| FCD-007 | PolicyRediscovery | LOW | [Value semantics](../../../../../../../../crates/icydb-core/src/value/semantics.rs):12/29 and [scalar registry](../../../../../../../../crates/icydb-schema/src/scalar_macros.rs) | two variant lists independently reproduce authoritative capability flags | LOCALIZE during a scalar-capability edit if registry projection preserves const/lightweight execution |
| FCD-008 | DuplicateFlow | MEDIUM | 59 identical factories in 59 files; representative [comparison tests](../../../../../../../../crates/icydb-core/src/db/query/construction/comparison/tests.rs):17 and [index metadata tests](../../../../../../../../crates/icydb-core/src/db/session/tests/cardinality_tiebreak/index_metadata.rs):12 | the same uniform budget profile and construction imports are maintained in 59 places | CONSOLIDATE as one bounded test-setup cleanup in existing request test support; preserve assertions and distinct profiles |

### FCD-004: Preserve accounting before changing stream state

The primary and index implementations have identical ensure/seek/consume
control: check the held head, admit and charge a pull, charge a comparison,
charge a skipped consumption, then change state and configure the next seek.
Failure and page-stop placement are observable contracts. Existing physical
seek tests already share assertion routines between the two backends.

Keep primary-key conversion, index-suffix conversion, refill sizing, physical
bounds, entity validation and prefix-merge resume state in their current
backend owners. A small statically dispatched owner-local helper is a candidate;
allocation, dynamic dispatch or a new general stream framework is not justified.
Reusing source is not evidence of smaller Wasm. Qualify raw Wasm and IC
instructions when implementing this hot-path change.

### FCD-005: Share direct edges, preserve closure authority

Both walkers enumerate record fields, enum payloads, newtype/list/set inner
types, map keys/values and tuple members. Their nested FieldType walkers recurse
through lists, append named sources and ignore scalars. The representation in
[fragment.rs](../../../../../../../../crates/icydb-schema/src/fragment.rs) is the natural owner of this direct-edge enumeration.

Their outer closure algorithms serve different authorities. Digest construction
includes canonical reachability and relation targets; application lowering
validates initial reachability, allocates new identities and stops at accepted
catalog definitions when extending an existing database. Keep those obligations
separate. Consolidation must not recreate accepted catalog definitions from
current generated models, change source ordering or add a second registry.

### FCD-006: Share error projection, preserve grammar boundaries

The two functions have the same mapping for trailing AS, DESCRIBE, HAVING,
INSERT, JOIN, FILTER, OVER, RETURNING, SHOW, WITH, set operations and UPDATE.
Their callers independently decide that a token is invalid in the current
context. Share only the diagnostic projection, with parse-error ownership;
statement feature admission and clause ordering stay parser-owned.

The standalone predicate parser is ungated while the SQL frontend is gated.
A future implementation must qualify both feature configurations and all mapped
codes. Current focused tests check the statement code stability and a predicate
trailing-clause rejection; they do not cover every token in both contexts.

### FCD-007: Preserve two distinct capability concepts

The current nine-variant numeric/coercion lists agree with registry flags, as
confirmed by separate registry-generated tests. The issue is maintenance
rediscovery, not a demonstrated incorrect result. Derive each capability from
its own registry flag if a simple const projection is available. Do not alias
one public capability to the other just because today's truth tables agree.
The broader coercion_family classification has different semantics and stays
separate.

### FCD-008: Centralize setup without deleting behavior tests

The exact-body group contains 59 helpers named root or request. Each builds a
uniform 16,000,000 test budget, failure headroom of 500,000,000 instructions and
64 KiB, then overrides one resource limit and calls new_for_tests. The existing
[request owner](../../../../../../../../crates/icydb-core/src/db/session/request.rs):116 already owns that test constructor.

One cfg(test) resource/limit factory can carry the shared profile. Retain every
assertion, request ownership and budget exhaustion boundary. Helpers with other
profiles stay distinct; this is not authorization to substitute production
request/read/mutation budgets or introduce a general fixture framework.

## Retained Separations And Supporting Observations

- Accepted-catalog checks in execution, persisted decoding and recovery remain
  independent fail-closed boundary enforcement. Similar AcceptedFieldKind
  match lists do different validation, decoding and traversal work. Combining
  them would erase distinct obligations or force Value materialization.
- Public InputValue/OutputValue wrappers retain different authoring/accepted
  boundaries even where constructors look alike. Thin typed dispatch through
  shared model/index authorities is retained.
- Query-only SQL rejection remains an entry boundary; normal read execution
  already converges. SQL write-shape adapters delegate to the common policy
  owner despite similar statement-specific wrappers.
- Request-wide, per-read and per-mutation budgets have different operation
  counts, instruction ceilings and failure reserves. Shared vector tails do
  not justify merging those distinct policies.
- Model and model-macro primitive-catalog tests protect separate generated
  consumers. Decimal property tests with identical bodies use different input
  domains. No behavioral test was identified as safely deletable from body
  equality alone.
- Canister fixtures, one/ten-entity compile surfaces, migration/index wrappers
  and replay DTO validators have distinct schema, lifecycle or evidence roles.
  Cross-package crate-path rewriters have different targets/error roles and
  package constraints; a new helper crate solely to remove their small overlap
  is not justified.
- Two schema-application restart test helpers also repeat the same reset and
  startup procedure in identity_field_removal and populated_field_removal.
  They are supporting test-setup evidence, outside the five active priorities;
  their lineage/data assertions remain distinct. No new backlog is created.

Stable historical anchors now converge: FCD-002 uses
try_fold_exact_first_components in [cardinality.rs](../../../../../../../../crates/icydb-core/src/db/index/cardinality.rs)
and execute_exact_first_component_metadata in
[exact_terminal.rs](../../../../../../../../crates/icydb-core/src/db/executor/aggregate/exact_terminal.rs).
FCD-003's exact target/outcome vocabulary is generalized. The older hidden
startup-work concern has an explicit [admission guard](../../../../../../../../crates/icydb-core/src/db/commit/recovery.rs):130
and [dedicated driver](../../../../../../../../crates/icydb-core/src/db/startup/driver.rs):41.
These observations do not rewrite historical reports or establish numerical
comparability with their earlier scopes.

## Complexity And State-Space Delta

This audit adds two evidence files (approximately 480 lines) and changes no
production/test source.
Its implementation shape is neutral: no behavior axes, execution routes,
persisted states, modes, configuration or compatibility paths were added.

The proposed reductions are concrete: two held-head control implementations to
one; two direct-dependency walkers to one; two diagnostic projections to one;
handwritten capability lists to registry projections; and 59 identical test
factories to one. Public concepts and persisted representations remain owned by
their existing authorities. Line savings are not estimated before implementation.
Wasm bytes, IC cycles and instructions are **unmeasured** for these proposals;
no performance improvement is claimed.

Extension probes expose current friction: changing the named-type shape needs
two equivalent walkers; changing seek accounting needs two control loops;
changing the shared uniform test profile needs 59 constructors. A cleanup should
reduce these switch sites without adding an independent behavior axis.

Recommended implementation order is FCD-005, FCD-008, then FCD-004 with permitted
cost measurements. FCD-006 and FCD-007 fit bounded edits to their respective
owners. Each needs its own authorized, end-to-end reviewable handoff.

## Focused Verification

| Check | Result | Evidence and limits |
| --- | --- | --- |
| Locked offline workspace metadata | PASS | 49 packages, 120 targets; inventory rather than full compilation |
| Exact/structural screening and owner trace | PASS | scan candidates manually qualified; not a whole-program reachability proof |
| Physical seek tests | PASS | 16 focused tests, including held-head/page-stop/accounting boundaries |
| Entity-creation named-type tests | PASS | 9 focused tests |
| Source-digest tests | PASS | 2 focused tests |
| Scalar capability registry tests | PASS | 2 focused tests, one per distinct capability |
| Parser diagnostic/trailing-clause tests | PASS | 2 focused tests; full token/context matrix not claimed |
| Schema-application field-removal tests | PASS | 7 focused tests across identity and populated paths |
| Representative duplicated request factories | PASS | 2 focused exact-limit/construction tests |
| Static invariant gates | PASS | layer authority, SQL branch ownership, schema/model boundary, generated endpoints, mutation atomicity, persisted version policy and executor non-panicking paths |
| Report references, structured JSON and whitespace | PASS | new output files only; source snapshot preservation checked |
| Full workspace suite | BLOCKED | user-owned validation, explicitly excluded by agent rules; not run |
| Wasm/cycle/instruction comparison | BLOCKED | audit performs no implementation or live cost experiment; unmeasured |

Total focused tests: **40 passed, zero failed**. A first physical test selector
matched zero tests; the corrected physical_seek_tests selector ran the 16 tests
counted above. No PocketIC/ICP lifecycle action or external service was used.
Clippy and formatting were not rerun because no Rust source changed.
