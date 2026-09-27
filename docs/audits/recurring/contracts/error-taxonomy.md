# Recurring Audit — Error Taxonomy

## Identity And Scope

- Report scope: `error-taxonomy`
- Method: `ERROR-5 + DOMAIN-1`
- Report: `docs/reports/recurring/YYYY/MM/DD/error-taxonomy/<run>/report.md`

Apply [audit governance](../../README.md), especially domain scope, finding
ownership, immutable reports, and executed-test evidence. Freeze the method
before execution. Record the trigger or requested baseline, selected owners,
sampled producer paths, and exclusions. A baseline covers the taxonomy axes and
public projection exhaustively, with explicitly named boundary samples; it does
not imply every error-producing call site was exercised.

This audit owns error meaning across construction, wrapping, diagnostic
projection, and public serialization in `icydb-core`,
`icydb-diagnostic-code`, and `icydb`. It does not re-prove recovery
equivalence, index membership, pagination, resource accounting, or security
reachability. Those domain audits own the underlying behavior; share compatible
evidence and link their findings when classification is affected. Mixed-domain
enums and dependency crossings alone are not classification defects.

## Method Changes

ERROR-5 replaces the historical Method V4 checklist:

- inspect numeric diagnostic codes and fact schemas, not only facade enums;
- distinguish internal runtime axes from public diagnostic axes and wire records;
- allow deliberate boundary-owned origin changes while checking retained meaning;
- classify persisted inconsistencies by authority, not byte well-formedness alone;
- compare equivalent failure contexts instead of demanding identical errors for
  rejected proposals and corrupt accepted state;
- remove obsolete helper/type assumptions and repeated result tables.

Comparisons with V4 are non-comparable for these obligations; affected deltas
are `N/A (method change)`. Retain named stable anchors where possible.

## Establish Current Authorities

Discover current owners before selecting proof. Inventory:

| Authority | Inspect |
| --- | --- |
| Internal runtime carrier | `crates/icydb-core/src/error/mod.rs`: classes, origins, details, constructors, relabeling |
| Query and planner conversion | `db/query/intent/errors/`, planner errors, cursor plan/decode errors |
| Compact public identity | `crates/icydb-diagnostic-code/src/{lib,registry}.rs`: leaf code, broad class, detail, origin |
| Context schemas | `crates/icydb-diagnostic-code/src/{fact,query_field}.rs`: allowed facts and field context |
| Public projection | `crates/icydb/src/error.rs`: core/query/bootstrap conversion and Candid record |
| Producer samples | persisted decode, store/index/identity, mutation, schema publication, recovery, admission |

Paths beginning with `db/` are relative to `crates/icydb-core/src/`.
Use current definitions instead of carrying forward historical type names.

At present the runtime has seven classes and eleven origins. Public diagnostics
also represent the `Query` class and `Runtime` origin. The public error is a
numeric code/class/origin record with bounded facts and optional query-field
context; convenience enums are not its wire layout. Re-enumerate these surfaces
from source on each run and record additions or removals.

## Required Obligations

### 1. Taxonomy And Projection

Trace internal class/origin/detail through diagnostic identity into the public
error. Check all current runtime classes and origins, all registered leaf codes,
and query validation/intent/plan/execute branches. Exhaustive source matches can
prove a mapping inventory; distinguish them from executed round-trip matrices.

Check that detail-derived codes do not accidentally override the intended
class, and that query execution retains runtime meaning. Record intentional
origin-sensitive codes, such as store corruption and cursor rejection.
Inspect public Candid shape and representative code, class, origin, and fact
round trips. Native serialization is not deployed endpoint evidence.

### 2. Origin And Context Fidelity

Trace canonical constructors and relabeling helpers in scope. Transparent
wrappers preserve origin; a documented boundary handoff may deliberately assign
a new origin. Recovery relabeling must preserve class and valid numeric facts
while dropping or rebuilding incompatible origin-scoped detail.

Verify leaf-code-specific fact schemas, ordering, bounds, and accepted identity
context. Invalid server-produced context must fail with a typed invariant error
without emitting partial facts. Distinguish this from handling untrusted decoded
client records; inspect their validation APIs and do not assume deserialization
validates every field automatically.

### 3. Classification By Trust Boundary

For each selected producer, state the authority and expected classification
before comparing the result:

| Failure context | Required distinction |
| --- | --- |
| Malformed persisted payload or inconsistent accepted row/index state | Corruption; well-formed bytes do not make accepted-state inconsistency a user error |
| Unsupported stored format identity/version | IncompatiblePersistedFormat at the owning decode/admission boundary; inspect framing rules rather than assuming every magic failure is Corruption |
| Rejected user query, cursor, schema proposal, or value policy | Query/Unsupported or the owner's typed policy error; no manufactured corruption |
| Missing requested row or conflicting mutation expectation | NotFound/Conflict where the operation contract requires it; ordinary absent lookup may succeed |
| Violated internal planner/executor contract | InvariantViolation; do not downgrade to user policy |
| Invalid generated diagnostic context | InvariantViolation at projection, with safe bounded output |

Include malformed cursor input versus internal cursor invariants, persisted
marker framing/version rejection, an accepted-index missing-row case, live
mutation expectations, and schema admission/publication samples in a baseline.

### 4. Cross-Path Meaning

Compare named live, replay, and alternate-frontend samples only after recording
whether their authority and failure context are equivalent. A new conflicting
proposal and a broken already-accepted durable effect need not share a class.
An intentional Recovery origin is not an origin-loss finding.

Trace at least one actual recovery failure to its public/bootstrap boundary and
one mutation/schema policy failure to its caller. Inspect typed assertions, not
error strings. Constructor tests alone do not prove producer routing; combine
them with a producer test and source propagation trace.

## Focused Proof Selection

Apply [executed-test evidence](../../README.md#executed-test-evidence).
Inspect assertions and feature gates, list every selected family with the exact
execution configuration, then execute. An `is_err()` assertion alone proves
rejection, not class/origin/detail preservation. Report missing proof explicitly.

| Obligation | Candidate proof owners |
| --- | --- |
| Runtime classes, origin mapping, cursor conversion, relabeling | Core `error/tests.rs`, exhaustive source matches |
| Leaf codes, public axes, fact schemas, query-field validation | Diagnostic-code `lib.rs`, `fact.rs`, `query_field.rs` tests |
| Public projection and Candid round trips | Facade `error/tests.rs` |
| Cursor producer errors | `db/cursor/tests/` plus core conversion matrix |
| Persisted marker class/origin and format rejection | `db/commit/store/tests.rs`; malformed corpus only for assertions it actually makes |
| Live/recovery and accepted-index failures | `db/session/write/`, `db/session/write.rs`, `db/session/tests/unit_ordering.rs` |
| Schema policy and publication | `db/schema/mutation/tests/`, core schema diagnostic tests |

Use focused unit selections: core and facade `--lib --features sql,migration`
when those boundaries are selected; diagnostic-code `--lib`. Narrow features
when the declared scope permits. Canister behavior needs separate named
integration proof only when claimed. Do not execute full workspace suites.
Static authority scripts are supplemental, not behavioral evidence.

## Report Contract

Include:

1. Metadata: definition/method, source and dirty-state identity, features,
   toolchain, baseline, comparability, and method changes.
2. Scope and current authority inventory, with exhaustive versus sampled coverage.
3. One boundary matrix: producer/context, expected class/origin/code/context,
   observed propagation, source evidence, and executed proof or gap.
4. Findings and verdict under shared governance; distinguish verification gaps
   from demonstrated misclassification.
5. Exact verification commands and selected/passed/failed/ignored counts, with
   listings separated from execution.
6. Follow-up and complexity delta; performance remains unmeasured unless
   separately authorized and measured with permitted metrics.

Do not duplicate adjacent audits' findings or create a debt ledger from the
report. If no actionable classification finding remains, state that explicitly.
