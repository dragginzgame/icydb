# Recurring Audit — State Machine And Transition Integrity

Method: `STATE-2` + `DOMAIN-1`.

Apply [Domain Scope And Change Triggers](../../README.md#domain-scope-and-change-triggers),
[Executed-Test Evidence](../../README.md#executed-test-evidence), and the shared
authorization contract. This audit asks whether a transition can be entered,
skipped, widened, or published out of order. It is a correctness audit, not
permission to refactor or implement findings.

## 0. Freeze Scope And Discover Current Owners

Record the requested baseline or trigger, code snapshot including dirty inputs,
selected families, owner symbols, and exclusions before evaluating transitions.
Follow producers, admission gates, consumers, durable authority and recovery;
a filename or historical test name is only a discovery hint. Freeze the method
for execution; subsequent corrections require a new run.

A broad baseline samples every family below. A change-triggered run covers the
affected families and adjacent gates necessary to prove their transitions.

| Family | Starting owner locations | Question |
| --- | --- | --- |
| Plan -> execution | `db/executor/planning/preparation.rs`, `db/executor/planning/route/planner/`, `db/executor/prepared_execution_plan/` | Does validated accepted-authority planning precede route assembly and execution? |
| Schema admission/publication | `db/schema/transition/`, `db/schema/mutation/ddl_admission.rs`, `db/schema/sql_ddl/`, `db/schema/application/` | Can unsupported drift or incomplete physical work become an accepted schema? |
| Save/update/delete | `db/session/write.rs`, `db/executor/mutation/commit_window.rs`, `db/commit/guard.rs` | Do validation and durable authority precede protected mutation? |
| Cursor admission | `db/session/query/dynamic.rs`, `db/cursor/token/`, `db/executor/planning/continuation/` | Can an invalid continuation reach row execution or alter its accepted contract? |
| Recovery/readiness | `db/commit/recovery.rs`, `db/startup/{observe,driver}.rs` | Can normal access or Ready bypass incomplete startup/marker work? |

Paths are relative to `crates/icydb-core/src/`. Recheck them and record moved
ownership in the report. Inventory current write frontends and direct schema
publication callers; do not assume every control operation uses the row
scheduler merely because it shares the marker protocol.

## 1. Model Actual States And Authorities

Produce one state/transition table: owner predicate or type, entry condition,
legal exits, durable authority, visibility/admission gate, and source evidence.
Model relevant facts such as accepted intent, validated/prepared plan, executing,
preflight complete, persisted marker/open commit window, partially/fully applied,
marker retired, startup recovery in progress, and Ready.

Separate mutually exclusive phases from overlapping facts. Executing work,
a retained marker and partially applied effects can coexist. “Recovered” is
not the opposite of “executing”; it is an admission fact bound to the current
runtime/storage identity. Read-only execution need not enter a commit window.

Distinguish these recovery cases explicitly:

- startup or interrupted-marker recovery: ordinary admission remains blocked
  through replay/fold/verification and readiness restoration;
- successful live commit with marker retired but committed journal tails:
  validated live visibility may remain usable while online convergence is
  pending; and
- fresh startup with no marker but nonempty tails: marker absence alone does
  not establish recovered readiness.

Use current owner predicates to establish the permitted combinations. Do not
require every journal tail to be empty during normal service or assume an
empty marker alone proves Ready. Generated-schema reconciliation and terminal
failure receipts may impose additional startup gates.

## 2. Verify Legal Handoffs And Illegal Entry

For each selected family, name the canonical decision owner, carried contract,
consumer, bypass candidates, and evidence that preconditions precede effects.

- **Plan:** trace validation, accepted authority, access lowering and staged
  route construction. Distinguish owner-authorized physical selection or
  fallback from an executor silently widening the accepted access contract.
  A private constructor is structural evidence only after its call sites are
  checked; a debug assertion alone is not production validation.
- **Schema:** trace catalog-native admission, staging, publication preflight,
  marker/journal work, accepted snapshot handoff and any application receipt.
  SQL and generated models are proposals, not alternate runtime authority.
  Sample both rejection without protected writes and a successful publication.
- **Mutation:** trace save/update/delete validation, uniqueness and relation
  checks, complete batch preflight, marker persistence, mechanical application
  and retirement. Identify direct control/schema paths separately and verify
  their shared durable protocol rather than demanding identical scheduling.
- **Cursor:** authenticate/decode and bind the current query, authority,
  order/window and route before row execution. Distinguish public token state
  from internal raw chunk anchors; do not impose a retired cursor shape.
- **Recovery:** distinguish observing readiness from driving recovery. Verify
  retained authority, replay/fold/verify progression, failure containment and
  normal admission only after the relevant gate completes.

Include illegal-entry evidence for unvalidated execution, mutation without
marker authority, normal access before startup recovery, premature schema
publication and invalid continuation. Unconstructible inputs may be proved by
current type visibility plus complete caller inspection; do not invent a test
that bypasses production encapsulation merely to demand runtime rejection.
A successful lifecycle test is not a negative-admission test.

## 3. Failure Cuts, Visibility And Logical Overlap

Use a compact matrix: cut/scenario, expected protected durable state, recovery
owner, visibility gate, inspected evidence, executed proof or gap, consequence.
At minimum cover the following applicable cuts:

1. Validation/preflight rejection before marker publication.
2. Marker persisted, no application yet.
3. Partial row/index or delete application.
4. Application completed before marker retirement.
5. Recovery verification failure and repeated recovery attempts.
6. Marker-free committed-tail convergence and restart.
7. Invalid cursor admission and continuation between mutation messages.

Before marker authority, rejection must leave protected durable state unchanged;
name that state rather than equating volatile cache work with database mutation.
After marker persistence, a normally returned error may retain partial work but
must retain durable authority, recovery wake-up and the required admission gate.
Successful retirement must not introduce a later fallible validation that loses
recovery ownership. Check clear-failure behavior as well as apply-failure behavior.

Do not use test-only rollback helpers as durable authority. Distinguish ordinary
returned errors, native test interruption, and IC traps/message rollback. Native
fault injection proves only its actual return path; it does not prove platform
rollback, generated timer delivery, or canister upgrade behavior.

For overlapping saves, save/delete, and cursor/mutation, identify the actual
message/transaction boundary, any await/re-entry point, marker exclusion,
predecessor checks and between-page consistency contract. Do not infer
multi-message atomicity or snapshot pagination from single-threaded execution.

## 4. Focused Evidence And Adjacent Audit Boundaries

Inspect assertions, resolve current feature/target selection, list matching
tests, then execute focused cases under the shared evidence contract. A broad
baseline needs sampled behavioral evidence for schema rejection/publication,
write preflight and interrupted apply, recovery/readiness admission, and cursor
rejection. Plan construction may use structural proof where invalid input is
unrepresentable. Inspect maintained negative assertions, not just test names.

For replay, require a sampled interrupted transition to reach its expected
accepted state and preserve the gate on verification failure. Deep execute vs
replay equivalence, all journal formats/corruption classes and exhaustive fault
matrices belong to [recovery consistency](../storage/storage-recovery-consistency.md).
Ordering/envelope algebra belongs to the [cursor](cursor-ordering.md) and
[range](../range/boundary-envelope-semantics.md) audits. Link exact reusable
evidence with matching source/configuration; another audit's PASS is not proof.
Do not demand those complete audits here or present this sample as their verdict.

Missing required evidence is a verification gap. Preserve blocked/failed
attempts and exact counts; never substitute zero-test success, source searches,
test-only scaffolding or an old test label for behavioral execution. Full
repository/workspace suites remain user-owned. Do not add anti-resurrection
tests for removed surfaces or broaden into unrelated feature work.

## 5. Report And Verdict

Write a new canonical `state-machine-integrity` run with:

1. Metadata, selection rationale, frozen scope, method changes and comparability.
2. State/transition model and incompatible-versus-permitted state pairs.
3. Authority/entrypoint map and legal/illegal handoff evidence.
4. Failure-cut and logical-overlap matrix, including visibility and wake-up.
5. Findings, unresolved proof, drift triggers and supported verdict.
6. Verification readout with exact commands and selected/passed/failed/ignored
   counts; label source-only or reused evidence and its actual coverage.

Apply [Findings And Verdicts](../../README.md#findings-and-verdicts).
Use `LOW`, `MEDIUM`, or `HIGH` findings with owner, present consequence,
disposition and action trigger. Do not introduce `PARTIAL` as a competing
verdict, numeric risk scores or a second debt ledger. A missing proof is not
by itself evidence of a runtime defect.

Run `01` compares with the latest prior comparable run; subsequent same-day
runs compare with run `01`. Record `N/A` when no comparable run exists and link
historical references separately. `STATE-2` distinguishes online convergence
from startup recovery, removes contradictory phase/exclusivity assumptions,
limits deep replay work to its owning audit, and replaces repeated inventories
with current transition evidence. Mark affected deltas `N/A (method change)`;
retain stable anchors such as marker-before-apply and gate-before-admission.
Historical reports are immutable.
