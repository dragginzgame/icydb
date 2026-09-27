# Targeted Review — Boundary Handoffs

Method: `HANDOFF-1`.

Use this playbook when a concrete change or finding leaves unclear whether a
producer's guarantee still holds at a consumer. Typical triggers are a changed
invariant owner, a new caller, a persisted or prepared artifact crossing a
boundary, or a change to accepted-schema identity or publication lifetime.
Select the named handoff and its reachable consumers; do not enumerate the
whole system or repeat the recurring domain audits.

Apply [audit governance](../../README.md), including authorization, finding
ownership, shared evidence, and executed-test evidence. Inspection returns
findings in the conversation. An authorized saved investigation uses
`docs/reports/investigations/YYYY/MM/DD/boundary-handoff-review/<run>/report.md`.
This playbook is not a recurring run and grants no implementation authority.

## 1. Establish The Gap

Record the trigger, source snapshot including relevant dirty changes, named
producer and consumer, invariant, and suspected loss of guarantee. Consult the
[domain ownership map](../../README.md#finding-ownership-and-shared-evidence)
and the relevant domain definition before collecting new proof.

If an owning audit already proves this exact handoff on compatible inputs,
cite that evidence and stop. A neighboring PASS alone is insufficient. If the
question is entirely a known domain obligation, route it to that audit rather
than opening a second finding or repeating its baseline. Keep a targeted
finding here only for a demonstrated handoff gap without an existing owner.

## 2. Trace The Carried Guarantee

Use one table for each selected invariant:

| Invariant / authority | Producer and enforcement | Carried artifact / validity | Consumer and effect | Bypass or stale-input candidate | Proof or gap |
| --- | --- | --- | --- | --- | --- |

Follow the actual call sites through the first protected read, write, or
publication. Check the following only where they apply to that handoff:

- Accepted schema snapshots own runtime row, index, constraint, and identity
  meaning. SQL and generated models supply proposals, reconciliation,
  model-only convenience, or tests; consumers must not reconstruct runtime
  authority from them.
- The carried contract preserves its entity/store/index identity, accepted
  revision or fingerprint, and validity lifetime. Trace which owner selects
  the accepted context and whether a cache, continuation, or delayed consumer
  can reuse a stale or differently bound artifact.
- Consumer assumptions follow from producer guarantees before protected
  effects. Inspect alternate callers and conversions that can erase proof.
  For row decoding, identify who proves the expected key matches the accepted
  row identity; for publication, identify who proves candidate completeness.
- Recovery, continuation, and alternate frontends need checks when they can
  enter this handoff or change its assumptions. Do not require every invariant
  to participate in every path; explain reachability and exclusions.

One semantic authority may require checks at several trust boundaries.
Distinguish policy rediscovery from protective revalidation after decoding,
authority changes, or delayed consumption. Do not demand enforcement exactly
once or remove guards because another layer also validates the invariant.
Use the owning range audit for directional bounds and the actual comparator;
do not assume ASC-only resume or serialized-byte ordering.

## 3. Prove Or Report The Gap

Reuse matching evidence first. Otherwise select focused proof of the missing
guarantee: valid input retains its contract and invalid, mismatched, or stale
input cannot reach the protected effect. Inspect assertions, features, targets,
and test listings under the shared executed-test contract before running tests.
Do not prescribe fixed historical selectors or run a workspace suite.

Type visibility and complete caller inspection may establish an unconstructible
invalid state. Label this source evidence; a debug assertion alone is not a
production guard. Missing required behavioral proof is a verification gap,
not a demonstrated runtime defect. Keep platform rollback or deployed behavior
outside the verdict unless the evidence actually exercises it.

## 4. Return A Bounded Result

Report the scope, owner, handoff table, evidence identity, and unresolved gap.
Saved reports also include the method, baseline or N/A, comparability, and exact
verification commands with selected/passed/failed/ignored counts. Historical
invariant-preservation runs are context, not comparable HANDOFF-1 baselines.

Apply shared severities and verdicts. Link one owning finding per cause, with
consequence, disposition, and action trigger. If coverage is sufficient, say
so and stop; do not create a backlog, a recurring sweep, or a runtime fix.
