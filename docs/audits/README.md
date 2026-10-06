# IcyDB audit overlay

Apply [the shared audit contract](../../audits/README.md) at reviewed Shared
Tooling `d957d1f8801885c5b69e4a9ef900155f5f2a8a9d`, recorded in
[the snapshot](../../.shared-tooling.snapshot). This overlay owns IcyDB's product
obligations, audit selection and report destinations. Shared methods own generic
inspection, evidence, findings and authorized cleanup; they add no schedule or
automatic full gate. Reports stay in `docs/reports/`.

[Code hygiene](../../audits/code-hygiene.md) uses
[the local style/architecture rules](../governance/code-hygiene/README.md) and
[architecture contracts](architecture-contracts.md). Existing domain audits
retain their distinct correctness and measurement questions. The four structural
and cleanup overlays below carry new `ICYDB-*` identities; their
[prior definitions](archive/shared-adoption/README.md) are frozen historical
references. Earlier reports and artifacts are unchanged and affected comparisons
are `N/A (method change)`.

## Audit Definitions

### Recurring audits

Recurring audits are stable, repeatable definitions that enforce architectural
contracts on a schedule or a documented change trigger.

Location:

- `docs/audits/recurring/<domain>/<focus>.md`

Domains currently include `access`, `contracts`, `crosscutting`, `executor`,
`range`, `security`, and `storage`.

The active crosscutting structural audits are exactly:

- `crosscutting-flow-convergence-and-duplication`: canonical ownership,
  equivalent-flow convergence, policy rediscovery, and justified protective or
  measured specialization; and
- `crosscutting-complexity-and-technical-debt`: state-space growth, ownership
  spread, extension friction, and evidenced current debt.

These two definitions replace the former separate canonical-authority,
complexity-accretion, DRY, flow-convergence, layer-violation,
module-structure, and velocity-preservation methods. Those superseded methods
are historical references under `docs/audits/archive/structural/` and are not
eligible for new runs.

The broad invariant-preservation sweep is also retired. Its definition is
preserved under [archive/integrity](archive/integrity/invariant-preservation.md)
and is ineligible for new recurring runs. Domain audits own its correctness
checks; the targeted [boundary handoff review](targeted/integrity/boundary-handoff-review.md)
preserves the check for guarantees lost between owners. Historical reports
remain unchanged.

Domain-safety audits follow the change triggers and scope contract below.
Performance, Wasm, and completeness retain their own scope and run conditions.
Do not fold their materially different correctness or empirical questions into
the two structural audits merely to reduce the audit count.

### Targeted playbooks

Targeted playbooks are reusable procedures for a bounded investigation or
cleanup slice that is not part of the recurring baseline.

Location:

- `docs/audits/targeted/<area>/<focus>.md`

Use the [boundary handoff review](targeted/integrity/boundary-handoff-review.md)
when a concrete producer/consumer change leaves invariant coverage unclear.
It is not an additional baseline sweep or a prerequisite for every domain run.

One-time release or investigation prompts belong with their owning design or
issue context. Their executed results still use the report hierarchy below.

## Report Locations

Reports are immutable outputs, classified by lifecycle:

- recurring run:
  `docs/reports/recurring/YYYY/MM/DD/<scope>/<run>/report.md`
- release closeout:
  `docs/reports/releases/<version>/closeout/YYYY-MM-DD/<run>/report.md`
- one-time investigation:
  `docs/reports/investigations/YYYY/MM/DD/<scope>/<run>/report.md`

`<run>` is a two-digit sequence beginning at `01`. Machine-readable findings
use `findings.json` beside the report. Supporting output belongs in the same
run's `artifacts/` directory.

See `docs/reports/README.md` for the report ownership and history contract.

## Naming

- recurring definition: `<focus>.md`
- targeted playbook: `<focus>.md`
- report scope: stable lowercase kebab-case
- run directory: `01`, `02`, ...
- human-readable result: `report.md`
- structured findings: `findings.json`

The path carries date, scope, and run identity, so report filenames must not
repeat those facts.

## Execution Discipline

### Domain Scope And Change Triggers



For domain-safety audits, default to the affected owners at minor-line closeout
or when a change touches a boundary below. A recurring label does not require
a weekly whole-system sweep. Run a broad domain baseline only when explicitly
requested; a trigger selects relevant coverage, not extra implementation authority.

| Audit | Change trigger | Distinct correctness question |
| --- | --- | --- |
| [Index integrity](recurring/access/access-index-integrity.md) | Index encoding, membership, uniqueness, row coupling, or catalog index publication | Do accepted index contracts preserve correct entries and row/index agreement? |
| [Error taxonomy](recurring/contracts/error-taxonomy.md) | Error construction, classification, wrapping, or public mapping | Are class, origin, and detail preserved through the boundary? |
| [Resource model](recurring/contracts/resource-model-compliance.md) | Budget admission, accounting, route selection, or boundedness policy | Are admitted operations bounded and exhaustion fail-closed? |
| [Cursor ordering](recurring/executor/cursor-ordering.md) | Tokens, signatures, anchors, ordering, or between-page state | Does continuation remain bound to the accepted query and paginate safely? |
| [State transitions](recurring/executor/executor-state-machine-integrity.md) | Plan handoff, mutation/publication lifecycle, or recovery admission | Can a transition bypass its owner or expose incomplete state? |
| [Range envelopes](recurring/range/boundary-envelope-semantics.md) | Bound encoding, tightening, comparison, or resume substitution | Are strictness, direction, and containment preserved? |
| [Security boundary](recurring/security/security-audit.md) | Untrusted input, persisted decode, namespace/cache identity, or admission policy | Can malformed or mismatched input cross a protected boundary or fail open? |
| [Recovery consistency](recurring/storage/storage-recovery-consistency.md) | Marker/journal protocol, live apply, replay, or startup publication | Does recovery converge to the same accepted state after every relevant interruption? |

Before analysis, record the trigger or requested baseline, affected behaviors,
owners, selected obligations, and excluded families with reasons. Scope follows
the contract through its producers, consumers, trust boundaries, and recovery
paths; it is not limited to changed lines. For example, a shared key codec
change can require index, range, cursor, and recovery proof even if only one
file changed. Uncertain reachability requires inspection, not automatic exclusion.

Within these definitions, required inventories, scenarios, verification
families, and output sections apply to that declared scope. Keep every applicable
obligation; summarize excluded sections once instead of producing empty tables.
An unavailable required proof is a verification gap, not an exclusion. Reuse
valid evidence as described below, or run focused verification within the user's
authorization. Out-of-scope behavior receives no `PASS`, and a scoped verdict
must not be presented as a whole-system verdict.

Record `DOMAIN-1` alongside the audit-local method tag to identify this scope
contract. On first adoption, describe the coverage change and mark affected
deltas `N/A (method change)`. Historical broad reports remain unchanged; only
explicitly equivalent obligations and evidence can remain comparable.

### Finding Ownership And Shared Evidence

The [common ownership/evidence contract](../../audits/README.md#ownership-and-history)
owns one finding per cause and source-bound evidence reuse. IcyDB routes domain
findings by their violated contract:

Use these ownership splits when coverage overlaps:

| Shared boundary | Owning questions and evidence reuse |
| --- | --- |
| Index / recovery | Index integrity owns accepted membership, uniqueness, and row agreement; recovery consistency owns interruption, replay, and convergence. Share the relevant mutation fixtures and assertions. |
| Cursor / range | Cursor ordering owns token binding and pagination; range envelopes owns physical bound lowering, direction, and containment. Share anchor and traversal proof where the obligations coincide. |
| Security / resource | Security owns adversarial reachability and fail-open exposure; resource compliance owns admission, accounting, and exhaustion. Share rejection evidence while checking the distinct threat and budget contexts. |
| State transitions / recovery | State transitions owns legal entry, publication, and readiness gates; recovery consistency owns replay equivalence and idempotence. Share interruption evidence without repeating the full recovery matrix. |
| Error taxonomy / domain audits | Error taxonomy owns class, origin, diagnostic context, and public projection; the domain audit owns the behavior producing that error. Share typed producer assertions. |

The targeted handoff review investigates the remaining boundary proof gap;
independent trust, corruption and recovery checks keep their owners. Reused tests
retain their original source, features, lockfile, configuration and runtime
identity and are never counted as new executions.

### Authorization And Read-Only Work

Apply [the shared authority contract](../../audits/README.md#authority-and-execution)
and [AGENTS.md](../../AGENTS.md). Audit/report instructions do not supply repair,
service mutation, dependency update, broad gate or publication authority. Existing
bounded implementation authority remains valid. Inspection-only work cannot
mutate a running service. Requested local validation may use the network
lifecycle permission only when necessary; report any lifecycle action.

### Findings And Verdicts

Apply [shared severity, verdict and evidence rules](../../audits/README.md#evidence-and-report-contract).
Do not convert historic scores into severities or use counts as cleanup targets.
Keep product/domain scopes explicit and name missing proof without treating it
as evidence of a runtime defect. GitHub issues are the only follow-up tracker.

### Daily baseline rule

For a recurring scope on a given day:

- run `01` is the canonical daily baseline;
- runs `02`, `03`, and later compare against run `01`, not the preceding rerun;
- run `01` compares against the latest prior comparable run for that scope, or
  records `N/A` if no comparable run exists.

For crosscutting structural runs, include hub import pressure only when it is
relevant to a finding:

- top imports for each hub module;
- unique sibling-subsystem import count;
- cross-layer dependency count;
- delta against the previous comparable report.

### Crosscutting run order

When a run includes crosscutting recurring audits, use this order:

1. `crosscutting-flow-convergence-and-duplication`
2. `crosscutting-complexity-and-technical-debt`
3. `crosscutting-completeness`, when the public contract is in scope
4. `crosscutting-perf-audit`, when instruction cost is in scope
5. `crosscutting-wasm-footprint`, when Wasm footprint is in scope

Summary reports must retain the same relative order for the scopes present.
Do not restate complete finding tables in a summary; link the owning report and
record only the combined verdict and cross-report dependencies.

## Required Report Preamble

Use [the shared identity contract](../../audits/README.md#evidence-and-report-contract)
with the selected local overlay/method identity, baseline and comparability.
Record source and relevant dirty work, trigger, affected owners and excluded
families. A method/scope change makes affected comparisons `N/A (method change)`;
name equivalent owner or measured anchors separately.

## Verification Readout

Use `PASS`, `FAIL` or `BLOCKED` for each selected check and identify missing proof.
Source inspection is distinct from behavioral execution. Full repository,
workspace and release gates remain user-owned. Use the Cargo home and target
selected by Make, locked dependency inputs and explicit package/target/features.
Raw Wasm bytes, IC cycles and instruction counts are the only performance
metrics. Missing permitted measurements remain unmeasured.

### Executed-Test Evidence

This contract applies to every audit definition and targeted playbook. A
successful process exit, source search, compiled binary, or test listing alone
is not passing behavioral evidence.

Before running a selected proof:

1. Map the required behavior to its current owner and inspect the assertions in
   the proposed tests. Source paths and names are discovery aids, not proof.
2. Check the owning package's current Cargo features and test target. Select
   `--lib` for unit tests or the specific `--test` target for integration tests;
   include required features explicitly. Do not broaden to a workspace suite.
   Use the repository's Cargo environment so listing and execution share the
   intended toolchain and build inputs.
3. List matching tests with the same package, target, features, filter, and
   ignored-test selection that execution will use. For example, a focused core
   unit selection uses `cargo test --locked -p icydb-core --lib --features sql
   <verified-filter> -- --list`, followed by the same selection without
   `--list`. Add `--exact` after `--` in both invocations when selecting one
   fully qualified test name.
4. Require at least one selected executable test and identify every mandatory
   case in a family. Ignored tests do not count unless explicitly executed;
   one unrelated passing test cannot satisfy a missing required case.

Record the exact command, source snapshot, proof obligation, selected tests
or bounded family, and passed/failed/ignored counts in the run's verification
readout. `PASS` requires all required cases to execute and pass. A command
that exits successfully with zero executed tests is `FAIL`; so are invalid
features, missing targets, and selectors that no longer name the required proof.
An environment or authorization restriction is `BLOCKED`, with its reason.
Separate source-inspection conclusions from behavioral checks that did not run.

Resolve a stale selection before execution when possible. If it is discovered
during a run, preserve the failed attempt and record the replacement and why
its assertions cover the same obligation. Do not silently drop the obligation,
count the replacement as equivalent based on its name, or retry an expensive
failure without new evidence. Definition edits need authorization; an audit
finding does not grant it. Historical reports remain immutable.

When the selected proof changes the method or coverage, follow the method-change
and comparability rules above. Reuse this contract rather than copying its
status and counting rules into each definition.

## History Preservation

Use [report ownership](../reports/README.md) for the existing recurring,
release-closeout and investigation hierarchy. Never overwrite reports or their
structured findings. New evidence receives a new run; historical definitions
are ineligible for new runs and keep their original source identity.

## Sources of Truth

- [Shared methods](../../audits/README.md): common procedure and evidence contract.
- [Architecture contracts](architecture-contracts.md): IcyDB invariants.
- `docs/audits/recurring/` and `docs/audits/targeted/`: local overlays/domain methods.
- [Reports](../reports/README.md): immutable evidence and local output layout.
