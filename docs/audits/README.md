# IcyDB Audit Governance

This directory contains audit definitions and reusable audit playbooks. Executed
results do not belong here; all report output is stored under `docs/reports/`.

## Audit Definitions

### Recurring audits

Recurring audits are stable, repeatable definitions that enforce architectural
contracts on a schedule or a documented change trigger.

Location:

- `docs/audits/recurring/<domain>/<focus>.md`

Domains currently include `access`, `contracts`, `crosscutting`, `executor`,
`integrity`, `range`, `security`, and `storage`.

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

Domain-safety audits follow the change triggers and scope contract below.
Performance, Wasm, and completeness retain their own scope and run conditions.
Do not fold their materially different correctness or empirical questions into
the two structural audits merely to reduce the audit count.

### Targeted playbooks

Targeted playbooks are reusable procedures for a bounded investigation or
cleanup slice that is not part of the recurring baseline.

Location:

- `docs/audits/targeted/<area>/<focus>.md`

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
| [Invariant preservation](recurring/integrity/invariant-preservation.md) | Invariant definition, enforcement ownership, or consumer/replay handoff | Does each affected invariant survive every relevant boundary? |
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

Choose the owning audit by the violated contract, not the number of directories
it crosses. A recovery or cursor defect remains a domain finding even when it
crosses several layers. Structural crosscutting audits own duplicated flow,
semantic ownership, state-space, and maintenance-friction findings.

Keep one owning finding per underlying cause; adjacent reports link it and
explain the consequence for their own scoped verdict. Do not copy finding tables
or create parallel debt entries. Distinct defects still need distinct findings.

Collect a shared owner map or test result once and link its exact report section
or artifact. Reuse requires the same relevant source snapshot (including dirty
changes) and compatible proof method. Behavioral evidence also requires matching
test selection, features, toolchain, configuration, and runtime conditions.
Inspect the assertions and state which obligation the evidence actually proves; a neighboring audit's
`PASS` alone is not evidence. If identity or applicability cannot be established,
collect fresh focused proof or record the gap. Reused tests are attributed to
their original execution, never counted as newly executed tests. Preserve the
original artifact and report; put any new output under its own authorized run.

### Authorization And Read-Only Work

The user's request and existing session authorization determine which actions
are in scope. Apply this contract to every recurring audit and targeted
playbook; report paths and verification checklists do not grant extra authority.

- A request to inspect, review, or give feedback is inspection-only. Return
  findings in the conversation unless saving a report is also requested.
- A request to run an audit authorizes its new report and necessary focused
  verification outputs, unless the user restricts writes. It does not authorize
  fixing findings, changing the audit definition, updating designs or debt
  ledgers, or modifying release metadata.
- An explicit read-only constraint means no repository writes, including
  reports, generated files, build outputs, or automatic formatting. Use source
  inspection and existing evidence. A specific request to save a report or run
  a test permits only the outputs necessary for that requested action; it does
  not grant general editing authority.
- A request to implement a finding or improve the audit authorizes the bounded
  change and its direct validation. Honor approval already given in the session;
  do not ask again merely because a playbook normally starts with inspection.

For inspection-only work, do not mutate services or start, stop, reset, or
reconfigure networks. A running service does not make its mutation endpoints
read-only. For requested validation or measurement, use the existing local
network permissions in `AGENTS.md` only as needed, and report lifecycle actions.
Do not deploy to or mutate unrelated application environments.

When a required check cannot run within the authorized scope, record `BLOCKED`
and the reason, continue independent inspection, and limit the verdict to the
available evidence. Do not run a writer merely because it skips a build or
describe source inspection as an executed behavioral check.

When report writing is authorized, reserve a new canonical run directory and
assemble that run's output there. Never overwrite a prior report, findings file,
or its evidence to record a correction or rerun. New evidence receives a new
run and links to the earlier result. A stale definition is a finding unless
updating definitions is already part of the authorized task.

For each audit run:

1. Use one audit definition or one explicitly bounded investigation scope.
2. Apply the no-build, state-space, and debt rules from
   `docs/governance/simplicity-and-maintainability.md`.
3. Keep the prompt and method fixed for the run.
4. Record findings with `LOW`, `MEDIUM`, or `HIGH` risk and an explicit
   disposition; do not create a composite score.
5. When report writing is authorized, write directly to a new canonical
   `docs/reports/` run directory; otherwise return findings in the conversation.
6. Never overwrite or delete a prior report or structured findings file.
7. Keep all machine-readable findings and generated artifacts beneath their
   owning run.
8. Do not create aliases, symlinks, compatibility directories, or duplicate
   copies at former report paths.
9. Limit structural reports to five active findings. Supporting observations
   remain evidence and do not become an implicit backlog.

### Findings And Verdicts

All recurring audits and targeted playbooks use individual findings with
`LOW`, `MEDIUM`, or `HIGH` severity. Justify severity with the current
consequence and affected boundary:

- `HIGH`: evidence of a serious correctness, integrity, security, availability,
  or authority failure requiring prompt attention.
- `MEDIUM`: a concrete bounded defect, verification gap, or maintenance burden
  requiring an owner decision or a named action trigger.
- `LOW`: limited present impact with a proportionate correction or explicit
  no-action disposition.

Missing evidence is a verification gap, not proof of a runtime defect. Explain
what is unknown and its consequence; do not infer severity from file size,
finding count, or the absence of an out-of-scope feature.

Use these overall verdicts when a method requests a verdict:

- `PASS`: the scoped requirements have sufficient evidence and no actionable
  findings remain.
- `PASS WITH FINDINGS`: the scoped requirements are supported, with explicit
  non-blocking findings or accepted debt.
- `FAIL`: a demonstrated scoped contract violation or unresolved required
  verification failure remains.
- `BLOCKED`: required evidence is unavailable and prevents a supported verdict;
  identify the missing evidence. A known violation must still be reported.

Do not compute overall risk indices, numerical maturity ratings, weighted
completeness averages, or score-based action thresholds. Preserve feature and
stage labels, individual findings, and measured quantities such as bytes,
instructions, or executed-test counts. Those quantities do not become a
composite health score. Follow-up depends on the finding's consequence,
disposition, or unresolved verification obligation.

For a method moving to this contract, record the method change and mark old
score comparisons `N/A (method change)`. Compare only explicitly retained
evidence, feature states, or measured anchors. Do not rewrite historical
reports or convert their scores into new severity labels.

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

Every report must record:

- audit definition or investigation scope;
- compared baseline report path, or `N/A`;
- code snapshot identifier;
- method tag/version;
- comparability status:
  - `comparable`, or
  - `non-comparable` with a concise reason.

If a metric formula, counting scope, or classification model changes:

1. bump the method tag;
2. add a `Method Changes` section;
3. mark affected deltas `N/A (method change)`;
4. retain at least one unchanged anchor metric where practical.

## Verification Readout

Every report must include command outcomes using only:

- `PASS`
- `FAIL`
- `BLOCKED`

For `BLOCKED`, record the concrete reason once and do not repeatedly run an
expensive command that is blocked by the same environment condition.

Full repository and workspace test suites remain user-owned under `AGENTS.md`.
Audit agents run only the focused validation appropriate to their scope.

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

## Actionability

Every `MEDIUM` or `HIGH` finding must include:

- owner boundary;
- concrete present friction;
- disposition or reason for accepted retention; and
- action trigger when it is not being fixed now.

Audit-local finding or issue inventories are immutable evidence. They do not
become the active technical-debt ledger automatically.

If no follow-up is required, state that explicitly.

## History Preservation

Reports and structured findings are append-only evidence:

- do not delete or overwrite prior reports or structured findings;
- relocation may simplify paths but must preserve content and ownership;
- a naming collision receives a new run number, never a compatibility suffix;
- execution-time paths quoted inside historical reports remain evidence of the
  original run and do not define a current repository location.

Superseded audit definitions may move to `docs/audits/archive/` without path
aliases. Historical reports retain the code snapshot and execution-time
definition path needed to interpret their result. A new method compares with
an old report only through explicitly named stable anchor evidence and otherwise
records `non-comparable (method change)`.

Generated artifacts are retained only while they provide a live baseline,
unique non-reproducible evidence, or detail not captured by the owning report.
Raw searches, duplicate formats, derived tables already summarized in the
report, and superseded comparison baselines should be deleted.

## Sources of Truth

- `docs/audits/README.md`: execution and storage policy
- `docs/audits/architecture-contracts.md`: architectural invariants enforced
- `docs/audits/recurring/`: recurring audit definitions
- `docs/audits/archive/`: inactive historical audit methods
- `docs/audits/targeted/`: targeted reusable playbooks
- `docs/reports/README.md`: report ownership and history layout
