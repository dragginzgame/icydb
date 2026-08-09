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

Performance, Wasm, completeness, and domain-safety definitions retain their
current scope until they receive a separate cadence and overlap review. Do not
fold their materially different correctness or empirical questions into the
two structural audits merely to reduce the audit count.

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

For each audit run:

1. Use one audit definition or one explicitly bounded investigation scope.
2. Apply the no-build, state-space, and debt rules from
   `docs/governance/simplicity-and-maintainability.md`.
3. Keep the prompt and method fixed for the run.
4. Record findings with `LOW`, `MEDIUM`, or `HIGH` risk and an explicit
   disposition; do not create a composite score.
5. Write the result directly to its canonical `docs/reports/` run directory.
6. Never overwrite or delete a prior report or structured findings file.
7. Keep all machine-readable findings and generated artifacts beneath their
   owning run.
8. Do not create aliases, symlinks, compatibility directories, or duplicate
   copies at former report paths.
9. Limit structural reports to five active findings. Supporting observations
   remain evidence and do not become an implicit backlog.

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
