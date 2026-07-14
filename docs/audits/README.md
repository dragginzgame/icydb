# IcyDB Audit Governance

This directory contains audit definitions and reusable audit playbooks. Executed
results do not belong here; all report output is stored under `docs/reports/`.

## Audit Definitions

### Recurring audits

Recurring audits are stable, repeatable definitions that enforce architectural
contracts on a schedule.

Location:

- `docs/audits/recurring/<domain>/<focus>.md`

Domains currently include `access`, `contracts`, `crosscutting`, `executor`,
`integrity`, `range`, `security`, and `storage`.

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
2. Keep the prompt and method fixed for the run.
3. Record findings with structured risk levels.
4. Write the result directly to its canonical `docs/reports/` run directory.
5. Never overwrite or delete a prior report or structured findings file.
6. Keep all machine-readable findings and generated artifacts beneath their
   owning run.
7. Do not create aliases, symlinks, compatibility directories, or duplicate
   copies at former report paths.

### Daily baseline rule

For a recurring scope on a given day:

- run `01` is the canonical daily baseline;
- runs `02`, `03`, and later compare against run `01`, not the preceding rerun;
- run `01` compares against the latest prior comparable run for that scope, or
  records `N/A` if no comparable run exists.

For crosscutting structure and velocity runs, include hub import pressure when
it is relevant:

- top imports for each hub module;
- unique sibling-subsystem import count;
- cross-layer dependency count;
- delta against the previous comparable report.

### Crosscutting run order

When a run includes crosscutting recurring audits, use this order:

1. `crosscutting-complexity-accretion`
2. `crosscutting-canonical-semantic-authority`
3. `crosscutting-dry-consolidation`
4. `crosscutting-layer-violation`
5. `crosscutting-module-structure`
6. `crosscutting-velocity-preservation`
7. `crosscutting-wasm-footprint`

Summary reports must retain the same relative order for the scopes present.
Include canonical semantic authority whenever a run evaluates semantic
ownership or representation drift across schema, build, frontends, planner,
runtime, diagnostics, or replay.

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

If a finding is `PARTIAL` or `FAIL`, or overall risk is at least `6`, include:

- owner boundary;
- concrete action;
- target report run or release slice.

DRY/consolidation reports also require follow-up whenever a high-risk,
divergence-prone pattern remains, regardless of aggregate score.

If no follow-up is required, state that explicitly.

## History Preservation

Reports and structured findings are append-only evidence:

- do not delete or overwrite prior reports or structured findings;
- relocation may simplify paths but must preserve content and ownership;
- a naming collision receives a new run number, never a compatibility suffix;
- execution-time paths quoted inside historical reports remain evidence of the
  original run and do not define a current repository location.

Generated artifacts are retained only while they provide a live baseline,
unique non-reproducible evidence, or detail not captured by the owning report.
Raw searches, duplicate formats, derived tables already summarized in the
report, and superseded comparison baselines should be deleted.

## Sources of Truth

- `docs/audits/README.md`: execution and storage policy
- `docs/audits/architecture-contracts.md`: architectural invariants enforced
- `docs/audits/recurring/`: recurring audit definitions
- `docs/audits/targeted/`: targeted reusable playbooks
- `docs/reports/README.md`: report ownership and history layout
