# IcyDB Audit How-To

This document defines how to run and store architecture audits under `docs/audits/`.

## 1. Audit Types

### Recurring audits
Recurring audits are stable, repeatable audit definitions that run on a schedule and enforce architectural contracts.

Location:
- `docs/audits/recurring/<domain>/<focus>.md`

### Oneoff audits
Oneoff audits are release-scoped or incident-scoped investigations that are not part of the recurring baseline.

Location:
- `docs/audits/oneoff/<version>-<topic>.md`

### Audit reports
Reports are historical outputs from audit runs.

Location:
- `docs/audits/reports/YYYY-MM/YYYY-MM-DD/<scope>.md`
- Reports must always be grouped by month, then day directory.

## 2. Domain Structure

Recurring audits are organized by subsystem domain:
- `planner/`
- `range/`
- `executor/`
- `access/`
- `storage/`
- `integrity/`
- `contracts/`
- `crosscutting/` for multi-subsystem audits

## 3. Naming Conventions

Use these file patterns:
- Recurring definitions: `<focus>.md` (domain context is the parent directory)
- Oneoff definitions: `<version>-<topic>.md`
- Reports (inside day directory): `<scope>.md`
- Required report directory: `docs/audits/reports/YYYY-MM/YYYY-MM-DD/`
- Report artifacts for a run: `docs/audits/reports/YYYY-MM/YYYY-MM-DD/artifacts/<scope>/<artifact>`

## 4. Audit Execution Discipline

For each audit run:
1. Use one audit definition per run.
2. Keep prompt scope fixed for the run.
3. Record findings with structured risk levels.
4. Save output as a new report file under `docs/audits/reports/YYYY-MM/YYYY-MM-DD/`.
5. Never overwrite prior run artifacts.
6. Keep day-level report roots clean: only report files (`*.md`) and `artifacts/` at `YYYY-MM/YYYY-MM-DD`, and write run artifacts under `YYYY-MM/YYYY-MM-DD/artifacts/<scope>/`.
7. Scope artifact directories by audit: `<scope>` must match the audit report scope (for example `wasm-footprint`) so artifacts are split by owning audit.

### Daily baseline rule (mandatory)

For each day directory (`YYYY-MM/YYYY-MM-DD`) and each audit scope:
- the first run file (`<scope>.md`) is the canonical baseline for that day
- same-day reruns (`<scope>-2.md`, `<scope>-3.md`, ...) must compare against
  `docs/audits/reports/YYYY-MM/YYYY-MM-DD/<scope>.md`
- reruns must not chain deltas against the previous rerun file

Baseline selection guidance:
- first run of day:
  - compare against the latest prior comparable report for the same scope, or
    `N/A` if no prior comparable report exists
- same-day rerun:
  - compare against that day's canonical baseline file for the same scope

For crosscutting structure/velocity runs, include the required Hub Import Pressure metric:
- top imports for each hub module
- unique sibling subsystem import count
- cross-layer dependency count
- delta vs previous report

### Crosscutting run order + summary integration (mandatory)

When a run includes crosscutting recurring audits, use this canonical order:

1. `crosscutting/crosscutting-complexity-accretion`
2. `crosscutting/crosscutting-canonical-semantic-authority`
3. `crosscutting/crosscutting-dry-consolidation`
4. `crosscutting/crosscutting-layer-violation`
5. `crosscutting/crosscutting-module-structure`
6. `crosscutting/crosscutting-velocity-preservation`
7. `crosscutting/crosscutting-wasm-footprint`

Rules:
- In day-level `summary.md`, `Audit Run Order and Results` must list
  crosscutting entries in the same relative order when present.
- For crosscutting-only reruns, preserve the same relative order for the subset
  being rerun.
- Include canonical semantic authority whenever the rerun scope includes
  semantic ownership/representation drift checks across schema/planner/runtime/
  EXPLAIN/replay surfaces.

### Required report preamble (every report)

Each report must include a short preamble block with:
- scope
- compared baseline report path
  - first run of day: latest prior comparable scope report path (or `N/A`)
  - same-day rerun: that day's canonical scope baseline path (`<scope>.md`)
- code snapshot identifier (for example `git rev-parse --short HEAD`, or `N/A`)
- method tag/version (for example `Method V3`)
- comparability status:
  - `comparable` (all tracked metrics use the same method), or
  - `non-comparable` (method changed, with one-line reason)

### Method-drift rule

If a metric formula, counting scope, or classification model changes:
1. bump the method tag in that report,
2. add a `Method Changes` section,
3. mark affected deltas as `N/A (method change)` instead of numeric deltas,
4. keep at least one unchanged anchor metric for continuity where possible.

### Verification readout discipline

Every report must include a `Verification Readout` section with command outcomes.

Allowed statuses:
- `PASS`
- `FAIL`
- `BLOCKED`

For `BLOCKED`, include a concrete reason.
If blocked by cross-filesystem execution errors (for example `Invalid cross-device link (os error 18)`), record it once and do not retry in the same run.

### Actionability discipline

If any finding is `PARTIAL`/`FAIL`, or if overall risk index is `>= 6`, include explicit follow-up actions with:
- owner boundary
- action
- target report date/run

For DRY/consolidation reports, if `High-risk divergence-prone patterns > 0`,
follow-up actions are mandatory even when the overall DRY risk index is below
`6`.

If no follow-up is required, state that explicitly.

## 5. History Preservation Rule

Audit history is append-only.

Required:
- No audit definition or report artifact may be deleted.
- Existing historical reports must remain accessible.
- If a relocation or rename collides with an existing filename, preserve the older artifact as `*_legacy.md`.

## 6. Required Governance Files

- `docs/audits/AUDIT-HOWTO.md`: operational process.
- `docs/audits/META-AUDIT.md`: architecture contract and dependency invariants.

## 7. Internal Linking Rule

Use normalized paths only:
- `docs/audits/recurring/...`
- `docs/audits/oneoff/...`
- `docs/audits/reports/...`

Do not reference deprecated locations.
