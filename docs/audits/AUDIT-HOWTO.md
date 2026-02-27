# IcyDB Audit How-To

This document defines how to run and store architecture audits under `docs/audits/`.

## 1. Audit Types

### Recurring audits
Recurring audits are stable, repeatable audit definitions that run on a schedule and enforce architectural contracts.

Location:
- `docs/audits/recurring/<domain>/<domain>-<focus>.md`

### Oneoff audits
Oneoff audits are release-scoped or incident-scoped investigations that are not part of the recurring baseline.

Location:
- `docs/audits/oneoff/<version>-<topic>.md`

### Audit reports
Reports are historical outputs from audit runs.

Location:
- `docs/audits/reports/YYYY-MM-DD/<scope>.md`
- Reports must always be grouped by day directory.

## 2. Domain Structure

Recurring audits are organized by subsystem domain:
- `planner/`
- `executor/`
- `cursor/`
- `access/`
- `storage/`
- `invariants/`
- `contracts/`
- `crosscutting/` for multi-subsystem audits

Domain descriptors are maintained in:
- `docs/audits/domains/`

## 3. Naming Conventions

Use these file patterns:
- Recurring definitions: `<domain>-<focus>.md`
- Oneoff definitions: `<version>-<topic>.md`
- Reports (inside day directory): `<scope>.md`
- Required report directory: `docs/audits/reports/YYYY-MM-DD/`

## 4. Audit Execution Discipline

For each audit run:
1. Use one audit definition per run.
2. Keep prompt scope fixed for the run.
3. Record findings with structured risk levels.
4. Save output as a new report file under `docs/audits/reports/YYYY-MM-DD/`.
5. Never overwrite prior run artifacts.

## 5. History Preservation Rule

Audit history is append-only.

Required:
- No audit definition or report may be deleted.
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
- `docs/audits/domains/...`

Do not reference deprecated locations.
