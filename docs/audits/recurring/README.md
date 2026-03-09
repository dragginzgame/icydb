# Recurring Audit Categories

This directory contains recurring architecture/correctness audits.

## Ownership Model

- each file is an audit definition (the repeatable checklist)
- each parent folder is the domain boundary that owns that audit surface
- reports are written to `docs/audits/reports/YYYY-MM/YYYY-MM-DD/`

## Cadence

- recurring audits run on the project's weekly audit cycle
- crosscutting and high-risk domains may be rerun within the same week when needed

## Scoring Interpretation

- `1-3`: low risk / structurally healthy
- `4-6`: moderate risk / manageable pressure
- `7-8`: high risk / requires monitoring and follow-up
- `9-10`: critical risk / structural instability

## Domain Map

### Planner/Range Semantics

- `range/boundary-envelope-semantics.md`

### Access

- `access/access-index-integrity.md`

### Executor

- `executor/executor-state-machine-integrity.md`
- `executor/cursor-ordering.md`

### Storage

- `storage/storage-recovery-consistency.md`

### Integrity

- `integrity/invariant-preservation.md`

### Crosscutting

- `crosscutting/crosscutting-complexity-accretion.md`
- `crosscutting/crosscutting-dry-consolidation.md`
- `crosscutting/crosscutting-layer-violation.md`
- `crosscutting/crosscutting-module-structure.md`
- `crosscutting/crosscutting-velocity-preservation.md`

### Contracts

- `contracts/error-taxonomy.md`
- `contracts/resource-model-compliance.md`

## Legacy Path Markers

- files ending in `_legacy.md` are preserved path markers after domain/file renames
- run recurring audits from the active files listed above, not from `_legacy.md` markers
