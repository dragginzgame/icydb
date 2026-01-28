# Query Diagnostics Contract

This document defines the stability and scope guarantees for query diagnostics.
Diagnostics are read-only and are intended for developer tooling, admin paths, and
debugging; they are not correctness proofs.

## Guarantees

- Explain determinism: `ExplainPlan` is deterministic for equivalent queries and plans.
- Fingerprint stability: `PlanFingerprint` is stable within a major version and is
  derived from the normalized explain projection.
- No implicit execution: diagnostics never execute a query unless explicitly requested.
- Observational only: diagnostics do not affect planning, execution, or results.

## Best-effort / May Change

- Trace event schemas are best-effort and may evolve between versions.
- Trace coverage may expand or contract across releases.

## Non-guarantees

- Diagnostics are not an authoritative or complete description of execution semantics.
- Diagnostics do not imply query correctness or data integrity.
