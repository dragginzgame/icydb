# IcyDB flow convergence and duplication overlay

Apply [the shared method](../../../../audits/flow-convergence-and-duplication.md).

IcyDB adopts Shared Tooling `d957d1f8801885c5b69e4a9ef900155f5f2a8a9d` through
[the snapshot](../../../../.shared-tooling.snapshot). Apply the shared method
and [local audit governance](../../README.md) together. This changes method
identity; earlier reports remain historical and affected comparisons are
`N/A (method change)`. No audit supplies implementation or broad-gate authority.

## Identity and scope

- Report scope: `flow-convergence-and-duplication`.
- Method: shared revision above plus `ICYDB-FCD-2`; record both identities.
- Reports: `docs/reports/recurring/YYYY/MM/DD/flow-convergence-and-duplication/<run>/report.md`.
- Trigger: affected owners at minor-line closeout, or changes to frontends,
  prepared plans, generated boundaries, diagnostics, replay or public adapters.
  A whole-system baseline needs an explicit request.

Trace participating SQL, Fluent, dynamic/typed session, prepared, facade,
generated, `EXPLAIN`, diagnostics, migration, recovery and replay entrypoints.
Include their direct fixtures and generated producers rather than inspecting
only edited lines. Exclude unrelated owners, historical prose and build output.

## IcyDB authority and retained boundaries

Accepted schema snapshots own runtime planning, execution, decoding and mutation.
Generated `EntityModel` / `IndexModel` belong only to proposal, reconciliation,
model-only convenience and tests. SQL DDL lowers into catalog-native mutation;
SQL text and generated models must not reconstruct accepted runtime authority.

Prepared plans carry normalized decisions into execution and `EXPLAIN`.
Publication, indexes, cursors, durable jobs, startup observation and replay keep
their owning contracts. Check equivalent prepared/non-prepared and SQL/Fluent
paths at the actual convergence point. Preserve independent corruption,
namespace, generation, trust and interruption-recovery checks.

Use [architecture contracts](../../architecture-contracts.md) for layer direction
and [simplicity rules](../../../governance/simplicity-and-maintainability.md) for
state-space decisions. Apply the pre-1.0 hard cuts and retained-data obligations
in [AGENTS.md](../../../../AGENTS.md). Retain measured specialization only with
matching raw Wasm, IC-cycle or instruction evidence.

## Focused evidence

Start with targeted source and caller inspection in `crates/`, then include
`schema/`, `canisters/` and `testing/` only for participating boundaries.
Use the owning invariant script, generated output or focused Cargo target as
needed. List and execute the same package, features and test filter using the
repository Cargo environment; full workspace/release gates remain user-owned.
There is no native timing proof. Record unavailable permitted measurements as
unmeasured. Follow [verification readout](../../README.md#verification-readout).
