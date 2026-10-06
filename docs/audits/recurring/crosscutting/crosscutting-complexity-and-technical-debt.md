# IcyDB complexity and technical debt overlay

Apply [the shared method](../../../../audits/complexity-and-technical-debt.md).

IcyDB adopts Shared Tooling `d957d1f8801885c5b69e4a9ef900155f5f2a8a9d` through
[the snapshot](../../../../.shared-tooling.snapshot). Apply the shared method
and [local audit governance](../../README.md) together. This changes method
identity; earlier reports remain historical and affected comparisons are
`N/A (method change)`. No audit supplies implementation or broad-gate authority.

## Identity and scope

- Report scope: `complexity-and-technical-debt`.
- Method: shared revision above plus `ICYDB-CTD-2`; record both identities.
- Reports: `docs/reports/recurring/YYYY/MM/DD/complexity-and-technical-debt/<run>/report.md`.
- Trigger: affected owners at minor-line closeout, new public/configuration axes,
  persisted state, routes, cursor/protocol formats or widely consumed variants,
  or repeated edits across unrelated owners. Broad baselines require a request.

Inspect maintained behavior in `crates/` and its participating generated,
canister and fixture consumers. Exclude historical definitions, obsolete formats
and build output from active state counts. Product correctness, completeness,
security and empirical performance retain their separate domain owners.

## Product decisions

Map state-space and ownership against [IcyDB architecture](../../architecture-contracts.md)
and [simplicity rules](../../../governance/simplicity-and-maintainability.md).
Use the local debt families `DuplicatedFlowDebt`, `StateSpaceDebt` and
`OwnershipDebt`; link the owning flow review for duplicated semantics.

Review SQL/Fluent/prepared execution, accepted catalog/model boundaries,
`EXPLAIN`, indexes/cursors and publication/recovery when affected. Accepted
snapshots are runtime authority; generated models and SQL remain frontend or
proposal inputs. Recovery, corruption containment, generated propagation and
facade contracts are intentional maintenance obligations, not avoidable states.
Apply pre-1.0 hard cuts without discarding effects, assets, liabilities or
same-contract recovery. GitHub issues remain the only active follow-up tracker.

## Focused evidence

Source inspection and at most three bounded extension rehearsals identify
current friction; they do not start feature work. Use relevant owner-local
invariants and focused target checks only when their assertions are needed.
Apply [verification readout](../../README.md#verification-readout) and retain
matching source, features, lockfile and host identity for reused evidence.
Only raw Wasm bytes, IC cycles and instructions are performance metrics. Never
substitute native timing; missing permitted costs remain unmeasured.
