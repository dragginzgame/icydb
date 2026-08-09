# Recurring Audit — Flow Convergence And Duplication

## Identity

- report scope: `flow-convergence-and-duplication`
- method: `FCD-1.0`
- report path:
  `docs/reports/recurring/YYYY/MM/DD/flow-convergence-and-duplication/<run>/report.md`

This method replaces the former canonical-semantic-authority,
DRY/consolidation, flow-convergence, and semantic portions of layer-violation
audits. Earlier reports are historical context and are non-comparable except
for explicitly named stable anchors.

## Purpose

Determine whether equivalent behavior has one canonical semantic owner and
converges through one maintained internal flow, without removing defensive
boundary checks or measured specializations that have distinct authority.

This audit targets duplicated flows and policy rediscovery. It is not a style,
line-count, general correctness, performance, or speculative redesign audit.

## Run Triggers

Run this audit:

- at minor-line closeout for owners changed by that line;
- after adding or changing a frontend-to-runtime path, execution route,
  planner artifact, public adapter, `EXPLAIN` projection, replay path, or
  generated boundary; or
- as a periodic broad baseline when explicitly requested.

The default closeout run is affected-owner scoped. Do not scan unrelated
subsystems merely because the method can do so.

## Core Contract

The maintained flow is:

```text
owner derives -> contract carries -> consumers project
```

Distinct public construction surfaces may remain separate. Equivalent internal
semantics must converge as early as practical. Downstream runtime,
diagnostics, replay, or `EXPLAIN` code must not reparse or reclassify a policy
already decided by its owner.

Similar-looking code is not automatically duplication debt. Independent
fail-closed checks, trust-boundary enforcement, recovery containment, and
measured hot-path specialization may remain separate when their reason is
explicit.

## Evidence Discipline

Enumerate evidence once for the run and reuse it throughout the report. Record:

- code snapshot and dirty-worktree relevance;
- affected behavior and owner boundaries;
- public and internal entrypoints;
- carried semantic/planner/runtime artifacts;
- downstream classification and projection sites;
- relevant focused tests and invariant gates; and
- the compared baseline or `N/A`.

Mention counts are discovery signals only. A duplicated-flow finding requires
inspection of the branch conditions, outcomes, and authority role.

## Method

### 1. Behavior And Owner Map

Select the behavior families in scope. For each, record:

| Behavior | Canonical owner | Inputs | Carried contract | Consumers |
| --- | --- | --- | --- | --- |

If the canonical owner is unclear or plural, carry that fact into the findings
rather than inventing an owner during the audit.

### 2. Flow Trace

Trace every maintained entry surface to its convergence point:

| Entry surface | Frontend-only work | Convergence point | Runtime path | Result projection |
| --- | --- | --- | --- | --- |

Inspect SQL, Fluent, prepared, generated, facade, recovery, diagnostics,
`EXPLAIN`, and replay surfaces only when they participate in the selected
behavior.

### 3. Duplication And Rediscovery Scan

Look for:

- equivalent semantic branch trees in multiple owners;
- runtime or `EXPLAIN` classifiers that infer an upstream decision;
- adapters that validate or reinterpret instead of translate;
- repeated conversions between equivalent representations;
- stale wrappers, aliases, compatibility paths, or fallback DTOs;
- separate prepared/non-prepared or SQL/Fluent execution implementations; and
- tests or generated code widening production surface solely for convenience.

### 4. Retention Gate

Before recommending convergence, determine whether separation:

- independently protects a trust boundary;
- preserves fail-closed corruption or recovery behavior;
- belongs to a distinct semantic owner; or
- is supported by current performance/Wasm evidence.

Do not consolidate across architectural layers merely to reduce repetition.
Do not replace direct hot-path code with allocation, dynamic dispatch, clone,
formatting, or monomorphization risk without measurement.

### 5. State-Space And Debt Projection

For each confirmed duplicated flow, state:

- which behavior axis is duplicated;
- which combinations or switch sites it multiplies;
- the current maintenance friction;
- the canonical convergence point; and
- whether removal changes public or persisted state.

Apply `docs/governance/simplicity-and-maintainability.md`. An audit finding is
evidence, not implementation authority.

## Finding Classification

Use exactly one class:

- `DuplicateFlow`: equivalent behavior executes through multiple maintained
  paths;
- `PolicyRediscovery`: a consumer independently derives an owner decision;
- `LateConvergence`: equivalent inputs converge only after avoidable duplicate
  work;
- `OwnershipLeak`: one layer depends on another layer's private decision;
- `StaleSurface`: a wrapper, adapter, alias, or fallback has no current
  authority reason;
- `ProtectiveDuplication`: intentional separate enforcement at a trust or
  recovery boundary; or
- `MeasuredSpecialization`: intentional separate implementation justified by
  current cost evidence.

Use `LOW`, `MEDIUM`, or `HIGH` risk. Do not compute a composite score.

## Finding Disposition

Every finding uses exactly one disposition:

- `DELETE`
- `CONSOLIDATE`
- `LOCALIZE`
- `KEEP — BOUNDARY`
- `KEEP — MEASURED HOT PATH`
- `NO ACTION`

The report may contain at most five active findings with `DELETE`,
`CONSOLIDATE`, or `LOCALIZE` dispositions. Additional observations remain
supporting evidence.

## Required Report

1. preamble and comparability;
2. verdict: `PASS`, `PASS WITH FINDINGS`, `FAIL`, or `BLOCKED`;
3. behavior/owner map;
4. flow trace;
5. findings table with ID, class, risk, owner, evidence, friction,
   disposition, and action trigger;
6. retained separations and their boundary or measured reason;
7. complexity/state-space delta; and
8. focused verification readout using `PASS`, `FAIL`, or `BLOCKED`.

If no action is warranted, say so. Do not manufacture cleanup work to populate
the report.

## Read-Only Default

The audit is read-only by default. It does not modify production code, promote
findings into a design, create an active debt ledger, or start external
services. Implementation requires a separately authorized bounded patch.
