> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# IcyDB Current Architecture State Audit

Date: 2026-07-28

Scope: present-state repository architecture, authority, frontend convergence,
validation, representation, recovery, and package boundaries

Method tag: `current-architecture-state-v1`

## Report preamble

- Baseline report: none. This is an independent first run, not a rerun of the
  0.213 closeout.

- Initial code snapshot:
  `defccc3ddc3dd969b41b862dbf944436c9b38c9f`.

- Final audited code snapshot:
  `634661d8cad8f09376dbc93944fbf7e55891300a`, tag `v0.213.37`.

- Snapshot movement: the repository advanced during the audit by the
  `Release 0.213.37` commit. That commit changes workspace release metadata and
  the README, not the architecture examined here.

- Current version: `0.213.37`.

- Branch: `main`, tracking `origin/main`.

- Initial and pre-report worktree: clean.

- Comparability: not directly comparable with the release closeout because this
  audit reconstructed the current system from dependency edges, construction
  sites, and runtime call sites before reading the closeout report.

## 1. Executive verdict

**Verdict: clean and coherent.**

There is one real accepted-schema-driven query planner, executor, structural
mutation pipeline, and physical row boundary. Generated Rust entity types do
not participate in core planning, expression evaluation, row validation, or
physical encoding. SQL, dynamic/structural, and typed writes converge on the
same accepted after-image and commit machinery. SQL and typed/dynamic reads
also use the same `StructuralQuery` planner/executor.

The initial audit also found a generated runtime entity route gate, an obsolete
schema-check CLI/API, parallel planner index metadata, lifecycle-dependent
constraint diagnostics, and row-order-dependent batch constraint admission.
R1 through R7 hard-deleted or consolidated those layers. `Db` now
contains only the sealed store registry,
`AcceptedRuntimeEntity` is projected from the current source-bound accepted
bundle, and every accepted secondary index lowers to one ordered semantic
key-item vector. One opaque mutation-constraint batch now carries all normal
save/delete admission into the commit window. The query feature owns accepted
planning, caching, execution, and structural projection; SQL owns only its
frontend semantics and response adaptation.

No P0 corruption or loss-of-atomicity defect was found. R5 corrected a P1
fail-closed defect in which valid unique swaps/cycles and self-referential
relation batches could be rejected according to input order.

### 3.2 Package-boundary verdict

**PASS:** source/model authoring is independently packageable and the core
query/persistence engine does not depend on model crates.

**PASS:** dynamic and typed reads compile and execute without SQL parsing,
lowering, DDL, mutation, or response DTO modules.

**PASS:** generated canister wiring supplies the sealed physical store registry
and optional frontend surfaces only. It no longer emits an entity route table.

No legacy feature alias or compatibility feature restoring generated
persistence was found.

### 6.2 R4/R5 consolidation result

R4 corrected the lifecycle split:

- accepted and activating `NOT NULL` rules compile into
  `CompiledAcceptedRowConstraints`;

- explicit null reaches the compiled accepted identity before a physically
  non-null slot can flatten it into a storage-admission error;

- active field-path and expression-index unique collisions carry the paired
  accepted constraint ID/name and key-item paths;

- active relation save/delete failures and pending relation projections carry
  the accepted or activation-reserved identity; and

- every live failure uses `ConstraintDiagnostic`, while physical decoding of
  an impossible persisted null remains corruption.

R5 then corrected orchestration and batch semantics:

- every normal save/delete commit window accepts an opaque
  `AcceptedMutationConstraintBatch`, so callers cannot bypass scheduling with
  raw row operations;

- row-local checks execute after defaults and managed timestamps;

### 7.1 Verdict

**The planner, executor, and database session are independent of user Rust
structs below the typed boundary.**

No user entity generic, Rust `TypeId`, generated row decoder, derive validator,
Rust field name, Rust default implementation, or generated physical codec was
found below the facade typed boundary. `EntityAuthority`, `SchemaInfo`,
`StructuralQuery`, logical/access plans, prepared execution plans, cursors,
projections, grouping, ordering, and terminals use accepted/runtime identities
and engine-neutral values.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/07/28/current-architecture-state/01/report.md).
