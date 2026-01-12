# REFACTOR.md

Purpose
- This document captures an architectural baseline for icydb and a conservative
  runtime contract for maintainers.
- It is intended to guide long-lived refactoring and operational decisions.
- It is not a user-facing SLA and does not describe future features.

Scope
- Workspace: schema-first code generation, runtime execution, stable-memory
  persistence, and observability hooks.
- Primary crates: `crates/icydb-core`, `crates/icydb-schema`,
  `crates/icydb-macros`, `crates/icydb-build`, `crates/icydb` (meta).

Repository layout (current)
- `crates/icydb-core`: runtime engine, storage, query planning, executors,
  values, validation, and observability.
- `crates/icydb-schema`: schema AST, builders, and validation.
- `crates/icydb-macros`: proc-macros for schema and entity/view generation.
- `crates/icydb-build`: build-time codegen glue for canister entrypoints,
  registries, and observability endpoints.
- `crates/icydb`: meta-crate re-exporting core/macros/schema/base helpers.
- `crates/test` and `crates/test_design`: integration and design tests.

Current architectural overview
- Schema-first: schema definitions drive code generation and runtime shape.
- Codegen: canister entrypoints and store registries are generated at build
  time; data/index stores are thread-local handles backed by stable memory.
- Runtime core: a single crate hosts storage, query planning, execution,
  validation/sanitization, and observability wiring.
- Storage: stable-memory BTreeMap stores for data and indexes; index entries
  are derived from entity values.
- Query pipeline: filter DSL -> query plan (keys/index/scan) -> executor ->
  response shaping.
- Observability: ephemeral metrics and storage snapshots; no durable audit log.

Runtime contract (maintainer-facing)

Definitions
- Operation: a single executor call (load/save/delete/upsert/exists) or a
  generated observability endpoint call.
- Write unit: a conceptual boundary for intended atomicity; not transactional
  and not guaranteed to commit or roll back as a unit.
- Store: a data store or index store backed by stable memory.
- Schema: compile-time entity/index definitions used for code generation.
- Index: derived hashed entry in an index store.

Consistency guarantees
- Single-entity writes (insert/update/replace/delete) attempt to keep data and
  index stores consistent but are not transactional across stores.
- Write units express intent only; atomicity is not guaranteed.
- Unique constraints are validated before index mutation for writes.
- Query results reflect store contents as read by the executor at execution
  time; no isolation level is declared.
- Batch writes (insert_many/update_many/replace_many) are fail-fast and
  non-atomic; partial success is allowed.

Undefined (consistency)
- Atomicity across data and index stores under errors or traps.
- Snapshot isolation across concurrent operations.
- Deterministic ordering unless explicitly enforced by query sort semantics.
- Automatic detection or repair of index corruption.

Failure semantics
- Internal operations return a structured `InternalError` (class + origin);
  public `Error` exposes class/origin/message at the API boundary.
- Errors are classified into a stable internal taxonomy; string representations
  are non-contractual.
- Operations may partially update stores before returning an error.
- No automatic rollback, repair, or compensation is provided.

Undefined (failure)
- Idempotency under retries unless caller enforces it.
- Stable mapping from internal error variants to class/origin across versions.
- Guaranteed detection of all index inconsistencies after partial failures.

Upgrade guarantees
- Stable-memory data persists across upgrades; in-memory runtime state does not.
- Serialization is not versioned at the runtime boundary.
- Only the exact same schema and runtime semantics that wrote the data are
  guaranteed to read it.

Undefined (upgrade)
- Forward or backward compatibility across any version change.
- Automatic migrations, backfills, or index rebuilds.
- Stable serialization across codegen or serializer changes.

Observability guarantees
- Metrics and snapshots are best-effort, in-memory views only.
- Metrics are ephemeral; reset on upgrade/redeploy and can be reset explicitly.
- Snapshots represent a point-in-time view taken during the endpoint call.

Undefined (observability)
- Durable audit trails or tamper-evident logs.
- Monotonic counters across upgrades or resets.
- Consistency between snapshots and concurrent operations.
- Stable schema for metrics fields across versions.

Global undefined behavior
- Any behavior not explicitly guaranteed above.
- Behavior after schema or code changes that alter serialization or index
  computation without a defined migration plan.
- Behavior after external modifications to stable memory.

Maintainer obligations implied by this contract
- Treat index/data updates as non-transactional; do not assume rollback.
- Treat serialization and schema evolution as compatibility boundaries.
- Treat metrics/snapshots as diagnostic only, not audit or compliance signals.

Structural risks (current)
- Cross-cutting concerns (metrics, validation, storage) live in the same
  runtime layer, increasing coupling and local reasoning cost.
- Boundary errors now expose class/origin, but retry guidance and stability
  guarantees for the mapping are not yet documented.
- Upgrade risk is high due to unversioned serialization and derived indexes.
- Observability lacks durable auditability and traceability.
- Multi-tenant and security boundaries are not modeled at the runtime layer.

Missing or under-modeled concepts
- Explicit consistency model and transaction boundaries.
- Public error contract (retry guidance, stability).
- Schema/version compatibility contract and migration lifecycle.
- Observability contract (structured logs, durable audit events).
- Tenancy, authorization, and quota boundaries.
- Lifecycle/state modeling for maintenance or read-only modes.

Strategic refactor directions (non-tactical)
- Separate runtime layers explicitly: storage engine, query/execution, policy
  validation, and ops/observability.
- Create a stable compatibility boundary for data formats and index layouts.
- Define and document a consistency model and unit-of-work boundary.
- Stabilize and document the error taxonomy; decide if/when to expose it at the
  boundary.
- Define a durable observability contract (audit events, logs, metrics schema).
- Model security and tenancy boundaries at the framework boundary.

Suggested long-term workstreams
- Compatibility and migration: formal schema evolution policy and tooling.
- Consistency and correctness: define guarantees and back them with tests.
- Observability: durable audit log and structured telemetry contract.
- Error model: stable taxonomy, documented class/origin mapping, reduce reliance
  on string-only boundary errors.
- Layering: enforce module boundaries and narrow cross-layer dependencies.
