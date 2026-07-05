
• 1. Verdict

   Roadmap Area                                   Verdict
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   SQL maturity without SQLite creep              Strong baseline, needs design
  ─────────────────────────────────────────────  ───────────────────────────────────────
   Safe ad-hoc/admin diagnostics                  Needs design
  ─────────────────────────────────────────────  ───────────────────────────────────────
   Native constraint layer                        Needs design
  ─────────────────────────────────────────────  ───────────────────────────────────────
   Schema migration state machine                 Needs design
  ─────────────────────────────────────────────  ───────────────────────────────────────
   Import/export and SQLite-shaped tooling        Missing foundation
  ─────────────────────────────────────────────  ───────────────────────────────────────
   Query admission and EXPLAIN quality            Needs design
  ─────────────────────────────────────────────  ───────────────────────────────────────
   Operator/admin diagnostics                     Needs design
  ─────────────────────────────────────────────  ───────────────────────────────────────
   Release gates and source-audit enforcement     Strong baseline
  ─────────────────────────────────────────────  ───────────────────────────────────────
   Stable-memory ownership and persisted-         Strong baseline, needs productization
   format policy
  ─────────────────────────────────────────────  ───────────────────────────────────────
   0.191+ design-doc candidates                   Needs design

  2. Executive Summary

  IcyDB’s strongest current foundation is still the IC-native reliability baseline:
  journal/marker recovery, fail-closed persisted decoding, accepted schema snapshots as
  runtime authority, generated stable-memory ownership, source-audit guards, and
  recovery/failpoint tests are real code and CI-backed. SQL is also more mature than a
  “toy” layer: SELECT, mutations, DDL, aggregates, projection, blob handling, EXPLAIN,
  and stable diagnostic codes are implemented and tested.

  The largest product gap is not “more SQL.” It is defining safe query lanes and
  admission rules so public canister APIs cannot accidentally expose unbounded scans,
  materialized sorts, large projections, or admin-only introspection. The next design
  effort should harden a deliberately bounded SQL/query contract, not chase SQLite
  compatibility.

  No source changes were made.

  3. Roadmap Gap Matrix

   Area                          SQL maturity
   Current Status                Strong
   Evidence                      docs/contracts/SQL_SUBSET.md, crates/icydb-core/src/db/
                                 sql/parser/model.rs, crates/icydb-core/src/db/session/
                                 tests
   Main Gap                      Freeze the intentionally bounded subset and avoid
                                 SQLite creep
   Recommended Design-Doc Owner  0.191 Query Admission And EXPLAIN
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                          Safe ad-hoc/admin diagnostics
   Current Status                Partial
   Evidence                      crates/icydb-build/src/db/sql.rs, crates/icydb-core/
                                 src/db/session/sql/write_policy.rs
   Main Gap                      Read-side public/admin lanes and budgets are not
                                 product-level contracts
   Recommended Design-Doc Owner  0.191 Query Admission And EXPLAIN
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                          Native constraints
   Current Status                Partial
   Evidence                      crates/icydb-core/src/model/field.rs, crates/icydb-
                                 core/src/db/index/plan/unique.rs, crates/icydb-core/
                                 src/db/relation/save_validate.rs
   Main Gap                      No first-class persisted check/custom constraint layer
   Recommended Design-Doc Owner  0.191 Constraint Layer And Write Admission
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                          Schema migration state
   Current Status                Partial/Weak
   Evidence                      crates/icydb-core/src/db/schema/mutation/mod.rs,
                                 crates/icydb-core/src/db/schema/mutation/runner.rs,
                                 crates/icydb-core/src/db/schema/reconcile.rs
   Main Gap                      No durable migration IDs, phases, watermarks, or
                                 reentry state
   Recommended Design-Doc Owner  0.191 Schema Migration State Machine
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                          Import/export tooling
   Current Status                Missing
   Evidence                      docs/design/ideas/version-gap-import-and-migration-
                                 scripts.md, docs/contracts/DURABILITY.md
   Main Gap                      No JSONL/CSV/SQLite-dump tooling or import threat model
                                 implementation
   Recommended Design-Doc Owner  0.192 Import/Export Tooling
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                          Query admission/EXPLAIN
   Current Status                Partial
   Evidence                      crates/icydb-core/src/db/query/explain/nodes/mod.rs,
                                 crates/icydb-core/src/db/query/explain/json.rs, crates/
                                 icydb-core/src/db/query/intent/access_requirement.rs
   Main Gap                      EXPLAIN is rich, but not tied to public admission
                                 policy
   Recommended Design-Doc Owner  0.191 Query Admission And EXPLAIN
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                          Operator diagnostics
   Current Status                Partial
   Evidence                      crates/icydb-core/src/db/diagnostics/storage_report.rs,
                                 crates/icydb-core/src/db/diagnostics/integrity.rs,
                                 crates/icydb-cli/src/observability
   Main Gap                      Recovery marker/journal/fold phase state is mostly
                                 internal
   Recommended Design-Doc Owner  0.192 Operator Durability Diagnostics
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                          Release gates/source audit
   Current Status                Strong
   Evidence                      .github/workflows/ci.yml, Makefile, crates/icydb-core/
                                 src/db/tests/ic_update_model.rs
   Main Gap                      Public API snapshot and wasm import inspection are not
                                 clearly release-blocking
   Recommended Design-Doc Owner  0.191 IC Storage Productization
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                          Stable memory/persisted policy
   Current Status                Strong/Partial
   Evidence                      docs/contracts/PERSISTED_FORMAT_POLICY.md, crates/
                                 icydb-schema/src/node/canister.rs, crates/icydb-core/
                                 src/db/commit/memory.rs
   Main Gap                      Backup/import/checksum/downgrade contract remains
                                 intentionally incomplete
   Recommended Design-Doc Owner  0.191 Durability Productization
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                          Design-doc candidates
   Current Status                Partial
   Evidence                      docs/design/0.190-ic-reliability-followup/0.190-
                                 design.md, docs/design/0.191-durability-productization-
                                 format-policy/0.191-design.md
   Main Gap                      Need focused post-0.190 docs for admission,
                                 constraints, migration, tooling
   Recommended Design-Doc Owner  Roadmap governance

  4. Detailed Findings

  A. SQL Maturity Without Becoming SQLite

   Feature   SELECT
   Status    Supported
   Evidence  crates/icydb-core/src/db/sql/parser/statement/select.rs, crates/icydb-core/
             src/db/session/sql/execute/select.rs,
             execute_sql_scalar_matrix_queries_match_expected_rows
   Notes     Single-entity SQL is documented. Public safety still depends on how the
             endpoint is exposed.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   INSERT
   Status    Supported
   Evidence  crates/icydb-core/src/db/sql/parser/statement/insert.rs, crates/icydb-core/
             src/db/session/sql/compiled.rs, SQL write tests
   Notes     VALUES and INSERT SELECT exist; bounded write diagnostics are precise.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   UPDATE
   Status    Supported/Policy-bound
   Evidence  crates/icydb-core/src/db/sql/parser/statement/update.rs, crates/icydb-core/
             src/db/session/sql/update_policy.rs
   Notes     Public policies require primary-key or bounded deterministic shapes.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   DELETE
   Status    Supported/Policy-bound
   Evidence  crates/icydb-core/src/db/sql/parser/statement/delete.rs, crates/icydb-core/
             src/db/session/sql/delete_policy.rs
   Notes     Same policy story as UPDATE.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   RETURNING
   Status    Partial
   Evidence  crates/icydb-core/src/db/session/sql/execute/write_returning.rs
   Notes     Supports * or named fields; bounds rows and Candid response bytes. No
             expression RETURNING.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   WHERE/filter expressions
   Status    Supported subset
   Evidence  crates/icydb-core/src/db/sql/lowering/select/mod.rs, sql_scalar.rs tests
   Notes     Rich but intentionally bounded. Unsupported forms get stable diagnostics.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   ORDER BY
   Status    Supported
   Evidence  docs/contracts/SQL_SUBSET.md, projection/order tests
   Notes     Blob ordering is rejected. Sort/materialization admission is not yet a
             public policy.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   LIMIT/OFFSET
   Status    Supported
   Evidence  parser/lowering/write policies
   Notes     Public bounded writes require LIMIT and reject OFFSET. Public reads do not
             have an equivalent default gate.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   Cursor/pagination
   Status    Partial
   Evidence  crates/icydb-core/src/db/session/sql/result.rs, grouped SQL tests
   Notes     Grouped SQL can return next_cursor; scalar SQL uses LIMIT/OFFSET. Fluent
             typed APIs have cursor semantics.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   Projection/typed output
   Status    Supported
   Evidence  crates/icydb-core/src/db/session/sql/projection/payload.rs, crates/icydb-
             core/src/db/session/sql/result.rs
   Notes     Outputs OutputValue rows with fixed-scale metadata.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   Blob output
   Status    Partial
   Evidence  docs/contracts/SQL_SUBSET.md, sql_blob.rs
   Notes     Blob literals/output/equality/OCTET_LENGTH exist; ordering rejected.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   Aggregates
   Status    Supported
   Evidence  sql_aggregate.rs, sql_grouped.rs, crates/icydb-core/src/db/session/sql/
             compiled.rs
   Notes     Global and grouped aggregates, DISTINCT/FILTER support.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   EXPLAIN
   Status    Partial/Strong primitive
   Evidence  crates/icydb-core/src/db/executor/explain/mod.rs, sql_explain.rs
   Notes     SELECT/DELETE plan/execution/json under sql-explain; not yet admission-
             policy authority.
  ──────────────────────────────────────────────────────────────────────────────────────
   Feature   Unsupported diagnostics
   Status    Strong
   Evidence  crates/icydb-diagnostic-code/src/lib.rs, crates/icydb-diagnostic-code/src/
             registry.rs
   Notes     Stable code registry exists. Public read-admission codes are missing.

  The repo should document IcyDB SQL as a small, mature, single-entity IC query
  language. The right boundary is already stated in docs/contracts/SQL_SUBSET.md: no
  joins, no SQLite compatibility promise, no arbitrary relational engine semantics, and
  SQL DDL remains a frontend over accepted schema mutation.

  B. Safe Ad-Hoc/Admin Diagnostics

  IcyDB partially separates lanes today:

  - Generated SQL/query/DDL/update/fixtures/snapshot/metrics endpoints are opt-in in
    crates/icydb-build/src/db/sql.rs.

  - Generated SQL query and DDL surfaces are controller-gated.
  - SQL writes have explicit policies in crates/icydb-core/src/db/session/sql/
    write_policy.rs, crates/icydb-core/src/db/session/sql/update_policy.rs, and crates/
    icydb-core/src/db/session/sql/delete_policy.rs.

  - Fluent queries can assert access paths using crates/icydb-core/src/db/query/intent/
    access_requirement.rs.

  Missing product-level read/admission policies:

  - Required LIMIT for public SELECT.
  - Max returned rows for general SELECT.
  - Max scanned rows.
  - Max response bytes for read projections.
  - Default public full-scan rejection.
  - Sort/materialization budget.
  - Projection budget.
  - Explicit admin-vs-public read lane.

  So the repo distinguishes admin/controller SQL from public mutation policy, but it
  does not yet distinguish “developer/admin ad-hoc read power” from “safe public query
  power” as a first-class read-side contract.

  C. Native Constraint Layer

   Constraint Class         Required fields
   Status                   Supported
   Static Schema            Yes
   Dynamic/Accepted Schema  Yes
   Write Preflight          Yes
   Notes                    crates/icydb-core/src/db/executor/mutation/
                            save_validation.rs, SQL write missing-required tests.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Optional/null fields
   Status                   Supported
   Static Schema            Yes
   Dynamic/Accepted Schema  Yes
   Write Preflight          Yes
   Notes                    Nullability is persisted and validated.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Defaults
   Status                   Supported
   Static Schema            Yes
   Dynamic/Accepted Schema  Yes
   Write Preflight          Yes
   Notes                    DB defaults and generated/write-managed fields exist.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Numeric min/max
   Status                   Partial
   Static Schema            Yes via validators
   Dynamic/Accepted Schema  Mostly no
   Write Preflight          Partial
   Notes                    Static validators exist; accepted runtime constraints do not
                            expose general min/max.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Text length
   Status                   Supported
   Static Schema            Yes
   Dynamic/Accepted Schema  Yes
   Write Preflight          Yes
   Notes                    Text { max_len } in crates/icydb-core/src/model/field.rs.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Blob size
   Status                   Supported
   Static Schema            Yes
   Dynamic/Accepted Schema  Yes
   Write Preflight          Yes
   Notes                    Blob { max_len }; blob tests cover byte length.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Decimal precision/scale
   Status                   Partial
   Static Schema            Scale yes
   Dynamic/Accepted Schema  Scale yes
   Write Preflight          Scale yes
   Notes                    Precision is not a general constraint.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Enum/category membership
   Status                   Partial
   Static Schema            Yes
   Dynamic/Accepted Schema  Yes
   Write Preflight          Yes
   Notes                    Enum field kind exists; broader category DDL is not a
                            product layer.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         List/set/map cardinality
   Status                   Partial
   Static Schema            Static validators
   Dynamic/Accepted Schema  No general accepted constraint
   Write Preflight          Deterministic storage only
   Notes                    Canonical set/map order is enforced.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Regex/sanitizers/custom validators
   Status                   Partial
   Static Schema            Static macro/base
   Dynamic/Accepted Schema  No
   Write Preflight          Static typed path only
   Notes                    Not persisted as accepted runtime constraints.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Unique constraints
   Status                   Supported
   Static Schema            Yes
   Dynamic/Accepted Schema  Yes
   Write Preflight          Yes
   Notes                    Implemented through unique indexes.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Compound unique constraints
   Status                   Supported
   Static Schema            Yes
   Dynamic/Accepted Schema  Yes
   Write Preflight          Yes
   Notes                    crates/icydb-core/src/db/index/plan/unique.rs validates
                            compound/expression unique indexes.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Check constraints
   Status                   Missing
   Static Schema            No first-class accepted check
   Dynamic/Accepted Schema  No
   Write Preflight          No
   Notes                    Could reuse expression/filter machinery later.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Reference existence
   Status                   Supported/Partial
   Static Schema            Strong relations
   Dynamic/Accepted Schema  Yes
   Write Preflight          Yes
   Notes                    crates/icydb-core/src/db/relation/save_validate.rs.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Delete restrict/cascade
   Status                   Restrict supported, cascade missing
   Static Schema            Yes
   Dynamic/Accepted Schema  Yes
   Write Preflight          Delete restrict
   Notes                    crates/icydb-core/src/db/relation/validate.rs.
  ──────────────────────────────────────────────────────────────────────────────────────
   Constraint Class         Conflict policies
   Status                   Missing
   Static Schema            No
   Dynamic/Accepted Schema  No
   Write Preflight          No
   Notes                    No SQLite-style conflict-policy layer.

  Compound indexes are not only query access structures. Unique field-path/expression
  indexes enforce integrity during write planning, and recovery tests cover unique-
  conflict fail-closed behavior. The main missing piece is a first-class accepted
  constraint catalog for check constraints, custom persisted validators, constraint
  diagnostics, and constraint readiness.

  D. Schema Migration State Machine

  Existing foundations:

  - Schema versions and fingerprints exist in accepted snapshots.
  - Persisted format versions exist for row, marker, journal, schema snapshot, cursor,
    and fold watermark codecs.

  - SQL DDL has expected/next schema-version contracts in crates/icydb-core/src/db/
    schema/mutation/ddl_admission.rs.

  - Add field, alter defaults/nullability, rename/drop field, create/drop index, and
    unique index DDL paths exist.

  - Physical index staging/rebuild logic exists in crates/icydb-core/src/db/schema/
    mutation/execution.rs and crates/icydb-core/src/db/schema/mutation/runner.rs.

  - Startup reconciliation uses accepted snapshots in crates/icydb-core/src/db/schema/
    reconcile.rs.

  Missing first-class migration model:

  - Migration IDs.
  - Planned/applied durable migration records.
  - Durable phase state.
  - Backfill watermarks.
  - Interruption/reentry contract.
  - Fail-closed incomplete-migration readiness state.
  - Persisted migration audit records.
  - General row rewrite/backfill execution beyond narrow supported cases.

  The 0.190 recovery model is a good template. A future migration state machine could
  use phases like Planned, BackfillingRows, RebuildingIndexes, ValidatingConstraints,
  PublishingSchema, and Complete, but it needs its own durable authority and readiness
  gates.

  E. Import/Export And SQLite-Shaped Tooling Compatibility

  Current implementation is weak to missing:

  - JSON/JSONL/CSV data export: missing.
  - Schema manifest export: partial via schema observability, not a portable manifest
    contract.

  - Fixture loading: present through generated fixtures and CLI paths.
  - Data import validation: missing.
  - SQLite file inspection: missing.
  - SQLite dump import: missing.
  - CREATE TABLE / CREATE INDEX translation: missing.
  - Offline migration tooling: idea-only in docs/design/ideas/version-gap-import-and-
    migration-scripts.md.

  - Backup/restore/import threat model: documented as unsupported/future in docs/
    contracts/DURABILITY.md and docs/contracts/PERSISTED_FORMAT_POLICY.md.

  - Checksum verification: not implemented.

  Recommendation: keep SQLite compatibility strictly at the tooling edge. Importing a
  SQLite dump or translating CREATE TABLE can be an offline migration assistant;
  embedding SQLite into canisters would conflict with IcyDB’s native marker/journal/
  recovery model.

  F. Query Admission And EXPLAIN Quality

  Strong primitives exist:

  - EXPLAIN can expose access route, index route, residual filter, predicate pushdown,
    ordering source, cursor, covering scan, and descriptor nodes through crates/icydb-
    core/src/db/query/explain/json.rs.

  - Plan nodes include full scan, key lookup, index prefix/range, branch set,
    intersection/union, materialized order, cursor resume, aggregate/materialized
    projection, and limit/offset in crates/icydb-core/src/db/query/explain/nodes/mod.rs.

  - Fluent access requirements can reject non-indexed or residual-filter plans in
    crates/icydb-core/src/db/query/intent/access_requirement.rs.

  - Stable diagnostics exist in crates/icydb-diagnostic-code/src/lib.rs.

  Equivalent existing errors:

  - AccessRequirementViolation::IndexRequired covers index-required fluent assertions.
  - AccessRequirementViolation::ResidualFilterForbidden covers residual-filter
    rejection.

  - SqlFeatureCode::* covers unsupported SQL features.
  - SqlLoweringCode::* covers SQL lowering/shape problems.
  - SqlWriteBoundaryCode::* covers staged-row, RETURNING row/byte, missing WHERE,
    missing LIMIT, and unsafe write-shape failures.

  Missing public query-admission diagnostics:

  - PublicQueryRequiresIndex.
  - PublicQueryRequiresLimit.
  - EstimatedScanExceedsBudget.
  - SortRequiresMaterialization.
  - ProjectionResponseMayExceedLimit.
  - UnboundedFullScanRejected.
  - A public response-size bound for SELECT.
  - A public scan/materialization budget surfaced in EXPLAIN.

  EXPLAIN is good enough to become the developer feedback surface for admission
  failures, but admission itself is not yet a product layer.

  G. Operator/Admin Diagnostics

  Available today:

  - Storage reports expose memory IDs, stable keys, storage mode, entries, memory bytes,
    schema fingerprints, corrupt keys, and index state through crates/icydb-core/src/db/
    diagnostics/storage_report.rs.

  - Integrity reports scan data/index consistency through crates/icydb-core/src/db/
    diagnostics/integrity.rs.

  - Query attribution and execution traces expose scan/materialization/return counts and
    instruction attribution.

  - CLI observability surfaces snapshot, schema, and metrics from crates/icydb-cli/src/
    observability.

  - SQL introspection supports SHOW MEMORY, SHOW STORES, SHOW INDEXES, SHOW COLUMNS, and
    DESCRIBE.

  Should be promoted to stable operator diagnostics:

  - Recovery marker present/absent.
  - Journal-tail count/bytes.
  - Fold watermark per store.
  - Last recovery phase.
  - Interrupted recovery-domain hint.
  - Recovery/read/write fail-closed counters.
  - Index rebuild/fold counters.
  - Checksum status once checksums exist.
  - Corruption/incompatible-format status as a compact health summary.
  - Build/feature-matrix diagnostics for deployed artifacts.

  H. Release Gates And Source-Audit Enforcement

  Strong enforced gates:

  - CI runs rustfmt, clippy, no-default checks/tests, feature-combination checks,
    workspace tests, SQL canister integration, wasm size reporting, and tag-release
    wasm/candid generation in .github/workflows/ci.yml.

  - Invariant scripts are wired through Makefile.
  - IC update-message source guards are in crates/icydb-core/src/db/tests/
    ic_update_model.rs.

  - Persisted malformed corpus tests live in crates/icydb-core/src/db/tests/
    persisted_format_corpus.rs.

  - Recovery/failpoint/large-index characterization tests live under crates/icydb-core/
    src/db/commit/tests.

  Partial or missing gates:

  - cargo test --workspace --all-features is not the exact CI shape; CI uses targeted
    supported feature combinations.

  - Public API snapshot gating was not found as a dedicated release blocker.
  - Wasm import inspection/audit tooling appears executable, but not clearly enforced as
    a blocking CI gate.

  - Recovery failpoint matrix and persisted corpus are enforced through tests, not a
    separately named release gate.

  Overall this is a strong baseline and should not be weakened.

  I. Stable-Memory Ownership And Persisted-Format Policy

  Strong current contract:

  - Schema-level memory IDs and stable keys are validated in crates/icydb-schema/src/
    node/canister.rs and crates/icydb-schema/src/node/store.rs.

  - Generated code registers commit/data/index/schema/journal memory domains in crates/
    icydb-build/src/db/store.rs.

  - Runtime commit allocation rejects memory ID/stable-key conflicts in crates/icydb-
    core/src/db/commit/memory.rs.

  - Row, journal, marker, schema, fold watermark, cursor, and index codecs have magic/
    version/bounded decode behavior.

  - docs/contracts/PERSISTED_FORMAT_POLICY.md clearly states the pre-1.0 hard-cut
    policy: one active internal format, fail closed on unknown future versions, no
    generated-model fallback reconstruction.

  Remaining gaps:

  - Raw stable-memory backup/restore/import is explicitly unsupported.
  - Checksums are not implemented.
  - Downgrade policy is not a product contract.
  - Foreign-memory detection is mostly magic/version/fingerprint based, not a full
    import validator.

  - Per-format compatibility matrix is not yet operator-facing.

  J. 0.191+ Design-Doc Candidates

  1. 0.191 Query Admission And EXPLAIN
     Problem: SQL/fluent reads need safe public admission distinct from admin
     diagnostics.
     Why now: SQL and EXPLAIN primitives exist; write lanes already have policy.
     Scope: public/admin/explain lanes, read budgets, full-scan rejection, stable
     diagnostics, EXPLAIN admission output.
     Non-goals: SQLite compatibility, joins, runtime SQLite, persisted format changes.

  2. 0.191 Constraint Layer And Write Admission
     Problem: field invariants, unique indexes, and relations exist, but no unified
     persisted constraint contract.
     Scope: accepted constraint catalog, check expressions, diagnostics, readiness,
     write preflight.
     Non-goals: joins, relational query engine, broad cascade semantics.

  3. 0.191 Schema Migration State Machine
     Problem: DDL/rebuild primitives exist without durable migration lifecycle.
     Scope: migration records, phases, watermarks, reentry, fail-closed incomplete
     state.
     Non-goals: compatibility fallbacks before 1.0, online multi-message complexity
     unless explicitly designed.

  4. 0.192 Import/Export Tooling And SQLite Migration Compatibility
     Problem: no portable data export/import tooling.
     Scope: JSONL/CSV/schema manifest, SQLite dump translation at tooling edge, import
     validation.
     Non-goals: embedding SQLite in canisters.

  5. 0.192 Operator Durability Diagnostics
     Problem: strong internal recovery state is not exposed as a compact operator health
     contract.
     Scope: marker/journal/fold/readiness/recovery status and counters.
     Non-goals: weakening fail-closed recovery or exposing raw unsafe repair controls.

  5. Recommended Next Design Doc

  Write 0.191 Query Admission And EXPLAIN first.

  This should come before constraints, migration, and import/export because it closes
  the most immediate product-safety gap: preventing public canister APIs from becoming
  unbounded admin query surfaces. It also reuses mature existing pieces: EXPLAIN
  descriptors, fluent access requirements, stable diagnostic codes, generated endpoint
  policy, and SQL write admission. It does not require persisted-format changes, so it
  is low-risk relative to the 0.190 reliability baseline.

  6. Draft Outline: 0.191 Query Admission And EXPLAIN

  Status: Proposed.

  Theme: Bounded query power for IC public, admin, diagnostic, and test/dev lanes.

  Context:
  IcyDB has a mature single-entity SQL subset, fluent query planning, rich EXPLAIN
  output, and bounded write policies. Public read admission is not yet a first-class
  contract.

  Goals:

  - Define explicit lanes: PublicRead, AdminAdHoc, DiagnosticExplain, DevTest.
  - Add a read-side QueryAdmissionPolicy.
  - Support required LIMIT, max returned rows, max scanned rows, max response bytes,
    index-required mode, full-scan rejection, sort/materialization budget, projection
    budget, and grouped-query budget.

  - Include admission decisions and rejections in text/JSON EXPLAIN.
  - Add stable diagnostic codes for public query rejection.
  - Wire generated public/admin surfaces to explicit policies.

  Non-goals:

  - SQLite compatibility.
  - Joins.
  - Runtime SQLite embedding.
  - Async/reentrant query execution.
  - Persisted-format changes.
  - Exact cost estimation beyond conservative IC-safe bounds.

  Existing baseline:

  - docs/contracts/SQL_SUBSET.md.
  - crates/icydb-core/src/db/session/sql/write_policy.rs.
  - crates/icydb-core/src/db/query/intent/access_requirement.rs.
  - crates/icydb-core/src/db/query/explain/json.rs.
  - crates/icydb-diagnostic-code/src/lib.rs.

  Proposed architecture:

  - Planner produces a QueryAdmissionSummary after access-path selection.
  - Public policies evaluate the chosen plan, not planner hints.
  - Admin/controller surfaces may opt into broader policies with explicit labels.
  - EXPLAIN-only mode reports whether execution would be admitted.
  - Response-size checks use conservative projection/output bounds where possible.
  - Generated canister config chooses policies per endpoint.

  Failure modes:

  - Missing required LIMIT.
  - Full scan rejected.
  - Index required but no index selected.
  - Estimated or bounded scan exceeds policy.
  - Materialized sort exceeds policy or is disallowed.
  - Projection response may exceed byte limit.
  - Grouped/materialized result exceeds memory policy.
  - Unsupported or nondeterministic SQL expression.

  Recovery/readiness behavior:

  - Admission runs only after recovery readiness checks.
  - Recovery-incomplete or marker-present unsafe state remains fail-closed.
  - Admission must never read raw stores or bypass accepted schema authority.
  - No marker, journal, or persisted schema format changes.

  API/schema changes:

  - New read admission policy types.
  - Generated build options for public/admin query policies.
  - New stable diagnostic codes.
  - EXPLAIN text/JSON admission fields.
  - No persisted schema change required.

  Validation plan:

  - Unit tests for policy admission/rejection.
  - SQL and fluent parity tests.
  - EXPLAIN JSON snapshot tests for admitted/rejected plans.
  - Generated canister integration tests for public vs admin endpoints.
  - Source-audit guard remains green for synchronous execution paths.
  - Docs update to SQL subset and generated endpoint contracts.

  Closeout criteria:

  - Public read endpoints cannot execute unbounded full scans by default.
  - Developers get precise diagnostic codes and EXPLAIN remediation.
  - Admin ad-hoc power remains explicit and controller-gated.
  - CI covers supported feature combinations.

  Relationship to 0.190:

  This strengthens the fail-closed IC model at the query boundary without changing
  marker/journal/recovery mechanics. It preserves 0.190 durability assumptions and
  prevents productization from turning SQL maturity into public-query risk.

  7. Non-Blocking Follow-Ups

  - Unified accepted constraint catalog.
  - Constraint-aware EXPLAIN/write admission.
  - Durable migration state machine with watermarks.
  - JSONL/CSV/schema manifest export.
  - Offline SQLite dump translation tooling.
  - Operator recovery status endpoint.
  - Checksum design after backup/import threat model.
  - Public API snapshot gate.
  - CI-enforced wasm import inspection.

  8. Risks

  - SQLite creep: adding SQL features without a bounded IC contract would dilute the
    architecture.

  - Runtime complexity: migration/import designs could accidentally duplicate recovery
    machinery instead of reusing its authority model.

  - Feature-gate leakage: SQL/admin/test helpers must stay gated and source-audited.
  - Public-query footguns: unbounded SELECT, sort, projection, and grouped
    materialization are the main immediate risk.

  - Reliability weakening: diagnostics and tooling must not bypass accepted schema
    snapshots, recovery readiness, or fail-closed persisted decoding.

  - Import/export threat: raw image import before checksum/threat modeling would
    undermine the durability story.

  Validation

  Read-only audit only. No files changed, no tests run.
