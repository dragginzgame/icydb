# Testing Strategy

This document defines **all test categories in IcyDB**, what each category is responsible for, where it lives in the repository, and—most importantly—**what it is allowed to depend on**.

The goal is to keep tests:

* Architecturally honest
* Maintainable over time
* Clear about intent and scope

---

## Test Taxonomy (Authoritative)

Classify tests by the boundary they exercise. Regression coverage is a discipline
within these categories, rather than a separate execution lane.

| Test type | Purpose | Location | Runtime |
| --- | --- | --- | --- |
| Unit | Local module/type invariants | Owning crate's test modules | Native |
| Native integration | Cross-module or public crate behavior | `crates/*/tests` and owning subsystem harnesses | Native |
| Schema / planner | Accepted-schema, lowering and planning semantics | `crates/icydb-core` | Native |
| Generated surface | Macro/model expansion, generated containers and adapter boundaries | `schema/test/fixtures`, `testing/model-*`, `testing/endpoint-installation`, model crate tests | Native or compile-only |
| Canister E2E | Generated endpoints, authorization, persistence, message boundaries and lifecycle | `testing/integration/tests`, using actors under `canisters/test` and `canisters/audit` | PocketIC via IC Testkit |
| Regression | A minimal reproduction of a repaired behavior | The category owning that behavior | Depends on the boundary |

The [root workspace](Cargo.toml) owns package membership;
[the integration manifest](testing/integration/Cargo.toml) explicitly declares
its test binaries. Do not infer test execution from fixture presence alone.

---

## Fixture Ownership (Authoritative)

Use fixture modules according to ownership, not convenience.

> `schema/test` = generated-model and canister schema fixtures
> `crates/*/testing` = engine-internal fixtures

### `schema/test`

Use for shared fixtures that represent runtime-facing behavior:

* Macro/schema contract fixture entities reused across test crates
* Canister integration fixture schemas
* Declarative schemas required by test framework coverage

### `crates/*/testing`

Use for crate-local engine scaffolding and internal invariants:

* Planner/executor/internal model scaffolds
* Internal helper constructors for white-box tests
* Boundary-contract fixtures that should not leak across crates

### Rule

Reusable generated schemas belong in `schema/test`; canister tests consume the
schemas appropriate to their actors. Crate-internal fixtures stay with the owning
crate. The standalone `icydb-testing-test-fixtures` package also owns native tests
of generated collection and newtype behavior; it remains exercised as a workspace
member even when another package does not depend on it.

---

## Canister SQL Query Ownership (Authoritative)

Generated controller-gated SQL administration endpoints are emitted by
`icydb-model` actor glue and execute through the public `icydb` facade.
Application caller-facing read endpoints remain canister-owned.

### Ownership Rule

* `icydb-model` owns generated actor wiring and configured dispatch for
  controller-gated `icydb_query`, DDL, fixture, and update administration
  endpoints.
* `icydb` owns the stable session helpers used by generated actor glue;
  `icydb-core` owns SQL lowering and execution semantics.
* Application-specific, caller-facing read endpoints are authored in the
  canister crate and use the bounded typed or dynamic read surfaces after
  application authorization.
* SQL statement entity parsing and identifier matching semantics remain SQL-owned helpers (for example `statement_entity_name` and `identifiers_tail_match`).
* Generated or canister-owned routing helpers may compose SQL-owned helpers,
  but must not re-implement SQL identifier semantics.

### Regression Rule

IC testkit integration coverage must assert deterministic dispatch behavior:

* Runtime routing is keyed by SQL entity identifier.
* Unsupported entity failures are direct and stable.
* Error payloads must not depend on speculative per-entity trial order (no chained `last_error` fallback behavior).

---

## 1. Unit Tests

### Purpose

Validate **local invariants** of a single module or type.

Examples:

* Ordering invariants
* Boundary conditions
* Error classification
* Small pure functions

### Characteristics

* Very fast
* Very focused
* Minimal setup

### Location

* Inline `#[cfg(test)] mod tests` next to the code

### Allowed Dependencies

* Same module or sibling modules
* Private helpers

### Forbidden

* Cross-crate behavior assertions
* Schema-level reasoning
* Persistence

---

## 2. Integration Tests (Intra-crate)

### Purpose

Verify **multiple modules within the same crate** work together correctly.

Examples:

* Query planner + predicate normalization
* Index model + schema validation
* Commit ops + executor ordering

### Location

Either:

* `#[cfg(test)] mod tests` using internal APIs, or
* `crates/<crate>/tests/*.rs`

### Allowed Dependencies

* Internal (non-public) APIs
* Real data structures

### Forbidden

* IC execution
* Canister lifecycle

---

## 3. Schema / Planner Tests (Critical Category)

### Purpose

Assert **semantic correctness** of:

* Accepted schema contracts
* Index selection
* Access path planning
* Predicate normalization
* Determinism guarantees

These tests are *pure logic* and are the primary guardrail for query correctness.

### Location

**`icydb-core`** (intentionally)

These tests live in core because they must:

* Call non-public planner functions
* Assert on internal types (`AccessPlan`, `AccessPath`)
* Avoid public "test hatches"

### Key Rule

> Planner logic must depend on **accepted schema**, not generated application
> models.

Tests in this category should:

* Construct accepted snapshots and `SchemaInfo` through canonical test builders
* Avoid generated entity declarations, model registries, or runtime callbacks

### Accepted Contract Construction Rules (Tests)

When a test needs accepted schema:

* Use the nearest canonical persisted-snapshot or accepted-catalog test builder
* Add a short comment when the fixture intentionally bypasses a public frontend
* Do not reconstruct accepted authority from generated model metadata

### Forbidden

* Canisters
* Persistence
* Serialization
* Proc-macro reliance

---

## Supported Feature-Matrix Checks

No-default support means the public facade and core compile without default
features, and their no-default smoke/unit suites run. The supported no-default
test matrix is:

* `cargo check -p icydb --no-default-features`
* `cargo check -p icydb-core --no-default-features`
* `cargo check --workspace --no-default-features`
* `cargo test -p icydb --no-default-features`
* `cargo test -p icydb-core --no-default-features`

Strict Clippy is enforced on the primary feature-rich workspace surface and on
the no-default `sql` configuration selected by `ci-core`. The maintained feature
sets are declared in [the facade](crates/icydb/Cargo.toml) and
[core](crates/icydb-core/Cargo.toml); do not invent feature combinations. No-default
all-targets Clippy is not a supported release gate because disabling SQL leaves
many SQL/test helper surfaces intentionally unused.

---

## 4. Generated Surface and Canister E2E Tests

### Native generated-surface coverage

Use model crate tests, the `testing/model-*` adapter packages,
[endpoint-installation fixtures](testing/endpoint-installation), and
[generated-schema fixtures](schema/test/fixtures) to check expansion, public
container behavior and model/runtime dependency boundaries. These native or
compile-only checks do not establish message, stable-memory or upgrade behavior.

### Canister E2E coverage

[The integration package](testing/integration) runs real test actors under
PocketIC through IC Testkit. Its tests cover generated endpoints, reads and
writes, caller/controller authorization, schema application, durable mutations,
interruption recovery and upgrades. Assert public canister results and retained
state; internal planner assertions belong in core tests.

IC Testkit owns server selection, provisioning, admission and lifecycle. IcyDB
selects actor inputs and scenarios through the maintained Testkit contract.
[Local setup](INSTALLING.md) documents the selected tool preparation; do not add
consumer-owned PocketIC pins or installers.

### Execution and CI coverage

The [Makefile](Makefile) owns local test selections and
[the CI workflow](.github/workflows/ci.yml) owns their automatic placement.
A native test, compiled actor or source declaration is not evidence of a live
canister scenario. Current lane selection is:

| Lane / target | Selected coverage | Execution |
| --- | --- | --- |
| `ci-core` | No-default facade/core checks and tests, explicit SQL checks and Clippy | CI |
| `ci-workspace` | Workspace Clippy/tests, all registered integration targets type-checked/linted, and the nine durability/lifecycle/authority integration binaries below | Native and PocketIC CI |
| `ci-sql-tier-a` | One seeded sample per scheduled SELECT/mutation obligation, using IcyDB, bundled SQLite and the independent mutation model; plus `sql_correctness` | Native CI |
| `ci-sql-tier-b` | `sql_canister` and `sql_perf_audit` | PocketIC CI |
| `test-durability` | Core commit/mutation-job suites plus the shared `_test-durability-integration` selection below | Explicit local selection; integration leaf also runs in workspace CI |
| `test-canister-artifact-contract` | Production/local Wasm export policy test, explicitly including its ignored test | Explicit local selection; also included by `make test` |
| `test-sql-tier-c-shard`, `test-sql-tier-c-merge`, `test-sql-tier-c-replay` | Generated/reference receipts and failure replay | Explicit local selection |

The SQL coverage manifest credits enabled tests registered by Cargo in the
workspace or Tier B selections above. Ignored tests, including conditional
`cfg_attr(..., ignore)`, and unselected integration binaries cannot satisfy its
automatic evidence obligations. The artifact-contract test remains a separately
requested check and is not credited by the automatic provider registry. Cargo
registration and CI topology admission establish eligibility, not an observed
pass of the referenced test; actual results belong to their CI run or local log.

Provider-to-CI bindings participate in the manifest revision used by Tier C.
After changing those declarations, rederive its golden identity and regenerate
receipts for the maintained manifest; do not reuse old receipts as current proof.

`ci-workspace` uses Clippy's `--tests` selection for the integration package,
including ignored test bodies. It also executes the same unfiltered
`_test-durability-integration` leaf as `test-durability`: `convergence_candidate`,
`durable_mutation_job_scale`, `recovery_closeout`, `logical_memory`,
`schema_migration_closeout`, `lifecycle_participant`, `read_authority`, `sql_guard`
and `schema_guard`. The workspace lane prepares and admits the locked server
through Testkit before dispatch; the core and Tier A lanes remain native-only.
Workflow fixtures reject missing test families, filters, ignored-only or
compile-only substitutions and omitted CI routing. Compile coverage alone is
not recovery, upgrade or authorization qualification; actual installed results
belong to the CI run
([#42 — PR CI durability integration coverage](https://github.com/dragginzgame/icydb/issues/42)).
`make test-unit` also executes workspace integration test binaries. For
development, select one binary/test
or use `test-integration-feedback` with its required `TEST_TARGET` and `TEST_NAME`;
that target runs one exact test and then the complete selected binary.

The terminal CI `check` job aggregates static, Rust and native macOS host lanes.
Branch protection and release publication additionally require `dependency_msrv`
and `wasm_size_report`; [branch-protection guidance](.github/BRANCH_PROTECTION.md)
owns that selection. Linux portable-shell fixtures do not qualify native macOS
or live canister behavior. Report skipped or ignored scenarios as unrun evidence.

Both native macOS lanes run the CLI observability report and endpoint-decoder
tests through the nonempty-test runner, alongside diagnostic escaping and
migration command tests. Report coverage includes ASCII layout, wide/combining
Unicode and control-bearing schema cells. These exercise IcyDB's integration
with Host Text; Host owns terminal escaping, cell widths and table padding.
Linux results do not substitute for those native passes
([#362 — Qualify the CLI Host Text integration](https://github.com/dragginzgame/icydb/issues/362)).

---

## 5. Regression Tests

### Purpose

Ensure **specific previously-broken behavior never regresses**.

Regression tests are not a separate location—they are a **label and discipline**.

### Rules

* Live next to the code that was fixed
* Reference the bug or invariant in comments
* Prefer minimal reproductions

Example:

```rust
// Regression: ORDER BY on non-orderable field must error (0.5.25)
```

---

## Test Placement Contract

Tests must live at the boundary that owns the behavior.

### Ownership Rule

A test should live in the lowest layer that promises the behavior being asserted.

Examples:

* Cursor contract assertions belong under cursor tests.
* Route policy matrix assertions belong under route tests.
* End-to-end execution behavior belongs in executor integration harnesses.

### Default Placement Rule

Prefer co-located test files over large inline blocks:

* Directory module: `tests.rs`
* Leaf file module: `*_tests.rs` sibling

Inline `#[cfg(test)] mod tests` is acceptable for small micro-tests only.

### Inline Test Limit

Inline tests are allowed only when all are true:

* Small (about `< 100` lines total)
* No shared fixtures/helpers
* Validates a local helper/invariant
* Improves readability of the code directly above it

When inline tests outgrow this, move them to `tests.rs` or `*_tests.rs`.

### Cross-Subsystem Rule

Cross-subsystem behaviors must live in explicit harnesses, not scattered leaf modules.

Examples:

* `crates/icydb-core/src/db/executor/tests/**`
* `crates/icydb-core/tests/*.rs`

### Test-Scaffolding Rule

Test scaffolding must not leak into production APIs.

If tests need helpers:

* Keep helpers in test modules/harness files, or
* Keep helpers behind `#[cfg(test)]` with crate-private visibility

Do not add public exports solely to satisfy tests.

---

## What We Explicitly Do NOT Have

* ❌ `__internal` test-only APIs
* ❌ Dual schema languages ("real" vs "test")
* ❌ Generated entity/index models used as accepted runtime authority
* ❌ Unlabelled hand-built accepted state that bypasses canonical validation
* ❌ `include_str!` used to enforce architectural invariants
* ❌ String-content matching used for semantic error classification/assertion

If a test requires one of these, the architecture is wrong.

---

## Design Principles (Non-Negotiable)

1. **Accepted authority over generated types**
   Planner logic depends on accepted schema contracts, not application-model
   declarations.

2. **Black-box vs white-box clarity**
   System tests are black-box. Core tests are white-box. Never mix them.

3. **No test-only public APIs**
   Tests live where internals already exist.

4. **Determinism is testable**
   Planner output must be stable across runs and input ordering.

---

## Architectural Guard Enforcement (Required)

Architectural invariants must be enforced structurally, not via source-text scanning.

### Allowed `include_str!` usage

`include_str!` is allowed only for fixture-style inputs:

* Snapshot fixtures
* Parser fixtures
* Large golden data inputs

### Prohibited `include_str!` usage

Do not use `include_str!` to police architecture, layering, or module boundaries.

Examples of prohibited use:

* Asserting a module contains/does not contain specific symbols
* Enforcing boundary ownership through source text
* Enforcing semantic authority through string search

### Required alternatives

Use structural tests instead:

* Type/signature guard tests
* Compile-fail visibility boundary tests
* Boundary contract tests (planner-owned semantics, executor invariant-only)
* Semantic parity/rejection tests over typed contracts

Rule:

> If correctness depends on source text content, architectural authority is leaking.

---

## Error Assertion Guidelines

Error tests must assert semantics through structure (types/variants), not presentation strings.

### Smells (Avoid)

* Message prefix/substring matching for behavior:
  * `starts_with("invalid cursor")`
  * `contains("Unsupported aggregate kind")`
* Asserting `Display` text for semantic correctness:
  * `assert_eq!(format!("{}", err), "...")`
* Layer-leaking deep variant matching when contract is higher-level
* Catch-all variant assertions when specific class/kind matters:
  * `matches!(err.kind, ErrorKind::Runtime(_))`
* Exact variant-count assertions unless freezing public ABI:
  * `variant_count::<ErrorKind>() == ...`
* Repeated deep exhaustive matching across many modules (semantic authority diffusion)

### Approved Patterns

* Boundary-level variant matching for semantic mapping guarantees
* Policy rejection matching on typed policy enums
* Signature guard checks via the type system
* Semantic helper assertions (class/origin/kind preserved across boundaries)

### Layering Rule

Test at the semantic boundary of the layer under test:

* Facade tests: assert facade taxonomy contracts
* Query-layer tests: assert query-layer taxonomy contracts
* Internal planner/executor tests: assert internal typed contracts only when they are the boundary

Do not over-fit tests to deeper implementation details than the layer contract requires.

### Formatting Tests

Testing `Display`/`to_string()` is valid only when testing formatting itself.
It is not valid as a substitute for semantic error assertions.

Rule:

> If correctness depends on error string content, semantic authority is leaking.

---

## When Adding a New Test, Ask

1. Am I testing *local logic* or *system behavior*?
2. Do I need internals, or only public APIs?
3. Can this be expressed purely in terms of models?

If the answers are unclear, stop and reassess.

---

## Summary

* Unit tests protect invariants
* Integration tests protect module composition
* Schema / planner tests protect correctness
* E2E tests protect the generated surface
* Regression tests protect history

Each class has a home. Each home has rules.

Violating those boundaries is how test suites rot.
