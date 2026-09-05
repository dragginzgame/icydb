# Nested-Relation Integrity And Recovery Audit

## Run Metadata And Scope

- Date: 2026-09-05 UTC.
- Method: `NREL-1.0` with `DOMAIN-1`; bounded investigation using the applicable
  storage/recovery audit obligations, plus nested-relation limit and reverse-edge
  contracts from the 0.253 design.
- Snapshot: `2e156dce34adedbf9981e473f1f16c3ebf267752`;
  tree `7eb88137093e4cd5ecb97a852967002480ee0416`.
- Source worktree: clean throughout the audit; only new report evidence added.
  [Source manifest](artifacts/source-snapshot.json) records inspected file hashes.
- Baseline: `N/A`; first run of this bounded investigation. The 0.253 landing
  receipts were context, not reused execution evidence.
- Comparability: non-comparable, no earlier identical scoped run or measured
  size/performance delta claimed.
- Trigger: user-requested follow-up to published 0.253 and audit-tooling work.
- Selected: repeated list/set/map projection, raw/lookup/reverse budgets,
  source replacement/deletion, same-batch target/source operations, shared
  commit preparation, interrupted recovery, and journal/schema idempotence.
- Excluded: Toko (explicit user instruction), new implementation, SQL DDL and
  physical migration publication, activation-job lifecycle, whole-database
  corruption sweeps, full suites, and new performance/Wasm baselines.

## Verdict

`FAIL`: the unique-target lookup cap counts successful validations rather than
all distinct validation requests. A separate nested-recovery proof gap remains.

All 27 selected repository tests passed: 25 core tests and two real-canister
tests. Two isolated probe checks also passed, one reproducing the counter defect.
Those successful test exits do not turn the demonstrated contract violation
into a passing audit. No row loss, orphaned reverse edge, or unsafe publication
was observed in the executed scenarios; untested nested interruptions are not
claimed safe.

## Findings

### NR-01 — MEDIUM: missing targets do not consume the unique-lookup budget

- Owner: `crates/icydb-core/src/db/relation/reverse_index.rs:154`,
  `RelationCommitBudget::validate_target_once`.
- Contract: section 10.2 of the 0.253 design caps distinct target validation
  requests at 3,276, whether satisfied by the final overlay or stable state.
- Evidence: `observed` is derived from `validated_target_keys.len() + 1`.
  A missing target returns `Ok(Some(key))` before insertion into that vector.
  In `project_row_with_target_lookup` (around line 609), missing keys are
  accumulated and iteration continues. `prepare_source_transition` checks the
  first missing target only after the complete projection returns.
- Reproduction: [isolated Rust probe](artifacts/lookup-budget-probe.rs) copies
  the method verbatim; byte-vector keys and a small error stand-in isolate its
  accounting. The copied body was mechanically compared with the audited source.
  The hit control performed 3,276 reads and rejected the next request. The miss
  probe accepted 3,277 distinct reads and retained a zero-length successful-key
  cache. The probe is not an end-to-end database mutation or stable-store cost
  measurement.
- Reachability: a repeated field can emit more than 3,276 distinct nonexistent
  target keys while remaining below the 5,460 raw-reference cap. The projection
  therefore performs excess lookups before returning its missing-target error.
- Consequence: the stated lookup-work bound and expected budget failure are not
  enforced on the miss-heavy path. This is a bounded resource-contract defect,
  not evidence that invalid references commit: missing targets still reject
  during preflight before marker persistence.
- Disposition: fix the shared budget owner in a separate authorized slice.
  Charge distinct attempted validations independently of successful-existence
  caching; do not let a cached miss become proof that a target exists. Add a
  maintained miss-heavy boundary test that also checks zero durable mutation.
- Trigger: before relying on the unique-lookup cap as a bound for rejected
  repeated-relation writes.

### NR-02 — MEDIUM: nested interruption/replay behavior lacks direct executable proof

- Owners: `testing/integration/tests/nested_relation_measurement_contract.rs`,
  `schema/audit/nested_relation/src/harness.rs`, and the relation recovery tests
  in `crates/icydb-core/src/db/session/write.rs`.
- Evidence: the repeated actor's six phases exercise normal insert, restriction,
  replacement, source deletion, same-batch insertion, and same-batch deletion.
  The raw-reference test checks exact admission and first-over-limit rejection.
  Neither test injects interruptions, upgrades/reopens the actor, or replays a
  nested reverse projection twice.
- The five executed `mixed_entity_recovery_after_` tests use the direct relation
  fixture. They prove shared recovery behavior and post-recovery target
  restriction, but do not exercise list/set/map extraction at those boundaries.
  Targeted discovery in core, integration, and the nested harness found no
  maintained nested/repeated interruption scenario covering this obligation.
- Consequence: the audit cannot directly establish nested reverse-edge parity
  and idempotence after interrupted replace/delete. This is missing proof,
  not a demonstrated recovery corruption bug.
- Disposition: add bounded nested-relation interruption coverage against the
  maintained current format. Assert final rows and reverse domains, old-edge
  removal/new-edge restriction, retry idempotence, and admission containment.
- Trigger: before claiming nested-relation recovery closeout from direct-only
  interruption evidence or extending nested mutation/recovery behavior.

## Mutation Inventory And Flow Comparison

| Scoped operation | Normal execution | Recovery | Evidence |
| --- | --- | --- | --- |
| Source insert/replace/delete | Final row overlay, accepted constraint schedule, old/new relation projection, coalesced reverse deltas, then durable marker | Journal row transitions use canonical before-state and accepted replay contexts, then the shared row preparer | `commit_window.rs:574`, `commit/prepare.rs:341`, `commit/recovery.rs:684`, `runtime_entity_catalog.rs:411` |
| Same-batch target/source changes | Complete final overlay supplies target existence and source-delete overrides | Canonical replay reader supplies the batch row view | Repeated actor phases 4/5; direct mixed-entity recovery family |
| Marker/journal lifecycle | Preflight precedes marker; apply errors retain marker/wake-up; success retires marker | Startup admits neither reads nor writes during incomplete recovery; journal tails may still require folding without a marker | `commit/guard.rs:294`, `commit/recovery.rs:113`, `commit/recovery.rs:242` |

Normal and replay preparation intentionally differ in target-existence
admission: normal writes validate the final target view; replay consumes the
already-admitted durable batch without treating transient row application order
as a new user constraint check. `CommitPrepareMode::validate_relation_targets`
owns that distinction. It is not a second relation interpreter or generated-model
fallback. Final-state equivalence, rather than identical transient phases, is
the relevant recovery obligation.

## Invariant, Ordering, And Error Checks

| Obligation | Evidence and conclusion |
| --- | --- |
| Accepted path authority | `accepted_nested_relation_source` derives terminal shape and field/member identities from the accepted row/value catalogs; invalid terminals fail closed. Source inspected. |
| Charge before deduplication | Nested walker charges terminal occurrences before conversion and canonical raw-key deduplication. Real actor accepts 5,460 repeated references and rejects 5,461 with the typed execution-budget code. |
| Batch/image budget ownership | One `RelationCommitBudget` is allocated before delete validation and row preflight. Old/new image budgets are shared across each row's scheduled relations. Exact counter tests pass; lookup misses remain NR-01. |
| Reverse identity/isolation | Keys encode the system domain, exact relation ID/generation, target identity, and source primary key. Unit identity/arity/accepted-authority tests pass. |
| Row/reverse coupling | Old/new projected entries are sorted and merged into prepared effects; only changed edges consume reverse-delta units. Commit preparation completes before marker creation. Repeated normal replace/delete flows pass. |
| Error ordering | Invalid targets and excessive raw references reject in preflight. Missing-target collection can delay the relation error beyond the distinct-lookup limit (NR-01). No error strings were used as typed runtime proof. |
| Idempotent shared controls | Journal duplicate append/retire/reopen and accepted-schema duplicate replay/fold tests pass. These do not substitute for nested reverse-edge retry assertions (NR-02). |

## Partial-Failure Symmetry

| Cut point | Direct mixed-entity proof | Nested list/set/map proof |
| --- | --- | --- |
| Marker persisted | PASS | Missing |
| Journal published | PASS | Missing |
| Row prefix published | PASS | Missing |
| All rows published | PASS | Missing |
| State materialized | PASS | Missing |

Direct recovery assertions check restored rows, target restriction through the
reverse relation, identity state, and entity revisions. Source tracing shows
nested projection converges on that same preparation path, but cannot establish
the missing nested behavioral assertions. NR-02 owns this gap; it is not counted
as five separate findings.

## Attack And Boundary Answers

- Ordinary admission is state-only; it does not replay work. The startup driver
  owns recovery, with marker and journal authority distinguished.
- A returned apply error retains durable recovery ownership and wake-up;
  test-only best-effort rollback is not production durability authority.
- Shared preflight and canonical replay contexts remain accepted-catalog-native.
  No generated-model runtime reconstruction was found in the inspected path.
- Duplicate raw references cannot evade the raw cap. Distinct missing targets
  can evade the lookup cap until another preflight condition rejects (NR-01).
- Normal repeated replacement releases old targets and protects the new target;
  source deletion releases its targets. These are behavioral checks, not a
  full physical reverse-index corruption sweep.
- Schema DDL/migration/activation publication questions are excluded from this
  run, not marked passing. They require their own affected-owner proof selection.

## Verification Readout

Cargo preflight used `CARGO_HOME=/home/adam/projects/icydb/.cache/cargo/icydb`
and `CARGO_TARGET_DIR=/home/adam/projects/icydb/target/icydb`, Rust 1.97.1,
and the locked dependency graph. Core selection was `-p icydb-core --lib
--features sql`. Integration selection was `-p icydb-testing-integration --test
nested_relation_measurement_contract`; that package enables its declared facade
features. Tests were listed before execution; none were ignored.

The core binary produced by the Cargo preflight was
`target/icydb/debug/deps/icydb_core-841b397c2d7e2115`. Its listed selections were
run directly with `RUST_TEST_THREADS=8`, avoiding contention with the independent
integration-target build. The binary and selection were not substituted.

| Selection/check | Result | Executed evidence |
| --- | --- | --- |
| `cargo test --locked -p icydb-core --lib --features sql db::relation::reverse_index::tests -- --list`, then the produced core binary with `db::relation::reverse_index::tests --nocapture` | PASS | 18 listed; 18 passed, 0 failed, 0 ignored |
| Core binary `mixed_entity_recovery_after_ --list`, then `mixed_entity_recovery_after_ --nocapture` | PASS | All five maintained cut points listed and passed; 0 failed/ignored |
| Core binary `exact_controls_append_replay_retire_and_reopen --list`, then the same filter with `--nocapture` | PASS | 1 listed and passed; 0 failed/ignored |
| Core binary `journaled_schema_candidate_replay_and_fold_are_idempotent --list`, then the same filter with `--nocapture` | PASS | 1 listed and passed; 0 failed/ignored |
| `cargo test --locked -p icydb-testing-integration --test nested_relation_measurement_contract repeated_actor_ -- --list` | PASS | Exactly two matching tests listed |
| Same integration selection with `-- --test-threads=1 --nocapture`, through the repository PocketIC wrapper | PASS | 2 passed, 0 failed/ignored; 3 unrelated tests filtered out; 35 seconds |
| `rustc --edition=2024 --test artifacts/lookup-budget-probe.rs -o <temporary-binary>`, then `--list` and `--nocapture` | PASS | 2 isolated checks; successful-hit control and confirmed miss-accounting defect; not repository runtime tests |
| Source-body comparison for the isolated probe | PASS | `validate_target_once` body identical to inspected production method |

The first PocketIC attempt was `BLOCKED` by sandbox localhost binding. After
approval for execution outside the sandbox, the identical focused selection
ran successfully on the pinned PocketIC 16.0.0 server. The wrapper created a
temporary local server and stopped it on completion; no unrelated network or
Toko environment was touched.

The SQL-only core test build emitted 64 dead-code warnings, primarily from
migration-only helpers. Compilation and execution succeeded. These warnings
were not a clippy result and were not silenced by changing source or flags.

Exploratory discovery found no `.cargo/config.toml`; Cargo paths were resolved
from the Makefile instead. Missing discovery paths and searches with no matches
were not counted as executable proof.

## Handoff

Only this new report, source manifest, and isolated probe were added. Runtime,
tests, audit definitions, versions, release notes, and historical reports were
not edited. Build caches and temporary local verification outputs were created
as needed. Full repository/workspace suites, Toko, and new size/performance
comparisons were not run. No fixes are included: follow-up is NR-01 and NR-02.
