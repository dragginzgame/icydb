# Error Taxonomy Audit

## Metadata And Scope

- Request: improve and execute the next least recently run audit.
- Definition: `docs/audits/recurring/contracts/error-taxonomy.md`.
- Method: **ERROR-5 + DOMAIN-1**, frozen before test discovery/execution.
- Snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace `0.261.11`,
  with pre-existing Cargo dependency edits and the boundary-audit storage test.
  Earlier audit definitions, reports, and changelog edits are retained.
- Compared baseline: **N/A**, no comparable ERROR-5 run exists.
- Historical reference: [2026-05-14 run 01](../../../../05/14/error-taxonomy/01/report.md),
  Method V4. Historical tests are not counted as current execution.
- Comparability: **non-comparable (method change)**; affected deltas are
  `N/A (method change)`. Seven runtime classes and corruption containment
  remain stable qualitative anchors.

Error taxonomy, recovery consistency, and resource-model compliance were tied
at 2026-05-14 as the least recently run active scopes after the preceding
boundary and state-transition runs. Alphabetical scope order selects error
taxonomy. Archived audits and targeted cleanup playbooks are not recurring runs.

Requested baseline: inspect all runtime and public taxonomy axes and registered
leaf identities, with sampled producer-to-consumer propagation across query,
cursor, storage, schema, mutation, recovery, and public serialization. Exhaustive
source matches establish the origin mapping inventory; behavioral tests cover
all runtime classes, public wire axes, registered leaf codes, and named boundary
samples. This is not an exhaustive audit of every constructor/call site.

Excluded: full replay equivalence, pagination and index correctness, all schema
operations, all feature combinations, deployed endpoint/IC trap/upgrade proof,
resource accounting, security reachability, and performance. These are distinct
domain obligations; no PASS is assigned to them here. Native Candid round trips
prove representation, not deployed endpoint behavior.

### Source And Configuration Identity

| Input | Identity |
| --- | --- |
| Definition SHA-256 | `bd02f058f581e8e155cf8dac61b941cd37600db1c707210ed956d4867f3968ac` |
| Cargo.toml SHA-256 | `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c` |
| Cargo.lock SHA-256 | `695051feaba3c8de26eed05f5db590dddd2568f1f7493924e24b87ca08767716` |
| Pre-existing index scan test SHA-256 | `635996f907893ddb6d3ab4b2dd34930e2d1bed95707b2d5639940d1b0dd6f938` |
| Rust | `rustc 1.98.1 (48a229cea 2026-09-01)` |
| Cargo home / target | `.cache/cargo/icydb` / `target/icydb`, resolved by Makefile |
| Core / facade features | `sql,migration`; native library tests, eight test threads |
| Diagnostic-code features | defaults; native library tests, eight test threads |

All other Rust source is unchanged from HEAD. This run changes no production
code, tests, Cargo input, or release metadata. No network lifecycle actions.

## Method Review

The 313-line prior definition missed the numeric diagnostic registry, bounded
fact schemas, optional query-field context, and the actual five-field Candid
record. It mentioned a removed message-rewriting helper and response-error
surface. Its unconditional origin rules conflicted with deliberate recovery
relabeling. Its well-formed index/row mismatch rule could incorrectly demand an
invariant error where accepted storage inconsistency is corruption.

ERROR-5 reduces the method to 159 lines, includes the diagnostic-code crate,
separates runtime and public axes, requires classification by trust boundary,
and replaces repetitive tables with one propagation matrix. It no longer
requires live proposals and corrupt accepted state to have identical errors.
Adjacent audits own behavior; this audit owns meaning and projection.

## Authority Inventory

| Owner | Inspected contract |
| --- | --- |
| Core `src/error/mod.rs` | Seven runtime classes; eleven origins; structured details and fact constructors; all `with_origin` call sites are recovery/startup handoffs apart from tests |
| Core `db/query/intent/errors/mod.rs` | Validate, Intent, Plan and Execute branches; specialized cursor/unordered plan identity; execution wraps the original internal error |
| Core cursor and planner errors | User cursor rejection versus internal cursor/planner contract violation |
| Diagnostic-code `src/lib.rs`, `registry.rs` | Eight public classes, twelve public origins, 275 registered leaf codes, code/detail reconstruction |
| Diagnostic-code `src/fact.rs`, `query_field.rs` | Per-code fact sequences and bounds; accepted identity; restricted query-field role/context |
| Facade `src/error.rs`, `src/db/startup.rs` | Numeric projection, class/origin, bounded facts, query-field validation, startup failure projection |

Public diagnostic classes add `Query` to the seven runtime classes; public
origins add `Runtime` to the eleven core origins. The Candid Error fields are
`class`, `code`, `facts`, `origin`, and `query_field`. Convenience kind enums are
not the serialized record. Tests iterate every registered leaf code; they do
not execute every error-producing path.

## Classification And Propagation Matrix

Core paths below are relative to `crates/icydb-core/src/`. Proof groups refer
to the verification readout. All rows have sufficient scoped evidence.

| Boundary / expected meaning | Observed propagation and source | Proof |
| --- | --- | --- |
| All seven runtime classes retain public meaning | `error/mod.rs::ErrorClass::diagnostic_code`; facade `Error::from_internal_error`; origin-sensitive Store and Cursor codes retain their broad class | A class matrix and helpers; B facade matrix; C registry axes and leaf reconstruction |
| All eleven core origins have public equivalents | Exhaustive `ErrorOrigin::diagnostic_origin` and facade conversion matches; twelve public wire origins include Runtime | Source inventory plus C wire matrix; B representative origins, not an eleven-origin facade behavioral matrix |
| Query admission is distinct from runtime failure | `db/query/intent/errors/mod.rs::QueryError` projects Validate/Intent/Plan; Execute returns wrapped diagnostic/facts; cursor plans retain E6 | Source match inventory; A cursor/query tests; B validation, planning, execution projections |
| External cursor rejection versus internal cursor invariant | Invalid payload/signature/window is Unsupported/Cursor; internal cursor contract is InvariantViolation/Cursor; grouped rejection carries typed reason facts | A seven cursor producer tests plus conversion matrix; B grouped cursor Candid facts |
| Stored marker framing versus unsupported format | Truncated/trailing/oversized marker is Corruption/Store; future marker version and non-current control-slot magic are IncompatiblePersistedFormat/Serialize | A six selected commit-store tests, each with typed class/origin assertions |
| Accepted index points to missing authoritative row | `db/session/tests/unit_ordering.rs` exercises dynamic, SQL, and grouped paths; typed Corruption/Store; ordinary absent primary lookup remains a successful empty result | A accepted-index missing-row test; source through `assert_store_corruption` and query execution |
| Live mutation expectation failures | Mixed batch missing delete is NotFound; duplicate insert is Conflict; earlier staged mutation remains unapplied | A mixed-batch producer test; `db/session/write.rs` caller boundary |
| Rejected index proposal versus corrupt current domain | CandidateUniqueConflict maps to Conflict/Index; CurrentDomainMismatch, DuplicateUniqueKey, and PhysicalKeyDecode map to Corruption/Store | A typed candidate-domain test and two unsupported-lowering tests; `db/schema/mutation/user_index_domain.rs::into_internal_error` source mapping |
| Schema publication race remains policy-owned | `error/mod.rs` schema admission detail projects a specific compact code instead of generic internal failure | A schema DDL admission/publication diagnostic tests; producer scope is sampled, not every DDL operation |
| Recovery retains corruption meaning across an intentional origin change | `commit/recovery.rs` wraps verified-effect errors with Recovery origin; StartupRecoveryFailure carries the error; startup driver publishes typed diagnostic/facts | A repeated verification-failure test and malformed-marker durable-failure test; recovery/startup source trace |
| Startup terminal/retryable classification retains cause | `startup/mod.rs::classify_terminal_failure` retains diagnostic and facts; receipt stores leaf code/origin/facts; facade `StartupFailure::from_core` delegates to validated Error projection | A terminal classification and durable receipt producer tests; B public startup and bucket-mismatch Candid tests; source trace connects these component proofs |
| Relabeling preserves valid context | `InternalError::with_origin` retains class and numeric facts; incompatible origin-scoped details are dropped/rebuilt deliberately | A all-class helper and recovery numeric-fact tests |
| Invalid server facts fail closed | Core and facade validate per-code schemas and return InvariantViolation without partial facts; public field context is validated independently | A invalid fact projection; B malformed facts/field context and round trips; C fact/field schemas |
| Accepted identity survives projection | Mutation, constraint, relation, and query diagnostics retain typed bounded numeric context | A accepted identity/context tests; B mutation/relation Candid tests; C schema constraints |

A candidate uniqueness conflict and an already-published missing index effect
are different authority contexts. Their Conflict and Corruption classes are
intentional. No recovery equivalence claim is inferred from that comparison.

Untrusted decoded facade records remain data: deserialization does not validate
all context. `validated_query_field` performs explicit field-context validation;
server projection validates facts before emission. The malformed-client-context
tests preserve the base error while rejecting invalid context. This is not a
server-side downgrade and is not a claim that arbitrary client records are trusted.

## Verification Readout

All final selectors were listed with the same configuration before execution.
The `error::tests::` core filter selects 45 central taxonomy tests plus one index
unique-diagnostic test and one grouped resource-diagnostic test. Their assertions
were inspected and their additional coverage is included, not mistaken for
central-module coverage. Cursor selects seven tests; the other fourteen core
selectors each select one test.

| Check | Outcome | Selected | Passed | Failed | Ignored |
| --- | --- | ---: | ---: | ---: | ---: |
| Initial core discovery with stale future-version selector | FAIL | 65 | 0 | 0 | 0 |
| Corrected core listing, before adding two startup samples | PASS | 66 | 0 | 0 | 0 |
| Final A core listing | PASS | 68 | 0 | 0 | 0 |
| Initial B facade listing, error module only | PASS | 27 | 0 | 0 | 0 |
| Final B facade listing, including startup projection | PASS | 29 | 0 | 0 | 0 |
| C diagnostic-code listing | PASS | 27 | 0 | 0 | 0 |
| A core execution | PASS | 68 | 68 | 0 | 0 |
| B facade execution | PASS | 29 | 29 | 0 | 0 |
| C diagnostic-code execution | PASS | 27 | 27 | 0 | 0 |

The initial core listing used `commit_marker_rejects_future_version`, which
matched zero tests. Its process exited successfully, but discovery failed the
required format-version obligation. Before execution it was replaced with
`commit_marker_future_version`, selecting the inspected
`commit_marker_future_version_fails_closed` test. The other selectors were
unchanged at that correction. Two inspected startup samples were subsequently
added to A, and the startup projection family to B, then both were relisted.
No behavioral test failed. Final total: **124 passed, 0 failed, 0 ignored**.

Exact final commands, run from the repository root; each test invocation was
first run with an additional trailing `--list`, then without it:

```bash
export CARGO_HOME="$(make --no-print-directory -s print-cargo-home)"
export CARGO_TARGET_DIR="$(make --no-print-directory -s print-cargo-target-dir)"
export RUST_TEST_THREADS=8

cargo test --locked -p icydb-core --lib --features sql,migration -- \
  error::tests:: db::cursor::tests:: \
  commit_marker_rejects_truncated_envelope_header \
  commit_marker_rejects_truncated_envelope_payload \
  commit_marker_rejects_trailing_payload_bytes \
  commit_marker_rejects_oversized_stored_payload_as_corruption \
  commit_marker_future_version \
  commit_control_slot_rejects_corrupt_magic \
  accepted_index_missing_row_is_typed_store_corruption \
  mixed_batch_commits_cross_entity_then_rejects_late_failures_atomically \
  recovery_verification_failure_retains_marker_and_admission_barrier \
  field_path_index_request_lowering_fails_closed_for_unsupported_indexes \
  expression_index_request_lowering_fails_closed_for_unsupported_indexes \
  complete_domain_stage_rejects_unique_collision_from_candidate_logical_fill \
  heap_only_malformed_marker_becomes_a_durable_database_control_failure \
  terminal_classification_is_typed_and_pending_or_internal_failures_remain_retryable

cargo test --locked -p icydb --lib --features sql,migration -- \
  error::tests:: db::startup::tests::

cargo test --locked -p icydb-diagnostic-code --lib
```

For C discovery, append `-- --list`. Final execution filtered out 2815 other
core tests and 57 other facade tests; diagnostic-code's 27 library tests are
the focused taxonomy package target. No full repository/workspace suite,
canister deployment, timing benchmark, or network lifecycle operation ran.

## Verdict And Follow-Up

**PASS** for the declared classification/projection scope. No actionable
misclassification or unresolved required verification gap was found. The stale
selector was resolved before behavioral execution. No runtime fix is proposed.

The separate requested holistic audit-definition review is inspection-only;
its consolidation recommendations are returned in the conversation, not turned
into runtime findings or automatic definition retirements here.

Complexity: two documentation files changed/added. The definition removes 154
net lines (313 to 159); this report records the run evidence. Runtime shape,
behavior axes, and production debt are unchanged. The method is simpler while
covering current diagnostic representation. Wasm bytes, cycles, and instruction
deltas are unmeasured; no performance claim is made. Governance-only changes
need no release note. Full-suite and deployed endpoint qualification remain
outside this audit's scope.
