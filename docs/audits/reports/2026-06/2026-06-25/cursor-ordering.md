# Cursor Ordering Audit - 2026-06-25

## 0. Run Metadata + Comparability Note

- scope: `cursor-ordering`
- recurring definition: `docs/audits/recurring/executor/cursor-ordering.md`
- compared baseline report path: `docs/audits/reports/2026-05/2026-05-11/cursor-ordering.md`
- code snapshot identifier: `d389eec3b` with dirty worktree context from earlier same-day audit/follow-up work
- method tag/version: `Method V5`
- comparability status: `non-comparable` - Method V5 replaces the removed `write_boundary_guards` integration-test target with current continuation source guards, accepted schema authority tests, and schema-fingerprint cache checks. It also adds focused grouped pagination evidence for current grouped page-finalization code.

## Audit Definition Update

| Audit Definition Drift | Update | Status | Risk |
| ---------------------- | ------ | ------ | ---- |
| Recurring method referenced `crates/icydb-core/tests/write_boundary_guards.rs`, but that top-level integration target no longer exists | Replaced with `db/executor/tests/continuation_structure.rs`, `db/executor/authority/entity.rs`, `db/query/fingerprint/shape_signature/tests/mod.rs`, and `db/session/tests/sql_surface.rs` evidence | PASS | Low |
| Method lacked an explicit current tag after verification-surface drift | Added `Method V5` and normalized verification statuses | PASS | Low |

## 1. Boundary Table

| Boundary | Owner | Verified? | Evidence | Risk |
| -------- | ----- | --------- | -------- | ---- |
| External cursor text decode | `db/cursor/string.rs`, `decode_optional_cursor_token` | Yes | source inspection; decode/error mapping test passed | Low |
| Token payload decode | `ContinuationToken::decode`, `GroupedContinuationToken::decode` | Yes | payload errors map through `CursorPlanError::from_token_wire_error`; cursor validation tests passed | Low |
| Scalar preparation | `prepare_cursor`, `cursor::spine` | Yes | `cursor_validation` passed; arity/type/signature/window cases covered | Low |
| Scalar revalidation | `PreparedLoadPlan::revalidate_cursor` -> `EntityAuthority` -> `PlannedContinuationContract` -> `cursor::revalidate_cursor` | Yes | source inspection confirms same `SchemaInfo`-backed spine as prepare | Low |
| Grouped preparation/revalidation | `prepare_grouped_cursor_token`, `revalidate_grouped_cursor`, grouped cursor spine | Yes | `cursor_validation`, grouped cursor unit tests, grouped page tests passed | Low |
| Index-range anchor validation | `db/cursor/anchor.rs` | Yes | canonical decode, identity, namespace, arity, envelope, and PK consistency source inspection; anchor/envelope tests passed | Low |
| Resume envelope rewrite | `db/index/envelope/mod.rs` | Yes | ASC upper-edge and DESC lower-edge empty-envelope tests passed | Low |
| Execution resume boundary | `LoadCursorResolver`, scalar/grouped route runtime preparation | Yes | source inspection shows cursor revalidation before runtime construction; composite/live-state tests passed | Low |
| Grouped page emission/resume | grouped continuation context, grouped fold page finalizers | Yes | SQL/fluent grouped cursor parity and grouped resume tests passed | Low |
| Schema-bound continuation identity | `PlannedContinuationContract::continuation_signature` | Yes | source inspection confirms accepted commit schema fingerprint is mixed into continuation signatures; cache fingerprint mismatch test passed | Low |

## 1A. Scalar Cursor Matrix

| Scenario | Owner | Protection | Evidence | Risk |
| -------- | ----- | ---------- | -------- | ---- |
| Missing required order | `validated_cursor_order` | validation-based invariant | source inspection | Low |
| Boundary arity mismatch | `validate_cursor_boundary_arity` | validation before execution | focused test passed | Low |
| Boundary value type mismatch | `validate_cursor_boundary_types` | `SchemaInfo`-backed validation before execution | focused test passed | Low |
| Primary-key type mismatch | structural primary-key cursor slot decode | validation before execution | focused test passed | Low |
| Signature/entity mismatch | `validate_cursor_signature` | token-boundary validation | focused test passed | Low |
| Initial offset mismatch | `validate_cursor_window_offset` | validation before execution | focused test passed | Low |
| Direction mismatch | `validate_cursor_direction` | validation before execution | source inspection; grouped direction has direct test | Low |
| Index-range raw anchor attached to non-range path | `validate_index_range_anchor` | validation before execution | source inspection | Low |

## 1B. Grouped Cursor Matrix

| Scenario | Owner | Protection | Evidence | Risk |
| -------- | ----- | ---------- | -------- | ---- |
| Grouped token on scalar path or scalar token on grouped path | `LoadCursorResolver`, token/lane-specific decode | execution-gated plus signature validation | cross-shape grouped test and source inspection | Low |
| Grouped direction mismatch | grouped cursor spine | validation-based | focused test passed | Low |
| Grouped offset mismatch | grouped cursor spine | validation-based | grouped unit tests | Low |
| Missing explicit grouped order | grouped cursor order-plan validation | structurally allowed; canonical group-key order applies | source inspection and grouped tests | Low |
| Empty explicit grouped order | `validate_grouped_cursor_order_plan` | validation-based | grouped unit tests | Low |
| Grouped page cursor emission | `GroupedContinuationContext::grouped_next_cursor`, grouped page finalizers | execution-gated and arity-checked | grouped SQL/fluent, limit-window, HAVING, projection, and DESC progression tests passed | Low |

## 1C. Accepted Authority Matrix

| Runtime Cursor Surface | Authority Source | Generated Fallback Possible? | Evidence | Risk |
| ---------------------- | ---------------- | ---------------------------- | -------- | ---- |
| Boundary field/expression type lookup | `SchemaInfo` passed into `validate_cursor_boundary_for_order` | No accepted-runtime fallback found | source inspection; accepted authority test passed | Low |
| Scalar cursor prepare/revalidate | `EntityAuthority::cursor_schema_info` | Fails closed if accepted schema info is absent | source inspection | Low |
| Field-path index anchor construction | accepted field-path index contract from `SchemaInfo` | No generated field-path fallback in accepted runtime branch | source inspection | Low |
| Expression index anchor construction | accepted `SchemaInfo` plus access contract | No generated wrapper lane found in cursor anchor construction | source inspection; dedicated replay test still absent | Low-Medium |
| Continuation signature | logical plan shape plus accepted commit schema fingerprint | Generated metadata is not reopened by cursor validation | source inspection; cache fingerprint mismatch test passed | Low |
| Source ownership | cursor and index-envelope modules | Executor cannot redefine cursor semantics without tripping guard tests | source guards passed | Low |
| Model-only/test construction | explicitly named test helpers | Allowed outside accepted runtime | source inspection | Low |

## 2. Failure Classification Table

| Failure Type | Expected Error | Actual Error | Correct? | Risk |
| ------------ | -------------- | ------------ | -------- | ---- |
| Invalid external cursor text | invalid continuation cursor | decode boundary maps text decode failures | Yes | Low |
| Malformed cursor payload | invalid continuation cursor payload | token wire errors map to cursor-plan taxonomy | Yes | Low |
| Boundary arity mismatch | continuation cursor boundary arity mismatch | focused test passed | Yes | Low |
| Boundary value type mismatch | continuation cursor boundary type mismatch | focused test passed | Yes | Low |
| Primary-key type mismatch | continuation cursor primary-key type mismatch | focused test passed | Yes | Low |
| Signature/entity mismatch | continuation cursor signature mismatch | focused test passed | Yes | Low |
| Initial offset mismatch | continuation cursor window mismatch | focused test passed | Yes | Low |
| Grouped direction mismatch | grouped direction mismatch payload | focused test passed | Yes | Low |
| Anchor outside envelope | anchor outside envelope / continuation invariant | focused envelope test passed | Yes | Low |
| Accepted schema info missing | cursor invariant before execution | `EntityAuthority::cursor_schema_info` fails closed | Yes | Low |

## 3. Envelope Safety Table

| Scenario | Can Escape Envelope? | Why / Why Not | Risk |
| -------- | -------------------- | ------------- | ---- |
| Cursor widens original index range | No | anchor is validated inside original planned envelope before resume-bound rewrite | Low |
| ASC anchor equals upper bound | No | lower becomes exclusive anchor and raw envelope becomes empty | Low |
| DESC anchor equals lower bound | No | upper becomes exclusive anchor and raw envelope becomes empty | Low |
| Anchor below lower or above upper | No | envelope containment rejects before bounds are rewritten | Low |
| Wrong index id | No | decoded anchor index id must match planned entity/index ordinal | Low |
| Wrong key namespace | No | decoded key kind must be `User` | Low |
| Wrong component arity | No | decoded component count must match planned index arity | Low |
| Boundary primary key differs from raw anchor | No | boundary/anchor primary-key consistency rejects mismatch | Low |

## 4. Duplication/Omission Safety Table

| Mechanism | Duplication Risk | Omission Risk | Explanation | Risk |
| --------- | ---------------- | ------------- | ----------- | ---- |
| Exclusive resume bound | Low | Low | resumed edge excludes the anchor row, and advancement checks are strict | Low |
| Last-emitted-row cursor boundary | Low | Low | next cursor derives from the last materialized row, not from untrusted token data | Low |
| Composite index-range pagination | Low | Low | focused monotonic pagination test matched unbounded execution | Low |
| Live delete between pages | Low | Low | focused live-state test allows shrink without cursor corruption | Low |
| Grouped pagination | Low | Low | grouped cursor parity, HAVING/offset/limit resume, projection, and DESC progression tests passed | Low |

## 5. Structural Mutation Table

| Property | Can Change? | Protection Mechanism | Risk |
| -------- | ----------- | -------------------- | ---- |
| entity path / continuation signature | No | shape and accepted-schema-bound signature validation | Low |
| initial offset | No | cursor window validation | Low |
| order field count | No | boundary arity validation | Low |
| boundary slot types | No | accepted `SchemaInfo`-backed validation | Low |
| primary-key slot type | No | structural primary-key decode after type validation | Low |
| index id | No | raw anchor identity validation | Low |
| key namespace | No | raw anchor namespace validation | Low |
| index component arity | No | raw anchor component-count validation | Low |
| scalar vs grouped lane | No | distinct token decode/prepare/revalidation and `LoadCursorResolver` mode checks | Low |
| accepted schema authority | No | `EntityAuthority` requires accepted schema info for cursor prepare/revalidate | Low |

## 6. Coverage Gaps

- No direct end-to-end test replays a pre-mutation cursor after an accepted schema/index DDL transition and asserts signature rejection. Source inspection shows accepted commit schema fingerprint binding, and cache fingerprint tests cover fail-closed plan reuse.
- Scalar cursor direction mismatch is source-inspected through `validate_cursor_direction`; grouped direction mismatch has direct test coverage.
- Expression-index cursor anchor construction uses accepted schema/access contracts, but there is no dedicated expression-index cursor replay test yet.

## 7. Overall Risk Assessment

- critical issues: none found
- medium-risk drift: none found in current runtime behavior
- low-risk observations: scalar validation remains centralized; grouped cursor emission now has direct current evidence; schema-bound signatures reduce stale-cursor risk after accepted schema changes
- tests to add if coverage thins: stale cursor replay after accepted schema mutation, scalar direction mismatch, expression-index cursor replay

## 8. Overall Cursor/Ordering Risk Index

**3/10**

Cursor ordering remains structurally healthy. The audit definition itself needed a refresh because it referenced a removed integration target, but the current cursor runtime passed the refreshed read-only baseline and the added grouped pagination checks.

## Verification Readout

- `PASS`: `cargo test -p icydb-core --features sql cursor_validation -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql pk_cursor_decode_error_mapping_is_explicit_for_all_cursor_variants -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql anchor_equal_to_upper_resumes_to_empty_envelope -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql desc_anchor_equal_to_lower_resumes_to_empty_envelope -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql load_composite_range_cursor_pagination_matches_unbounded_and_anchor_is_strictly_monotonic -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql load_cursor_live_state_delete_between_pages_can_shrink_remaining_results -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql runtime_continuation_semantic_definitions_stay_cursor_owned -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql runtime_cursor_signature_validation_internals_stay_cursor_owned -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql authority_finalization_uses_authority_schema_when_shape_is_missing -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql shared_query_plan_cache_schema_fingerprint_method_mismatch_fails_closed -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql sql_and_fluent_grouped_execution_match_groups_aggregates_and_cursor -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql grouped_select_helper_limit_window_emits_cursor_and_resumes_next_group_page -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql grouped_select_helper_multi_aggregate_having_offset_limit_cursor_resumes_consistently -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql grouped_select_pagination_preserves_cursor_with_extra_group_projection_columns -- --nocapture`
- `PASS`: `cargo test -p icydb-core --features sql grouped_select_additive_desc_order_preserves_rows_and_cursor_progression -- --nocapture`
