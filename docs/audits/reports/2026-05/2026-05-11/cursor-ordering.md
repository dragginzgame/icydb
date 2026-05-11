# Cursor Ordering Audit - 2026-05-11

## Run Metadata + Comparability Note

- scope: `cursor-ordering`
- recurring definition: `docs/audits/recurring/executor/cursor-ordering.md`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-12/cursor-ordering.md`
- code snapshot identifier: `a4ed38245`
- method tag/version: `Method V4`
- comparability status: `non-comparable` - Method V4 adds accepted runtime schema authority, scalar/grouped lane matrices, live-state cursor drift checks, and the current write-boundary guard baseline. The 2026-03-12 baseline only recorded two envelope regression tests.

## Boundary Table

| Boundary | Owner | Verified? | Evidence | Risk |
| -------- | ----- | --------- | -------- | ---- |
| External cursor text decode | `db/cursor/string.rs`, `decode_optional_cursor_token` | Yes | `pk_cursor_decode_error_mapping_is_explicit_for_all_cursor_variants` | Low |
| Token payload decode | `ContinuationToken::decode`, `GroupedContinuationToken::decode` | Yes | cursor decode mapping and grouped cursor validation tests | Low |
| Scalar preparation | `prepare_cursor`, cursor spine | Yes | `cursor_validation` test filter | Low |
| Scalar revalidation | `revalidate_cursor`, cursor spine | Yes | source inspection shows same spine and `SchemaInfo` path as prepare | Low |
| Grouped preparation | `prepare_grouped_cursor_token`, grouped cursor spine | Yes | grouped cursor direction/cross-shape tests under `cursor_validation` | Low |
| Index-range anchor validation | `db/cursor/anchor.rs` | Yes | anchor containment, identity, and boundary consistency source inspection | Low |
| Resume envelope rewrite | `db/index/envelope/mod.rs` | Yes | ASC upper-edge and DESC lower-edge empty-envelope tests | Low |
| Runtime execution resume | executor pagination paths | Yes | composite monotonic pagination and live delete-between-pages tests | Low |

## Scalar Cursor Matrix

| Scenario | Owner | Protection | Evidence | Risk |
| -------- | ----- | ---------- | -------- | ---- |
| Missing required order | `validated_cursor_order` | validation-based | source inspection | Low |
| Boundary arity mismatch | `validate_cursor_boundary_arity` | validation-based before execution | `load_cursor_rejects_boundary_arity_mismatch_at_plan_time` | Low |
| Boundary value type mismatch | `validate_cursor_boundary_types` | validation-based before execution | `load_cursor_rejects_boundary_value_type_mismatch_at_plan_time` | Low |
| Primary-key type mismatch | `decode_typed_primary_key_cursor_slot` | validation-based before execution | `load_cursor_rejects_primary_key_type_mismatch_at_plan_time` | Low |
| Signature/entity mismatch | `validate_cursor_signature` | token-boundary validation | `load_cursor_rejects_wrong_entity_path_at_plan_time` | Low |
| Offset mismatch | `validate_cursor_window_offset` | validation-based before execution | `load_cursor_rejects_offset_mismatch_at_plan_time` | Low |
| DESC resume from boundary | cursor boundary comparison and directional envelope rewrite | execution-gated plus envelope structural check | `desc_cursor_resume_matrix_matches_unbounded_execution` covered by suite; focused DESC lower-edge envelope test passed | Low |

## Grouped Cursor Matrix

| Scenario | Owner | Protection | Evidence | Risk |
| -------- | ----- | ---------- | -------- | ---- |
| Grouped token on scalar path | token/lane-specific decode plus signature validation | validation-based | `grouped_cursor_rejects_cross_shape_resume_token_at_plan_time` | Low |
| Direction mismatch | `validate_grouped_cursor_direction` | validation-based | `grouped_cursor_rejects_descending_direction_at_plan_time` | Low |
| Offset mismatch | `revalidate_grouped_cursor`, grouped cursor spine | validation-based | grouped cursor unit tests and source inspection | Low |
| Missing explicit order | `validate_grouped_cursor_order_plan` | structurally allowed; grouped defaults to group-key order | source inspection | Low |
| Empty explicit grouped order | `validate_grouped_cursor_order_plan` | validation-based | cursor unit tests | Low |
| Grouped page cursor emission | grouped fold page finalization | execution-gated | grouped pagination tests exist; not rerun in this focused audit | Moderate |

## Accepted Authority Matrix

| Runtime Cursor Surface | Authority Source | Generated Fallback Possible? | Evidence | Risk |
| ---------------------- | ---------------- | ---------------------------- | -------- | ---- |
| Boundary field type lookup | `SchemaInfo` passed into `validate_cursor_boundary_for_order` | No accepted-runtime fallback found | source inspection; `cursor_boundary_validation_uses_authority_schema_info` passed | Low |
| Scalar cursor prepare/revalidate | `EntityAuthority::cursor_schema_info` | Fails closed if absent | source inspection; guard test passed | Low |
| Field-path index anchor key construction | accepted field-path index contract from `SchemaInfo` | No field-path generated fallback | source inspection; guard tests passed | Low |
| Expression index anchor key construction | accepted access contract/schema info lane | no generated wrapper lane in accepted runtime | write-boundary guards passed | Low |
| Continuation signature | logical plan continuation signature | no generated metadata fallback observed | shape-signature tests exist; not rerun in this focused audit | Moderate |
| Model-only/test construction | explicitly named test/model-only helpers | Allowed outside accepted runtime | guard tests passed | Low |

## Failure Classification Table

| Failure Type | Expected Error | Actual Error | Correct? | Risk |
| ------------ | -------------- | ------------ | -------- | ---- |
| Invalid external cursor text | invalid continuation cursor | decode mapping tested | Yes | Low |
| Malformed cursor payload | token wire error mapped to cursor-plan taxonomy | decode mapping tested | Yes | Low |
| Boundary arity mismatch | continuation cursor boundary arity mismatch | focused test passed | Yes | Low |
| Boundary value type mismatch | continuation cursor boundary type mismatch | focused test passed | Yes | Low |
| Primary-key type mismatch | continuation cursor primary-key type mismatch | focused test passed | Yes | Low |
| Signature/entity mismatch | continuation cursor signature mismatch | focused test passed | Yes | Low |
| Initial offset mismatch | continuation cursor window mismatch | focused test passed | Yes | Low |
| Grouped direction mismatch | continuation cursor direction mismatch | focused test passed | Yes | Low |
| Anchor outside envelope | anchor outside envelope | focused test passed | Yes | Low |
| Accepted schema info missing | cursor invariant violation before execution | source inspection; guard requires fail-closed path | Yes | Low |

## Envelope Safety Table

| Scenario | Can Escape Envelope? | Why / Why Not | Risk |
| -------- | -------------------- | ------------- | ---- |
| Cursor widens original index range | No | anchor is validated inside original envelope before bound rewrite | Low |
| ASC anchor equals upper bound | No | lower becomes exclusive anchor and raw envelope collapses empty | Low |
| DESC anchor equals lower bound | No | upper becomes exclusive anchor and raw envelope collapses empty | Low |
| Anchor below lower or above upper | No | `validate_index_scan_continuation_envelope` and cursor anchor envelope check reject | Low |
| Wrong index id | No | anchor decoded key id must match planned index ordinal/tag | Low |
| Wrong key namespace | No | decoded key kind must be `User` | Low |
| Wrong component arity | No | decoded component count must match planned index arity | Low |
| Boundary primary key differs from raw anchor | No | boundary/anchor primary-key consistency check rejects | Low |

## Duplication/Omission Safety Table

| Mechanism | Duplication Risk | Omission Risk | Explanation | Risk |
| --------- | ---------------- | ------------- | ----------- | ---- |
| Exclusive resume bound | Low | Low | anchor row is excluded and candidate advancement is strict | Low |
| Last-emitted-row cursor boundary | Low | Low | next cursor boundary derives from the last materialized row | Low |
| Composite range pagination | Low | Low | focused monotonic pagination test matched unbounded execution | Low |
| Live delete between pages | Low | Low | focused live-state test allows shrink without cursor corruption | Low |
| Grouped pagination | Low | Moderate | grouped lane has dedicated guards; focused run did not rerun the full grouped pagination matrix | Moderate |

## Structural Mutation Table

| Property | Can Change? | Protection Mechanism | Risk |
| -------- | ----------- | -------------------- | ---- |
| entity path / signature | No | continuation signature validation | Low |
| initial offset | No | window offset validation | Low |
| order field count | No | boundary arity validation | Low |
| boundary slot types | No | `SchemaInfo`-backed boundary type validation | Low |
| primary-key slot type | No | typed PK decode after boundary validation | Low |
| index id | No | raw anchor identity validation | Low |
| key namespace | No | raw anchor key-kind validation | Low |
| index component arity | No | raw anchor component count validation | Low |
| scalar vs grouped lane | No | separate token types and grouped/scalar validation paths | Low |
| accepted schema authority | No | cursor authority guard requires `SchemaInfo` through session/executor authority | Low |

## Coverage Gaps

- Grouped page cursor emission/resume has broad test coverage, but this focused audit did not rerun the full grouped SQL pagination matrix.
- Continuation signature invalidation after accepted schema contract changes is covered indirectly through shape-signature tests and accepted schema fingerprint guards; no single end-to-end stale-cursor-after-schema-mutation test exists yet because catalog-native runtime schema mutation publication is not enabled beyond metadata-safe field additions.
- Expression-index cursor anchor construction depends on the accepted access contract lane; guard coverage proves no generated wrapper lane remains, but a dedicated expression-index cursor replay test would be useful when schema mutation work starts allowing expression index rebuild publication.

## Overall Risk Assessment

- critical issues: none found
- medium-risk drift: grouped pagination and future schema mutation cursor invalidation should be watched as 0.152 adds accepted snapshot mutation semantics
- low-risk observations: scalar cursor validation is centralized, envelope rewrite semantics are structurally small, and accepted `SchemaInfo` is now threaded through the cursor spine
- tests to add if coverage thins: end-to-end stale cursor after accepted schema mutation, and an expression-index cursor replay case once rebuild publication is implemented

## Overall Cursor/Ordering Risk Index

**3/10**

Cursor ordering remains structurally healthy. The main residual risk is future drift from catalog-native schema mutation publication and expression-index rebuild visibility, not current cursor runtime behavior.

## Verification Readout

- `cargo test -p icydb-core cursor_validation --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core pk_cursor_decode_error_mapping_is_explicit_for_all_cursor_variants --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_equal_to_upper_resumes_to_empty_envelope --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core desc_anchor_equal_to_lower_resumes_to_empty_envelope --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core load_composite_range_cursor_pagination_matches_unbounded_and_anchor_is_strictly_monotonic --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core load_cursor_live_state_delete_between_pages_can_shrink_remaining_results --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core --test write_boundary_guards -- --nocapture` -> PASS

## Follow-Up Actions

- owner boundary: cursor plus schema mutation publication
- action: add an end-to-end stale cursor rejection test after catalog-native schema mutations can publish accepted snapshot changes beyond current metadata-safe additions
- target report date/run: next `cursor-ordering` run after 0.152 rebuild publication work begins
