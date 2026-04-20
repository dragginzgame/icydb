use super::support::*;

type VerboseDiagnosticsMapBuilder = fn() -> BTreeMap<String, String>;
type VerbosePushdownMatrixCase<'a> = (&'a str, VerboseDiagnosticsMapBuilder, &'a str);
type VerboseFallbackMatrixCase<'a> = (&'a str, VerboseDiagnosticsMapBuilder);

#[test]
fn explain_execution_verbose_top_n_seek_shape_snapshot_is_stable() {
    let verbose = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::desc("id"))
        .offset(2)
        .limit(3)
        .explain_execution_verbose()
        .expect("top-n verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Streaming",
        "diag.r.continuation_applied=false",
        "diag.r.limit=Some(3)",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=not_applicable",
        "diag.r.top_n_seek=fetch(6)",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=none",
        "diag.r.projected_fields=[\"id\", \"rank\"]",
        "diag.r.load_order_route_contract=direct_streaming",
        "diag.r.load_order_route_reason=none",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=order_mat",
        "diag.r.access_choice_chosen=FullScan",
        "diag.r.access_choice_chosen_reason=non_index_access",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.d.has_top_n_seek=true",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_filter=false",
        "diag.p.mode=Load(LoadSpec { limit: Some(3), offset: 2 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=none",
        "diag.p.distinct=false",
        "diag.p.page=Page { limit: Some(3), offset: 2 }",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "top-n verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn explain_execution_verbose_reports_secondary_order_pushdown_rejection_reason() {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("label"))
        .explain_execution_verbose()
        .expect("execution verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_eq!(
        diagnostics.get("diag.r.secondary_order_pushdown"),
        Some(&"rejected(OrderFieldsDoNotMatchIndex(index=group_rank,prefix_len=1,expected_suffix=[\"rank\"],expected_full=[\"group\", \"rank\"],actual=[\"label\"]))".to_string()),
        "verbose execution explain should expose explicit route rejection reason",
    );
    assert_eq!(
        diagnostics.get("diag.p.mode"),
        Some(&"Load(LoadSpec { limit: None, offset: 0 })".to_string()),
        "verbose execution explain should include logical plan mode diagnostics",
    );
}

#[test]
fn explain_execution_verbose_reports_temporal_ranked_order_shape_parity() {
    let top_like_verbose = Query::<PlanTemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::desc("occurred_on"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal top-like verbose explain should build");
    let bottom_like_verbose = Query::<PlanTemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("occurred_on"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal bottom-like verbose explain should build");

    let top_like = verbose_diagnostics_map(&top_like_verbose);
    let bottom_like = verbose_diagnostics_map(&bottom_like_verbose);
    let parity_keys = [
        "diag.r.execution_mode",
        "diag.r.continuation_applied",
        "diag.r.limit",
        "diag.r.fast_path_order",
        "diag.r.secondary_order_pushdown",
        "diag.r.top_n_seek",
        "diag.r.index_range_limit_pushdown",
        "diag.r.predicate_stage",
        "diag.r.projected_fields",
        "diag.r.projection_pushdown",
        "diag.r.covering_read",
        "diag.r.access_choice_chosen",
        "diag.r.access_choice_chosen_reason",
        "diag.r.access_choice_alternatives",
        "diag.r.access_choice_rejections",
        "diag.d.has_top_n_seek",
        "diag.d.has_index_range_limit_pushdown",
        "diag.d.has_index_predicate_prefilter",
        "diag.d.has_residual_filter",
        "diag.p.mode",
        "diag.p.order_pushdown",
        "diag.p.predicate_pushdown",
        "diag.p.distinct",
        "diag.p.page",
        "diag.p.consistency",
    ];
    for key in parity_keys {
        assert_eq!(
            top_like.get(key),
            bottom_like.get(key),
            "temporal top-like vs bottom-like ranked query shapes should keep verbose diagnostic parity for key {key}",
        );
    }
}

#[test]
fn explain_execution_verbose_temporal_ranked_shape_snapshot_is_stable() {
    let verbose = Query::<PlanTemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::desc("occurred_on"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal ranked verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Materialized",
        "diag.r.continuation_applied=false",
        "diag.r.limit=Some(2)",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=not_applicable",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=none",
        "diag.r.projected_fields=[\"id\", \"occurred_on\", \"occurred_at\", \"elapsed\"]",
        "diag.r.load_order_route_contract=materialized_fallback",
        "diag.r.load_order_route_reason=requires_materialized_sort",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=order_mat",
        "diag.r.access_choice_chosen=FullScan",
        "diag.r.access_choice_chosen_reason=non_index_access",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_filter=false",
        "diag.p.mode=Load(LoadSpec { limit: Some(2), offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=none",
        "diag.p.distinct=false",
        "diag.p.page=Page { limit: Some(2), offset: 0 }",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "temporal ranked verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn explain_execution_verbose_reports_index_range_limit_pushdown_hints() {
    let range_predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Gte,
            Value::Uint(100),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Lt,
            Value::Uint(200),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::Eq,
            Value::Text("keep".to_string()),
            CoercionId::Strict,
        )),
    ]);

    let verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(range_predicate)
        .order_term(crate::db::asc("code"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .explain_execution_verbose()
        .expect("index-range verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_eq!(
        diagnostics.get("diag.r.index_range_limit_pushdown"),
        Some(&"fetch(3)".to_string()),
        "verbose execution explain should freeze index-range pushdown fetch diagnostics",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_index_range_limit_pushdown"),
        Some(&"true".to_string()),
        "descriptor diagnostics should report index-range pushdown node presence",
    );
    assert_eq!(
        diagnostics.get("diag.r.predicate_stage"),
        Some(&"residual_post_access".to_string()),
        "verbose execution explain should freeze predicate-stage diagnostics",
    );
}

#[test]
fn explain_execution_verbose_rejection_shape_snapshot_is_stable() {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("label"))
        .explain_execution_verbose()
        .expect("execution verbose explain should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Materialized",
        "diag.r.continuation_applied=false",
        "diag.r.limit=None",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=rejected(OrderFieldsDoNotMatchIndex(index=group_rank,prefix_len=1,expected_suffix=[\"rank\"],expected_full=[\"group\", \"rank\"],actual=[\"label\"]))",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=index_prefilter(strict_all_or_none)",
        "diag.r.projected_fields=[\"id\", \"group\", \"rank\", \"label\"]",
        "diag.r.load_order_route_contract=materialized_fallback",
        "diag.r.load_order_route_reason=requires_materialized_sort",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=order_mat",
        "diag.r.access_choice_chosen=IndexPrefix(group_rank)",
        "diag.r.access_choice_chosen_reason=single_candidate",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.r.predicate_index_capability=fully_indexable",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=true",
        "diag.d.has_residual_filter=false",
        "diag.p.mode=Load(LoadSpec { limit: None, offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=applied(index_prefix)",
        "diag.p.distinct=false",
        "diag.p.page=None",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "verbose diagnostics snapshot drifted; output ordering and values are part of the explain contract",
    );
}

#[test]
fn explain_execution_verbose_index_range_pushdown_shape_snapshot_is_stable() {
    let range_predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Gte,
            Value::Uint(100),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Lt,
            Value::Uint(200),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::Eq,
            Value::Text("keep".to_string()),
            CoercionId::Strict,
        )),
    ]);

    let verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(range_predicate)
        .order_term(crate::db::asc("code"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .explain_execution_verbose()
        .expect("index-range verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Streaming",
        "diag.r.continuation_applied=false",
        "diag.r.limit=Some(2)",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=eligible(index=code_unique,prefix_len=0)",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=fetch(3)",
        "diag.r.predicate_stage=residual_post_access",
        "diag.r.projected_fields=[\"id\", \"code\", \"label\"]",
        "diag.r.load_order_route_contract=materialized_fallback",
        "diag.r.load_order_route_reason=residual_predicate_blocks_direct_streaming",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=order_mat",
        "diag.r.access_choice_chosen=IndexRange(code_unique)",
        "diag.r.access_choice_chosen_reason=single_candidate",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.r.predicate_index_capability=partially_indexable",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=true",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_filter=true",
        "diag.p.mode=Load(LoadSpec { limit: Some(2), offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=applied(index_range)",
        "diag.p.distinct=false",
        "diag.p.page=Page { limit: Some(2), offset: 0 }",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "index-range verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

fn deterministic_prefix_choice_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanDeterministicChoiceEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("handle"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("deterministic prefix explain should build");

    verbose_diagnostics_map(&verbose)
}

fn deterministic_range_choice_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_term(crate::db::asc("score"))
        .order_term(crate::db::asc("label"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("deterministic range explain should build");

    verbose_diagnostics_map(&verbose)
}

fn deterministic_range_choice_desc_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_term(crate::db::desc("score"))
        .order_term(crate::db::desc("label"))
        .order_term(crate::db::desc("id"))
        .explain_execution_verbose()
        .expect("descending deterministic range explain should build");

    verbose_diagnostics_map(&verbose)
}

fn deterministic_equality_prefix_suffix_order_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_term(crate::db::asc("label"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("deterministic equality-prefix suffix-order explain should build");

    verbose_diagnostics_map(&verbose)
}

fn deterministic_equality_prefix_suffix_order_desc_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_term(crate::db::desc("label"))
        .order_term(crate::db::desc("id"))
        .explain_execution_verbose()
        .expect("descending deterministic equality-prefix suffix-order explain should build");

    verbose_diagnostics_map(&verbose)
}

fn deterministic_order_only_choice_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanOrderOnlyChoiceEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("alpha"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("deterministic order-only explain should build");

    verbose_diagnostics_map(&verbose)
}

fn deterministic_composite_order_only_choice_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanDeterministicChoiceEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("tier"))
        .order_term(crate::db::asc("handle"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("deterministic composite order-only explain should build");

    verbose_diagnostics_map(&verbose)
}

fn deterministic_composite_order_only_choice_desc_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanDeterministicChoiceEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::desc("tier"))
        .order_term(crate::db::desc("handle"))
        .order_term(crate::db::desc("id"))
        .explain_execution_verbose()
        .expect("descending deterministic composite order-only explain should build");

    verbose_diagnostics_map(&verbose)
}

fn assert_order_compatible_choice_diagnostics(
    label: &str,
    diagnostics: &BTreeMap<String, String>,
    expected_choice_prefix: &str,
) {
    assert!(
        diagnostics
            .get("diag.r.access_choice_chosen")
            .is_some_and(|choice| choice.starts_with(expected_choice_prefix)),
        "{label}: verbose explain must project one deterministic order-compatible access family",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "{label}: planner-choice explain must report the canonical order-compatibility tie-break",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| rejections.contains("order_compatible_preferred")),
        "{label}: verbose explain must report that at least one competing route lost on the canonical order-compatibility tie-break",
    );
}

fn empty_contract_is_null_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").is_null())
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn empty_contract_strict_in_empty_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").in_list(std::iter::empty::<Ulid>()))
        .explain_execution_verbose()
        .expect("strict IN [] verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn equivalent_in_permuted_set_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").in_list([8_u32, 7_u32, 8_u32]))
        .explain_execution_verbose()
        .expect("permuted IN verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn equivalent_in_canonical_set_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").in_list([7_u32, 8_u32]))
        .explain_execution_verbose()
        .expect("canonical IN verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn equivalent_between_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Gte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Lte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
        ]))
        .order_term(crate::db::asc("code"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("equivalent-between verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn equivalent_strict_eq_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Eq,
            Value::Uint(100),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("code"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("strict-eq verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn equivalent_prefix_like_starts_with_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text("foo".to_string()),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("label"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("starts-with verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn equivalent_prefix_like_range_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Gte,
                Value::Text("foo".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Lt,
                Value::Text("fop".to_string()),
                CoercionId::Strict,
            )),
        ]))
        .order_term(crate::db::asc("label"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("equivalent-range verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn max_unicode_prefix_like_starts_with_diagnostics() -> BTreeMap<String, String> {
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text(prefix),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("label"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("max-unicode starts-with verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn max_unicode_prefix_like_lower_bound_diagnostics() -> BTreeMap<String, String> {
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::Gte,
            Value::Text(prefix),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("label"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("equivalent lower-bound verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn non_strict_predicate_fallback_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::NumericWiden,
        )))
        .explain_execution_verbose()
        .expect("non-strict predicate verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn secondary_is_null_fallback_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").is_null())
        .explain_execution_verbose()
        .expect("secondary is-null verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn empty_prefix_starts_with_fallback_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("label").text_starts_with(""))
        .explain_execution_verbose()
        .expect("empty-prefix starts-with verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn non_empty_prefix_starts_with_fallback_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("label").text_starts_with("label"))
        .explain_execution_verbose()
        .expect("non-empty starts-with verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn text_contains_ci_fallback_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("label").text_contains_ci("label"))
        .explain_execution_verbose()
        .expect("text-contains-ci verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn strict_ends_with_fallback_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::EndsWith,
            Value::Text("fix".to_string()),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("ends-with verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn non_strict_ends_with_fallback_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::EndsWith,
            Value::Text("fix".to_string()),
            CoercionId::TextCasefold,
        )))
        .explain_execution_verbose()
        .expect("non-strict ends-with verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn collection_contains_fallback_diagnostics() -> BTreeMap<String, String> {
    let verbose = Query::<PlanPhaseEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "tags",
            CompareOp::Contains,
            Value::Uint(7),
            CoercionId::CollectionElement,
        )))
        .explain_execution_verbose()
        .expect("collection contains verbose explain should build");

    verbose_diagnostics_map(&verbose)
}

fn assert_verbose_diagnostic_parity(
    label: &str,
    left: &BTreeMap<String, String>,
    right: &BTreeMap<String, String>,
    keys: &[&str],
) {
    for key in keys {
        assert_eq!(
            left.get(*key),
            right.get(*key),
            "{label}: equivalent forms should keep verbose diagnostic parity for key {key}",
        );
    }
}

fn assert_verbose_pushdown_reason_case(
    label: &str,
    diagnostics: &BTreeMap<String, String>,
    expected_pushdown: &str,
    expected_stage: Option<&str>,
    forbidden_pushdown: Option<&str>,
) {
    assert_eq!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&expected_pushdown.to_string()),
        "{label}: verbose explain must keep the expected predicate pushdown reason",
    );

    if let Some(expected_stage) = expected_stage {
        assert_eq!(
            diagnostics.get("diag.r.predicate_stage"),
            Some(&expected_stage.to_string()),
            "{label}: verbose explain must keep the expected route predicate-stage contract",
        );
    }

    if let Some(forbidden_pushdown) = forbidden_pushdown {
        assert_ne!(
            diagnostics.get("diag.p.predicate_pushdown"),
            Some(&forbidden_pushdown.to_string()),
            "{label}: verbose explain must not collapse into the wrong fallback classification",
        );
    }
}

#[test]
fn explain_execution_verbose_order_compatible_choice_matrix() {
    let cases: &[VerbosePushdownMatrixCase<'_>] = &[
        (
            "prefix choice",
            deterministic_prefix_choice_diagnostics,
            "IndexPrefix(",
        ),
        (
            "range choice",
            deterministic_range_choice_diagnostics,
            "IndexRange(",
        ),
        (
            "descending range choice",
            deterministic_range_choice_desc_diagnostics,
            "IndexRange(",
        ),
        (
            "equality-prefix suffix-order choice",
            deterministic_equality_prefix_suffix_order_diagnostics,
            "IndexPrefix(",
        ),
        (
            "descending equality-prefix suffix-order choice",
            deterministic_equality_prefix_suffix_order_desc_diagnostics,
            "IndexPrefix(",
        ),
        (
            "order-only choice",
            deterministic_order_only_choice_diagnostics,
            "IndexRange(",
        ),
        (
            "composite order-only choice",
            deterministic_composite_order_only_choice_diagnostics,
            "IndexRange(",
        ),
        (
            "descending composite order-only choice",
            deterministic_composite_order_only_choice_desc_diagnostics,
            "IndexRange(",
        ),
    ];

    for (label, build_diagnostics, expected_choice_prefix) in cases.iter().copied() {
        let diagnostics = build_diagnostics();
        assert_order_compatible_choice_diagnostics(label, &diagnostics, expected_choice_prefix);
    }
}

#[test]
fn explain_execution_verbose_equivalent_predicate_contract_matrix() {
    type DiagnosticsBuilder = fn() -> BTreeMap<String, String>;

    let cases: &[(&str, DiagnosticsBuilder, DiagnosticsBuilder, &[&str])] = &[
        (
            "empty contract",
            empty_contract_is_null_diagnostics,
            empty_contract_strict_in_empty_diagnostics,
            &["diag.p.predicate_pushdown", "diag.r.predicate_stage"],
        ),
        (
            "canonical IN set",
            equivalent_in_permuted_set_diagnostics,
            equivalent_in_canonical_set_diagnostics,
            &["diag.r.predicate_stage"],
        ),
        (
            "equivalent BETWEEN and strict equality",
            equivalent_between_diagnostics,
            equivalent_strict_eq_diagnostics,
            &["diag.p.predicate_pushdown", "diag.r.predicate_stage"],
        ),
        (
            "prefix-like and bounded range",
            equivalent_prefix_like_starts_with_diagnostics,
            equivalent_prefix_like_range_diagnostics,
            &["diag.p.predicate_pushdown", "diag.r.predicate_stage"],
        ),
        (
            "max-unicode prefix-like and lower bound",
            max_unicode_prefix_like_starts_with_diagnostics,
            max_unicode_prefix_like_lower_bound_diagnostics,
            &["diag.p.predicate_pushdown", "diag.r.predicate_stage"],
        ),
    ];

    for (label, build_left, build_right, keys) in cases.iter().copied() {
        let left = build_left();
        let right = build_right();

        assert_verbose_diagnostic_parity(label, &left, &right, keys);
    }

    let empty_contract = empty_contract_is_null_diagnostics();
    assert_eq!(
        empty_contract.get("diag.p.predicate_pushdown"),
        Some(&"applied(empty_access_contract)".to_string()),
        "empty-contract matrix must keep the explicit empty-access predicate reason frozen",
    );
}

#[test]
fn explain_execution_scalar_surface_defers_projection_and_grouped_node_families() {
    let by_key = Query::<PlanSimpleEntity>::new(MissingRowPolicy::Ignore)
        .by_id(Ulid::from_u128(9_301))
        .explain_execution()
        .expect("by-key execution descriptor should build");
    let pushdown_rejected = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("label"))
        .explain_execution()
        .expect("pushdown-rejected descriptor should build");
    let index_range = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Gte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Lt,
                Value::Uint(200),
                CoercionId::Strict,
            )),
        ]))
        .order_term(crate::db::asc("code"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .explain_execution()
        .expect("index-range descriptor should build");

    for descriptor in [&by_key, &pushdown_rejected, &index_range] {
        for deferred in [
            ExplainExecutionNodeType::ProjectionMaterialized,
            ExplainExecutionNodeType::GroupedAggregateHashMaterialized,
            ExplainExecutionNodeType::GroupedAggregateOrderedMaterialized,
        ] {
            assert!(
                !explain_execution_contains_node_type(descriptor, deferred),
                "scalar execution descriptors intentionally defer materialized projection/grouped node family {} in this owner-local surface",
                deferred.as_str(),
            );
        }
    }
}

#[test]
fn explain_execution_verbose_non_strict_fallback_shape_snapshot_is_stable() {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::NumericWiden,
        )))
        .explain_execution_verbose()
        .expect("non-strict fallback verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Materialized",
        "diag.r.continuation_applied=false",
        "diag.r.limit=None",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=not_applicable",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=residual_post_access",
        "diag.r.projected_fields=[\"id\", \"group\", \"rank\", \"label\"]",
        "diag.r.load_order_route_contract=materialized_fallback",
        "diag.r.load_order_route_reason=residual_predicate_blocks_direct_streaming",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=access_not_cov",
        "diag.r.access_choice_chosen=FullScan",
        "diag.r.access_choice_chosen_reason=non_index_access",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_filter=true",
        "diag.p.mode=Load(LoadSpec { limit: None, offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=fallback(non_strict_compare_coercion)",
        "diag.p.distinct=false",
        "diag.p.page=None",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "non-strict fallback verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn explain_execution_verbose_fallback_reason_matrix() {
    type DiagnosticsBuilder = fn() -> BTreeMap<String, String>;

    let cases: &[(&str, DiagnosticsBuilder, &str, Option<&str>)] = &[
        (
            "non-strict indexed compare",
            non_strict_predicate_fallback_diagnostics,
            "fallback(non_strict_compare_coercion)",
            Some("residual_post_access"),
        ),
        (
            "primary-key is-null empty contract",
            empty_contract_is_null_diagnostics,
            "applied(empty_access_contract)",
            None,
        ),
        (
            "secondary is-null full scan",
            secondary_is_null_fallback_diagnostics,
            "fallback(is_null_full_scan)",
            None,
        ),
        (
            "empty-prefix starts-with",
            empty_prefix_starts_with_fallback_diagnostics,
            "fallback(starts_with_empty_prefix)",
            Some("residual_post_access"),
        ),
        (
            "non-empty starts-with full scan",
            non_empty_prefix_starts_with_fallback_diagnostics,
            "fallback(full_scan)",
            None,
        ),
        (
            "text contains-ci",
            text_contains_ci_fallback_diagnostics,
            "fallback(text_operator_full_scan)",
            Some("residual_post_access"),
        ),
        (
            "strict ends-with",
            strict_ends_with_fallback_diagnostics,
            "fallback(text_operator_full_scan)",
            None,
        ),
    ];

    for (label, build_diagnostics, expected_pushdown, expected_stage) in cases.iter().copied() {
        let diagnostics = build_diagnostics();
        assert_verbose_pushdown_reason_case(
            label,
            &diagnostics,
            expected_pushdown,
            expected_stage,
            None,
        );
    }
}

#[test]
fn explain_execution_verbose_non_strict_fallback_precedence_matrix() {
    let cases: &[VerboseFallbackMatrixCase<'_>] = &[
        (
            "non-strict ends-with",
            non_strict_ends_with_fallback_diagnostics,
        ),
        (
            "collection-element contains",
            collection_contains_fallback_diagnostics,
        ),
    ];

    for (label, build_diagnostics) in cases.iter().copied() {
        let diagnostics = build_diagnostics();
        assert_verbose_pushdown_reason_case(
            label,
            &diagnostics,
            "fallback(non_strict_compare_coercion)",
            None,
            Some("fallback(text_operator_full_scan)"),
        );
    }
}

#[test]
fn explain_execution_verbose_reports_strict_text_prefix_like_index_range_pushdown_stage() {
    let starts_with_verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text("foo".to_string()),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("label"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("starts-with verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&starts_with_verbose);
    assert_eq!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"applied(index_range)".to_string()),
        "strict field-key text starts-with should surface the bounded index-range pushdown reason",
    );
    assert_eq!(
        diagnostics.get("diag.r.predicate_stage"),
        Some(&"index_prefilter(strict_all_or_none)".to_string()),
        "strict field-key text starts-with should compile to one strict index prefilter stage",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_index_predicate_prefilter"),
        Some(&"true".to_string()),
        "strict field-key text starts-with should emit the strict index prefilter flag",
    );
    assert_eq!(
        diagnostics.get("diag.d.has_residual_filter"),
        Some(&"false".to_string()),
        "strict field-key text starts-with should not keep residual filtering once the bounded range is exact",
    );
}

#[test]
fn explain_execution_verbose_is_null_fallback_shape_snapshot_is_stable() {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").is_null())
        .explain_execution_verbose()
        .expect("is-null fallback verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diag.r.execution_mode=Materialized",
        "diag.r.continuation_applied=false",
        "diag.r.limit=None",
        "diag.r.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diag.r.secondary_order_pushdown=not_applicable",
        "diag.r.top_n_seek=disabled",
        "diag.r.index_range_limit_pushdown=disabled",
        "diag.r.predicate_stage=residual_post_access",
        "diag.r.projected_fields=[\"id\", \"group\", \"rank\", \"label\"]",
        "diag.r.load_order_route_contract=materialized_fallback",
        "diag.r.load_order_route_reason=residual_predicate_blocks_direct_streaming",
        "diag.r.projection_pushdown=false",
        "diag.r.covering_read=access_not_cov",
        "diag.r.access_choice_chosen=FullScan",
        "diag.r.access_choice_chosen_reason=non_index_access",
        "diag.r.access_choice_alternatives=[]",
        "diag.r.access_choice_rejections=[]",
        "diag.d.has_top_n_seek=false",
        "diag.d.has_index_range_limit_pushdown=false",
        "diag.d.has_index_predicate_prefilter=false",
        "diag.d.has_residual_filter=true",
        "diag.p.mode=Load(LoadSpec { limit: None, offset: 0 })",
        "diag.p.order_pushdown=missing_model_context",
        "diag.p.predicate_pushdown=fallback(is_null_full_scan)",
        "diag.p.distinct=false",
        "diag.p.page=None",
        "diag.p.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "is-null fallback verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}
