use super::*;

#[test]
fn explain_execution_verbose_top_n_seek_shape_snapshot_is_stable() {
    let verbose = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
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
        "diag.d.has_residual_predicate_filter=false",
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
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("label")
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
        .order_by_desc("occurred_on")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal top-like verbose explain should build");
    let bottom_like_verbose = Query::<PlanTemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_by("occurred_on")
        .order_by("id")
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
        "diag.d.has_residual_predicate_filter",
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
        .order_by_desc("occurred_on")
        .order_by("id")
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
        "diag.d.has_residual_predicate_filter=false",
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
        .filter(range_predicate)
        .order_by("code")
        .order_by("id")
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
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("label")
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
        "diag.d.has_residual_predicate_filter=false",
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
        .filter(range_predicate)
        .order_by("code")
        .order_by("id")
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
        "diag.d.has_residual_predicate_filter=true",
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

#[test]
fn explain_execution_verbose_prefix_choice_prefers_order_compatible_index_when_rank_ties() {
    let verbose = Query::<PlanDeterministicChoiceEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by("handle")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic prefix explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexPrefix(z_tier_handle_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible prefix index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when predicate rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_label_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_range_choice_prefers_order_compatible_index_when_rank_ties() {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
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
        .order_by("score")
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic range explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_score_label_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible range index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when range rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible range index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_range_choice_desc_prefers_order_compatible_index_when_rank_ties() {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
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
        .order_by_desc("score")
        .order_by_desc("label")
        .order_by_desc("id")
        .explain_execution_verbose()
        .expect("descending deterministic range explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_score_label_idx)".to_string()),
        "descending verbose explain must project the planner-selected order-compatible range index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "descending planner-choice explain must report the canonical order-compatibility tie-break when range rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "descending verbose explain must report the lexicographically earlier but order-incompatible range index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_equality_prefix_suffix_order_prefers_order_compatible_index_when_rank_ties()
 {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
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
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic equality-prefix suffix-order explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexPrefix(z_tier_score_label_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible equality-prefix suffix-order index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when equality-prefix suffix-order rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible equality-prefix suffix-order index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_equality_prefix_suffix_order_desc_prefers_order_compatible_index_when_rank_ties()
 {
    let verbose = Query::<PlanDeterministicRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
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
        .order_by_desc("label")
        .order_by_desc("id")
        .explain_execution_verbose()
        .expect("descending deterministic equality-prefix suffix-order explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexPrefix(z_tier_score_label_idx)".to_string()),
        "descending verbose explain must project the planner-selected order-compatible equality-prefix suffix-order index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "descending planner-choice explain must report the canonical order-compatibility tie-break when equality-prefix suffix-order rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "descending verbose explain must report the lexicographically earlier but order-incompatible equality-prefix suffix-order index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_order_only_choice_prefers_order_compatible_index_when_rank_ties() {
    let verbose = Query::<PlanOrderOnlyChoiceEntity>::new(MissingRowPolicy::Ignore)
        .order_by("alpha")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic order-only explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_alpha_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible fallback index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when order-only ranking ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_beta_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible fallback index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_composite_order_only_choice_prefers_order_compatible_index_when_rank_ties()
 {
    let verbose = Query::<PlanDeterministicChoiceEntity>::new(MissingRowPolicy::Ignore)
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .explain_execution_verbose()
        .expect("deterministic composite order-only explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_handle_idx)".to_string()),
        "verbose explain must project the planner-selected order-compatible composite fallback index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "planner-choice explain must report the canonical order-compatibility tie-break when composite order-only ranking ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_label_idx=order_compatible_preferred")
            }),
        "verbose explain must report the lexicographically earlier but order-incompatible composite fallback index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_verbose_composite_order_only_choice_desc_prefers_order_compatible_index_when_rank_ties()
 {
    let verbose = Query::<PlanDeterministicChoiceEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("tier")
        .order_by_desc("handle")
        .order_by_desc("id")
        .explain_execution_verbose()
        .expect("descending deterministic composite order-only explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_handle_idx)".to_string()),
        "descending verbose explain must project the planner-selected order-compatible composite fallback index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "descending planner-choice explain must report the canonical order-compatibility tie-break when composite order-only ranking ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_label_idx=order_compatible_preferred")
            }),
        "descending verbose explain must report the lexicographically earlier but order-incompatible composite fallback index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn explain_execution_scalar_surface_defers_projection_and_grouped_node_families() {
    let by_key = Query::<PlanSimpleEntity>::new(MissingRowPolicy::Ignore)
        .by_id(Ulid::from_u128(9_301))
        .explain_execution()
        .expect("by-key execution descriptor should build");
    let pushdown_rejected = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("label")
        .explain_execution()
        .expect("pushdown-rejected descriptor should build");
    let index_range = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
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
        .order_by("code")
        .order_by("id")
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
fn explain_execution_verbose_reports_equivalent_empty_contract_reason_paths() {
    let is_null_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").is_null())
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");
    let strict_in_empty_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").in_list(std::iter::empty::<Ulid>()))
        .explain_execution_verbose()
        .expect("strict IN [] verbose explain should build");

    let is_null_diagnostics = verbose_diagnostics_map(&is_null_verbose);
    let strict_in_empty_diagnostics = verbose_diagnostics_map(&strict_in_empty_verbose);
    assert_eq!(
        is_null_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"applied(empty_access_contract)".to_string()),
        "primary-key is-null should surface empty-contract predicate diagnostics",
    );
    assert_eq!(
        strict_in_empty_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"applied(empty_access_contract)".to_string()),
        "strict IN [] should surface empty-contract predicate diagnostics",
    );
}

#[test]
fn explain_execution_verbose_reports_equivalent_empty_contract_route_stage_parity() {
    let is_null_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").is_null())
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");
    let strict_in_empty_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").in_list(std::iter::empty::<Ulid>()))
        .explain_execution_verbose()
        .expect("strict IN [] verbose explain should build");

    let is_null_diagnostics = verbose_diagnostics_map(&is_null_verbose);
    let strict_in_empty_diagnostics = verbose_diagnostics_map(&strict_in_empty_verbose);
    assert_eq!(
        is_null_diagnostics.get("diag.r.predicate_stage"),
        strict_in_empty_diagnostics.get("diag.r.predicate_stage"),
        "equivalent empty-contract predicates should keep route predicate-stage diagnostics in parity",
    );
}

#[test]
fn explain_execution_verbose_reports_non_strict_predicate_fallback_reason_path() {
    let non_strict_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::NumericWiden,
        )))
        .explain_execution_verbose()
        .expect("non-strict predicate verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&non_strict_verbose);
    assert_eq!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "non-strict indexed compare should surface full-scan fallback predicate diagnostics",
    );
    assert_eq!(
        diagnostics.get("diag.r.predicate_stage"),
        Some(&"residual_post_access".to_string()),
        "non-strict indexed compare should execute as residual post-access predicate stage",
    );
}

#[test]
fn explain_execution_verbose_reports_is_null_predicate_pushdown_reason_paths() {
    let primary_key_is_null_verbose = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("id").is_null())
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");
    let secondary_is_null_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").is_null())
        .explain_execution_verbose()
        .expect("secondary is-null verbose explain should build");

    let primary_key_diagnostics = verbose_diagnostics_map(&primary_key_is_null_verbose);
    let secondary_diagnostics = verbose_diagnostics_map(&secondary_is_null_verbose);

    assert_eq!(
        primary_key_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"applied(empty_access_contract)".to_string()),
        "impossible primary-key IS NULL should surface empty-contract predicate pushdown diagnostics",
    );
    assert_eq!(
        secondary_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(is_null_full_scan)".to_string()),
        "non-primary IS NULL should surface full-scan fallback predicate diagnostics",
    );
}

#[test]
fn explain_execution_verbose_non_strict_fallback_shape_snapshot_is_stable() {
    let verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
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
        "diag.d.has_residual_predicate_filter=true",
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
fn explain_execution_verbose_reports_empty_prefix_starts_with_fallback_reason_path() {
    let empty_prefix_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("label").text_starts_with(""))
        .explain_execution_verbose()
        .expect("empty-prefix starts-with verbose explain should build");
    let non_empty_prefix_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("label").text_starts_with("label"))
        .explain_execution_verbose()
        .expect("non-empty starts-with verbose explain should build");

    let empty_prefix_diagnostics = verbose_diagnostics_map(&empty_prefix_verbose);
    let non_empty_prefix_diagnostics = verbose_diagnostics_map(&non_empty_prefix_verbose);
    assert_eq!(
        empty_prefix_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(starts_with_empty_prefix)".to_string()),
        "empty-prefix starts-with should surface the explicit empty-prefix fallback reason",
    );
    assert_eq!(
        non_empty_prefix_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(full_scan)".to_string()),
        "non-empty starts-with over a non-indexed field should remain generic full-scan fallback",
    );
    assert_eq!(
        empty_prefix_diagnostics.get("diag.r.predicate_stage"),
        Some(&"residual_post_access".to_string()),
        "empty-prefix starts-with fallback should preserve residual predicate stage diagnostics",
    );
}

#[test]
fn explain_execution_verbose_reports_text_operator_fallback_reason_path() {
    let text_contains_ci_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("label").text_contains_ci("label"))
        .explain_execution_verbose()
        .expect("text-contains-ci verbose explain should build");
    let ends_with_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::EndsWith,
            Value::Text("fix".to_string()),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("ends-with verbose explain should build");

    let text_contains_ci_diagnostics = verbose_diagnostics_map(&text_contains_ci_verbose);
    let ends_with_diagnostics = verbose_diagnostics_map(&ends_with_verbose);
    assert_eq!(
        text_contains_ci_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "text contains-ci should surface dedicated text-operator full-scan fallback reason",
    );
    assert_eq!(
        ends_with_diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "ends-with compare should surface dedicated text-operator full-scan fallback reason",
    );
    assert_eq!(
        text_contains_ci_diagnostics.get("diag.r.predicate_stage"),
        Some(&"residual_post_access".to_string()),
        "text-operator fallback should preserve residual predicate-stage diagnostics",
    );
}

#[test]
fn explain_execution_verbose_reports_equivalent_in_set_route_stage_parity() {
    let in_permuted_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").in_list([8_u32, 7_u32, 8_u32]))
        .explain_execution_verbose()
        .expect("permuted IN verbose explain should build");
    let in_canonical_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("group").in_list([7_u32, 8_u32]))
        .explain_execution_verbose()
        .expect("canonical IN verbose explain should build");

    let in_permuted_diagnostics = verbose_diagnostics_map(&in_permuted_verbose);
    let in_canonical_diagnostics = verbose_diagnostics_map(&in_canonical_verbose);
    assert_eq!(
        in_permuted_diagnostics.get("diag.r.predicate_stage"),
        in_canonical_diagnostics.get("diag.r.predicate_stage"),
        "equivalent canonical IN sets should keep route predicate-stage diagnostics in parity",
    );
}

#[test]
fn explain_execution_verbose_reports_equivalent_between_and_eq_parity() {
    let equivalent_between_verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
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
        .order_by("code")
        .order_by("id")
        .explain_execution_verbose()
        .expect("equivalent-between verbose explain should build");
    let strict_eq_verbose = Query::<PlanUniqueRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Eq,
            Value::Uint(100),
            CoercionId::Strict,
        )))
        .order_by("code")
        .order_by("id")
        .explain_execution_verbose()
        .expect("strict-eq verbose explain should build");

    let between_diagnostics = verbose_diagnostics_map(&equivalent_between_verbose);
    let eq_diagnostics = verbose_diagnostics_map(&strict_eq_verbose);
    assert_eq!(
        between_diagnostics.get("diag.p.predicate_pushdown"),
        eq_diagnostics.get("diag.p.predicate_pushdown"),
        "equivalent BETWEEN-style bounds and strict equality should report identical pushdown reason labels",
    );
    assert_eq!(
        between_diagnostics.get("diag.r.predicate_stage"),
        eq_diagnostics.get("diag.r.predicate_stage"),
        "equivalent BETWEEN-style bounds and strict equality should preserve route predicate-stage parity",
    );
}

#[test]
fn explain_execution_verbose_reports_equivalent_prefix_like_route_stage_parity() {
    let starts_with_verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text("foo".to_string()),
            CoercionId::Strict,
        )))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("starts-with verbose explain should build");
    let equivalent_range_verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
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
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("equivalent-range verbose explain should build");

    let starts_with_diagnostics = verbose_diagnostics_map(&starts_with_verbose);
    let equivalent_range_diagnostics = verbose_diagnostics_map(&equivalent_range_verbose);
    assert_eq!(
        starts_with_diagnostics.get("diag.p.predicate_pushdown"),
        equivalent_range_diagnostics.get("diag.p.predicate_pushdown"),
        "equivalent prefix-like and bounded-range forms should report identical predicate pushdown reason labels",
    );
    assert_eq!(
        starts_with_diagnostics.get("diag.r.predicate_stage"),
        equivalent_range_diagnostics.get("diag.r.predicate_stage"),
        "equivalent prefix-like and bounded-range forms should preserve route predicate-stage parity",
    );
}

#[test]
fn explain_execution_verbose_reports_strict_text_prefix_like_index_range_pushdown_stage() {
    let starts_with_verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text("foo".to_string()),
            CoercionId::Strict,
        )))
        .order_by("label")
        .order_by("id")
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
        diagnostics.get("diag.d.has_residual_predicate_filter"),
        Some(&"false".to_string()),
        "strict field-key text starts-with should not keep residual filtering once the bounded range is exact",
    );
}

#[test]
fn explain_execution_verbose_reports_max_unicode_prefix_like_parity() {
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let starts_with_verbose = Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text(prefix.clone()),
            CoercionId::Strict,
        )))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("max-unicode starts-with verbose explain should build");
    let equivalent_lower_bound_verbose =
        Query::<PlanTextPrefixEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Gte,
                Value::Text(prefix),
                CoercionId::Strict,
            )))
            .order_by("label")
            .order_by("id")
            .explain_execution_verbose()
            .expect("equivalent lower-bound verbose explain should build");

    let starts_with_diagnostics = verbose_diagnostics_map(&starts_with_verbose);
    let lower_bound_diagnostics = verbose_diagnostics_map(&equivalent_lower_bound_verbose);
    assert_eq!(
        starts_with_diagnostics.get("diag.p.predicate_pushdown"),
        lower_bound_diagnostics.get("diag.p.predicate_pushdown"),
        "max-unicode prefix-like and equivalent lower-bound forms should report identical predicate pushdown reason labels",
    );
    assert_eq!(
        starts_with_diagnostics.get("diag.r.predicate_stage"),
        lower_bound_diagnostics.get("diag.r.predicate_stage"),
        "max-unicode prefix-like and equivalent lower-bound forms should preserve route predicate-stage parity",
    );
}

#[test]
fn explain_execution_verbose_non_strict_ends_with_uses_non_strict_fallback_precedence() {
    let non_strict_ends_with_verbose = Query::<PlanPushdownEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::EndsWith,
            Value::Text("fix".to_string()),
            CoercionId::TextCasefold,
        )))
        .explain_execution_verbose()
        .expect("non-strict ends-with verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&non_strict_ends_with_verbose);
    assert_eq!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "non-strict ends-with should report non-strict compare fallback reason",
    );
    assert_ne!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "non-strict ends-with should not be classified as text-operator fallback",
    );
}

#[test]
fn explain_execution_verbose_keeps_collection_contains_on_generic_full_scan_fallback() {
    let collection_contains_verbose = Query::<PlanPhaseEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tags",
            CompareOp::Contains,
            Value::Uint(7),
            CoercionId::CollectionElement,
        )))
        .explain_execution_verbose()
        .expect("collection contains verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&collection_contains_verbose);
    assert_eq!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "collection-element contains should continue to report non-strict compare fallback",
    );
    assert_ne!(
        diagnostics.get("diag.p.predicate_pushdown"),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "collection-element contains should not be classified as text-operator fallback",
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
        "diag.d.has_residual_predicate_filter=true",
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
