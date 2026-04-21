use super::*;

fn assert_verbose_access_choice(verbose: &str, expected_choice_prefix: &str, context: &str) {
    let diagnostics = session_verbose_diagnostics_map(verbose);

    assert!(
        diagnostics
            .get("diag.r.access_choice_chosen")
            .is_some_and(|choice| choice.starts_with(expected_choice_prefix)),
        "{context} must project one deterministic order-compatible access family",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "{context} must report the canonical order-compatibility tie-break",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| rejections.contains("order_compatible_preferred")),
        "{context} must report that at least one competing route lost on the canonical order-compatibility tie-break",
    );
}

fn assert_verbose_access_choice_reason(
    verbose: &str,
    expected_choice_prefix: &str,
    expected_reason: &str,
    context: &str,
) {
    let diagnostics = session_verbose_diagnostics_map(verbose);

    assert!(
        diagnostics
            .get("diag.r.access_choice_chosen")
            .is_some_and(|choice| choice.starts_with(expected_choice_prefix)),
        "{context} must project one deterministic access family",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&expected_reason.to_string()),
        "{context} must report the expected canonical access-choice reason",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| rejections.contains(expected_reason)),
        "{context} must report that at least one competing route lost on the same canonical access-choice reason",
    );
}

#[test]
fn fluent_load_explain_execution_surface_adapters_are_available() {
    reset_session_sql_store();
    let session = sql_session();
    let query = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("id").eq(Ulid::from_u128(9_201)))
        .order_term(crate::db::asc("id"));
    let descriptor = query
        .explain_execution()
        .expect("fluent execution descriptor explain should build");

    let text = query
        .explain_execution_text()
        .expect("fluent execution text explain should build");
    assert!(
        text.contains("ByKeyLookup"),
        "fluent execution text surface should include root node type",
    );
    assert_eq!(
        text,
        descriptor.render_text_tree(),
        "fluent execution text surface should be canonical descriptor text rendering",
    );

    let json = query
        .explain_execution_json()
        .expect("fluent execution json explain should build");
    assert!(
        json.contains("\"node_type\":\"ByKeyLookup\""),
        "fluent execution json surface should include canonical root node type",
    );
    assert_eq!(
        json,
        descriptor.render_json_canonical(),
        "fluent execution json surface should be canonical descriptor json rendering",
    );

    let verbose = query
        .explain_execution_verbose()
        .expect("fluent execution verbose explain should build");
    assert!(
        verbose.contains("diag.r.secondary_order_pushdown="),
        "fluent execution verbose surface should include diagnostics",
    );
}

#[test]
fn session_fluent_verbose_prefix_choice_prefers_order_compatible_index_when_rank_ties() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionDeterministicChoiceEntity>()
        .filter(crate::db::FieldRef::new("tier").eq("gold"))
        .order_term(crate::db::asc("handle"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("session deterministic prefix verbose explain should build");

    assert_verbose_access_choice(
        &verbose,
        "IndexPrefix(",
        "session fluent verbose prefix explain",
    );
}

#[test]
fn session_fluent_verbose_range_choice_matrix_prefers_order_compatible_index_when_rank_ties() {
    for (context, descending) in [
        ("session fluent verbose range explain", false),
        ("session descending verbose range explain", true),
    ] {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();
        let mut query =
            session
                .load::<SessionDeterministicRangeEntity>()
                .filter(crate::db::FilterExpr::and(vec![
                    crate::db::FieldRef::new("tier").eq("gold"),
                    crate::db::FieldRef::new("score").gt(10_u64),
                ]));
        query = if descending {
            query
                .order_term(crate::db::desc("score"))
                .order_term(crate::db::desc("label"))
                .order_term(crate::db::desc("id"))
        } else {
            query
                .order_term(crate::db::asc("score"))
                .order_term(crate::db::asc("label"))
                .order_term(crate::db::asc("id"))
        };
        let verbose = query
            .explain_execution_verbose()
            .unwrap_or_else(|err| panic!("{context} should build: {err}"));

        assert_verbose_access_choice(&verbose, "IndexRange(", context);
    }
}

#[test]
fn session_fluent_verbose_range_choice_prefers_stronger_bounds_before_lexicographic_tiebreak() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionRangeStrengthEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("tier").eq("gold"),
            crate::db::FieldRef::new("score").gt(10_u64),
            crate::db::FieldRef::new("score").lt(20_u64),
            crate::db::FieldRef::new("label").gt("m"),
        ]))
        .explain_execution_verbose()
        .expect("session range-strength verbose explain should build");

    assert_verbose_access_choice_reason(
        &verbose,
        "IndexRange(",
        "stronger_range_bounds_preferred",
        "session fluent verbose range-strength explain",
    );
}

#[test]
fn session_fluent_verbose_choice_prefers_lower_residual_burden_before_order_compatibility() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionResidualRankingEntity>()
        .filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("active").eq(true),
            crate::db::FieldRef::new("archived").eq(false),
            crate::db::FieldRef::new("tier").eq("gold"),
        ]))
        .explain_execution_verbose()
        .expect("session residual-ranking verbose explain should build");

    assert_verbose_access_choice_reason(
        &verbose,
        "IndexPrefix(",
        "residual_burden_preferred",
        "session fluent verbose residual-ranking explain",
    );
    assert!(
        verbose.contains("Access choice:")
            && verbose.contains("  Candidates:")
            && verbose.contains("  Scoring:")
            && verbose.contains("  Decision:")
            && verbose.contains("reason: residual_burden_preferred"),
        "session fluent verbose residual-ranking explain must render the human-readable access-choice section",
    );
}

#[test]
fn session_fluent_verbose_explain_reports_shared_query_plan_reuse_after_first_build() {
    reset_session_sql_store();
    let session = sql_session();
    let query = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("age").gte(20_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2);

    let first = query
        .explain_execution_verbose()
        .expect("first fluent verbose explain should build");
    let second = query
        .explain_execution_verbose()
        .expect("second fluent verbose explain should build");
    let first_diagnostics = session_verbose_diagnostics_map(&first);
    let second_diagnostics = session_verbose_diagnostics_map(&second);

    assert_eq!(
        first_diagnostics.get("diag.s.semantic_reuse_artifact"),
        Some(&"shared_prepared_query_plan".to_string()),
        "session fluent verbose explain must label the shipped semantic reuse artifact",
    );
    assert_eq!(
        first_diagnostics.get("diag.s.semantic_reuse"),
        Some(&"miss".to_string()),
        "the first fluent verbose explain should miss the shared prepared query-plan cache",
    );
    assert_eq!(
        second_diagnostics.get("diag.s.semantic_reuse_artifact"),
        Some(&"shared_prepared_query_plan".to_string()),
        "repeat fluent verbose explain must keep the same semantic reuse artifact class",
    );
    assert_eq!(
        second_diagnostics.get("diag.s.semantic_reuse"),
        Some(&"hit".to_string()),
        "the second fluent verbose explain should hit the shared prepared query-plan cache",
    );
}

#[test]
fn session_fluent_verbose_explain_keeps_distinct_semantic_identity_on_reuse_miss() {
    reset_session_sql_store();
    let session = sql_session();
    let left = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("age").gte(20_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .explain_execution_verbose()
        .expect("left fluent verbose explain should build");
    let right = session
        .load::<SessionSqlEntity>()
        .filter(crate::db::FieldRef::new("age").gte(20_u64))
        .order_term(crate::db::desc("age"))
        .order_term(crate::db::desc("id"))
        .limit(1)
        .explain_execution_verbose()
        .expect("right fluent verbose explain should build");
    let left_diagnostics = session_verbose_diagnostics_map(&left);
    let right_diagnostics = session_verbose_diagnostics_map(&right);

    for (context, diagnostics) in [
        ("left fluent verbose explain", &left_diagnostics),
        ("right fluent verbose explain", &right_diagnostics),
    ] {
        assert_eq!(
            diagnostics.get("diag.s.semantic_reuse_artifact"),
            Some(&"shared_prepared_query_plan".to_string()),
            "{context} must keep the shipped semantic reuse artifact label visible",
        );
        assert_eq!(
            diagnostics.get("diag.s.semantic_reuse"),
            Some(&"miss".to_string()),
            "{context} must miss reuse when semantic ordering or limit identity differs",
        );
    }
}

#[test]
fn session_fluent_verbose_equality_prefix_suffix_order_matrix_prefers_order_compatible_index_when_rank_ties()
 {
    for (context, descending) in [
        (
            "session fluent verbose equality-prefix suffix-order explain",
            false,
        ),
        (
            "session descending verbose equality-prefix suffix-order explain",
            true,
        ),
    ] {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();
        let mut query =
            session
                .load::<SessionDeterministicRangeEntity>()
                .filter(crate::db::FilterExpr::and(vec![
                    crate::db::FieldRef::new("tier").eq("gold"),
                    crate::db::FieldRef::new("score").eq(20_u64),
                ]));
        query = if descending {
            query
                .order_term(crate::db::desc("label"))
                .order_term(crate::db::desc("id"))
        } else {
            query
                .order_term(crate::db::asc("label"))
                .order_term(crate::db::asc("id"))
        };
        let verbose = query
            .explain_execution_verbose()
            .unwrap_or_else(|err| panic!("{context} should build: {err}"));

        assert_verbose_access_choice(&verbose, "IndexPrefix(", context);

        if descending {
            let diagnostics = session_verbose_diagnostics_map(&verbose);

            assert_eq!(
                diagnostics.get("diag.r.load_order_route_contract"),
                Some(&"materialized_boundary".to_string()),
                "session descending verbose explain must expose the materialized-boundary route contract for descending non-unique equality-prefix suffix-order shapes",
            );
            assert_eq!(
                diagnostics.get("diag.r.load_order_route_reason"),
                Some(&"descending_non_unique_secondary_prefix_not_admitted".to_string()),
                "session descending verbose explain must expose the planner-owned materialized-boundary reason for descending non-unique equality-prefix suffix-order shapes",
            );
        }
    }
}

#[test]
fn session_fluent_verbose_order_only_choice_prefers_order_compatible_index_when_rank_ties() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_term(crate::db::asc("alpha"))
        .order_term(crate::db::asc("id"))
        .explain_execution_verbose()
        .expect("session deterministic order-only verbose explain should build");

    assert_verbose_access_choice(
        &verbose,
        "IndexRange(",
        "session fluent verbose order-only explain",
    );

    let diagnostics = session_verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.load_order_route_contract"),
        Some(&"direct_streaming".to_string()),
        "session fluent verbose explain must expose the direct ordered-load route contract for admitted order-only fallback shapes",
    );
    assert_eq!(
        diagnostics.get("diag.r.load_order_route_reason"),
        Some(&"none".to_string()),
        "session fluent verbose explain must keep direct order-only fallback admission reason-free once the chosen route is already streaming-safe",
    );
}

#[test]
fn session_fluent_verbose_composite_order_only_choice_matrix_prefers_order_compatible_index_when_rank_ties()
 {
    for (context, descending) in [
        ("session fluent verbose composite order-only explain", false),
        (
            "session descending verbose composite order-only explain",
            true,
        ),
    ] {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();
        let mut query = session.load::<SessionDeterministicChoiceEntity>();
        query = if descending {
            query
                .order_term(crate::db::desc("tier"))
                .order_term(crate::db::desc("handle"))
                .order_term(crate::db::desc("id"))
        } else {
            query
                .order_term(crate::db::asc("tier"))
                .order_term(crate::db::asc("handle"))
                .order_term(crate::db::asc("id"))
        };
        let verbose = query
            .explain_execution_verbose()
            .unwrap_or_else(|err| panic!("{context} should build: {err}"));

        assert_verbose_access_choice(&verbose, "IndexRange(", context);
    }
}
