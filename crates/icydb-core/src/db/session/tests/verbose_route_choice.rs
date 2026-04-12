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

#[test]
fn fluent_load_explain_execution_surface_adapters_are_available() {
    reset_session_sql_store();
    let session = sql_session();
    let query = session
        .load::<SessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(Ulid::from_u128(9_201)),
            CoercionId::Strict,
        )))
        .order_by("id");
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
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by("handle")
        .order_by("id")
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
        let mut query = session
            .load::<SessionDeterministicRangeEntity>()
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
            ]));
        query = if descending {
            query
                .order_by_desc("score")
                .order_by_desc("label")
                .order_by_desc("id")
        } else {
            query.order_by("score").order_by("label").order_by("id")
        };
        let verbose = query
            .explain_execution_verbose()
            .unwrap_or_else(|err| panic!("{context} should build: {err}"));

        assert_verbose_access_choice(&verbose, "IndexRange(", context);
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
        let mut query = session
            .load::<SessionDeterministicRangeEntity>()
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
            ]));
        query = if descending {
            query.order_by_desc("label").order_by_desc("id")
        } else {
            query.order_by("label").order_by("id")
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
        .order_by("alpha")
        .order_by("id")
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
                .order_by_desc("tier")
                .order_by_desc("handle")
                .order_by_desc("id")
        } else {
            query.order_by("tier").order_by("handle").order_by("id")
        };
        let verbose = query
            .explain_execution_verbose()
            .unwrap_or_else(|err| panic!("{context} should build: {err}"));

        assert_verbose_access_choice(&verbose, "IndexRange(", context);
    }
}
