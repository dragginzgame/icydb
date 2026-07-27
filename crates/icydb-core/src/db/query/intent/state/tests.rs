use super::*;
use crate::{
    db::query::plan::{
        FieldSlot, OrderDirection,
        expr::{FieldId, Function},
    },
    value::Value,
};

#[test]
fn query_intent_new_starts_in_load_scalar_mode() {
    let intent = QueryIntent::new();

    std::assert_matches!(intent.mode(), QueryMode::Load(_));
    std::assert_matches!(
        intent.mode(),
        QueryMode::Load(LoadSpec {
            limit: None,
            offset: 0
        })
    );
    assert!(
        !intent.is_grouped(),
        "new intent must start in scalar shape without grouped policy flags"
    );
    std::assert_matches!(intent.mode(), QueryMode::Load(_));
}

#[test]
fn delete_mode_tracks_offset_in_mode_spec() {
    let intent = QueryIntent::new().set_delete_mode().apply_offset(5);

    assert!(
        matches!(
            intent.mode(),
            QueryMode::Delete(DeleteSpec { offset: 5, .. })
        ),
        "offset requested in delete mode must remain visible on the delete spec"
    );
    assert!(
        matches!(intent.mode(), QueryMode::Delete(_)),
        "delete mode must expose delete-mode query state"
    );
}

#[test]
fn grouped_load_to_delete_preserves_grouping_policy_without_group_shape() {
    let mut intent = QueryIntent::new();
    let _ = intent
        .ensure_grouped_mut()
        .expect("load intent should materialize grouped shape");
    assert!(
        intent.grouped().is_some(),
        "load mode grouped intent should expose grouped shape"
    );

    let intent = intent.set_delete_mode();

    std::assert_matches!(intent.mode(), QueryMode::Delete(_));
    assert!(
        intent.is_grouped(),
        "delete mode should preserve grouped-delete policy signal"
    );
    assert!(
        intent.grouped().is_none(),
        "delete mode must not carry grouped shape state"
    );
}

#[test]
fn group_field_slot_deduplicates_by_slot_index() {
    let mut intent = QueryIntent::new();
    intent.push_group_field_slot(FieldSlot::from_test_slot(4, "rank"));
    intent.push_group_field_slot(FieldSlot::from_test_slot(4, "duplicate-rank"));

    let grouped = intent
        .grouped()
        .expect("grouped shape should be materialized after grouped slot push");

    assert_eq!(
        grouped.group.group_fields.len(),
        1,
        "group field slots should be deduplicated by stable model slot index"
    );
}

#[test]
fn append_predicate_ands_multiple_filters() {
    let mut intent = QueryIntent::new();
    intent.append_predicate(Predicate::True);
    intent.append_predicate(Predicate::False);

    assert!(
        matches!(
            intent
                .scalar()
                .filter
                .as_ref()
                .and_then(NormalizedFilter::predicate_subset),
            Some(Predicate::And(clauses)) if clauses.len() == 2
        ),
        "multiple filters should be preserved as a stable AND chain"
    );
}

#[test]
fn append_predicate_keeps_predicate_only_authority_without_filter_expr() {
    let mut intent = QueryIntent::new();
    intent.append_predicate(Predicate::And(vec![Predicate::True, Predicate::False]));

    let filter = intent
        .scalar()
        .filter
        .as_ref()
        .expect("predicate append should create one scalar filter");

    assert!(
        filter.logical_filter_expr().is_none(),
        "predicate-only filters should not expose a logical filter expression",
    );
    assert!(
        matches!(
            filter.semantic_authority,
            FilterSemanticAuthority::PredicateOnly
        ),
        "predicate-only filters should carry explicit predicate-only authority instead of a placeholder expression",
    );
    assert!(
        filter.predicate_subset().is_some(),
        "predicate-only filters should retain predicate access-planning identity",
    );
    assert_eq!(
        filter.predicate_coverage(),
        FilterPredicateCoverage::Full,
        "predicate-only filters should be full user-visible filter authorities",
    );
    assert!(
        filter
            .predicate_coverage()
            .covers_user_visible_filter_semantics(),
        "predicate-only filters should not need a visible expression for full semantic coverage",
    );
    assert!(
        !filter.predicate_subset_covers_expr(),
        "predicate-only filters should not report expression-subset coverage",
    );
}

#[test]
fn append_extractable_predicate_to_unextractable_expr_marks_partial_coverage() {
    let unextractable_expr = normalize_bool_expr(Expr::FunctionCall {
        function: Function::Coalesce,
        args: vec![
            Expr::Field(FieldId::new("flag")),
            Expr::Literal(Value::Bool(false)),
        ],
    });
    let mut intent = QueryIntent::new();
    intent.append_filter_expr(unextractable_expr);
    intent.append_predicate(Predicate::True);

    let filter = intent
        .scalar()
        .filter
        .as_ref()
        .expect("mixed filter append should create one scalar filter");

    assert_eq!(
        filter.predicate_coverage(),
        FilterPredicateCoverage::Partial,
        "combined coverage should record that only part of the user-visible filter is predicate-backed",
    );
    assert!(
        !filter
            .predicate_coverage()
            .covers_user_visible_filter_semantics(),
        "partial predicate coverage must not be treated as full semantic coverage",
    );
    assert!(
        filter.predicate_subset().is_some(),
        "the extractable predicate-only half should still feed access planning",
    );
    assert!(
        !filter.predicate_subset_covers_expr(),
        "the visible-expression planner projection should remain uncovered",
    );
}

#[test]
fn push_order_terms_preserve_declared_order_sequence() {
    let mut intent = QueryIntent::new();
    intent.push_order_term(crate::db::asc("rank").lower());
    intent.push_order_term(crate::db::desc("created_at").lower());

    let fields = intent
        .scalar()
        .order
        .as_ref()
        .expect("order should exist after order helper calls")
        .fields
        .clone();

    assert_eq!(
        fields,
        vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("created_at", OrderDirection::Desc),
        ],
        "typed order-term sequence should match user declaration order"
    );
}
