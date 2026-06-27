use super::*;
use crate::{
    db::query::{
        intent::{IntentError, KeyAccessKind},
        plan::{
            FieldSlot, OrderDirection,
            expr::{FieldId, Function},
        },
    },
    value::Value,
};

#[test]
fn query_intent_new_starts_in_load_scalar_mode() {
    let intent = QueryIntent::<u64>::new();

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
    let intent = QueryIntent::<u64>::new().set_delete_mode().apply_offset(5);

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
    let mut intent = QueryIntent::<u64>::new();
    let _ = intent.ensure_grouped_mut();
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
fn grouped_scalar_flags_survive_mode_transition() {
    let mut intent = QueryIntent::<u64>::new();
    intent.scalar_mut().key_access_conflict = true;
    let _ = intent.ensure_grouped_mut();

    let intent = intent.set_delete_mode();

    assert!(
        intent.scalar().key_access_conflict,
        "mode transitions must preserve scalar conflict flags"
    );
}

#[test]
fn group_field_slot_deduplicates_by_slot_index() {
    let mut intent = QueryIntent::<u64>::new();
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
fn having_clause_requires_grouped_shape() {
    let mut intent = QueryIntent::<u64>::new();

    let result = intent.push_having_expr(Expr::Literal(Value::Bool(true)));

    assert!(
        matches!(result, Err(IntentError::HavingRequiresGroupBy)),
        "having clauses should reject scalar shape"
    );
}

#[test]
fn delete_grouping_policy_accepts_having_clause_when_group_requested() {
    let mut intent = QueryIntent::<u64>::new();
    intent.push_group_field_slot(FieldSlot::from_test_slot(0, "id"));

    let mut intent = intent.set_delete_mode();
    let result = intent.push_having_expr(Expr::Literal(Value::Bool(true)));

    assert!(
        result.is_ok(),
        "delete mode should preserve grouped-delete policy signal for having checks"
    );
    assert!(
        intent.grouped().is_none(),
        "delete mode should not materialize grouped shape state"
    );
    assert!(
        intent.is_grouped(),
        "delete mode should keep grouped policy flag after having clause"
    );
}

#[test]
fn append_predicate_ands_multiple_filters() {
    let mut intent = QueryIntent::<u64>::new();
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
    let mut intent = QueryIntent::<u64>::new();
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
        "the legacy visible-expression projection should remain false when no expression is visible",
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
    let mut intent = QueryIntent::<u64>::new();
    intent.append_filter_expr(unextractable_expr);
    intent.append_predicate(Predicate::True);

    let filter = intent
        .scalar()
        .filter
        .as_ref()
        .expect("mixed filter append should create one scalar filter");

    assert_eq!(
        filter.predicate_coverage(),
        FilterPredicateCoverage::Partial {
            reason: PredicateCoverageGapReason::UnsupportedFilterSemantics,
        },
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
    let mut intent = QueryIntent::<u64>::new();
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

#[test]
fn key_access_conflict_flag_only_flips_for_mixed_access_kinds() {
    let mut intent = QueryIntent::<u64>::new();
    intent.set_by_id(10);
    intent.set_by_id(20);

    assert!(
        !intent.scalar().key_access_conflict,
        "reusing the same key access kind should not mark conflicts"
    );
    assert!(
        matches!(
            intent.scalar().key_access.as_ref().map(|state| state.kind),
            Some(KeyAccessKind::Single)
        ),
        "latest same-kind key access should remain single-key access"
    );

    intent.set_only(20);

    assert!(
        intent.scalar().key_access_conflict,
        "mixing key access kinds should mark intent key-access conflict"
    );
    assert!(
        matches!(
            intent.scalar().key_access.as_ref().map(|state| state.kind),
            Some(KeyAccessKind::Only)
        ),
        "latest mixed-kind key access should keep most recent origin kind"
    );
}
