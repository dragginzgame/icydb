use super::*;
use crate::{
    db::query::plan::{FieldSlot, GroupField, GroupFieldSet, OrderDirection, expr::FieldId},
    value::Value,
};

#[test]
fn rejected_filter_extraction_preserves_existing_intent() {
    use crate::db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
    };

    let mut intent = QueryIntent::new();
    let original = Predicate::eq("tenant".into(), Value::Text("retained".into()));
    intent.append_predicate(original.clone());
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 0),
    );
    let error = PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        intent.append_filter_expr(Expr::Field(FieldId::new("enabled")), work)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    let retained = intent.scalar().filter.as_ref().expect("existing filter");
    assert_eq!(retained.predicate_subset(), Some(&original));
    assert_eq!(retained.predicate_coverage(), FilterPredicateCoverage::Full);
    assert!(retained.logical_filter_expr().is_none());
}

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
    crate::db::query::preparation::with_preparation_work(|work| {
        let mut intent = QueryIntent::new();
        let mut fields = GroupFieldSet::empty();
        fields
            .push(
                GroupField::Direct(FieldSlot::from_test_slot(4, "rank")),
                work,
            )
            .unwrap();
        fields
            .push(
                GroupField::Direct(FieldSlot::from_test_slot(4, "duplicate-rank")),
                work,
            )
            .unwrap();
        intent.set_group_fields(fields);

        let grouped = intent
            .grouped()
            .expect("grouped shape should be materialized after grouped slot push");

        assert_eq!(
            grouped.group.group_fields.len(),
            1,
            "group field slots should be deduplicated by stable model slot index"
        );
    });
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
fn appended_predicate_preserves_uncovered_expression_semantics() {
    // Supply the extraction result at this owner's boundary so expanding the
    // compiler's capabilities does not change the coverage-transition fixture.
    let expression = Expr::Field(FieldId::new("flag"));
    let mut filter = NormalizedFilter {
        semantic_authority: FilterSemanticAuthority::ExpressionBacked(expression.clone()),
        predicate_subset: None,
        predicate_coverage: FilterPredicateCoverage::None,
    };
    let appended = Predicate::eq("tenant".into(), Value::Text("retained".into()));
    filter.append_predicate(appended.clone());

    assert_eq!(filter.logical_filter_expr(), Some(&expression));
    assert_eq!(filter.predicate_subset(), Some(&appended));
    assert_eq!(
        filter.predicate_coverage(),
        FilterPredicateCoverage::Partial
    );
    assert!(
        !filter
            .predicate_coverage()
            .covers_user_visible_filter_semantics(),
        "an appended predicate must not claim coverage of the retained expression",
    );
    assert!(!filter.predicate_subset_covers_expr());
}

#[test]
fn order_spec_preserves_declared_order_sequence() {
    let mut intent = QueryIntent::new();
    intent.set_order_spec(crate::db::query::plan::OrderSpec {
        fields: vec![
            crate::db::asc("rank").lower(),
            crate::db::desc("created_at").lower(),
        ],
    });

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
