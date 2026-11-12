use super::*;
use crate::db::{
    predicate::Predicate,
    query::{plan::expr::BinaryOp, preparation::PreparationWork},
};
use crate::{
    db::query::plan::{FieldSlot, GroupField, GroupFieldSet, OrderDirection, expr::FieldId},
    value::Value,
};

#[test]
fn rejected_filter_extraction_preserves_existing_intent() {
    use crate::db::{
        RequestExecutionRoot,
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
    };

    let schema = crate::db::query::plan::exact_metadata_schema(&[], &[]);
    let mut intent = QueryIntent::new();
    let original = Predicate::eq("tenant".into(), Value::Text("retained".into()));
    let original_expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field("tenant".into())),
        right: Box::new(Expr::Literal(Value::Text("retained".into()))),
    };
    crate::db::query::preparation::with_preparation_work(|work| {
        intent.append_admitted_filter(
            AdmittedFilter::from_expr(&schema, original_expr.clone(), work)?,
            work,
        )
    })
    .unwrap();
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 0),
    );
    let error = PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        intent.append_admitted_filter(
            AdmittedFilter::from_expr(&schema, Expr::Field(FieldId::new("enabled")), work)?,
            work,
        )
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    let retained = intent.scalar().filter.as_ref().expect("existing filter");
    assert_eq!(retained.predicate_subset(), Some(&original));
    assert!(retained.predicate_subset_covers_expr());
    assert_eq!(retained.logical_filter_expr(), &original_expr);
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
fn appended_expression_preserves_uncovered_expression_semantics() {
    let schema = crate::db::query::plan::exact_metadata_schema(&[], &[]);
    let expression = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("age"))),
            right: Box::new(Expr::Literal(Value::Int64(1))),
        }),
        right: Box::new(Expr::Literal(Value::Int64(5))),
    };
    let mut filter = crate::db::query::preparation::with_preparation_work(|work| {
        AdmittedFilter::from_expr(&schema, expression, work)
    })
    .unwrap();
    let appended = Predicate::eq("tenant".into(), Value::Text("retained".into()));
    let clause = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field("tenant".into())),
        right: Box::new(Expr::Literal(Value::Text("retained".into()))),
    };
    crate::db::query::preparation::with_preparation_work(|work| {
        filter.append(AdmittedFilter::from_expr(&schema, clause, work)?, work)
    })
    .unwrap();

    assert!(matches!(
        filter.logical_filter_expr(),
        Expr::Binary {
            op: BinaryOp::And,
            ..
        }
    ));
    assert_eq!(filter.predicate_subset(), Some(&appended));
    assert!(
        !filter.predicate_subset_covers_expr(),
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
