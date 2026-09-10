mod order;

use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{MissingRowPolicy, Predicate},
        query::{
            builder::min_by,
            plan::{
                FieldSlot, GroupAggregateSpec, GroupField, GroupFieldSet, GroupSpec,
                GroupedExecutionConfig, LoadSpec, OrderDirection, OrderSpec, OrderTerm, QueryMode,
                expr::{Expr, ProjectionField, ProjectionSelection},
                logical_builder::{LogicalPlanningInputs, logical_query_from_logical_inputs},
            },
            preparation::PreparationWork,
        },
        schema::AcceptedFieldKind,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn root(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn assert_budget_error(error: QueryError, resource: Resource) {
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
    );
}

#[test]
fn logical_clauses_preserve_scalar_grouped_and_shared_slot_identity() {
    let filter = Expr::Literal(Value::Bool(true));
    let order = OrderSpec {
        fields: vec![OrderTerm::field("rank", OrderDirection::Desc)],
    };
    let having = Expr::Aggregate(min_by("rank").distinct());
    let direct = FieldSlot::from_test_accepted_kind(0, "rank", AcceptedFieldKind::Int32);
    let path = GroupField::scalar_path_for_test(
        "profile.rank",
        "profile",
        vec!["rank".into()],
        1,
        AcceptedFieldKind::Int32,
    );
    for fields in [
        GroupFieldSet::Direct(vec![direct.clone()]),
        GroupFieldSet::PathAware(vec![GroupField::Direct(direct.clone()), path]),
    ] {
        let group = GroupSpec {
            group_fields: fields,
            aggregates: vec![GroupAggregateSpec::from_shape(min_by("rank").into_shape())],
            execution: GroupedExecutionConfig::planner_default_bounded(),
        };
        for grouped in [false, true] {
            let request = root(Resource::TemporaryBytes, 16_000_000);
            let copied = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
                logical_query_from_logical_inputs(
                    LogicalPlanningInputs::new(
                        QueryMode::Load(LoadSpec::new()),
                        Some(&filter),
                        true,
                        Some(&order),
                        true,
                        grouped.then_some(&group),
                        grouped.then_some(&having),
                    ),
                    Some(Predicate::True),
                    MissingRowPolicy::Ignore,
                    work,
                )
            })
            .unwrap();
            assert_eq!(copied.filter_expr.as_ref(), Some(&filter));
            assert_eq!(copied.order.as_ref(), Some(&order));
            assert_eq!(copied.group.as_ref(), grouped.then_some(&group));
            assert_eq!(copied.having_expr.as_ref(), grouped.then_some(&having));
            assert_eq!(copied.normalized_predicate, Some(Predicate::True));
            assert!(copied.distinct && copied.filter_predicate_covers_expr);
            if let Some(group) = copied.group {
                let copied_slot = group.group_fields.get(0).unwrap().as_direct().unwrap();
                assert!(std::sync::Arc::ptr_eq(&copied_slot.field, &direct.field));
            }
        }
    }
}

#[test]
fn logical_copy_charges_exact_backing_cumulatively_in_every_lane() {
    let order = OrderSpec {
        fields: vec![OrderTerm::field("rank", OrderDirection::Asc)],
    };
    let bytes = (size_of::<OrderTerm>() + "rank".len()) as u64;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let request = root(Resource::TemporaryBytes, bytes);
        let copy = || {
            PreparationWork::run(&request.scope(), lane, |work| {
                logical_query_from_logical_inputs(
                    LogicalPlanningInputs::new(
                        QueryMode::Load(LoadSpec::new()),
                        None,
                        false,
                        Some(&order),
                        false,
                        None,
                        None,
                    ),
                    None,
                    MissingRowPolicy::Ignore,
                    work,
                )
            })
        };
        assert_eq!(copy().unwrap().order.as_ref(), Some(&order));
        assert_eq!(request.observed(Resource::TemporaryBytes), bytes);
        assert_budget_error(copy().unwrap_err(), Resource::TemporaryBytes);
        // The second attempt rejects the clause backing before visiting its expression.
        assert_eq!(request.observed(Resource::PredicateExpressionSteps), 6);
    }
}

#[test]
fn rejected_order_backing_does_not_visit_operands_or_change_source() {
    let order = OrderSpec {
        fields: vec![OrderTerm::field("rank", OrderDirection::Desc)],
    };
    let before = order.clone();
    let request = root(Resource::TemporaryBytes, size_of::<OrderTerm>() as u64 - 1);
    let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        logical_query_from_logical_inputs(
            LogicalPlanningInputs::new(
                QueryMode::Load(LoadSpec::new()),
                None,
                false,
                Some(&order),
                false,
                None,
                None,
            ),
            None,
            MissingRowPolicy::Ignore,
            work,
        )
    })
    .unwrap_err();
    assert_budget_error(error, Resource::TemporaryBytes);
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 0);
    assert_eq!(order, before);
}

#[test]
fn stripped_filter_never_copies_its_payload() {
    let filter = Expr::Literal(Value::Text("not retained".into()));
    let request = root(Resource::TemporaryBytes, 0);
    let query = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        logical_query_from_logical_inputs(
            LogicalPlanningInputs::new(
                QueryMode::Load(LoadSpec::new()),
                Some(&filter),
                true,
                None,
                false,
                None,
                None,
            )
            .without_filter_expr(),
            None,
            MissingRowPolicy::Ignore,
            work,
        )
    })
    .unwrap();
    assert!(query.filter_expr.is_none());
    assert!(!query.filter_predicate_covers_expr);
    assert_eq!(request.observed(Resource::TemporaryBytes), 0);
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 0);
}

#[test]
fn projection_copy_preserves_duplicates_aliases_and_typed_rejection() {
    for source in [
        ProjectionSelection::All,
        ProjectionSelection::Fields(vec!["rank".into(), "rank".into()]),
        ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
            expr: Expr::Literal(Value::Text("payload".into())),
            alias: Some("label".into()),
        }]),
    ] {
        let request = root(Resource::TemporaryBytes, 16_000_000);
        let copied = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
            source.copy_for_preparation(work)
        })
        .unwrap();
        assert_eq!(copied, source);
        let bytes = request.observed(Resource::TemporaryBytes);
        if bytes > 0 {
            let short = root(Resource::TemporaryBytes, bytes - 1);
            let error = PreparationWork::run(&short.scope(), Lane::Diagnostic, |work| {
                source.copy_for_preparation(work)
            })
            .unwrap_err();
            assert_budget_error(error, Resource::TemporaryBytes);
        }
    }
}
