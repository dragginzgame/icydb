//! Grouped order admission preserves lane selection, diagnostics and request ownership.

use super::validate_group_cursor_constraints;
use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    predicate::MissingRowPolicy,
    query::{
        builder::count,
        plan::{
            AccessPlannedQuery, FieldSlot, GroupAggregateSpec, GroupFieldSet, GroupSpec,
            GroupedExecutionConfig, OrderDirection, OrderSpec, OrderTerm, PageSpec, ScalarPlan,
            expr::{BinaryOp, Expr, Function},
            validate::{GroupPlanError, PlanErrorKind, PlanPolicyError},
        },
        preparation::PreparationWork,
    },
};
use crate::value::Value;
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn fixture(exprs: Vec<Expr>, limit: Option<u32>) -> (ScalarPlan, GroupSpec) {
    let mut logical = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore)
        .scalar_plan()
        .clone();
    logical.order = Some(OrderSpec {
        fields: exprs
            .into_iter()
            .map(|expr| OrderTerm::new(expr, OrderDirection::Asc))
            .collect(),
    });
    logical.page = Some(PageSpec { limit, offset: 0 });
    let group = GroupSpec {
        group_fields: GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(0, "key")]),
        aggregates: vec![GroupAggregateSpec::from_aggregate_expr(count())],
        execution: GroupedExecutionConfig::planner_default_bounded(),
    };
    (logical, group)
}

fn validate(
    root: &RequestExecutionRoot,
    lane: Lane,
    logical: &ScalarPlan,
    group: &GroupSpec,
) -> Result<(), QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        validate_group_cursor_constraints(logical, group, work)
    })
}

fn assert_policy(error: QueryError, expected: GroupPlanError) {
    let QueryError::Plan(error) = error else {
        panic!("expected plan error")
    };
    let PlanErrorKind::Policy(error) = error.into_kind() else {
        panic!("expected policy error")
    };
    let PlanPolicyError::Group(error) = *error else {
        panic!("expected grouped error")
    };
    assert_eq!(*error, expected);
}

fn assert_resource(error: QueryError, resource: Resource) {
    assert!(matches!(error, QueryError::Execute(_)));
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
    );
}

#[test]
fn absent_order_is_free_and_canonical_order_keeps_finite_limit_policy() {
    let (mut logical, mut group) = fixture(vec![Expr::Field("key".into())], None);
    validate(
        &request(Resource::TemporaryBytes, 0),
        Lane::PublicRead,
        &logical,
        &group,
    )
    .unwrap();
    group.execution.max_groups = 0;
    assert_policy(
        validate(
            &request(Resource::TemporaryBytes, 0),
            Lane::PublicRead,
            &logical,
            &group,
        )
        .unwrap_err(),
        GroupPlanError::OrderRequiresLimit,
    );
    logical.order = None;
    let root = request(Resource::PredicateExpressionSteps, 0);
    validate(&root, Lane::Diagnostic, &logical, &group).unwrap();
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn later_heap_trigger_changes_earlier_term_admission_before_limit_policy() {
    let computed = Expr::Binary {
        op: BinaryOp::Mul,
        left: Box::new(Expr::Field("key".into())),
        right: Box::new(Expr::Literal(Value::Nat64(2))),
    };
    let (mut logical, group) = fixture(vec![computed, Expr::Aggregate(count())], Some(3));
    validate(
        &request(Resource::TemporaryBytes, 0),
        Lane::PublicRead,
        &logical,
        &group,
    )
    .unwrap();
    logical.page = None;
    assert_policy(
        validate(
            &request(Resource::TemporaryBytes, 0),
            Lane::PublicRead,
            &logical,
            &group,
        )
        .unwrap_err(),
        GroupPlanError::OrderRequiresLimit,
    );
    // Once Top-K is selected, a later unknown field must still reject before LIMIT.
    logical
        .order
        .as_mut()
        .unwrap()
        .fields
        .push(OrderTerm::field("other", OrderDirection::Asc));
    assert_policy(
        validate(
            &request(Resource::TemporaryBytes, 0),
            Lane::PublicRead,
            &logical,
            &group,
        )
        .unwrap_err(),
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys,
    );
}

#[test]
fn grouped_order_steps_are_exact_cumulative_and_preserve_resource_errors() {
    for terms in [
        vec![Expr::Field("key".into())],
        vec![Expr::Aggregate(count()), Expr::Field("key".into())],
    ] {
        let (logical, group) = fixture(terms, Some(3));
        let baseline = request(Resource::PredicateExpressionSteps, 16_000_000);
        validate(&baseline, Lane::PublicRead, &logical, &group).unwrap();
        let exact = baseline.observed(Resource::PredicateExpressionSteps);
        assert!(exact > 0);
        assert_eq!(baseline.observed(Resource::TemporaryBytes), 0);
        for lane in [Lane::PublicRead, Lane::Diagnostic] {
            assert_resource(
                validate(
                    &request(Resource::PredicateExpressionSteps, exact - 1),
                    lane,
                    &logical,
                    &group,
                )
                .unwrap_err(),
                Resource::PredicateExpressionSteps,
            );
            let root = request(Resource::PredicateExpressionSteps, exact);
            validate(&root, lane, &logical, &group).unwrap();
            assert_resource(
                validate(&root, lane, &logical, &group).unwrap_err(),
                Resource::PredicateExpressionSteps,
            );
            validate(
                &request(Resource::PredicateExpressionSteps, exact),
                lane,
                &logical,
                &group,
            )
            .unwrap();
        }
    }
}

#[test]
fn unsupported_labels_are_bounded_and_first_term_errors_are_preserved() {
    let unsupported = Expr::FunctionCall {
        function: Function::Abs,
        args: vec![Expr::Field("key".into())],
    };
    for top_k in [false, true] {
        let mut terms = vec![unsupported.clone()];
        if top_k {
            terms.push(Expr::Aggregate(count()));
        }
        terms.push(Expr::Field("other".into()));
        let (logical, group) = fixture(terms, None);
        let label = logical.order.as_ref().unwrap().fields[0].rendered_label();
        assert_resource(
            validate(
                &request(Resource::TemporaryBytes, 0),
                Lane::Diagnostic,
                &logical,
                &group,
            )
            .unwrap_err(),
            Resource::TemporaryBytes,
        );
        assert_policy(
            validate(
                &request(Resource::TemporaryBytes, 16_000_000),
                Lane::Diagnostic,
                &logical,
                &group,
            )
            .unwrap_err(),
            GroupPlanError::OrderExpressionNotAdmissible { term: label },
        );
    }
}
