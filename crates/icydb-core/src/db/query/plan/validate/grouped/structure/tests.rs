//! HAVING lookup preserves semantic errors and admits repeated comparison work.

use super::{
    resolve_group_having_aggregate_index, validate_group_projection_expr_compatibility,
    validate_group_structure, validate_grouped_having_structure,
};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            builder::{AggregateExpr, count, min_by, sum},
            plan::{
                AggregateKind, FieldSlot, GroupAggregateSpec, GroupField, GroupFieldSet, GroupSpec,
                GroupedExecutionConfig, exact_metadata_schema,
                expr::{BinaryOp, Expr, FieldPath, ProjectionField, ProjectionSpec},
                validate::{ExprPlanError, GroupPlanError, PlanError},
            },
            preparation::PreparationWork,
        },
        schema::AcceptedFieldKind,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag, QueryFieldRole,
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

fn group(aggregates: Vec<AggregateExpr>) -> GroupSpec {
    GroupSpec {
        group_fields: GroupFieldSet::Direct(vec![]),
        aggregates: aggregates
            .into_iter()
            .map(GroupAggregateSpec::from_aggregate_expr)
            .collect(),
        execution: GroupedExecutionConfig::planner_default_bounded(),
    }
}

fn filtered_sum(value: i64) -> AggregateExpr {
    sum("age").with_filter_expr(Expr::Binary {
        op: BinaryOp::Gte,
        left: Box::new(Expr::Field("rank".into())),
        right: Box::new(Expr::Literal(Value::Int64(value))),
    })
}

fn validate(
    group: &GroupSpec,
    having: Option<&Expr>,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<(), QueryError> {
    let schema = exact_metadata_schema(&[], &[]);
    PreparationWork::run(&root.scope(), lane, |work| {
        validate_group_structure(&schema, group, &ProjectionSpec::default(), having, work)
    })
}

#[test]
fn having_lookup_keeps_first_match_and_canonical_count_distinct_rules() {
    let target = filtered_sum(7);
    let candidates = group(vec![
        count(),
        filtered_sum(6),
        target.clone(),
        target.clone(),
    ]);
    let root = request(Resource::TemporaryBytes, 0);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        assert_eq!(
            resolve_group_having_aggregate_index(&candidates, &target, work)?,
            Some(2)
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);

    for (candidate, target) in [
        (
            count(),
            AggregateExpr::from_expression_input(
                AggregateKind::Count,
                Expr::Literal(Value::Text("x".repeat(8192))),
            ),
        ),
        (min_by("age"), min_by("age").distinct()),
    ] {
        let root = request(Resource::NestedValueSteps, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert_eq!(
                resolve_group_having_aggregate_index(&group(vec![candidate]), &target, work)?,
                Some(0)
            );
            Ok(())
        })
        .unwrap();
    }
    // Header mismatch stops before inspecting a large literal operand.
    let target = AggregateExpr::from_expression_input(
        AggregateKind::Sum,
        Expr::Literal(Value::List(vec![Value::Bool(true); 1000])),
    );
    let root = request(Resource::NestedValueSteps, 0);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        assert_eq!(
            resolve_group_having_aggregate_index(&group(vec![count()]), &target, work)?,
            None
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 1);
}

#[test]
fn having_validation_enforces_exact_and_cumulative_request_limits() {
    let target = filtered_sum(7);
    let group = group(vec![
        count(),
        filtered_sum(6),
        target.clone(),
        target.clone(),
    ]);
    let before = group.clone();
    let having = Expr::Binary {
        op: BinaryOp::Gte,
        left: Box::new(Expr::Aggregate(target)),
        right: Box::new(Expr::Literal(Value::Int64(0))),
    };
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let measured = request(Resource::TemporaryBytes, 0);
        validate(&group, Some(&having), &measured, lane).unwrap();
        for resource in [
            Resource::PredicateExpressionSteps,
            Resource::NestedValueSteps,
        ] {
            let exact = measured.observed(resource);
            assert!(exact > 0);
            for limit in [exact - 1, exact, 2 * exact] {
                let root = request(resource, limit);
                for attempt in 1..=3 {
                    let result = validate(&group, Some(&having), &root, lane);
                    if attempt * exact <= limit {
                        result.unwrap();
                    } else {
                        let facts = result.unwrap_err().diagnostic_facts();
                        assert!(
                            facts.contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                        );
                        assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
                        break;
                    }
                }
                assert_eq!(root.observed(Resource::TemporaryBytes), 0);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::QueryExecutions), 0);
            }
            validate(&group, Some(&having), &request(resource, exact), lane).unwrap();
        }
    }
    assert_eq!(group, before);
}

#[test]
fn having_exhaustion_is_not_a_missing_aggregate_error() {
    let group = group(vec![filtered_sum(6)]);
    let having = Expr::Binary {
        op: BinaryOp::Gte,
        left: Box::new(Expr::Aggregate(filtered_sum(7))),
        right: Box::new(Expr::Literal(Value::Int64(0))),
    };
    let error = validate(
        &group,
        Some(&having),
        &request(Resource::NestedValueSteps, 0),
        Lane::Diagnostic,
    )
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::NestedValueSteps.raw()
    )));
    let error = validate(
        &group,
        Some(&having),
        &request(Resource::TemporaryBytes, 0),
        Lane::Diagnostic,
    )
    .unwrap_err();
    let expected = QueryError::from(PlanError::from(
        GroupPlanError::having_aggregate_index_out_of_bounds(0, 1, 1),
    ));
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
}

#[test]
fn absent_having_needs_no_lookup_budget() {
    let root = request(Resource::PredicateExpressionSteps, 0);
    validate(&group(vec![count()]), None, &root, Lane::Diagnostic).unwrap();
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
}

fn projection(exprs: Vec<Expr>) -> ProjectionSpec {
    ProjectionSpec::from_fields_for_test(
        exprs
            .into_iter()
            .map(|expr| ProjectionField::Scalar { expr, alias: None })
            .collect(),
    )
}

#[test]
fn grouped_field_membership_admits_exact_and_cumulative_work() {
    for (fields, expr) in [
        (
            GroupFieldSet::Direct(vec![
                FieldSlot::from_test_slot(0, "rank"),
                FieldSlot::from_test_slot(1, "age"),
            ]),
            Expr::Field("age".into()),
        ),
        (
            GroupFieldSet::PathAware(vec![GroupField::scalar_path_for_test(
                "profile.age",
                "profile",
                vec!["age".into()],
                0,
                AcceptedFieldKind::Int32,
            )]),
            Expr::FieldPath(FieldPath::new("profile", vec!["age".into()])),
        ),
    ] {
        let mut group = group(vec![count()]);
        group.group_fields = fields;
        let projection = projection(vec![expr.clone()]);
        for having in [false, true] {
            for lane in [Lane::PublicRead, Lane::Diagnostic] {
                let run = |root: &RequestExecutionRoot| {
                    PreparationWork::run(&root.scope(), lane, |work| {
                        if having {
                            validate_grouped_having_structure(&group, Some(&expr), work)
                        } else {
                            validate_group_projection_expr_compatibility(&group, &projection, work)
                        }
                    })
                };
                let baseline = request(Resource::TemporaryBytes, 0);
                run(&baseline).unwrap();
                let exact = baseline.observed(Resource::PredicateExpressionSteps);
                // One leaf visit plus each candidate's existing comparison charge.
                assert_eq!(
                    exact,
                    1 + group
                        .group_fields
                        .iter()
                        .map(|f| 1 + f.field().len() as u64)
                        .sum::<u64>()
                );
                let root = request(Resource::PredicateExpressionSteps, exact);
                run(&root).unwrap();
                for error in [
                    run(&request(Resource::PredicateExpressionSteps, exact - 1)).unwrap_err(),
                    run(&root).unwrap_err(),
                ] {
                    assert!(matches!(error, QueryError::Execute(_)));
                    assert!(error.diagnostic_facts().contains(&(
                        DiagnosticFactTag::BudgetResource,
                        Resource::PredicateExpressionSteps.raw(),
                    )));
                }
                run(&request(Resource::PredicateExpressionSteps, exact)).unwrap();
                assert_eq!(baseline.observed(Resource::TemporaryBytes), 0);
            }
        }
    }
}

#[test]
fn projection_membership_skips_aggregate_operands_and_keeps_first_error() {
    let mut group = group(vec![count()]);
    group.group_fields = GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(0, "age")]);
    let aggregate = Expr::Aggregate(filtered_sum(7));
    let run = |projection: &ProjectionSpec, root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            validate_group_projection_expr_compatibility(&group, projection, work)
        })
    };
    // The aggregate's row-level filter mentions rank, which is not a group key.
    run(
        &projection(vec![aggregate.clone()]),
        &request(Resource::PredicateExpressionSteps, 1),
    )
    .unwrap();
    let fields = projection(vec![
        aggregate,
        Expr::Field("rank".into()),
        Expr::Field("later".into()),
    ]);
    // Just enough work to reach the first invalid projection, not the later field.
    let error = run(&fields, &request(Resource::PredicateExpressionSteps, 6)).unwrap_err();
    let expected = QueryError::from(PlanError::from(
        ExprPlanError::grouped_projection_references_non_group_field(1),
    ));
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    group.group_fields = GroupFieldSet::default();
    PreparationWork::run(
        &request(Resource::PredicateExpressionSteps, 0).scope(),
        Lane::Diagnostic,
        |work| validate_group_projection_expr_compatibility(&group, &fields, work),
    )
    .unwrap();
}

#[test]
fn having_field_rejections_preserve_compare_index_and_bound_labels() {
    let mut group = group(vec![count()]);
    group.group_fields = GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(0, "age")]);
    let expr = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field("age".into())),
            right: Box::new(Expr::Literal(Value::Int64(1))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::FieldPath(FieldPath::new(
                "profile",
                vec!["rank".into()],
            ))),
            right: Box::new(Expr::Field("later".into())),
        }),
    };
    let run = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            validate_grouped_having_structure(&group, Some(&expr), work)
        })
    };
    let error = run(&request(Resource::TemporaryBytes, 0)).unwrap_err();
    assert!(matches!(error, QueryError::Execute(_)));
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
    let error = run(&request(Resource::TemporaryBytes, 16_000_000)).unwrap_err();
    let expected = QueryError::from(
        PlanError::from(GroupPlanError::having_non_group_field_reference(
            1,
            "profile.rank",
        ))
        .attach_query_field(QueryFieldRole::Having),
    );
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
}
