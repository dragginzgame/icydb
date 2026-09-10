//! Order preparation preserves scalar semantics and charges before construction.

use super::*;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::{
                AccessPlannedQuery, LogicalPlan, OrderDirection, OrderTerm, ResolvedOrder,
                ResolvedOrderField, ResolvedOrderValueSource,
                expr::{
                    BinaryOp, CompiledExpr, Expr, FieldId, UnaryOp,
                    compile_scalar_projection_expr_with_schema,
                },
            },
            preparation::PreparationWork,
        },
    },
    error::InternalError,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn order_plan(setup: &DbSession<TestCanister>, fields: Vec<OrderTerm>) -> AccessPlannedQuery {
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let (prepared, _) = setup
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            &catalog,
            &StructuralQuery::new(MissingRowPolicy::Ignore)
                .order_spec(OrderSpec {
                    fields: vec![asc("id").lower()],
                })
                .limit(10),
            DiagnosticExecutionLane::TrustedRead,
        )
        .unwrap();
    let mut plan = prepared.logical_plan().clone();
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        panic!("scalar fixture required");
    };
    scalar.order = Some(OrderSpec { fields });
    plan
}

#[test]
fn direct_order_construction_limits_preserve_directions_and_duplicate_slots() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let schema = catalog.accepted_schema_info();
    let slot = schema.field_slot_index("id").unwrap();
    let plan = order_plan(
        &setup,
        vec![
            OrderTerm::field("id", OrderDirection::Asc),
            OrderTerm::field("id", OrderDirection::Desc),
        ],
    );
    let (projection_bytes, projection_steps) = projection_metadata::cost(&plan, schema);
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        for (resource, exact) in [
            (
                Resource::TemporaryBytes,
                projection_bytes
                    + (2 * size_of::<ResolvedOrderField>() + 4 * size_of::<usize>()) as u64,
            ),
            (Resource::PredicateExpressionSteps, projection_steps + 11),
        ] {
            for limit in [exact - 1, exact] {
                let root = request(resource, limit);
                let mut candidate = plan.clone();
                let before = candidate.clone();
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    let projection = candidate.prepare_projection(schema, work)?;
                    candidate.finalize_static_execution_planning_contract_with_schema(
                        schema, projection, work,
                    )
                });
                if limit == exact {
                    result.unwrap();
                    let order = candidate.resolved_order().unwrap();
                    assert_eq!(order.direct_field_slots().unwrap(), vec![slot, slot]);
                    assert_eq!(order.fields()[0].direction(), OrderDirection::Asc);
                    assert_eq!(order.fields()[1].direction(), OrderDirection::Desc);
                    assert_eq!(candidate.order_referenced_slots().unwrap(), &[slot]);
                } else {
                    assert!(
                        result
                            .unwrap_err()
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                    assert_eq!(candidate.resolved_order(), before.resolved_order());
                }
                assert_eq!(root.observed(resource), exact);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn expression_order_seam_is_budgeted_and_preserves_compiled_output() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let schema = catalog.accepted_schema_info();
    let expr = Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("id"))),
        right: Box::new(Expr::Field(FieldId::new("id"))),
    };
    let expected = ResolvedOrderValueSource::expression(
        crate::db::query::preparation::with_preparation_work(|work| {
            compile_scalar_projection_expr_with_schema(schema, &expr, work)
        })
        .unwrap()
        .unwrap(),
    );
    let plan = order_plan(&setup, vec![OrderTerm::new(expr, OrderDirection::Asc)]);
    let (_, projection_steps) = projection_metadata::cost(&plan, schema);
    // Three seam nodes, fifteen compiler steps (three nodes and two lookup/copy
    // label pairs), then one order field, one compiled node, two slots and a duplicate comparison.
    for limit in [22, 23] {
        let root = request(Resource::PredicateExpressionSteps, projection_steps + limit);
        let mut candidate = plan.clone();
        let result =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                let projection = candidate.prepare_projection(schema, work)?;
                candidate.finalize_static_execution_planning_contract_with_schema(
                    schema, projection, work,
                )
            });
        if limit == 23 {
            result.unwrap();
            assert_eq!(
                candidate.resolved_order().unwrap().fields()[0].source(),
                &expected
            );
        } else {
            assert!(result.unwrap_err().diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::PredicateExpressionSteps.raw(),
            )));
        }
        assert_eq!(
            root.observed(Resource::PredicateExpressionSteps),
            projection_steps + 23
        );
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn order_seam_and_missing_fields_keep_typed_rejections() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let expected = QueryError::execute(InternalError::query_invalid_logical_plan()).diagnostic();
    for expr in [
        Expr::Field(FieldId::new("absent")),
        Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("absent"))),
            right: Box::new(Expr::Field(FieldId::new("id"))),
        },
        Expr::Aggregate(crate::db::query::builder::count()),
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Field(FieldId::new("id"))),
        },
    ] {
        let mut plan = order_plan(&setup, vec![OrderTerm::new(expr, OrderDirection::Asc)]);
        let error = PreparationWork::run(
            setup.db.request_execution_scope(),
            DiagnosticExecutionLane::PublicRead,
            |work| {
                let projection = plan.prepare_projection(catalog.accepted_schema_info(), work)?;
                plan.finalize_static_execution_planning_contract_with_schema(
                    catalog.accepted_schema_info(),
                    projection,
                    work,
                )
            },
        )
        .unwrap_err();
        assert_eq!(error.diagnostic(), expected);
    }
}

#[test]
fn order_slot_construction_preserves_first_reference_order_at_exact_limits() {
    let order = ResolvedOrder::new(vec![
        ResolvedOrderField::new(
            ResolvedOrderValueSource::direct_field(8),
            OrderDirection::Desc,
        ),
        ResolvedOrderField::new(
            ResolvedOrderValueSource::expression(CompiledExpr::Add {
                left_slot: 3,
                left_field: "left".into(),
                right_slot: 8,
                right_field: "right".into(),
            }),
            OrderDirection::Asc,
        ),
        ResolvedOrderField::new(
            ResolvedOrderValueSource::direct_field(3),
            OrderDirection::Asc,
        ),
        ResolvedOrderField::new(
            ResolvedOrderValueSource::direct_field(9),
            OrderDirection::Asc,
        ),
    ]);
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        for (resource, exact) in [
            (Resource::PredicateExpressionSteps, 16),
            (Resource::TemporaryBytes, 4 * size_of::<usize>() as u64),
        ] {
            for limit in [exact - 1, exact] {
                let root = request(resource, limit);
                let result =
                    PreparationWork::run(&root.scope(), lane, |work| order.referenced_slots(work));
                if limit == exact {
                    assert_eq!(result.unwrap(), [8, 3, 9]);
                } else {
                    assert!(
                        result
                            .unwrap_err()
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                }
                assert_eq!(root.observed(resource), exact);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn order_slot_rejection_preserves_prior_entries_and_duplicates_need_no_backing() {
    for (resource, limit) in [
        (Resource::PredicateExpressionSteps, 1),
        (Resource::TemporaryBytes, 4 * size_of::<usize>() as u64 - 1),
    ] {
        let mut slots = vec![8];
        let capacity = slots.capacity();
        let root = request(resource, limit);
        let error =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                ResolvedOrderValueSource::direct_field(3).extend_referenced_slots(&mut slots, work)
            })
            .unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
        assert_eq!(slots, [8]);
        assert_eq!(slots.capacity(), capacity);
    }
    let root = request(Resource::TemporaryBytes, 0);
    let mut slots = vec![8];
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        ResolvedOrderValueSource::direct_field(8).extend_referenced_slots(&mut slots, work)
    })
    .unwrap();
    assert_eq!(slots, [8]);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 2);
}

#[test]
fn slot_free_order_expressions_still_charge_node_visits() {
    let order = ResolvedOrder::new(vec![ResolvedOrderField::new(
        ResolvedOrderValueSource::expression(CompiledExpr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(CompiledExpr::Literal(crate::value::Value::Bool(true))),
        }),
        OrderDirection::Asc,
    )]);
    for limit in [2, 3] {
        let root = request(Resource::PredicateExpressionSteps, limit);
        let result =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                order.referenced_slots(work)
            });
        if limit == 3 {
            assert!(result.unwrap().is_empty());
        } else {
            assert!(result.unwrap_err().diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::PredicateExpressionSteps.raw(),
            )));
        }
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 3);
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
}
