//! Projection slot construction uses current authority and request accounting.

use super::*;
use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::{
        plan::{
            AccessPlannedQuery, LogicalPlan,
            expr::{Expr, FieldId as ExprFieldId, ProjectionField, ProjectionSpec},
            lower_direct_projection_layouts_with_schema,
        },
        preparation::PreparationWork,
    },
    schema::SchemaInfo,
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

fn scalar_logical(setup: &DbSession<TestCanister>) -> LogicalPlan {
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let (plan, _) = setup
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
    plan.logical_plan().logical.clone()
}

#[test]
fn direct_layout_boundaries_preserve_field_order_and_duplicate_policy() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let schema = catalog.accepted_schema_info();
    let logical = scalar_logical(&setup);
    for (names, unique, steps) in [
        (&["rare", "id"][..], true, 11),
        (&["rare", "id", "rare"][..], false, 15),
    ] {
        let spec = ProjectionSpec::from_fields_for_test(
            names
                .iter()
                .map(|name| ProjectionField::Scalar {
                    expr: Expr::Field(ExprFieldId::new(*name)),
                    alias: None,
                })
                .collect(),
        );
        let expected = names
            .iter()
            .map(|name| schema.field_slot_index(name).unwrap())
            .collect::<Vec<_>>();
        let bytes = (names.len() * size_of::<usize>() * if unique { 2 } else { 1 }) as u64;
        for lane in [
            DiagnosticExecutionLane::PublicRead,
            DiagnosticExecutionLane::TrustedRead,
        ] {
            for (resource, exact) in [
                (Resource::TemporaryBytes, bytes),
                (Resource::PredicateExpressionSteps, steps),
            ] {
                for limit in [exact - 1, exact] {
                    let root = request(resource, limit);
                    let result = PreparationWork::run(&root.scope(), lane, |work| {
                        lower_direct_projection_layouts_with_schema(schema, &logical, &spec, work)
                    });
                    if limit == exact {
                        let (consuming, raw) = result.unwrap();
                        assert_eq!(raw.as_deref(), Some(expected.as_slice()));
                        assert_eq!(consuming.as_deref(), unique.then_some(expected.as_slice()));
                    } else {
                        assert!(
                            result
                                .unwrap_err()
                                .diagnostic_facts()
                                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                        );
                    }
                    assert_eq!(root.observed(resource), exact);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn non_direct_or_unresolved_layout_is_unavailable_not_a_budget_error() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let logical = scalar_logical(&setup);
    for expr in [
        Expr::Literal(crate::value::Value::Null),
        Expr::Field(ExprFieldId::new("absent")),
    ] {
        let spec = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr,
            alias: None,
        }]);
        let root = request(Resource::TemporaryBytes, 16_000_000);
        let result =
            PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                lower_direct_projection_layouts_with_schema(
                    catalog.accepted_schema_info(),
                    &logical,
                    &spec,
                    work,
                )
            })
            .unwrap();
        assert_eq!(result, (None, None));
    }
}

// Other finalization tests isolate their owner while including this owner's
// current charges. Exact slot-construction units are pinned below.
pub(super) fn cost(plan: &AccessPlannedQuery, schema: &SchemaInfo) -> (u64, u64) {
    let root = request(Resource::TemporaryBytes, 16_000_000);
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        let projection = plan.prepare_projection(schema, work)?;
        if plan.grouped_plan().is_none() {
            crate::db::query::plan::expr::compile_scalar_projection_plan_with_schema(
                schema,
                &projection,
                work,
            )
            .map_err(QueryError::execute)?;
        }
        crate::db::query::plan::lower_direct_projection_layouts_with_schema(
            schema,
            &plan.logical,
            &projection,
            work,
        )?;
        projection.referenced_slots_for_schema(schema, work)?;
        projection.is_schema_identity_for(schema, work)
    })
    .unwrap();
    (
        root.observed(Resource::TemporaryBytes),
        root.observed(Resource::PredicateExpressionSteps),
    )
}

#[test]
fn projection_slot_boundaries_preserve_sorted_unique_output() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let schema = catalog.accepted_schema_info();
    let spec = ProjectionSpec::from_fields_for_test(
        ["rare", "id", "rare"]
            .map(|name| ProjectionField::Scalar {
                expr: Expr::Field(ExprFieldId::new(name)),
                alias: None,
            })
            .into(),
    );
    let expected = [
        schema.field_slot_index("id").unwrap(),
        schema.field_slot_index("rare").unwrap(),
    ];
    assert!(expected[0] < expected[1]);
    // Three leaf visits + labels, two comparisons, one shifted slot.
    let steps = 3 + 4 + 2 + 4 + 2 + 1;
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        for (resource, exact) in [
            (Resource::TemporaryBytes, 4 * size_of::<usize>() as u64),
            (Resource::PredicateExpressionSteps, steps),
        ] {
            for limit in [exact - 1, exact] {
                let root = request(resource, limit);
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    spec.referenced_slots_for_schema(schema, work)
                });
                if limit == exact {
                    assert_eq!(result.unwrap(), expected);
                } else {
                    assert!(
                        result
                            .unwrap_err()
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                    );
                }
                assert_eq!(root.observed(resource), exact);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}
