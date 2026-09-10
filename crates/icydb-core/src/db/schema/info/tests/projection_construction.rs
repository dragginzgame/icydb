//! Accepted projection construction and its single owned finalization handoff.

use super::{newtype_query_schema, resolve_group_field};
use crate::{
    db::{
        MissingRowPolicy, QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            builder::{count, min_by},
            plan::{
                AccessPlannedQuery, GroupAggregateSpec, GroupFieldSet, GroupPlan, GroupSpec,
                GroupedExecutionConfig, LogicalPlan,
                expr::{
                    Alias, Expr, FieldPath, ProjectionField, ProjectionSelection, ProjectionSpec,
                },
            },
            preparation::{PreparationWork, with_preparation_work},
        },
        schema::{AcceptedFieldKind, PersistedNestedLeafSnapshot, SchemaInfo},
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

fn plan() -> AccessPlannedQuery {
    AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore)
}

fn scalar(expr: Expr, alias: Option<&str>) -> ProjectionField {
    ProjectionField::Scalar {
        expr,
        alias: alias.map(Alias::new),
    }
}

fn fields(projection: &ProjectionSpec) -> Vec<ProjectionField> {
    projection.fields().cloned().collect()
}

fn assert_budget_error(error: QueryError, resource: Resource) {
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
    );
}

#[test]
fn all_projection_preserves_sparse_accepted_slot_order() {
    let mut schema = newtype_query_schema();
    for sparse in [false, true] {
        if sparse {
            for (_, field) in &mut schema.fields {
                field.slot = 7 + (3 - field.slot) * 4;
            }
        }
        let expected = schema.field_names_in_slot_order();
        let projection =
            with_preparation_work(|work| plan().prepare_projection(&schema, work)).unwrap();
        assert_eq!(
            projection
                .fields()
                .map(ProjectionField::direct_field_name)
                .collect::<Vec<_>>(),
            expected.into_iter().map(Some).collect::<Vec<_>>(),
        );
    }
}

#[test]
fn explicit_projection_preserves_duplicates_aliases_and_typed_payloads() {
    let schema = newtype_query_schema();
    let mut plan = plan();
    for selection in [
        ProjectionSelection::Fields(vec!["name".into(), "id".into(), "name".into()]),
        ProjectionSelection::Exprs(vec![
            scalar(Expr::Field("id".into()), Some("alias")),
            scalar(
                Expr::Literal(Value::List(vec![Value::Text("owned".into())])),
                None,
            ),
        ]),
    ] {
        let expected = match &selection {
            ProjectionSelection::Fields(names) => names
                .iter()
                .map(|name| scalar(Expr::Field(name.clone()), None))
                .collect::<Vec<_>>(),
            ProjectionSelection::Exprs(fields) => fields.clone(),
            ProjectionSelection::All => unreachable!(),
        };
        plan.projection_selection = selection.clone();
        let projection =
            with_preparation_work(|work| plan.prepare_projection(&schema, work)).unwrap();
        assert_eq!(fields(&projection), expected);
        assert_eq!(plan.projection_selection, selection);
    }
}

#[test]
fn projection_copies_reject_at_exact_payload_boundaries_and_accumulate() {
    let schema = newtype_query_schema();
    let mut plan = plan();
    plan.projection_selection =
        ProjectionSelection::Fields(vec!["name".into(), "id".into(), "name".into()]);
    let bytes = (3 * size_of::<ProjectionField>() + 10) as u64;
    let steps = 3 + 10;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for (resource, exact) in [
            (Resource::TemporaryBytes, bytes),
            (Resource::PredicateExpressionSteps, steps),
        ] {
            for limit in [exact - 1, exact] {
                let request = root(resource, limit);
                let result = PreparationWork::run(&request.scope(), lane, |work| {
                    plan.prepare_projection(&schema, work)
                });
                if limit == exact {
                    assert_eq!(result.unwrap().len(), 3);
                    assert_budget_error(
                        PreparationWork::run(&request.scope(), lane, |work| {
                            plan.prepare_projection(&schema, work)
                        })
                        .unwrap_err(),
                        resource,
                    );
                } else {
                    assert_budget_error(result.unwrap_err(), resource);
                }
                assert!(plan.projection_spec().is_err());
                assert_eq!(request.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn all_projection_admits_shared_sort_scratch_before_construction() {
    let schema = newtype_query_schema();
    let generous = root(Resource::TemporaryBytes, 16_000_000);
    PreparationWork::run(&generous.scope(), Lane::Diagnostic, |work| {
        plan().prepare_projection(&schema, work)
    })
    .unwrap();
    for resource in [
        Resource::TemporaryBytes,
        Resource::PredicateExpressionSteps,
        Resource::SortComparisons,
    ] {
        let exact = generous.observed(resource);
        assert!(exact > 0);
        for limit in [0, exact - 1, exact] {
            let request = root(resource, limit);
            let result = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
                plan().prepare_projection(&schema, work)
            });
            if limit == exact {
                assert_eq!(result.unwrap().len(), schema.field_count());
            } else {
                assert_budget_error(result.unwrap_err(), resource);
            }
            assert_eq!(request.observed(Resource::RowsVisited), 0);
        }
    }
}

fn grouped_plan(schema: &SchemaInfo) -> AccessPlannedQuery {
    let mut plan = plan();
    plan.logical = LogicalPlan::Grouped(GroupPlan {
        scalar: plan.scalar_plan().clone(),
        group: GroupSpec {
            group_fields: GroupFieldSet::PathAware(vec![
                resolve_group_field(schema, "id").unwrap(),
                resolve_group_field(schema, "profile.name").unwrap(),
            ]),
            aggregates: vec![
                GroupAggregateSpec::from_aggregate_expr(count()),
                GroupAggregateSpec::from_aggregate_expr(min_by("id").distinct()),
            ],
            execution: GroupedExecutionConfig::planner_default_bounded(),
        },
        having_expr: None,
    });
    plan
}

fn scalar_path_schema() -> SchemaInfo {
    let mut schema = newtype_query_schema();
    let (_, profile) = schema
        .fields
        .iter_mut()
        .find(|(name, _)| name.as_ref() == "profile")
        .unwrap();
    profile.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
        vec!["name".into()],
        AcceptedFieldKind::Text { max_len: Some(64) },
        false,
    )];
    schema
}

#[test]
fn grouped_projection_preserves_paths_semantic_distinct_and_explicit_selection() {
    let schema = scalar_path_schema();
    let mut plan = grouped_plan(&schema);
    let expected = vec![
        scalar(Expr::Field("id".into()), None),
        scalar(
            Expr::FieldPath(FieldPath::new("profile", vec!["name".into()])),
            None,
        ),
        scalar(Expr::Aggregate(count()), None),
        // MIN ignores authored DISTINCT in its semantic projection.
        scalar(Expr::Aggregate(min_by("id")), None),
    ];
    for selection in [
        ProjectionSelection::All,
        ProjectionSelection::Fields(vec!["id".into()]),
    ] {
        plan.projection_selection = selection;
        let projection =
            with_preparation_work(|work| plan.prepare_projection(&schema, work)).unwrap();
        assert_eq!(fields(&projection), expected);
    }
    let explicit = vec![scalar(Expr::Aggregate(count()), Some("total"))];
    plan.projection_selection = ProjectionSelection::Exprs(explicit.clone());
    let projection = with_preparation_work(|work| plan.prepare_projection(&schema, work)).unwrap();
    assert_eq!(fields(&projection), explicit);
}

#[test]
fn finalization_moves_projection_backing_and_group_binding_preserves_shape() {
    let schema = scalar_path_schema();
    for mut plan in [plan(), grouped_plan(&schema)] {
        let projection =
            with_preparation_work(|work| plan.prepare_projection(&schema, work)).unwrap();
        let expected = fields(&projection);
        let backing = projection.fields().as_slice().as_ptr();
        with_preparation_work(|work| {
            plan.finalize_static_execution_planning_contract_with_schema(&schema, projection, work)
        })
        .unwrap();
        assert_eq!(
            plan.projection_spec().unwrap().fields().as_slice().as_ptr(),
            backing
        );
        assert_eq!(fields(plan.projection_spec().unwrap()), expected);
        let rebound = with_preparation_work(|work| plan.prepare_projection(&schema, work)).unwrap();
        assert_eq!(fields(&rebound), expected);
    }
}
