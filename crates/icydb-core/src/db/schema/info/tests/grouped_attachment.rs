//! Owned grouped attachment preparation under the existing request budget.

use super::newtype_query_schema;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::{
                AggregateKind, GroupedAggregateExecutionSpec, expr::Expr,
                grouped_aggregate_execution_specs,
            },
            preparation::PreparationWork,
        },
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

fn count_rows() -> GroupedAggregateExecutionSpec {
    GroupedAggregateExecutionSpec::from_uncompiled_inputs(
        AggregateKind::Count,
        None,
        None,
        None,
        false,
    )
}

fn filtered_min() -> GroupedAggregateExecutionSpec {
    GroupedAggregateExecutionSpec::from_uncompiled_inputs(
        AggregateKind::Min,
        None,
        Some(Expr::Literal(Value::Text("abc".into()))),
        Some(Expr::Literal(Value::Bool(true))),
        true,
    )
}

fn assert_budget_error(error: QueryError, resource: Resource) {
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
    );
}

#[test]
fn count_attachments_reuse_list_capacity_without_new_backing() {
    let schema = newtype_query_schema();
    let mut specs = Vec::with_capacity(8);
    specs.extend([count_rows(), count_rows()]);
    let pointer = specs.as_ptr();
    let capacity = specs.capacity();
    let request = root(Resource::TemporaryBytes, 0);
    let resolved = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        grouped_aggregate_execution_specs(&schema, specs, work).map_err(QueryError::execute)
    })
    .unwrap();
    assert_eq!(resolved.as_ptr(), pointer);
    assert_eq!(resolved.capacity(), capacity);
    assert_eq!(resolved, [count_rows(), count_rows()]);
    assert_eq!(request.observed(Resource::TemporaryBytes), 0);
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 2);
}

#[test]
fn attachment_compilation_keeps_owned_syntax_and_charges_only_compiled_payloads() {
    let schema = newtype_query_schema();
    let spec = filtered_min();
    let expected = spec.clone();
    let Some(Expr::Literal(Value::Text(text))) = spec.input_expr() else {
        panic!("text input");
    };
    let pointer = text.as_ptr();
    let specs = vec![spec];
    let request = root(Resource::TemporaryBytes, 3);
    let resolved = PreparationWork::run(&request.scope(), Lane::PublicRead, |work| {
        grouped_aggregate_execution_specs(&schema, specs, work).map_err(QueryError::execute)
    })
    .unwrap();
    let resolved = &resolved[0];
    let Some(Expr::Literal(Value::Text(text))) = resolved.input_expr() else {
        panic!("text input");
    };
    assert_eq!(text.as_ptr(), pointer);
    assert_eq!(resolved.semantic_key(), expected.semantic_key());
    assert_eq!(resolved.filter_expr(), expected.filter_expr());
    assert!(resolved.compiled_input_expr().is_some());
    assert!(resolved.compiled_filter_expr().is_some());
    assert!(!resolved.distinct());
    assert_eq!(request.observed(Resource::TemporaryBytes), 3);

    let specs = vec![filtered_min()];
    let rejected = root(Resource::TemporaryBytes, 2);
    let error = PreparationWork::run(&rejected.scope(), Lane::PublicRead, |work| {
        grouped_aggregate_execution_specs(&schema, specs, work).map_err(QueryError::execute)
    })
    .unwrap_err();
    assert_budget_error(error, Resource::TemporaryBytes);
}

#[test]
fn failed_filter_compilation_does_not_publish_partial_attachments() {
    let schema = newtype_query_schema();
    let mut spec = filtered_min();
    let original = spec.clone();
    // One spec visit plus the text input's four steps fits; its filter does not.
    let request = root(Resource::PredicateExpressionSteps, 5);
    let error = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        spec.resolve_with_schema_info(&schema, work)
            .map_err(QueryError::execute)
    })
    .unwrap_err();
    assert_budget_error(error, Resource::PredicateExpressionSteps);
    assert_eq!(spec, original);
    assert_eq!(request.observed(Resource::TemporaryBytes), 3);
}

#[test]
fn attachment_visits_are_cumulative_and_empty_lists_are_free() {
    let schema = newtype_query_schema();
    for lane in [Lane::PublicRead, Lane::Diagnostic] {
        let request = root(Resource::PredicateExpressionSteps, 1);
        let error = PreparationWork::run(&request.scope(), lane, |work| {
            assert!(
                grouped_aggregate_execution_specs(&schema, Vec::new(), work)
                    .map_err(QueryError::execute)?
                    .is_empty()
            );
            let first = grouped_aggregate_execution_specs(&schema, vec![count_rows()], work)
                .map_err(QueryError::execute)?;
            assert_eq!(first.len(), 1);
            grouped_aggregate_execution_specs(&schema, vec![count_rows()], work)
                .map_err(QueryError::execute)
        })
        .unwrap_err();
        assert_budget_error(error, Resource::PredicateExpressionSteps);
        assert_eq!(request.observed(Resource::PredicateExpressionSteps), 2);
    }
}
