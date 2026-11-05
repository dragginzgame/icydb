use super::{CompiledStructuralAggregateRequest, StructuralAggregateRequest};
use crate::{
    db::{
        QueryError,
        query::plan::{
            exact_metadata_schema,
            expr::{Expr, FieldId, ProjectionField, ProjectionSpec},
        },
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            with_query_execution_budget_for_tests,
        },
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope as Scope,
    DiagnosticExecutionLane as Lane, DiagnosticFactTag,
};

#[test]
fn structural_compilation_distinguishes_resource_failures_from_invalid_shapes() {
    for malformed_having in [false, true] {
        let request = StructuralAggregateRequest::new(
            vec![],
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Literal(Value::Nat64(1)),
                alias: None,
            }]),
            Some(if malformed_having {
                Expr::Field(FieldId::new("missing"))
            } else {
                Expr::Literal(Value::Bool(true))
            }),
            exact_metadata_schema(&[], &[]),
        );
        for limit in [0, 16_000_000] {
            let result = with_query_execution_budget_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::TemporaryBytes, limit),
                HardExecutionContext::new(Scope::Execution, Lane::PublicRead, 0),
                || {
                    CompiledStructuralAggregateRequest::compile(&request)
                        .map_err(QueryError::execute)
                },
            );
            if limit == 0 {
                assert!(result.err().unwrap().diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw()
                )));
            } else if malformed_having {
                assert_eq!(
                    result.err().unwrap().diagnostic().code(),
                    InternalError::query_executor_invariant()
                        .diagnostic()
                        .code()
                );
            } else {
                let compiled = result.unwrap();
                assert_eq!(compiled.projection().len(), 1);
                assert!(compiled.having().is_some());
            }
        }
    }
}
