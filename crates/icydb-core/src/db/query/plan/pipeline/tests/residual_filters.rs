//! Exact access and covering terminals retain unproved expression semantics.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{MissingRowPolicy, Predicate},
        query::{
            intent::QueryModel,
            plan::{
                AccessPlannedQuery, exact_metadata_schema,
                expr::{BinaryOp, Expr},
            },
            preparation::{PreparationWork, with_preparation_work},
        },
        schema::SchemaInfo,
    },
    value::Value,
};
use std::rc::Rc;

fn expression() -> Expr {
    Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field("age".into())),
            right: Box::new(Expr::Literal(Value::Int64(1))),
        }),
        right: Box::new(Expr::Literal(Value::Int64(5))),
    }
}

fn assemble(
    query: &QueryModel,
    schema: &SchemaInfo,
    predicate: Option<Predicate>,
    access: AccessPlan<Value>,
    work: &PreparationWork<'_>,
) -> AccessPlannedQuery {
    super::super::assemble_query_model_plan(
        query,
        &[],
        Rc::new(schema.clone()),
        predicate,
        None,
        None,
        access,
        None,
        work,
    )
    .unwrap()
}

#[test]
fn residual_filter_preservation_partial_exact_access_retains_expression_in_both_append_orders() {
    let schema = exact_metadata_schema(&[], &["age"]);
    for (predicate, access) in [
        (
            Predicate::eq("id".into(), Value::Int64(1)),
            AccessPlan::by_key(Value::Int64(1)),
        ),
        (
            Predicate::in_("id".into(), vec![Value::Int64(1), Value::Int64(2)]),
            AccessPlan::by_keys(vec![Value::Int64(1), Value::Int64(2)]),
        ),
    ] {
        for expression_first in [true, false] {
            with_preparation_work(|work| {
                let query = QueryModel::new(MissingRowPolicy::Ignore);
                let query = if expression_first {
                    query
                        .filter_expr(expression(), work)
                        .unwrap()
                        .filter_normalized_predicate(predicate.clone())
                } else {
                    query
                        .filter_normalized_predicate(predicate.clone())
                        .filter_expr(expression(), work)
                        .unwrap()
                };
                assert!(!query.filter_predicate_fully_covers_expression());
                let plan = assemble(
                    &query,
                    &schema,
                    Some(predicate.clone()),
                    access.clone(),
                    work,
                );
                assert_eq!(plan.access, access);
                assert!(plan.scalar_plan().predicate.is_none());
                assert!(plan.residual_filter_expr().unwrap().is_some());
                assert!(plan.effective_runtime_filter_program().is_some());
                Ok::<(), crate::db::QueryError>(())
            })
            .unwrap();
        }
    }
}

#[test]
fn residual_filter_preservation_complete_exact_access_keeps_redundancy_proof() {
    let schema = exact_metadata_schema(&[], &[]);
    with_preparation_work(|work| {
        let predicate = Predicate::eq("id".into(), Value::Int64(1));
        let full_expression = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field("id".into())),
            right: Box::new(Expr::Literal(Value::Int64(1))),
        };
        for query in [
            QueryModel::new(MissingRowPolicy::Ignore)
                .filter_normalized_predicate(predicate.clone()),
            QueryModel::new(MissingRowPolicy::Ignore)
                .filter_expr(full_expression, work)
                .unwrap(),
        ] {
            assert!(query.filter_predicate_fully_covers_expression());
            let plan = assemble(
                &query,
                &schema,
                Some(predicate.clone()),
                AccessPlan::by_key(Value::Int64(1)),
                work,
            );
            assert!(plan.scalar_plan().predicate.is_none());
            assert!(plan.scalar_plan().filter_expr.is_none());
            assert!(!plan.has_any_residual_filter().unwrap());
        }
        Ok::<(), crate::db::QueryError>(())
    })
    .unwrap();
}

#[cfg(feature = "sql")]
#[test]
fn residual_filter_preservation_covering_terminal_and_explain_require_compatibility() {
    use crate::db::{
        access::SemanticIndexAccessContract,
        executor::{
            explain::assemble_scalar_aggregate_execution_descriptor_with_projection,
            route::AggregateRouteShape,
        },
        query::plan::{
            AggregateKind, covering_strict_predicate_compatible,
            index_covering_existing_rows_terminal_eligible,
        },
    };

    let schema = exact_metadata_schema(&[("age_idx", &["age"])], &["maybe"]);
    let index = SemanticIndexAccessContract::from_accepted_field_path_index(
        &schema.field_path_indexes()[0],
    )
    .unwrap();
    with_preparation_work(|work| {
        for has_expression in [false, true] {
            let query = QueryModel::new(MissingRowPolicy::Ignore);
            let query = if has_expression {
                query.filter_expr(expression(), work).unwrap()
            } else {
                query
            };
            let plan = assemble(
                &query,
                &schema,
                None,
                AccessPlan::index_prefix_from_contract(index.clone(), vec![Value::Int64(1)]),
                work,
            );
            assert!(plan.scalar_plan().predicate.is_none());
            let compatible = covering_strict_predicate_compatible(
                plan.residual_filter_contract().unwrap(),
                None,
            );
            assert_eq!(compatible, !has_expression);
            assert_eq!(
                index_covering_existing_rows_terminal_eligible(&plan, compatible),
                !has_expression
            );
            for kind in [AggregateKind::Count, AggregateKind::Exists] {
                let descriptor = assemble_scalar_aggregate_execution_descriptor_with_projection(
                    &plan,
                    AggregateRouteShape::new_from_schema_info(kind, None, &schema),
                    kind,
                    None,
                    work,
                )
                .unwrap();
                assert_eq!(descriptor.covering_projection, !has_expression);
            }
        }
        Ok::<(), crate::db::QueryError>(())
    })
    .unwrap();
}
