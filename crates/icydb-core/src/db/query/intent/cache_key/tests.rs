use crate::{
    db::{
        predicate::MissingRowPolicy,
        query::{
            builder::aggregate,
            intent::StructuralQuery,
            plan::{
                AggregateKind, GroupAggregateSpec, OrderDirection, OrderSpec, OrderTerm,
                expr::{Alias, Expr, ProjectionField, ProjectionSelection},
            },
        },
    },
    retained::RetainedBytes,
    types::{Decimal, U256},
    value::Value,
};

use super::{AggregateCacheKey, OrderTermCacheKey, ProjectionCacheKey};
use crate::db::{
    QueryError, RequestExecutionRoot,
    query::{
        plan::expr::{BinaryOp, CaseWhenArm, FieldPath, Function, UnaryOp},
        preparation::{PreparationWork, with_preparation_work},
    },
    resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};
use std::{
    hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
    rc::Rc,
};

#[test]
fn mixed_filter_cache_identity_preserves_both_filter_authorities() {
    use crate::db::predicate::{Predicate, predicate_fingerprint_normalized};

    let schema = crate::db::query::plan::exact_metadata_schema(&[], &[]);
    with_preparation_work(|work| {
        let key = |id, threshold, expression_first| {
            let predicate = Predicate::eq("id".into(), Value::Nat64(id));
            let expr = Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Field("amount".into())),
                    right: Box::new(Expr::Literal(Value::Nat64(1))),
                }),
                right: Box::new(Expr::Literal(Value::Nat64(threshold))),
            };
            let scope = Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field("id".into())),
                right: Box::new(Expr::Literal(Value::Nat64(id))),
            };
            let query = StructuralQuery::new(MissingRowPolicy::Ignore);
            let query = if expression_first {
                query
                    .filter_expr(&schema, expr, work)
                    .unwrap()
                    .filter_expr(&schema, scope, work)
                    .unwrap()
            } else {
                query
                    .filter_expr(&schema, scope, work)
                    .unwrap()
                    .filter_expr(&schema, expr, work)
                    .unwrap()
            };
            query
                .structural_cache_key_with_normalized_predicate_fingerprint(
                    Some(predicate_fingerprint_normalized(&predicate, work).unwrap()),
                    work,
                )
                .unwrap()
        };
        let original = key(1, 5, true);
        for expression_first in [true, false] {
            assert_eq!(key(1, 5, expression_first), original);
            assert_ne!(key(2, 5, expression_first), original);
            assert_ne!(key(1, 10, expression_first), original);
        }
    });
}

#[test]
fn key_construction_admits_backing_and_retries_without_partial_memos() {
    let make_query = || {
        let expr = Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::FieldPath(FieldPath::new(
                        "record",
                        vec!["inner".into()],
                    ))),
                    right: Box::new(Expr::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(Expr::Literal(Value::Null)),
                    }),
                },
                Expr::Aggregate(
                    aggregate::sum("amount").with_filter_expr(Expr::Field("flag".into())),
                ),
            )],
            else_expr: Box::new(Expr::FunctionCall {
                function: Function::Coalesce,
                args: vec![Expr::Field("fallback".into())],
            }),
        };
        StructuralQuery::new(MissingRowPolicy::Ignore)
            .order_spec(OrderSpec {
                fields: vec![OrderTerm::new(expr.clone(), OrderDirection::Asc)],
            })
            .projection_selection(ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
                expr,
                alias: Some(Alias::new("output")),
            }]))
    };
    let request = |resource, limit| {
        RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(resource, limit),
        )
    };
    let build = |query: &StructuralQuery, root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), DiagnosticExecutionLane::Diagnostic, |work| {
            query.structural_cache_key_with_normalized_predicate_fingerprint(None, work)
        })
    };
    let measured = request(Resource::TemporaryBytes, 16_000_000);
    let expected = build(&make_query(), &measured).unwrap();
    // Independent retained traversal counts exactly the new backing for this
    // key: the handle itself is returned inline, not allocated separately.
    assert_eq!(
        measured.observed(Resource::TemporaryBytes),
        (RetainedBytes::measure(&expected, usize::MAX).unwrap() - size_of_val(&expected)) as u64
    );
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let exact = measured.observed(resource);
        let query = make_query();
        let denied = request(resource, exact - 1);
        for _ in 0..2 {
            let error: QueryError = build(&query, &denied).unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
            );
        }
        let admitted = request(resource, exact);
        assert_eq!(build(&query, &admitted).unwrap(), expected);
        assert_eq!(admitted.observed(resource), exact);
        // A completed memo can be shared without allocating/copying again.
        assert_eq!(build(&query, &denied).unwrap(), expected);
        assert_eq!(build(&query, &admitted).unwrap(), expected);
        assert_eq!(admitted.observed(resource), exact);
    }
}

#[test]
fn shared_structural_key_preserves_content_identity_and_retention() {
    with_preparation_work(|work| {
        let make_key = || {
            StructuralQuery::new(MissingRowPolicy::Ignore)
                .projection_selection(ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
                    expr: Expr::Aggregate(aggregate::sum("amount".repeat(100))),
                    alias: Some(Alias::new("output".repeat(100))),
                }]))
                .structural_cache_key_with_normalized_predicate_fingerprint(None, work)
                .unwrap()
        };
        let key = make_key();
        let cloned = key.clone();
        // Sharing is the resource contract of key cloning, not pointer-based identity.
        assert!(Rc::ptr_eq(&key.0, &cloned.0));
        let independent = make_key();
        assert!(!Rc::ptr_eq(&key.0, &independent.0));
        assert_eq!(key, independent);
        let hash = BuildHasherDefault::<DefaultHasher>::default();
        assert_eq!(hash.hash_one(&key), hash.hash_one(&independent));
        // The handle must preserve the payload's existing content hash.
        assert_eq!(hash.hash_one(&key), hash.hash_one(key.0.as_ref()));

        let bytes = RetainedBytes::measure(&key, usize::MAX).unwrap();
        let payload_bytes = RetainedBytes::measure(key.0.as_ref(), usize::MAX).unwrap();
        assert!(bytes >= size_of_val(&key) + 2 * size_of::<usize>() + payload_bytes);
        assert_eq!(RetainedBytes::measure(&key, bytes), Some(bytes));
        assert!(RetainedBytes::measure(&key, bytes - 1).is_none());
        drop(key);
        assert_eq!(cloned, independent);
        assert_eq!(RetainedBytes::measure(&cloned, usize::MAX), Some(bytes));
    });
}

#[test]
fn projection_cache_keys_preserve_optional_alias_text() {
    with_preparation_work(|work| {
        let key = |alias: Option<&str>| {
            ProjectionCacheKey::from_projection_selection(
                &ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
                    expr: Expr::Field("label".into()),
                    alias: alias.map(Alias::new),
                }]),
                work,
            )
            .unwrap()
        };
        let aliases = [None, Some(""), Some("first"), Some("second"), Some("FIRST")];
        for left in aliases {
            for right in aliases {
                assert_eq!(key(left) == key(right), left == right);
            }
        }
    });
}

#[test]
fn projection_cache_retention_includes_alias_backing() {
    with_preparation_work(|work| {
        let alias = "output".repeat(100);
        let key = ProjectionCacheKey::from_projection_selection(
            &ProjectionSelection::Exprs(vec![ProjectionField::Scalar {
                expr: Expr::Field("label".into()),
                alias: Some(Alias::new(alias.as_str())),
            }]),
            work,
        )
        .unwrap();
        let bytes = RetainedBytes::measure(&key, usize::MAX).unwrap();
        assert!(
            bytes
                >= size_of::<ProjectionCacheKey>()
                    + size_of::<(super::ProjectionExprCacheKey, Option<String>)>()
                    + "label".len()
                    + alias.len()
        );
        assert_eq!(RetainedBytes::measure(&key, bytes), Some(bytes));
        assert!(RetainedBytes::measure(&key, bytes - 1).is_none());
        let cloned = key.clone();
        assert_eq!(RetainedBytes::measure(&cloned, usize::MAX), Some(bytes));
        assert_eq!(key, cloned);
    });
}

#[test]
fn order_cache_keys_preserve_typed_operands_and_direction() {
    with_preparation_work(|work| {
        let key = |expr, direction| {
            OrderTermCacheKey::from_order_spec(
                &OrderSpec {
                    fields: vec![OrderTerm::new(expr, direction)],
                },
                work,
            )
            .unwrap()
        };
        let decimal = Expr::Literal(Value::Decimal(Decimal::from(1_u64)));
        let wide = Expr::Literal(Value::U256(U256::from(1_u64)));
        for (left, right) in [
            (decimal.clone(), wide.clone()),
            (
                Expr::Aggregate(aggregate::AggregateExpr::from_expression_input(
                    AggregateKind::Sum,
                    decimal,
                )),
                Expr::Aggregate(aggregate::AggregateExpr::from_expression_input(
                    AggregateKind::Sum,
                    wide,
                )),
            ),
        ] {
            assert_ne!(
                key(left, OrderDirection::Asc),
                key(right, OrderDirection::Asc)
            );
        }
        let field = Expr::Field("label".into());
        assert_ne!(
            key(field.clone(), OrderDirection::Asc),
            key(field, OrderDirection::Desc),
        );
        assert_eq!(
            key(Expr::Aggregate(aggregate::count()), OrderDirection::Asc),
            key(
                Expr::Aggregate(aggregate::AggregateExpr::from_expression_input(
                    AggregateKind::Count,
                    Expr::Literal(Value::Nat64(1)),
                )),
                OrderDirection::Asc,
            ),
        );
    });
}

#[test]
fn order_cache_retention_includes_nested_operands() {
    with_preparation_work(|work| {
        let field = "amount".repeat(100);
        let filter = "filter".repeat(100);
        let order = OrderSpec {
            fields: vec![OrderTerm::new(
                Expr::Aggregate(
                    aggregate::sum(field.clone())
                        .with_filter_expr(Expr::Field(filter.clone().into())),
                ),
                OrderDirection::Desc,
            )],
        };
        let key = OrderTermCacheKey::from_order_spec(&order, work).unwrap();
        let bytes = RetainedBytes::measure(&key, usize::MAX).unwrap();
        assert!(
            bytes
                >= size_of_val(&key)
                    + size_of::<OrderTermCacheKey>()
                    + 2 * size_of::<super::ProjectionExprCacheKey>()
                    + field.len()
                    + filter.len()
        );
        assert_eq!(RetainedBytes::measure(&key, bytes), Some(bytes));
        assert!(RetainedBytes::measure(&key, bytes - 1).is_none());
    });
}

#[test]
fn aggregate_cache_keys_preserve_typed_inputs_and_canonical_equivalence() {
    with_preparation_work(|work| {
        let from_input = |kind, value| {
            aggregate::AggregateExpr::from_expression_input(kind, Expr::Literal(value))
        };
        for kind in [AggregateKind::Sum, AggregateKind::Min, AggregateKind::Max] {
            let decimal = from_input(kind, Value::Decimal(Decimal::from(1_u64)));
            let wide = from_input(kind, Value::U256(U256::from(1_u64)));
            assert_ne!(
                AggregateCacheKey::from_aggregate_expr(&decimal, work).unwrap(),
                AggregateCacheKey::from_aggregate_expr(&wide, work).unwrap(),
            );
            for aggregate in [decimal, wide] {
                assert_eq!(
                    AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
                    AggregateCacheKey::from_group_aggregate_spec(
                        &GroupAggregateSpec::from_aggregate_expr(aggregate.clone()),
                        work
                    )
                    .unwrap(),
                );
            }
        }
        for value in [Value::Nat64(1), Value::U256(U256::from(1_u64))] {
            assert_eq!(
                AggregateCacheKey::from_aggregate_expr(&aggregate::count(), work).unwrap(),
                AggregateCacheKey::from_aggregate_expr(
                    &from_input(AggregateKind::Count, value),
                    work
                )
                .unwrap(),
            );
        }
        assert_ne!(
            AggregateCacheKey::from_aggregate_expr(
                &from_input(AggregateKind::Count, Value::Nat64(1)).distinct(),
                work
            )
            .unwrap(),
            AggregateCacheKey::from_aggregate_expr(
                &from_input(AggregateKind::Count, Value::U256(U256::from(1_u64))).distinct(),
                work
            )
            .unwrap(),
        );
        for aggregate in [aggregate::min_by("amount"), aggregate::max_by("amount")] {
            assert_eq!(
                AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
                AggregateCacheKey::from_aggregate_expr(&aggregate.clone().distinct(), work)
                    .unwrap(),
            );
        }
        assert_eq!(
            AggregateCacheKey::from_aggregate_expr(
                &from_input(AggregateKind::Sum, Value::Nat64(1)),
                work
            )
            .unwrap(),
            AggregateCacheKey::from_aggregate_expr(
                &from_input(AggregateKind::Sum, Value::Decimal(Decimal::from(1_u64)),),
                work
            )
            .unwrap(),
        );
    });
}

#[test]
fn aggregate_cache_retention_includes_both_structural_operands() {
    with_preparation_work(|work| {
        let field = "input".repeat(100);
        let filter = "filter".repeat(100);
        let aggregate =
            aggregate::sum(field.clone()).with_filter_expr(Expr::Field(filter.clone().into()));
        let key = AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap();
        let bytes = RetainedBytes::measure(&key, usize::MAX).unwrap();
        let minimum = size_of::<AggregateCacheKey>()
            + 2 * size_of::<super::ProjectionExprCacheKey>()
            + field.len()
            + filter.len();
        assert!(bytes >= minimum);
        assert_eq!(RetainedBytes::measure(&key, bytes), Some(bytes));
        assert!(RetainedBytes::measure(&key, bytes - 1).is_none());
        let cloned = key.clone();
        assert_eq!(RetainedBytes::measure(&cloned, usize::MAX), Some(bytes));
        assert_eq!(key, cloned);
    });
}

#[test]
fn scalar_and_grouped_aggregate_cache_identity_stays_shared() {
    with_preparation_work(|work| {
        let aggregate = aggregate::sum("amount")
            .with_filter_expr(Expr::Literal(Value::Bool(true)))
            .distinct();
        let grouped = GroupAggregateSpec::from_aggregate_expr(aggregate.clone());

        assert_eq!(
            AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
            AggregateCacheKey::from_group_aggregate_spec(&grouped, work).unwrap(),
        );

        let different_filter = aggregate::sum("amount")
            .with_filter_expr(Expr::Literal(Value::Bool(false)))
            .distinct();
        assert_ne!(
            AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
            AggregateCacheKey::from_aggregate_expr(&different_filter, work).unwrap(),
        );
        assert_ne!(
            AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
            AggregateCacheKey::from_aggregate_expr(
                &aggregate::sum("other_amount").distinct(),
                work
            )
            .unwrap(),
        );
        assert_ne!(
            AggregateCacheKey::from_aggregate_expr(&aggregate, work).unwrap(),
            AggregateCacheKey::from_aggregate_expr(&aggregate::sum("amount"), work).unwrap(),
        );
    });
}
