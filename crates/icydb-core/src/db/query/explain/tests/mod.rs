//! Module: db::query::explain::tests
//! Covers EXPLAIN node ownership and descriptor-shaping invariants for the
//! query layer.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod aggregate;
mod descriptor;
mod determinism;
mod grouped;
mod order_pushdown;
mod semantic_changes;

use super::*;
use crate::db::access::{
    AccessPath, AccessPlan, SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection,
};
use crate::db::predicate::{CompareOp, MissingRowPolicy, Predicate};
use crate::db::query::plan::{
    AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec, GroupSpec,
    GroupedExecutionConfig, LoadSpec, LogicalPlan, OrderDirection, OrderSpec, QueryMode,
    group_aggregate_spec_expr,
};
use crate::model::{field::FieldKind, index::IndexModel};
use crate::traits::EntitySchema;
use crate::types::Ulid;
use crate::value::Value;
use std::ops::Bound;

const PUSHDOWN_INDEX_FIELDS: [&str; 1] = ["tag"];
const PUSHDOWN_INDEX: IndexModel = IndexModel::generated(
    "explain::pushdown_tag",
    "explain::pushdown_store",
    &PUSHDOWN_INDEX_FIELDS,
    false,
);

crate::test_entity! {
ident = ExplainPushdownEntity,
    id = Ulid,
    entity_name = "PushdownEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&PUSHDOWN_INDEX],
}

fn aggregate_having_expr(
    group: &GroupSpec,
    index: usize,
    op: CompareOp,
    value: Value,
) -> crate::db::query::plan::expr::Expr {
    having_compare_expr(
        crate::db::query::plan::expr::Expr::Aggregate(group_aggregate_spec_expr(
            group
                .aggregates
                .get(index)
                .expect("grouped HAVING aggregate should exist"),
        )),
        op,
        value,
    )
}

fn having_compare_expr(
    left: crate::db::query::plan::expr::Expr,
    op: CompareOp,
    value: Value,
) -> crate::db::query::plan::expr::Expr {
    if matches!(value, Value::Null) {
        let function = match op {
            CompareOp::Eq => Some(crate::db::query::plan::expr::Function::IsNull),
            CompareOp::Ne => Some(crate::db::query::plan::expr::Function::IsNotNull),
            CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::In
            | CompareOp::NotIn
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => None,
        };

        if let Some(function) = function {
            return crate::db::query::plan::expr::Expr::FunctionCall {
                function,
                args: vec![left],
            };
        }
    }

    crate::db::query::plan::expr::Expr::Binary {
        op: match op {
            CompareOp::Eq
            | CompareOp::In
            | CompareOp::NotIn
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => crate::db::query::plan::expr::BinaryOp::Eq,
            CompareOp::Ne => crate::db::query::plan::expr::BinaryOp::Ne,
            CompareOp::Lt => crate::db::query::plan::expr::BinaryOp::Lt,
            CompareOp::Lte => crate::db::query::plan::expr::BinaryOp::Lte,
            CompareOp::Gt => crate::db::query::plan::expr::BinaryOp::Gt,
            CompareOp::Gte => crate::db::query::plan::expr::BinaryOp::Gte,
        },
        left: Box::new(left),
        right: Box::new(crate::db::query::plan::expr::Expr::Literal(value)),
    }
}
