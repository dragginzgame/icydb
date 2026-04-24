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
    AccessPath, AccessPlan, PushdownApplicability, SecondaryOrderPushdownRejection,
};
use crate::db::predicate::{CompareOp, MissingRowPolicy, Predicate};
use crate::db::query::plan::{
    AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec, GroupSpec,
    GroupedExecutionConfig, LoadSpec, LogicalPlan, OrderDirection, OrderSpec, QueryMode,
    group_aggregate_spec_expr, grouped_having_compare_expr,
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
    grouped_having_compare_expr(
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
