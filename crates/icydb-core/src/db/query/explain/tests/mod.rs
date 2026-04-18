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
    AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingClause,
    GroupHavingSymbol, GroupSpec, GroupedExecutionConfig, LoadSpec, LogicalPlan, OrderDirection,
    OrderSpec, QueryMode,
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

fn having_compare(symbol: GroupHavingSymbol, op: CompareOp, value: Value) -> GroupHavingClause {
    GroupHavingClause { symbol, op, value }
}
