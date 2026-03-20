//! Module: db::executor::route::contracts::shape
//! Responsibility: module-local ownership and contracts for db::executor::route::contracts::shape.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::query::{builder::AggregateExpr, plan::GroupedPlanStrategyHint};

///
/// FastPathOrder
///
/// Shared fast-path precedence model used by load and aggregate routing.
/// Routing implementations remain separate, but they iterate one canonical order.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum FastPathOrder {
    PrimaryKey,
    SecondaryPrefix,
    PrimaryScan,
    IndexRange,
    Composite,
}

// Contract: fast-path precedence is a stability boundary. Any change here must
// be intentional, accompanied by route-order tests, and called out in changelog.
pub(in crate::db::executor) const LOAD_FAST_PATH_ORDER: [FastPathOrder; 3] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::IndexRange,
];

// Contract: aggregate dispatch precedence is ordered for semantic and
// performance stability. Do not reorder casually.
pub(in crate::db::executor) const AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 5] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::PrimaryScan,
    FastPathOrder::IndexRange,
    FastPathOrder::Composite,
];

// Contract: grouped aggregate routes are materialized-only in this audit pass
// and must not participate in scalar aggregate fast-path dispatch.
pub(in crate::db::executor) const GROUPED_AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 0] = [];

// Contract: mutation routes are materialized-only and do not participate in
// load/aggregate fast-path precedence.
pub(in crate::db::executor) const MUTATION_FAST_PATH_ORDER: [FastPathOrder; 0] = [];

///
/// RouteIntent
///

pub(in crate::db::executor::route) enum RouteIntent {
    Load,
    Aggregate {
        aggregate: AggregateExpr,
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    },
    AggregateGrouped {
        grouped_plan_strategy_hint: GroupedPlanStrategyHint,
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    },
}

///
/// RouteShapeKind
///
/// Planner-to-router semantic execution shape contract.
/// This shape is independent from streaming/materialized execution policy and
/// allows route dispatch migration away from feature-combination branching.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum RouteShapeKind {
    LoadScalar,
    AggregateCount,
    AggregateNonCount,
    AggregateGrouped,
    MutationDelete,
}
