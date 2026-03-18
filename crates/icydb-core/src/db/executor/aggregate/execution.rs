//! Module: executor::aggregate::execution
//! Responsibility: aggregate execution descriptor/input payload contracts.
//! Does not own: aggregate execution branching logic.
//! Boundary: shared immutable payloads between aggregate orchestration helpers.

use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            ExecutionPlan, ExecutionPreparation, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            traversal::row_read_consistency_for_plan,
        },
        index::IndexPredicateProgram,
        predicate::MissingRowPolicy,
        query::builder::AggregateExpr,
        query::plan::AccessPlannedQuery,
    },
    traits::{EntityKind, EntityValue},
};

///
/// AggregateFastPathInputs
///
/// Aggregate fast-path execution inputs bundled for one dispatch entry.
/// Keeps branch routing parameters aligned between aggregate path helpers.
///

pub(in crate::db::executor) struct AggregateFastPathInputs<'exec, 'ctx, E: EntityKind + EntityValue>
{
    pub(in crate::db::executor) ctx: &'exec Context<'ctx, E>,
    pub(in crate::db::executor) logical_plan: &'exec AccessPlannedQuery,
    pub(in crate::db::executor) route_plan: &'exec ExecutionPlan,
    pub(in crate::db::executor) index_prefix_specs: &'exec [LoweredIndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'exec [LoweredIndexRangeSpec],
    pub(in crate::db::executor) index_predicate_program: Option<&'exec IndexPredicateProgram>,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) kind: super::AggregateKind,
    pub(in crate::db::executor) fold_mode: super::AggregateFoldMode,
}

impl<E> AggregateFastPathInputs<'_, '_, E>
where
    E: EntityKind + EntityValue,
{
    /// Return row-read missing-row policy for this aggregate fast-path attempt.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.logical_plan)
    }
}

///
/// AggregateExecutionDescriptor
///
/// Canonical aggregate execution descriptor constructed once from a terminal
/// aggregate spec and validated plan shape before execution branching.
///

pub(in crate::db::executor) struct AggregateExecutionDescriptor {
    pub(in crate::db::executor) aggregate: AggregateExpr,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) route_plan: ExecutionPlan,
    pub(in crate::db::executor) execution_preparation: ExecutionPreparation,
}

///
/// PreparedAggregateStreamingInputs
///
/// PreparedAggregateStreamingInputs owns canonical aggregate streaming setup
/// state after `ExecutablePlan` is consumed into logical plan form.
///

pub(in crate::db::executor) struct PreparedAggregateStreamingInputs<
    'ctx,
    E: EntityKind + EntityValue,
> {
    pub(in crate::db::executor) ctx: Context<'ctx, E>,
    pub(in crate::db::executor) logical_plan: AccessPlannedQuery,
    pub(in crate::db::executor) index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    pub(in crate::db::executor) index_range_specs: Vec<LoweredIndexRangeSpec>,
}

impl<E> PreparedAggregateStreamingInputs<'_, E>
where
    E: EntityKind + EntityValue,
{
    /// Return row-read missing-row policy for prepared aggregate streaming.
    #[must_use]
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.logical_plan)
    }
}
