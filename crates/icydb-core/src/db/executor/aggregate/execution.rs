use crate::{
    db::{
        Context,
        access::IndexPredicateProgram,
        direction::Direction,
        executor::{ExecutionPlan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec},
        query::{plan::AccessPlannedQuery, predicate::PredicateFieldSlots},
    },
    traits::{EntityKind, EntityValue},
};

///
/// AggregateFastPathInputs
///
/// AggregateFastPathInputs
///
/// Aggregate fast-path execution inputs bundled for one dispatch entry.
/// Keeps branch routing parameters aligned between aggregate path helpers.
///

pub(in crate::db::executor) struct AggregateFastPathInputs<'exec, 'ctx, E: EntityKind + EntityValue>
{
    pub(in crate::db::executor) ctx: &'exec Context<'ctx, E>,
    pub(in crate::db::executor) logical_plan: &'exec AccessPlannedQuery<E::Key>,
    pub(in crate::db::executor) route_plan: &'exec ExecutionPlan,
    pub(in crate::db::executor) index_prefix_specs: &'exec [LoweredIndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'exec [LoweredIndexRangeSpec],
    pub(in crate::db::executor) index_predicate_program: Option<&'exec IndexPredicateProgram>,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) kind: super::AggregateKind,
    pub(in crate::db::executor) fold_mode: super::AggregateFoldMode,
}

///
/// AggregateExecutionDescriptor
///
/// AggregateExecutionDescriptor
///
/// Canonical aggregate execution descriptor constructed once from a terminal
/// aggregate spec and validated plan shape before execution branching.
///

pub(in crate::db::executor) struct AggregateExecutionDescriptor {
    pub(in crate::db::executor) spec: super::AggregateSpec,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) route_plan: ExecutionPlan,
    pub(in crate::db::executor) strict_index_predicate_program: Option<IndexPredicateProgram>,
}

///
/// PreparedAggregateStreamingInputs
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
    pub(in crate::db::executor) logical_plan: AccessPlannedQuery<E::Key>,
    pub(in crate::db::executor) index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    pub(in crate::db::executor) index_range_specs: Vec<LoweredIndexRangeSpec>,
    pub(in crate::db::executor) predicate_slots: Option<PredicateFieldSlots>,
}
