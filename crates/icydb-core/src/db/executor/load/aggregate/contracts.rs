use crate::{
    db::{
        Context,
        executor::{
            AccessStreamBindings, ExecutionPlan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            fold::{AggregateFoldMode, AggregateKind, AggregateSpec},
            load::execute::ExecutionInputs,
        },
        index::predicate::IndexPredicateProgram,
        query::{plan::AccessPlannedQuery, plan::Direction, predicate::PredicateFieldSlots},
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
    pub(in crate::db::executor) logical_plan: &'exec AccessPlannedQuery<E::Key>,
    pub(in crate::db::executor) route_plan: &'exec ExecutionPlan,
    pub(in crate::db::executor) index_prefix_specs: &'exec [LoweredIndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'exec [LoweredIndexRangeSpec],
    pub(in crate::db::executor) index_predicate_program: Option<&'exec IndexPredicateProgram>,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) kind: AggregateKind,
    pub(in crate::db::executor) fold_mode: AggregateFoldMode,
}

///
/// AggregateExecutionDescriptor
///
/// Canonical aggregate execution descriptor constructed once from a terminal
/// aggregate spec and validated plan shape before execution branching.
///

pub(in crate::db::executor) struct AggregateExecutionDescriptor {
    pub(in crate::db::executor) spec: AggregateSpec,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) route_plan: ExecutionPlan,
    pub(in crate::db::executor) strict_index_predicate_program: Option<IndexPredicateProgram>,
}

///
/// PreparedAggregateStreamingInputs
///
/// PreparedAggregateStreamingInputs owns the canonical aggregate streaming
/// execution state after `ExecutablePlan` is consumed into logical plan form.
/// This keeps aggregate streaming branches aligned on one setup boundary.
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

impl<E> PreparedAggregateStreamingInputs<'_, E>
where
    E: EntityKind + EntityValue,
{
    // Build canonical execution stream bindings for aggregate streaming paths.
    pub(in crate::db::executor) const fn execution_inputs(
        &self,
        direction: Direction,
    ) -> ExecutionInputs<'_, E> {
        ExecutionInputs {
            ctx: &self.ctx,
            plan: &self.logical_plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: self.index_prefix_specs.as_slice(),
                index_range_specs: self.index_range_specs.as_slice(),
                index_range_anchor: None,
                direction,
            },
            predicate_slots: self.predicate_slots.as_ref(),
        }
    }
}
