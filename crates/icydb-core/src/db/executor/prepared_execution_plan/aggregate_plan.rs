use crate::{
    db::executor::{
        AccessPlannedQuery, EntityAuthority, ExecutionPreparation, LoweredIndexPrefixSpec,
        PreparedAggregateStreamingPlanHandoff, PreparedLoadPlan,
        prepared_execution_plan::{
            PreparedExecutionPlanCore, build_prepared_execution_plan_core_with_lowered_access,
            contracts::GroupSpec,
        },
    },
    error::InternalError,
};
use std::sync::Arc;

///
/// PreparedAggregatePlan
///
/// Generic-free aggregate-plan boundary consumed by aggregate terminal and
/// runtime preparation after the typed `PreparedExecutionPlan<E>` shell is no
/// longer needed.
///

#[derive(Debug)]
pub(in crate::db::executor) struct PreparedAggregatePlan {
    pub(in crate::db::executor::prepared_execution_plan) authority: EntityAuthority,
    pub(in crate::db::executor::prepared_execution_plan) core: PreparedExecutionPlanCore,
}

impl PreparedAggregatePlan {
    #[must_use]
    pub(in crate::db::executor) fn authority(&self) -> EntityAuthority {
        self.authority.clone()
    }

    #[must_use]
    pub(in crate::db::executor) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    #[must_use]
    pub(in crate::db::executor) fn execution_preparation(&self) -> ExecutionPreparation {
        self.core.get_or_init_aggregate_execution_preparation()
    }

    pub(in crate::db::executor) fn index_prefix_specs(&self) -> &[LoweredIndexPrefixSpec] {
        self.core.residents.index_prefix_specs.as_ref()
    }

    pub(in crate::db::executor) fn into_streaming_handoff(
        self,
    ) -> PreparedAggregateStreamingPlanHandoff {
        let Self { authority, core } = self;
        let residents = core.into_residents();

        PreparedAggregateStreamingPlanHandoff {
            authority,
            logical_plan: residents.plan,
            continuation_identity: residents.continuation_identity,
            index_prefix_specs: residents.index_prefix_specs,
            index_range_specs: residents.index_range_specs,
        }
    }

    /// Re-shape one prepared aggregate plan into one grouped prepared load plan
    /// without reconstructing a typed `PreparedExecutionPlan<E>` shell.
    pub(in crate::db::executor) fn into_grouped_load_plan(
        self,
        group: GroupSpec,
    ) -> Result<PreparedLoadPlan, InternalError> {
        let Self { authority, core } = self;
        let residents = core.into_residents();
        let mut grouped_plan = Arc::unwrap_or_clone(residents.plan).into_grouped(group);

        // Grouped DISTINCT rewrites change continuation/static execution planning contract,
        // but `AccessPlannedQuery::into_grouped` carries the same access payload,
        // so preserve lowered access specs and refresh only grouped metadata.
        authority.finalize_static_execution_planning_contract(&mut grouped_plan)?;

        Ok(PreparedLoadPlan {
            authority: authority.clone(),
            core: build_prepared_execution_plan_core_with_lowered_access(
                authority,
                grouped_plan,
                residents.continuation_identity,
                residents.index_prefix_specs,
                residents.index_range_specs,
            ),
        })
    }
}
