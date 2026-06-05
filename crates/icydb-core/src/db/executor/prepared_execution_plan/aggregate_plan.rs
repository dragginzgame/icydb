use crate::{
    db::executor::{
        EntityAuthority, ExecutionPreparation, ExecutorPlanError,
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
    pub(in crate::db::executor) fn execution_preparation(&self) -> ExecutionPreparation {
        self.core.get_or_init_aggregate_execution_preparation()
    }

    pub(in crate::db::executor) fn into_streaming_handoff(
        self,
    ) -> Result<PreparedAggregateStreamingPlanHandoff, InternalError> {
        let Self { authority, core } = self;
        let residents = core.into_residents();

        if residents.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if residents.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(PreparedAggregateStreamingPlanHandoff {
            authority,
            logical_plan: residents.plan,
            schema_fingerprint: residents.schema_fingerprint,
            index_prefix_specs: residents.index_prefix_specs,
            index_range_specs: residents.index_range_specs,
        })
    }

    /// Re-shape one prepared aggregate plan into one grouped prepared load plan
    /// without reconstructing a typed `PreparedExecutionPlan<E>` shell.
    #[must_use]
    pub(in crate::db::executor) fn into_grouped_load_plan(
        self,
        group: GroupSpec,
    ) -> PreparedLoadPlan {
        let Self { authority, core } = self;
        let residents = core.into_residents();
        let mut grouped_plan = Arc::unwrap_or_clone(residents.plan).into_grouped(group);

        // Grouped DISTINCT rewrites change continuation/static execution planning contract,
        // but `AccessPlannedQuery::into_grouped` carries the same access payload,
        // so preserve lowered access specs and refresh only grouped metadata.
        authority.finalize_static_execution_planning_contract(&mut grouped_plan);

        PreparedLoadPlan {
            authority: authority.clone(),
            core: build_prepared_execution_plan_core_with_lowered_access(
                authority,
                grouped_plan,
                residents.schema_fingerprint,
                residents.index_prefix_specs,
                residents.index_prefix_spec_invalid,
                residents.index_range_specs,
                residents.index_range_spec_invalid,
            ),
        }
    }
}
