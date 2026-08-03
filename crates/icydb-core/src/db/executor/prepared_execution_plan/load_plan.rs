use super::contracts::AccessPlannedQuery;
use crate::{
    db::{
        cursor::{ContinuationSignature, ValidatedGroupedCursor},
        executor::{
            EntityAuthority, GroupedPaginationWindow, PreparedGroupedRuntimeResidents,
            PreparedScalarPlanCore, PreparedScalarRuntimeHandoff,
            pipeline::contracts::{CursorEmissionMode, ProjectionMaterializationMode},
            prepared_execution_plan::{PreparedAccessPlanHandoff, PreparedExecutionPlanCore},
            terminal::RetainedSlotLayout,
        },
    },
    error::InternalError,
};

///
/// PreparedLoadPlan
///
/// Generic-free load-plan boundary consumed by continuation resolution and
/// load pipeline preparation after frontend binding is complete.
///

#[derive(Debug)]
pub(in crate::db::executor) struct PreparedLoadPlan {
    pub(in crate::db::executor::prepared_execution_plan) authority: EntityAuthority,
    pub(in crate::db::executor::prepared_execution_plan) core: PreparedExecutionPlanCore,
}

impl PreparedLoadPlan {
    #[must_use]
    pub(in crate::db::executor) fn authority(&self) -> EntityAuthority {
        self.authority.clone()
    }

    #[must_use]
    pub(in crate::db::executor) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    pub(in crate::db::executor) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<ContinuationSignature, InternalError> {
        self.core.continuation_signature_for_runtime()
    }

    pub(in crate::db::executor) fn grouped_cursor_boundary_arity(
        &self,
    ) -> Result<usize, InternalError> {
        self.core.grouped_cursor_boundary_arity()
    }

    pub(in crate::db::executor) fn grouped_pagination_window(
        &self,
        cursor: &ValidatedGroupedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        self.core.grouped_pagination_window(cursor)
    }

    // Collapse the scalar runtime handoff into one structural extraction so
    // callers do not restate the same authority/projection/layout/index/plan
    // unpacking sequence at every scalar entrypoint.

    /// Consume one typed prepared execution plan into scalar runtime handoff
    /// while using a caller-owned retained-slot layout for this execution only.
    pub(in crate::db::executor) fn into_scalar_runtime_handoff_with_retained_slot_layout(
        self,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
        retained_slot_layout: RetainedSlotLayout,
    ) -> Result<PreparedScalarRuntimeHandoff, InternalError> {
        self.into_scalar_runtime_handoff_with_layout_override(
            projection_materialization,
            cursor_emission,
            Some(retained_slot_layout),
        )
    }

    fn into_scalar_runtime_handoff_with_layout_override(
        self,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
        retained_slot_layout_override: Option<RetainedSlotLayout>,
    ) -> Result<PreparedScalarRuntimeHandoff, InternalError> {
        let Self { authority, core } = self;
        let prepared_projection_contract = if projection_materialization.validate_projection()
            && !core.plan().projection_is_model_identity()?
        {
            core.get_or_init_projection_shape(authority.clone())?
        } else {
            None
        };
        let retained_slot_layout = match retained_slot_layout_override {
            Some(layout) => Some(layout),
            None => core.get_or_init_scalar_layout(
                authority.clone(),
                projection_materialization,
                cursor_emission,
            )?,
        };
        let execution_preparation = core.get_or_init_scalar_execution_preparation();

        Ok(PreparedScalarRuntimeHandoff {
            authority,
            execution_preparation,
            prepared_projection_contract,
            retained_slot_layout,
            plan_core: PreparedScalarPlanCore { core },
        })
    }

    /// Clone cached grouped preparation and layout as one provenance-bound
    /// resident bundle.
    pub(in crate::db::executor) fn cloned_grouped_runtime_residents(
        &self,
    ) -> Result<Option<PreparedGroupedRuntimeResidents>, InternalError> {
        let Some(residents) = self
            .core
            .get_or_init_grouped_runtime_residents(self.authority.clone())?
        else {
            return Ok(None);
        };

        Ok(Some(residents.as_ref().clone()))
    }

    pub(in crate::db::executor) fn into_access_plan_handoff(self) -> PreparedAccessPlanHandoff {
        let Self { authority: _, core } = self;
        let residents = core.into_residents();

        PreparedAccessPlanHandoff {
            plan: residents.plan,
            index_prefix_specs: residents.index_prefix_specs,
            index_range_specs: residents.index_range_specs,
        }
    }
}
