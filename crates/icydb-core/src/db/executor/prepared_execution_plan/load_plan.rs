use super::contracts::{AccessPlannedQuery, ExecutionOrdering, QueryMode};
use crate::{
    db::{
        cursor::{ContinuationSignature, ValidatedCursor, ValidatedGroupedCursor},
        executor::{
            EntityAuthority, GroupedPaginationWindow, PreparedGroupedRuntimeResidents,
            PreparedScalarPlanCore, PreparedScalarRuntimeHandoff,
            pipeline::contracts::{CursorEmissionMode, ProjectionMaterializationMode},
            prepared_execution_plan::{
                PreparedAccessPlanHandoff, PreparedExecutionPlanCore,
                build_prepared_execution_plan_core_with_shared_lowered_access,
            },
            terminal::RetainedSlotLayout,
        },
        query::plan::AcceptedContinuationIdentity,
    },
    error::InternalError,
};
use std::sync::Arc;

///
/// PreparedLoadPlan
///
/// Generic-free load-plan boundary consumed by continuation resolution and
/// load pipeline preparation after the typed `PreparedExecutionPlan<E>` shell is no
/// longer needed.
///

#[derive(Debug)]
pub(in crate::db::executor) struct PreparedLoadPlan {
    pub(in crate::db::executor::prepared_execution_plan) authority: EntityAuthority,
    pub(in crate::db::executor::prepared_execution_plan) core: PreparedExecutionPlanCore,
}

impl PreparedLoadPlan {
    /// Build a generic-free prepared load plan from already-lowered shared
    /// residents that came from the canonical prepared-plan core.
    ///
    /// This keeps the large logical plan and lowered access specs shared across
    /// aggregate-to-materialized fallback handoffs while still refreshing the
    /// load continuation contract against the current logical plan shape.
    ///
    #[must_use]
    pub(in crate::db::executor) fn from_valid_shared_residents(
        authority: EntityAuthority,
        plan: Arc<AccessPlannedQuery>,
        continuation_identity: Option<AcceptedContinuationIdentity>,
        index_prefix_specs: Arc<[crate::db::executor::LoweredIndexPrefixSpec]>,
        index_range_specs: Arc<[crate::db::executor::LoweredIndexRangeSpec]>,
    ) -> Self {
        Self {
            authority: authority.clone(),
            core: build_prepared_execution_plan_core_with_shared_lowered_access(
                authority,
                plan,
                continuation_identity,
                index_prefix_specs,
                index_range_specs,
            ),
        }
    }

    #[must_use]
    pub(in crate::db::executor) fn authority(&self) -> EntityAuthority {
        self.authority.clone()
    }

    #[must_use]
    pub(in crate::db::executor) fn mode(&self) -> QueryMode {
        self.core.mode()
    }

    #[must_use]
    pub(in crate::db::executor) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    pub(in crate::db::executor) fn index_prefix_specs(
        &self,
    ) -> &[crate::db::executor::LoweredIndexPrefixSpec] {
        self.core.residents.index_prefix_specs.as_ref()
    }

    pub(in crate::db::executor) fn index_range_specs(
        &self,
    ) -> &[crate::db::executor::LoweredIndexRangeSpec] {
        self.core.residents.index_range_specs.as_ref()
    }

    pub(in crate::db::executor) fn execution_ordering(
        &self,
    ) -> Result<ExecutionOrdering, InternalError> {
        self.core.execution_ordering()
    }

    pub(in crate::db::executor) fn revalidate_cursor(
        &self,
        cursor: ValidatedCursor,
    ) -> Result<ValidatedCursor, InternalError> {
        self.core.revalidate_cursor(self.authority.clone(), cursor)
    }

    pub(in crate::db::executor) fn revalidate_grouped_cursor(
        &self,
        cursor: ValidatedGroupedCursor,
    ) -> Result<ValidatedGroupedCursor, InternalError> {
        self.core.revalidate_grouped_cursor(cursor)
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
    pub(in crate::db::executor) fn into_scalar_runtime_handoff(
        self,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
    ) -> Result<PreparedScalarRuntimeHandoff, InternalError> {
        self.into_scalar_runtime_handoff_with_layout_override(
            projection_materialization,
            cursor_emission,
            None,
        )
    }

    /// Consume one typed prepared execution plan into scalar runtime handoff
    /// while using a caller-owned retained-slot layout for this execution only.
    #[cfg(feature = "sql")]
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
        let Self { authority, core } = self;
        let residents = core.into_residents();

        PreparedAccessPlanHandoff {
            authority,
            plan: residents.plan,
            index_prefix_specs: residents.index_prefix_specs,
            index_range_specs: residents.index_range_specs,
        }
    }
}
