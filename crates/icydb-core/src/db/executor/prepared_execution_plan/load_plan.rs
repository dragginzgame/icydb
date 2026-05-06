use crate::{
    db::{
        cursor::{ContinuationSignature, GroupedPlannedCursor, PlannedCursor},
        executor::{
            EntityAuthority, ExecutorPlanError, GroupedPaginationWindow, PreparedScalarPlanCore,
            PreparedScalarRuntimeParts,
            pipeline::contracts::{CursorEmissionMode, ProjectionMaterializationMode},
            prepared_execution_plan::{
                PreparedAccessPlanParts, PreparedExecutionPlanCore, PreparedGroupedRuntimeParts,
                build_prepared_execution_plan_core_with_shared_lowered_access,
            },
            terminal::RetainedSlotLayout,
        },
        query::plan::{AccessPlannedQuery, ExecutionOrdering, QueryMode},
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
    pub(in crate::db::executor) fn from_valid_shared_parts(
        authority: EntityAuthority,
        plan: Arc<AccessPlannedQuery>,
        index_prefix_specs: Arc<[crate::db::executor::LoweredIndexPrefixSpec]>,
        index_range_specs: Arc<[crate::db::executor::LoweredIndexRangeSpec]>,
    ) -> Self {
        Self {
            authority: authority.clone(),
            core: build_prepared_execution_plan_core_with_shared_lowered_access(
                authority,
                plan,
                index_prefix_specs,
                false,
                index_range_specs,
                false,
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
    ) -> Result<&[crate::db::executor::LoweredIndexPrefixSpec], InternalError> {
        if self.core.shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }

        Ok(self.core.shared.index_prefix_specs.as_ref())
    }

    pub(in crate::db::executor) fn index_range_specs(
        &self,
    ) -> Result<&[crate::db::executor::LoweredIndexRangeSpec], InternalError> {
        if self.core.shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(self.core.shared.index_range_specs.as_ref())
    }

    pub(in crate::db::executor) fn execution_ordering(
        &self,
    ) -> Result<ExecutionOrdering, InternalError> {
        self.core.execution_ordering()
    }

    pub(in crate::db::executor) fn revalidate_cursor(
        &self,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError> {
        self.core.revalidate_cursor(self.authority.clone(), cursor)
    }

    pub(in crate::db::executor) fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
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
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        self.core.grouped_pagination_window(cursor)
    }

    // Collapse the scalar runtime handoff into one structural extraction so
    // callers do not restate the same authority/projection/layout/index/plan
    // unpacking sequence at every scalar entrypoint.
    pub(in crate::db::executor) fn into_scalar_runtime_parts(
        self,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
    ) -> Result<PreparedScalarRuntimeParts, InternalError> {
        self.into_scalar_runtime_parts_with_layout_override(
            projection_materialization,
            cursor_emission,
            None,
        )
    }

    /// Consume one typed prepared execution plan into scalar runtime parts
    /// while using a caller-owned retained-slot layout for this execution only.
    pub(in crate::db::executor) fn into_scalar_runtime_parts_with_retained_slot_layout(
        self,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
        retained_slot_layout: RetainedSlotLayout,
    ) -> Result<PreparedScalarRuntimeParts, InternalError> {
        self.into_scalar_runtime_parts_with_layout_override(
            projection_materialization,
            cursor_emission,
            Some(retained_slot_layout),
        )
    }

    fn into_scalar_runtime_parts_with_layout_override(
        self,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
        retained_slot_layout_override: Option<RetainedSlotLayout>,
    ) -> Result<PreparedScalarRuntimeParts, InternalError> {
        let Self { authority, core } = self;
        let prepared_projection_shape = if projection_materialization.validate_projection()
            && !core.plan().projection_is_model_identity()
        {
            core.get_or_init_projection_shape(authority.clone())
        } else {
            None
        };
        let retained_slot_layout = retained_slot_layout_override.or_else(|| {
            core.get_or_init_scalar_layout(
                authority.clone(),
                projection_materialization,
                cursor_emission,
            )
        });
        let execution_preparation = core.get_or_init_scalar_execution_preparation();
        if core.shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if core.shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(PreparedScalarRuntimeParts {
            authority,
            execution_preparation,
            prepared_projection_shape,
            retained_slot_layout,
            plan_core: PreparedScalarPlanCore { core },
        })
    }

    #[must_use]
    pub(in crate::db::executor) fn cloned_grouped_runtime_parts(
        &self,
    ) -> PreparedGroupedRuntimeParts {
        let Some(residents) = self
            .core
            .get_or_init_grouped_runtime_residents(self.authority.clone())
        else {
            return PreparedGroupedRuntimeParts {
                execution_preparation: None,
                grouped_slot_layout: None,
            };
        };

        PreparedGroupedRuntimeParts {
            execution_preparation: Some(residents.execution_preparation()),
            grouped_slot_layout: Some(residents.grouped_slot_layout()),
        }
    }

    pub(in crate::db::executor) fn into_access_plan_parts(
        self,
    ) -> Result<PreparedAccessPlanParts, InternalError> {
        let Self { authority: _, core } = self;
        let shared = core.into_shared();

        if shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(PreparedAccessPlanParts {
            plan: shared.plan,
            index_prefix_specs: shared.index_prefix_specs,
            index_range_specs: shared.index_range_specs,
        })
    }
}
