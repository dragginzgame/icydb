use crate::db::{
    executor::{
        EntityAuthority, ExecutionPreparation, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
        PreparedScalarPlanCore,
        prepared_execution_plan::build_prepared_execution_plan_core_with_lowered_access,
        projection::PreparedProjectionShape, terminal::RetainedSlotLayout,
    },
    query::plan::AccessPlannedQuery,
};
use std::sync::Arc;

///
/// PreparedScalarRuntimeParts
///
/// Structural scalar runtime handoff extracted from one prepared load plan.
/// Scalar entrypoints use this bundle to consume the authority, projection,
/// retained-slot, and lowered-index residents together instead of restating the
/// same wrapper sequence before route/runtime assembly.
///

pub(in crate::db::executor) struct PreparedScalarRuntimeParts {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) execution_preparation: ExecutionPreparation,
    pub(in crate::db::executor) prepared_projection_shape: Option<Arc<PreparedProjectionShape>>,
    pub(in crate::db::executor) retained_slot_layout: Option<RetainedSlotLayout>,
    pub(in crate::db::executor) plan_core: PreparedScalarPlanCore,
}

impl PreparedScalarRuntimeParts {
    /// Rebuild this scalar runtime handoff with scalar pagination removed from
    /// the execution plan while preserving prepared projection/layout residents.
    ///
    /// DISTINCT projection materialization needs this execution-only shape so
    /// route planning and ordered windows do not bound the stream before the
    /// final projected-row DISTINCT window runs.
    pub(in crate::db::executor) fn into_scalar_page_suppressed(self) -> Self {
        let Self {
            authority,
            execution_preparation,
            prepared_projection_shape,
            retained_slot_layout,
            plan_core,
        } = self;
        let shared = plan_core.core.into_shared();
        let execution_plan = shared.plan.clone_without_scalar_page();
        let core = build_prepared_execution_plan_core_with_lowered_access(
            authority.clone(),
            execution_plan,
            shared.index_prefix_specs,
            shared.index_prefix_spec_invalid,
            shared.index_range_specs,
            shared.index_range_spec_invalid,
        );

        Self {
            authority,
            execution_preparation,
            prepared_projection_shape,
            retained_slot_layout,
            plan_core: PreparedScalarPlanCore { core },
        }
    }
}

///
/// PreparedGroupedRuntimeParts
///
/// Grouped runtime residents cloned from one prepared load plan.
/// Grouped entrypoints use this pair as one explicit handoff so the grouped
/// runtime boundary does not expose two separate clone-only wrappers.
///

pub(in crate::db::executor) struct PreparedGroupedRuntimeParts {
    pub(in crate::db::executor) execution_preparation: Option<ExecutionPreparation>,
    pub(in crate::db::executor) grouped_slot_layout: Option<RetainedSlotLayout>,
}

///
/// PreparedAccessPlanParts
///
/// Structural prepared-plan payload consumed by delete and grouped/scalar
/// structural entrypoints. It keeps the logical plan and lowered access specs
/// together so consumers do not peel the same immutable residents back out
/// through parallel wrappers.
///

pub(in crate::db::executor) struct PreparedAccessPlanParts {
    pub(in crate::db::executor) plan: Arc<AccessPlannedQuery>,
    pub(in crate::db::executor) index_prefix_specs: Arc<[LoweredIndexPrefixSpec]>,
    pub(in crate::db::executor) index_range_specs: Arc<[LoweredIndexRangeSpec]>,
}

///
/// PreparedAggregateStreamingPlanParts
///
/// Prepared aggregate access residents moved together when aggregate execution
/// leaves the generic prepared-plan shell. Keeping these fields bundled avoids
/// tuple-shaped handoffs and makes the shared `Arc` ownership boundary explicit.
///

pub(in crate::db::executor) struct PreparedAggregateStreamingPlanParts {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) logical_plan: Arc<AccessPlannedQuery>,
    pub(in crate::db::executor) index_prefix_specs: Arc<[LoweredIndexPrefixSpec]>,
    pub(in crate::db::executor) index_range_specs: Arc<[LoweredIndexRangeSpec]>,
}

///
/// SharedPreparedProjectionRuntimeParts
///
/// Structural shared-prepared payload needed by projection runtime adapters.
/// Projection adapters consume this bundle directly so they do not restate the
/// same authority/plan/projection extraction across separate shared-plan
/// accessor calls.
///

pub(in crate::db::executor) struct SharedPreparedProjectionRuntimeParts {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) prepared_projection_shape: Option<Arc<PreparedProjectionShape>>,
    pub(in crate::db::executor) scalar_runtime: PreparedScalarRuntimeParts,
}
