use crate::db::executor::prepared_execution_plan::build_prepared_execution_plan_core_from_bound_plan;
use crate::db::executor::{
    EntityAuthority, ExecutionPreparation, PreparedScalarPlanCore,
    projection::PreparedProjectionContract, terminal::RetainedSlotLayout,
};
use crate::error::InternalError;
use std::rc::Rc;

///
/// PreparedScalarRuntimeHandoff
///
/// Structural scalar runtime handoff extracted from one prepared load plan.
/// Scalar entrypoints use this bundle to consume the authority, projection,
/// retained-slot, and lowered-index residents together instead of restating the
/// same wrapper sequence before route/runtime assembly.
///

pub(in crate::db::executor) struct PreparedScalarRuntimeHandoff {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) execution_preparation: Rc<ExecutionPreparation>,
    pub(in crate::db::executor) prepared_projection_contract:
        Option<Rc<PreparedProjectionContract>>,
    pub(in crate::db::executor) retained_slot_layout: Option<RetainedSlotLayout>,
    pub(in crate::db::executor) plan_core: PreparedScalarPlanCore,
}

impl PreparedScalarRuntimeHandoff {
    /// Rebuild this scalar runtime handoff with scalar pagination removed from
    /// the execution plan while preserving prepared projection/layout residents.
    ///
    /// DISTINCT projection materialization needs this execution-only shape so
    /// route planning and ordered windows do not bound the stream before the
    /// final projected-row DISTINCT window runs.
    pub(in crate::db::executor) fn into_scalar_page_suppressed(
        self,
    ) -> Result<Self, InternalError> {
        let Self {
            authority,
            execution_preparation,
            prepared_projection_contract,
            retained_slot_layout,
            plan_core,
        } = self;
        // Rebuild from the access inputs without copying unused cached preparation.
        let residents = &plan_core.core.residents;
        let execution_plan = residents.plan.clone_without_scalar_page();
        let core = build_prepared_execution_plan_core_from_bound_plan(
            &authority,
            execution_plan,
            residents.continuation_identity,
            &crate::db::resource::budget::ExecutionConstructionBudget,
        )?;

        Ok(Self {
            authority,
            execution_preparation,
            prepared_projection_contract,
            retained_slot_layout,
            plan_core: PreparedScalarPlanCore { core },
        })
    }
}

///
/// SharedPreparedProjectionRuntimeHandoff
///
/// Structural shared-prepared payload needed by projection runtime adapters.
/// Projection adapters consume this bundle directly so they do not restate the
/// same authority/plan/projection extraction across separate shared-plan
/// accessor calls.
///

pub(in crate::db::executor) struct SharedPreparedProjectionRuntimeHandoff {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) prepared_projection_contract: Rc<PreparedProjectionContract>,
    pub(in crate::db::executor) scalar_runtime: PreparedScalarRuntimeHandoff,
}
