use super::contracts::AccessPlannedQuery;
use super::contracts::{CoveringHybridReadExecutionPlan, CoveringReadExecutionPlan};
use crate::db::executor::PreparedLoadPlan;
use crate::db::executor::{
    PreparedScalarPlanCore, PreparedScalarRuntimeHandoff, SharedPreparedProjectionRuntimeHandoff,
    pipeline::contracts::{CursorEmissionMode, ProjectionMaterializationMode},
};
use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        executor::{
            EntityAuthority, ExecutionFamily,
            prepared_execution_plan::{
                PreparedExecutionPlanCore,
                build_prepared_execution_plan_core_with_schema_fingerprint,
            },
        },
    },
    error::InternalError,
};
use std::rc::Rc;

///
/// SharedPreparedExecutionPlan
///
/// SharedPreparedExecutionPlan is the generic-free prepared executor shell
/// cached below the SQL/fluent frontend split. It preserves one canonical
/// prepared execution contract without retaining runtime cursor state or
/// executor scratch buffers.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SharedPreparedExecutionPlan {
    authority: EntityAuthority,
    core: PreparedExecutionPlanCore,
}

impl SharedPreparedExecutionPlan {
    pub(in crate::db) fn from_plan(
        authority: EntityAuthority,
        mut plan: AccessPlannedQuery,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<Self, InternalError> {
        authority.finalize_planner_route_profile(&mut plan)?;

        Ok(Self {
            authority: authority.clone(),
            core: build_prepared_execution_plan_core_with_schema_fingerprint(
                authority,
                plan,
                Some(schema_fingerprint),
            )?,
        })
    }

    #[must_use]
    pub(in crate::db) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    #[must_use]
    pub(in crate::db::executor) fn execution_shape_fingerprint_prefix(&self) -> u64 {
        self.core.execution_shape_fingerprint_prefix()
    }

    pub(in crate::db) fn execution_family(&self) -> Result<ExecutionFamily, InternalError> {
        self.core.execution_family()
    }

    /// Return the accepted-schema-bound scalar continuation signature.
    pub(in crate::db) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<crate::db::cursor::ContinuationSignature, InternalError> {
        self.core.continuation_signature_for_runtime()
    }

    /// Borrow the accepted schema authority frozen into this shared plan.
    pub(in crate::db) fn accepted_schema_authority(
        &self,
    ) -> Result<&crate::db::schema::AcceptedSchemaAuthority, InternalError> {
        self.authority.accepted_schema_authority()
    }

    /// Validate an already-decoded grouped continuation token.
    pub(in crate::db) fn prepare_grouped_cursor_token(
        &self,
        cursor: Option<crate::db::cursor::GroupedContinuationToken>,
    ) -> Result<crate::db::cursor::ValidatedGroupedCursor, crate::db::executor::ExecutorPlanError>
    {
        let Some(contract) = self.core.residents.continuation.as_ref() else {
            return Err(crate::db::executor::ExecutorPlanError::grouped_cursor_preparation_requires_grouped_plan());
        };

        contract
            .prepare_grouped_cursor_token(self.authority.entity_path(), cursor)
            .map_err(crate::db::executor::ExecutorPlanError::from)
    }

    /// Consume this generic-free shared plan into grouped/scalar load runtime.
    #[must_use]
    pub(in crate::db::executor) fn into_prepared_load_plan(self) -> PreparedLoadPlan {
        let Self { authority, core } = self;

        PreparedLoadPlan { authority, core }
    }

    #[must_use]
    pub(in crate::db) const fn authority_ref(&self) -> &EntityAuthority {
        &self.authority
    }

    #[must_use]
    pub(in crate::db) fn authority(&self) -> EntityAuthority {
        self.authority.clone()
    }

    pub(in crate::db) fn index_prefix_specs(
        &self,
    ) -> &[crate::db::executor::LoweredIndexPrefixSpec] {
        self.core.residents.index_prefix_specs.as_ref()
    }

    pub(in crate::db::executor) fn index_range_specs(
        &self,
    ) -> &[crate::db::executor::LoweredIndexRangeSpec] {
        self.core.residents.index_range_specs.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor) fn projection_covering_read_execution_plan(
        &self,
    ) -> Option<Rc<CoveringReadExecutionPlan>> {
        self.core
            .get_or_init_projection_covering_read_execution_plan(self.authority.clone())
    }

    #[must_use]
    pub(in crate::db::executor) fn hybrid_covering_read_plan(
        &self,
    ) -> Option<Rc<CoveringHybridReadExecutionPlan>> {
        self.core
            .get_or_init_hybrid_covering_read_plan(self.authority.clone())
    }

    #[cfg(all(test, feature = "diagnostics"))]
    pub(in crate::db) fn has_projection_covering_read_plan_for_tests(&self) -> bool {
        self.projection_covering_read_execution_plan().is_some()
    }

    #[cfg(all(test, feature = "diagnostics"))]
    pub(in crate::db) fn has_hybrid_covering_read_plan_for_tests(&self) -> bool {
        self.hybrid_covering_read_plan().is_some()
    }

    // Projection runtime adapters consume these three shared prepared residents
    // together, so hand them off as one bundle instead of re-reading the same
    // plan shell through parallel field-level accessors.
    pub(in crate::db::executor) fn into_projection_runtime_handoff(
        self,
    ) -> Result<SharedPreparedProjectionRuntimeHandoff, InternalError> {
        let Self { authority, core } = self;
        let prepared_projection_contract = core.get_or_init_projection_shape(authority.clone())?;
        let retained_slot_layout = core.get_or_init_scalar_layout(
            authority.clone(),
            ProjectionMaterializationMode::RetainSlotRows,
            CursorEmissionMode::Suppress,
        )?;
        let execution_preparation = core.get_or_init_scalar_execution_preparation();
        let scalar_runtime = PreparedScalarRuntimeHandoff {
            authority: authority.clone(),
            execution_preparation,
            prepared_projection_contract: prepared_projection_contract.clone(),
            retained_slot_layout,
            plan_core: PreparedScalarPlanCore { core },
        };

        Ok(SharedPreparedProjectionRuntimeHandoff {
            authority,
            prepared_projection_contract,
            scalar_runtime,
        })
    }
}
