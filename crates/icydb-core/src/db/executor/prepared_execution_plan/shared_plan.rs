use super::contracts::AccessPlannedQuery;
#[cfg(feature = "sql")]
use super::contracts::{CoveringHybridReadExecutionPlan, CoveringReadExecutionPlan};
#[cfg(feature = "sql")]
use crate::{
    db::executor::{
        ExecutorPlanError, PreparedScalarPlanCore, PreparedScalarRuntimeHandoff,
        SharedPreparedProjectionRuntimeHandoff,
        pipeline::contracts::{CursorEmissionMode, ProjectionMaterializationMode},
    },
    error::InternalError,
};
use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        executor::{
            EntityAuthority,
            prepared_execution_plan::{
                PreparedExecutionPlan, PreparedExecutionPlanCore,
                build_prepared_execution_plan_core_with_schema_fingerprint,
            },
        },
    },
    traits::EntityKind,
};
use std::marker::PhantomData;

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
    #[must_use]
    pub(in crate::db) fn from_plan(
        authority: EntityAuthority,
        mut plan: AccessPlannedQuery,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        authority.finalize_planner_route_profile(&mut plan);

        Self {
            authority: authority.clone(),
            core: build_prepared_execution_plan_core_with_schema_fingerprint(
                authority,
                plan,
                Some(schema_fingerprint),
            ),
        }
    }

    #[must_use]
    pub(in crate::db) fn typed_clone<E: EntityKind>(&self) -> PreparedExecutionPlan<E> {
        assert!(
            self.authority.entity_path() == E::PATH,
            "shared prepared plan entity mismatch: cached for '{}', requested '{}'",
            self.authority.entity_path(),
            E::PATH,
        );

        PreparedExecutionPlan {
            authority: self.authority.clone(),
            core: self.core.clone(),
            marker: PhantomData,
        }
    }

    #[must_use]
    pub(in crate::db) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    #[must_use]
    pub(in crate::db) fn authority(&self) -> EntityAuthority {
        self.authority.clone()
    }

    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn validate_lowered_access_specs(
        &self,
    ) -> Result<(), InternalError> {
        if self.core.residents.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if self.core.residents.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(())
    }

    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn projection_covering_read_execution_plan(
        &self,
    ) -> Option<std::sync::Arc<CoveringReadExecutionPlan>> {
        self.core
            .get_or_init_projection_covering_read_execution_plan(self.authority.clone())
    }

    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn hybrid_covering_read_plan(
        &self,
    ) -> Option<std::sync::Arc<CoveringHybridReadExecutionPlan>> {
        self.core
            .get_or_init_hybrid_covering_read_plan(self.authority.clone())
    }

    // Projection runtime adapters consume these three shared prepared residents
    // together, so hand them off as one bundle instead of re-reading the same
    // plan shell through parallel field-level accessors.
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn into_projection_runtime_handoff(
        self,
    ) -> Result<SharedPreparedProjectionRuntimeHandoff, InternalError> {
        let Self { authority, core } = self;
        let prepared_projection_contract = core.get_or_init_projection_shape(authority.clone());
        let retained_slot_layout = core.get_or_init_scalar_layout(
            authority.clone(),
            ProjectionMaterializationMode::RetainSlotRows,
            CursorEmissionMode::Suppress,
        );
        let execution_preparation = core.get_or_init_scalar_execution_preparation();
        if core.residents.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if core.residents.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }
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
