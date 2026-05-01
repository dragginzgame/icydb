use crate::{
    db::{
        executor::{
            EntityAuthority, ExecutorPlanError, PreparedScalarPlanCore, PreparedScalarRuntimeParts,
            pipeline::contracts::{CursorEmissionMode, ProjectionMaterializationMode},
            prepared_execution_plan::{
                PreparedExecutionPlan, PreparedExecutionPlanCore,
                SharedPreparedProjectionRuntimeParts, build_prepared_execution_plan_core,
            },
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
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
    ) -> Self {
        authority.finalize_planner_route_profile(&mut plan);

        Self {
            authority,
            core: build_prepared_execution_plan_core(authority, plan),
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
            core: self.core.clone(),
            marker: PhantomData,
        }
    }

    #[must_use]
    pub(in crate::db) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    // Projection runtime adapters consume these three shared prepared residents
    // together, so hand them off as one bundle instead of re-reading the same
    // plan shell through parallel field-level accessors.
    pub(in crate::db::executor) fn into_projection_runtime_parts(
        self,
    ) -> Result<SharedPreparedProjectionRuntimeParts, InternalError> {
        let Self { authority, core } = self;
        let prepared_projection_shape = core.get_or_init_projection_shape(authority);
        let retained_slot_layout = core.get_or_init_scalar_layout(
            authority,
            ProjectionMaterializationMode::RetainSlotRows,
            CursorEmissionMode::Suppress,
        );
        let execution_preparation = core.get_or_init_scalar_execution_preparation();
        if core.shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if core.shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }
        let scalar_runtime = PreparedScalarRuntimeParts {
            authority,
            execution_preparation,
            prepared_projection_shape: prepared_projection_shape.clone(),
            retained_slot_layout,
            plan_core: PreparedScalarPlanCore { core },
        };

        Ok(SharedPreparedProjectionRuntimeParts {
            authority,
            prepared_projection_shape,
            scalar_runtime,
        })
    }
}
