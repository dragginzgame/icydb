use super::contracts::AccessPlannedQuery;
use super::contracts::{CoveringHybridReadExecutionPlan, CoveringReadExecutionPlan};
use crate::db::executor::PreparedLoadPlan;
use crate::db::executor::{
    PreparedScalarPlanCore, PreparedScalarRuntimeHandoff, SharedPreparedProjectionRuntimeHandoff,
};
use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        cursor::{CursorPlanError, TokenWireError, ValidatedGroupedCursor},
        executor::{
            EntityAuthority, ExecutionFamily,
            prepared_execution_plan::{
                PreparedExecutionPlanCore,
                build_prepared_execution_plan_core_with_schema_fingerprint,
            },
        },
        query::construction::ConstructionBudget,
        schema::{enum_catalog::ValueAdmissionBudget, literal_matches_type},
    },
    error::InternalError,
    value::Value,
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
    pub(in crate::db) fn cache_retention_available(&self) -> bool {
        self.core.cache_retention_available()
    }

    pub(in crate::db) fn attach_cache_retention(
        &self,
        entry: &Rc<crate::db::session::CacheEntryWeight>,
    ) {
        self.core.attach_cache_retention(entry);
    }

    /// Retain an already-finalized planner result without reprojecting its metadata.
    pub(in crate::db) fn from_plan(
        authority: EntityAuthority,
        plan: AccessPlannedQuery,
        schema_fingerprint: CommitSchemaFingerprint,
        budget: &dyn ConstructionBudget,
    ) -> Result<Self, InternalError> {
        let core = build_prepared_execution_plan_core_with_schema_fingerprint(
            &authority,
            plan,
            schema_fingerprint,
            budget,
        )?;
        Ok(Self { authority, core })
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
    ) -> &crate::db::schema::AcceptedSchemaAuthority {
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

        let cursor = contract
            .prepare_grouped_cursor_token(self.authority.entity_path(), cursor)
            .map_err(crate::db::executor::ExecutorPlanError::from)?;
        self.validate_grouped_cursor_boundary(&cursor)?;

        Ok(cursor)
    }

    // Validate the authenticated tuple against existing accepted value owners;
    // never coerce a cursor boundary into a different resume position.
    fn validate_grouped_cursor_boundary(
        &self,
        cursor: &ValidatedGroupedCursor,
    ) -> Result<(), crate::db::executor::ExecutorPlanError> {
        let Some(values) = cursor.last_group_key() else {
            return Ok(());
        };
        let grouped = self.core.plan().grouped_plan().ok_or_else(
            crate::db::executor::ExecutorPlanError::grouped_cursor_preparation_requires_grouped_plan,
        )?;
        let invalid = || CursorPlanError::from_token_wire_error(TokenWireError::Decode);
        if values.len() != grouped.group.group_fields.len() {
            return Err(invalid().into());
        }
        let schema = self.authority.accepted_schema_info();
        let mut budget = ValueAdmissionBudget::standard();
        for (field, value) in grouped.group.group_fields.iter().zip(values) {
            if field.as_direct().is_some() {
                let contract = schema
                    .accepted_field_contract(field.field())
                    .ok_or_else(CursorPlanError::continuation_cursor_invariant)?;
                contract
                    .validate_group_key(value, &mut budget)
                    .map_err(|_| invalid())?;
            } else {
                // Scalar record paths may be missing under nullable parents.
                // Their accepted query type is already newtype-resolved.
                let ty = schema
                    .accepted_query_field_type(field.field())
                    .ok_or_else(CursorPlanError::continuation_cursor_invariant)?;
                if !matches!(value, Value::Null) && !literal_matches_type(value, &ty) {
                    return Err(invalid().into());
                }
            }
        }
        Ok(())
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

    pub(in crate::db) fn index_range_specs(&self) -> &[crate::db::executor::LoweredIndexRangeSpec] {
        self.core.residents.index_range_specs.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor) fn projection_covering_read_execution_plan(
        &self,
    ) -> Option<Rc<CoveringReadExecutionPlan>> {
        self.core
            .get_or_init_projection_covering_read_execution_plan(&self.authority)
    }

    #[must_use]
    pub(in crate::db::executor) fn hybrid_covering_read_plan(
        &self,
    ) -> Option<Rc<CoveringHybridReadExecutionPlan>> {
        self.core
            .get_or_init_hybrid_covering_read_plan(&self.authority)
    }

    #[cfg(test)]
    pub(in crate::db) fn has_projection_covering_read_plan_for_tests(&self) -> bool {
        self.projection_covering_read_execution_plan().is_some()
    }

    #[cfg(test)]
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
        let prepared_projection_contract = core
            .get_or_init_projection_shape(&authority)?
            .ok_or_else(InternalError::query_executor_invariant)?;
        let retained_slot_layout = core.get_or_init_cursorless_retained_slot_layout(&authority)?;
        let execution_preparation = core.get_or_init_scalar_execution_preparation()?;
        let scalar_runtime = PreparedScalarRuntimeHandoff {
            authority: authority.clone(),
            execution_preparation,
            prepared_projection_contract: Some(Rc::clone(&prepared_projection_contract)),
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

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(SharedPreparedExecutionPlan {
Self{authority,core} => [authority,core],
});
