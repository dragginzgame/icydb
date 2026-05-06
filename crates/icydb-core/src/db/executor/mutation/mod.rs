//! Module: executor::mutation
//! Responsibility: mutation execution preflight and shared commit-window entry helpers.
//! Does not own: relation semantics or logical-plan construction.
//! Boundary: write-path setup shared by save/delete executors.

pub(super) mod commit_window;
pub(super) mod save;
mod save_validation;

use crate::{
    db::{
        Db,
        commit::ensure_recovered,
        data::{
            DataKey, PersistedRow, SerializedStructuralPatch, StructuralPatch,
            serialize_entity_slots_as_complete_serialized_patch, serialize_structural_patch_fields,
            serialize_structural_patch_fields_with_accepted_contract,
        },
        executor::{
            Context, EntityAuthority,
            route::{RoutePlanRequest, build_execution_route_plan},
            validate_executor_plan_for_authority,
        },
        query::plan::AccessPlannedQuery,
        schema::AcceptedRowDecodeContract,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

pub(super) use commit_window::{
    PreparedRowOpDelta, commit_delete_row_ops_with_window,
    commit_delete_row_ops_with_window_for_path, commit_prepared_single_save_row_op_with_window,
    commit_save_row_ops_with_window, emit_index_delta_metrics,
    synchronized_store_handles_for_prepared_row_ops,
};

///
/// MutationInput
///
///
/// MutationInput is the shared internal mutation payload staged above
/// the persisted-row patch boundary.
/// It carries only the structural row key and the already serialized slots so
/// later write-path stages do not need to keep full typed entities alive once
/// save/update preflight has completed.
/// Those slots may describe either one sparse structural patch or one complete
/// typed after-image, depending on the ingress surface.
///

pub(in crate::db::executor) struct MutationInput {
    data_key: DataKey,
    serialized_slots: SerializedStructuralPatch,
}

impl MutationInput {
    /// Build one structural mutation input from already lowered key + patch data.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        data_key: DataKey,
        serialized_slots: SerializedStructuralPatch,
    ) -> Self {
        Self {
            data_key,
            serialized_slots,
        }
    }

    /// Lower one typed entity into the shared structural mutation input.
    pub(in crate::db::executor) fn from_entity<E>(entity: &E) -> Result<Self, InternalError>
    where
        E: PersistedRow + EntityValue,
    {
        let key = entity.id().key();
        let data_key = DataKey::try_new::<E>(key)?;
        let serialized_slots = serialize_entity_slots_as_complete_serialized_patch(entity)?;

        Ok(Self::new(data_key, serialized_slots))
    }

    /// Lower one key + structural patch pair into the shared mutation input.
    ///
    /// The optional accepted row contract keeps schema-transition structural
    /// writes on the selected persisted row shape before bytes cross the
    /// executor mutation boundary.
    pub(in crate::db::executor) fn from_structural_patch<E>(
        key: E::Key,
        patch: &StructuralPatch,
        accepted_row_decode_contract: Option<AcceptedRowDecodeContract>,
    ) -> Result<Self, InternalError>
    where
        E: PersistedRow + EntityValue,
    {
        let data_key = DataKey::try_new::<E>(key)?;
        let serialized_slots = match accepted_row_decode_contract {
            Some(contract) => {
                serialize_structural_patch_fields_with_accepted_contract(E::MODEL, contract, patch)?
            }
            None => serialize_structural_patch_fields(E::MODEL, patch)?,
        };

        Ok(Self::new(data_key, serialized_slots))
    }

    /// Borrow the target row key for this mutation input.
    #[must_use]
    pub(in crate::db::executor) const fn data_key(&self) -> &DataKey {
        &self.data_key
    }

    /// Borrow the serialized slots for this mutation input.
    #[must_use]
    pub(in crate::db::executor) const fn serialized_slots(&self) -> &SerializedStructuralPatch {
        &self.serialized_slots
    }
}

/// Run mutation write-entry recovery checks and return a write-ready context.
pub(in crate::db::executor) fn mutation_write_context<E>(
    db: &'_ Db<E::Canister>,
) -> Result<Context<'_, E>, InternalError>
where
    E: EntityKind + EntityValue,
{
    ensure_recovered(db)?;

    Ok(db.context::<E>())
}

/// Validate mutation-plan executor contracts using authority only.
pub(in crate::db::executor) fn preflight_mutation_plan_for_authority(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    validate_executor_plan_for_authority(&authority, plan)?;
    build_execution_route_plan(plan, RoutePlanRequest::MutationDelete)?;

    Ok(())
}
