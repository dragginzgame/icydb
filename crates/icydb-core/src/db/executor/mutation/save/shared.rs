use crate::{
    db::{
        Db,
        commit::CommitRowOp,
        data::{DataKey, PersistedRow, RawRow},
        executor::{
            Context, ExecutorError,
            mutation::{
                PreparedRowOpDelta, commit_prepared_single_save_row_op_with_window,
                commit_save_row_ops_with_window, synchronized_store_handles_for_prepared_row_ops,
            },
        },
    },
    error::InternalError,
    metrics::sink::Span,
    traits::EntityValue,
};

use crate::db::executor::mutation::save::{SaveExecutor, SaveRule};
use crate::db::schema::AcceptedRowDecodeContract;

impl<E: PersistedRow + EntityValue> SaveExecutor<E> {
    // Resolve the "before" row according to one canonical save rule.
    pub(super) fn resolve_existing_row_for_rule(
        ctx: &Context<'_, E>,
        data_key: &DataKey,
        save_rule: SaveRule,
        accepted_row_decode_contract: Option<&AcceptedRowDecodeContract>,
    ) -> Result<Option<RawRow>, InternalError> {
        let raw_key = data_key.to_raw()?;

        match save_rule {
            SaveRule::RequireAbsent => {
                if let Some(existing) = ctx.with_store(|store| store.get(&raw_key))? {
                    Self::validate_existing_row_identity(
                        data_key,
                        &existing,
                        accepted_row_decode_contract,
                    )?;
                    return Err(ExecutorError::KeyExists(data_key.clone()).into());
                }

                Ok(None)
            }
            SaveRule::RequirePresent => {
                let old_row = ctx
                    .with_store(|store| store.get(&raw_key))?
                    .ok_or_else(|| InternalError::store_not_found(data_key.to_string()))?;
                Self::validate_existing_row_identity(
                    data_key,
                    &old_row,
                    accepted_row_decode_contract,
                )?;

                Ok(Some(old_row))
            }
            SaveRule::AllowAny => {
                let old_row = ctx.with_store(|store| store.get(&raw_key))?;
                if let Some(old) = old_row.as_ref() {
                    Self::validate_existing_row_identity(
                        data_key,
                        old,
                        accepted_row_decode_contract,
                    )?;
                }

                Ok(old_row)
            }
        }
    }

    // Decode an existing row and verify it is consistent with the target data key.
    fn validate_existing_row_identity(
        data_key: &DataKey,
        row: &RawRow,
        accepted_row_decode_contract: Option<&AcceptedRowDecodeContract>,
    ) -> Result<(), InternalError> {
        if let Some(accepted_row_decode_contract) = accepted_row_decode_contract {
            return Self::validate_existing_row_identity_with_accepted_contract(
                data_key,
                row,
                accepted_row_decode_contract,
            );
        }

        Self::validate_existing_row_identity_with_generated_contract(data_key, row)
    }

    // Decode an existing generated-layout row and verify it is consistent with
    // the target data key before mutation staging treats it as the before image.
    fn validate_existing_row_identity_with_generated_contract(
        data_key: &DataKey,
        row: &RawRow,
    ) -> Result<(), InternalError> {
        Self::map_existing_row_identity_error(
            data_key,
            Self::ensure_persisted_row_invariants(data_key, row),
        )
    }

    // Decode an existing accepted-layout row and verify it is consistent with
    // the target data key before mutation staging treats it as the before image.
    fn validate_existing_row_identity_with_accepted_contract(
        data_key: &DataKey,
        row: &RawRow,
        accepted_row_decode_contract: &AcceptedRowDecodeContract,
    ) -> Result<(), InternalError> {
        Self::map_existing_row_identity_error(
            data_key,
            Self::ensure_persisted_row_invariants_with_accepted_contract(
                data_key,
                row,
                accepted_row_decode_contract.clone(),
            ),
        )
    }

    // Preserve the existing row-identity error taxonomy while allowing accepted
    // and generated validation lanes to stay branch-free.
    fn map_existing_row_identity_error(
        data_key: &DataKey,
        result: Result<(), InternalError>,
    ) -> Result<(), InternalError> {
        result.map_err(|err| match (err.class(), err.origin()) {
            (
                crate::error::ErrorClass::Corruption,
                crate::error::ErrorOrigin::Serialize | crate::error::ErrorOrigin::Store,
            ) => err,
            _ => InternalError::from(ExecutorError::persisted_row_invariant_violation(
                data_key,
                &err.message,
            )),
        })?;

        Ok(())
    }

    // Open + apply commit mechanics for one logical row operation.
    pub(super) fn commit_prepared_single_row(
        db: &Db<E::Canister>,
        marker_row_op: CommitRowOp,
        prepared_row_op: crate::db::commit::PreparedRowCommitOp,
        on_index_applied: impl FnOnce(&PreparedRowOpDelta),
        on_data_applied: impl FnOnce(),
    ) -> Result<(), InternalError> {
        let synchronized_store_handles = synchronized_store_handles_for_prepared_row_ops(
            db,
            std::slice::from_ref(&prepared_row_op),
        );

        // FIRST STABLE WRITE: commit marker is persisted before any mutations.
        commit_prepared_single_save_row_op_with_window(
            marker_row_op,
            prepared_row_op,
            synchronized_store_handles,
            "save_row_apply",
            on_index_applied,
            || {
                on_data_applied();
            },
        )?;

        Ok(())
    }

    // Open + apply commit mechanics for an atomic staged row-op batch.
    pub(super) fn commit_atomic_batch(
        db: &Db<E::Canister>,
        marker_row_ops: Vec<CommitRowOp>,
        span: &mut Span<E>,
    ) -> Result<(), InternalError> {
        let rows_touched = u64::try_from(marker_row_ops.len()).unwrap_or(u64::MAX);
        commit_save_row_ops_with_window::<E>(
            db,
            marker_row_ops,
            "save_batch_atomic_row_apply",
            || {
                span.set_rows(rows_touched);
            },
        )?;

        Ok(())
    }
}

// Fold one single-row prepared delta into one saturated batch accumulator.
pub(super) const fn accumulate_prepared_row_op_delta(
    total: &mut PreparedRowOpDelta,
    delta: &PreparedRowOpDelta,
) {
    total.index_inserts = total.index_inserts.saturating_add(delta.index_inserts);
    total.index_removes = total.index_removes.saturating_add(delta.index_removes);
    total.reverse_index_inserts = total
        .reverse_index_inserts
        .saturating_add(delta.reverse_index_inserts);
    total.reverse_index_removes = total
        .reverse_index_removes
        .saturating_add(delta.reverse_index_removes);
}
