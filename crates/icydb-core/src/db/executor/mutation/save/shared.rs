use crate::{
    db::{
        Db,
        commit::CommitRowOp,
        data::{DataKey, PersistedRow, RawRow},
        executor::{
            Context, ExecutorError,
            mutation::{
                PreparedRowOpDelta, commit_prepared_single_save_row_op_with_window,
                commit_save_row_ops_with_window_and_schema_fingerprint,
                save::{SaveExecutor, SaveRule},
                synchronized_store_handles_for_prepared_row_ops,
            },
        },
        schema::{AcceptedRowDecodeContract, SchemaInfo},
    },
    error::InternalError,
    metrics::sink::Span,
    traits::EntityValue,
};

impl<E: PersistedRow + EntityValue> SaveExecutor<E> {
    // Resolve the "before" row through the accepted row contract selected from
    // the stored schema snapshot. This keeps accepted identity validation
    // explicit at the mutation lane boundary instead of optional inside lookup.
    pub(super) fn resolve_existing_row_for_rule_with_accepted_contract(
        ctx: &Context<'_, E>,
        data_key: &DataKey,
        save_rule: SaveRule,
        accepted_row_decode_contract: &AcceptedRowDecodeContract,
        accepted_schema_info: &SchemaInfo,
    ) -> Result<Option<RawRow>, InternalError> {
        Self::resolve_existing_row_for_rule_with_identity_validator(
            ctx,
            data_key,
            save_rule,
            |data_key, row| {
                Self::validate_existing_row_identity_with_accepted_contract(
                    data_key,
                    row,
                    accepted_row_decode_contract,
                    accepted_schema_info,
                )
            },
        )
    }

    // Resolve the "before" row according to one canonical save rule after the
    // caller has selected the identity validator for the active schema lane.
    fn resolve_existing_row_for_rule_with_identity_validator(
        ctx: &Context<'_, E>,
        data_key: &DataKey,
        save_rule: SaveRule,
        validate_existing_row: impl Fn(&DataKey, &RawRow) -> Result<(), InternalError>,
    ) -> Result<Option<RawRow>, InternalError> {
        let raw_key = data_key.to_raw()?;

        match save_rule {
            SaveRule::RequireAbsent => {
                if let Some(existing) = ctx.with_store(|store| store.get(&raw_key))? {
                    validate_existing_row(data_key, &existing)?;

                    return Err(ExecutorError::KeyExists(data_key.clone()).into());
                }

                Ok(None)
            }
            SaveRule::RequirePresent => {
                let old_row = ctx
                    .with_store(|store| store.get(&raw_key))?
                    .ok_or_else(|| InternalError::store_not_found(data_key.to_string()))?;
                validate_existing_row(data_key, &old_row)?;

                Ok(Some(old_row))
            }
            SaveRule::AllowAny => {
                let old_row = ctx.with_store(|store| store.get(&raw_key))?;
                if let Some(old) = old_row.as_ref() {
                    validate_existing_row(data_key, old)?;
                }

                Ok(old_row)
            }
        }
    }

    // Decode an existing accepted-layout row and verify it is consistent with
    // the target data key before mutation staging treats it as the before image.
    fn validate_existing_row_identity_with_accepted_contract(
        data_key: &DataKey,
        row: &RawRow,
        accepted_row_decode_contract: &AcceptedRowDecodeContract,
        accepted_schema_info: &SchemaInfo,
    ) -> Result<(), InternalError> {
        Self::map_existing_row_identity_error(
            data_key,
            Self::ensure_persisted_row_invariants_with_accepted_contract(
                data_key,
                row,
                accepted_row_decode_contract.clone(),
                accepted_schema_info,
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
        schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
        span: &mut Span<E>,
    ) -> Result<(), InternalError> {
        let rows_touched = u64::try_from(marker_row_ops.len()).unwrap_or(u64::MAX);
        commit_save_row_ops_with_window_and_schema_fingerprint::<E>(
            db,
            marker_row_ops,
            "save_batch_atomic_row_apply",
            schema_fingerprint,
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
