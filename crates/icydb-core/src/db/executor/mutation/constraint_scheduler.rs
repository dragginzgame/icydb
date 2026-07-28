//! Module: executor::mutation::constraint_scheduler
//! Responsibility: enforce accepted mutation-constraint phase ordering before
//! normal writes can enter commit preflight.
//! Does not own: row-program semantics, index proofs, relation proofs, or
//! commit-marker recovery.
//! Boundary: accepted final after-images -> constraint schedule -> commit batch.

use crate::{
    db::{
        Db,
        commit::{CommitRowOp, CommitSchemaFingerprint},
        data::{
            AcceptedFieldWriteProvenance, DecodedDataStoreKey, RawDataStoreKey, RawRow,
            StructuralRowContract, StructuralSlotReader,
        },
        schema::{
            AcceptedRowDecodeContract, CompiledAcceptedRowConstraints,
            accepted_row_constraint_write_error,
        },
        write_context::MutationMode,
    },
    error::{ConstraintDiagnostic, ConstraintDiagnosticKind, InternalError},
    traits::CanisterKind,
};
use std::collections::BTreeSet;

enum AcceptedMutationConstraintSchedule<'a> {
    Save {
        mode: MutationMode,
        row_decode_contract: AcceptedRowDecodeContract,
        schema_fingerprint: CommitSchemaFingerprint,
        row_constraints: &'a CompiledAcceptedRowConstraints,
    },
    Delete {
        raw_keys: BTreeSet<crate::db::data::RawDataStoreKey>,
    },
}

#[expect(
    clippy::too_many_arguments,
    reason = "the accepted scheduler keeps identity, mode, provenance, row contract, fingerprint, and compiled constraints explicit"
)]
fn validate_row_local_after_image(
    entity_path: &str,
    mode: MutationMode,
    data_key: &RawDataStoreKey,
    row: &RawRow,
    provenance: &[Option<AcceptedFieldWriteProvenance>],
    accepted_row_decode_contract: AcceptedRowDecodeContract,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    constraints: &CompiledAcceptedRowConstraints,
) -> Result<(), InternalError> {
    match constraints.unique_activation_write_blocker(mode, provenance) {
        Ok(Some(barrier)) => {
            let primary_key = data_key
                .encoded_primary_key_bytes()
                .ok_or_else(InternalError::store_invariant)?;
            return Err(InternalError::mutation_constraint_activation_write_blocked(
                ConstraintDiagnostic::write_activation_blocked(
                    barrier.constraint_id().get(),
                    barrier.constraint_name().to_string(),
                    ConstraintDiagnosticKind::Unique,
                    entity_path.to_string(),
                    Some(primary_key.to_vec()),
                    barrier.field_paths().to_vec(),
                ),
            ));
        }
        Ok(None) => {}
        Err(_) => return Err(InternalError::accepted_row_constraint_program_corrupt()),
    }

    if constraints.is_empty() {
        return Ok(());
    }
    let contract = StructuralRowContract::from_owned_accepted_decode_contract(
        entity_path.to_string(),
        accepted_row_decode_contract,
    );
    let row_fields =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(row, &contract)?;
    let values = row_fields.decode_selected_slot_values(constraints.required_slots())?;
    let primary_key = data_key
        .encoded_primary_key_bytes()
        .ok_or_else(InternalError::store_invariant)?;

    constraints
        .evaluate(accepted_schema_fingerprint, values.as_slice())
        .map_err(|error| {
            accepted_row_constraint_write_error(entity_path, Some(primary_key.to_vec()), error)
        })
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum AcceptedMutationConstraintBatchKind {
    Save,
    Delete,
}

///
/// AcceptedMutationConstraintBatch
///
/// Opaque proof that one normal-write batch passed the logical constraint
/// phases required before storage-backed commit preflight.
///

pub(in crate::db) struct AcceptedMutationConstraintBatch {
    kind: AcceptedMutationConstraintBatchKind,
    rows: Vec<CommitRowOp>,
}

impl AcceptedMutationConstraintBatch {
    /// Return whether this validated batch has no durable row transition.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(in crate::db::executor) fn into_save_rows(self) -> Result<Vec<CommitRowOp>, InternalError> {
        if self.kind != AcceptedMutationConstraintBatchKind::Save {
            return Err(InternalError::query_executor_invariant());
        }
        Ok(self.rows)
    }

    pub(in crate::db::executor) fn into_delete_rows(
        self,
    ) -> Result<Vec<CommitRowOp>, InternalError> {
        if self.kind != AcceptedMutationConstraintBatchKind::Delete {
            return Err(InternalError::query_executor_invariant());
        }
        Ok(self.rows)
    }
}

///
/// AcceptedMutationConstraintScheduler
///
/// Normal-write lifecycle owner for accepted constraint scheduling.
///
/// Save construction proves row-local constraints over policy-complete final
/// after-images. Delete completion proves relation protection over the complete
/// batch key set. The opaque output is then consumed by storage-backed
/// unique/relation preflight before any commit marker is opened.
///

pub(in crate::db) struct AcceptedMutationConstraintScheduler<'a, C: CanisterKind> {
    db: &'a Db<C>,
    entity_path: &'a str,
    schedule: AcceptedMutationConstraintSchedule<'a>,
    rows: Vec<CommitRowOp>,
}

impl<'a, C: CanisterKind> AcceptedMutationConstraintScheduler<'a, C> {
    /// Start one accepted save schedule that receives each after-image after
    /// database-owned policy has resolved its write intent.
    pub(in crate::db) fn for_save(
        db: &'a Db<C>,
        entity_path: &'a str,
        mode: MutationMode,
        row_decode_contract: AcceptedRowDecodeContract,
        schema_fingerprint: CommitSchemaFingerprint,
        row_constraints: &'a CompiledAcceptedRowConstraints,
        row_capacity: usize,
    ) -> Self {
        Self {
            db,
            entity_path,
            schedule: AcceptedMutationConstraintSchedule::Save {
                mode,
                row_decode_contract,
                schema_fingerprint,
                row_constraints,
            },
            rows: Vec::with_capacity(row_capacity),
        }
    }

    /// Start one accepted delete schedule that will prove relation protection
    /// against the complete submitted key set.
    pub(in crate::db) fn for_delete(
        db: &'a Db<C>,
        entity_path: &'a str,
        row_capacity: usize,
    ) -> Self {
        Self {
            db,
            entity_path,
            schedule: AcceptedMutationConstraintSchedule::Delete {
                raw_keys: BTreeSet::new(),
            },
            rows: Vec::with_capacity(row_capacity),
        }
    }

    /// Evaluate one logical save after-image and stage its optional physical
    /// transition for later storage-backed preflight.
    pub(in crate::db) fn schedule_save_after_image(
        &mut self,
        data_key: &DecodedDataStoreKey,
        row: &RawRow,
        provenance: &[Option<AcceptedFieldWriteProvenance>],
        row_op: Option<CommitRowOp>,
    ) -> Result<(), InternalError> {
        let AcceptedMutationConstraintSchedule::Save {
            mode,
            row_decode_contract,
            schema_fingerprint,
            row_constraints,
        } = &self.schedule
        else {
            return Err(InternalError::query_executor_invariant());
        };
        let raw_key = data_key.to_raw()?;
        validate_row_local_after_image(
            self.entity_path,
            *mode,
            &raw_key,
            row,
            provenance,
            row_decode_contract.clone(),
            *schema_fingerprint,
            row_constraints,
        )?;

        if let Some(row_op) = row_op {
            if row_op.entity_path.as_ref() != self.entity_path
                || row_op.key != raw_key
                || row_op.schema_fingerprint != *schema_fingerprint
                || row_op.after.as_deref() != Some(row.as_bytes())
            {
                return Err(InternalError::query_executor_invariant());
            }
            self.rows.push(row_op);
        }

        Ok(())
    }

    /// Remove the last staged save transition when bounded resumable SQL
    /// chooses the largest durable prefix.
    #[cfg(feature = "query")]
    pub(in crate::db) fn pop_last_save_row(&mut self) -> Result<(), InternalError> {
        if !matches!(
            self.schedule,
            AcceptedMutationConstraintSchedule::Save { .. }
        ) || self.rows.pop().is_none()
        {
            return Err(InternalError::query_executor_invariant());
        }
        Ok(())
    }

    /// Borrow staged save transitions for bounded commit-window sizing.
    #[cfg(feature = "query")]
    pub(in crate::db) fn save_rows(&self) -> Result<&[CommitRowOp], InternalError> {
        if !matches!(
            self.schedule,
            AcceptedMutationConstraintSchedule::Save { .. }
        ) {
            return Err(InternalError::query_executor_invariant());
        }
        Ok(self.rows.as_slice())
    }

    /// Stage one delete transition. Relation protection is deferred until the
    /// complete batch key set is known.
    pub(in crate::db) fn schedule_delete(
        &mut self,
        row_op: CommitRowOp,
    ) -> Result<(), InternalError> {
        let AcceptedMutationConstraintSchedule::Delete { raw_keys } = &mut self.schedule else {
            return Err(InternalError::query_executor_invariant());
        };
        if row_op.entity_path.as_ref() != self.entity_path
            || row_op.before.is_none()
            || row_op.after.is_some()
        {
            return Err(InternalError::query_executor_invariant());
        }
        if !raw_keys.insert(row_op.key.clone()) {
            let data_key = DecodedDataStoreKey::try_from_raw(&row_op.key)
                .map_err(|_| InternalError::query_executor_invariant())?;
            return Err(InternalError::mutation_atomic_save_duplicate_key(
                self.entity_path,
                data_key,
            ));
        }
        self.rows.push(row_op);
        Ok(())
    }

    /// Complete logical scheduling and return the only batch shape accepted by
    /// normal commit-window entrypoints.
    pub(in crate::db) fn finish(self) -> Result<AcceptedMutationConstraintBatch, InternalError> {
        let kind = match self.schedule {
            AcceptedMutationConstraintSchedule::Save { .. } => {
                AcceptedMutationConstraintBatchKind::Save
            }
            AcceptedMutationConstraintSchedule::Delete { raw_keys } => {
                self.db
                    .validate_delete_relations(self.entity_path, &raw_keys)?;
                AcceptedMutationConstraintBatchKind::Delete
            }
        };

        Ok(AcceptedMutationConstraintBatch {
            kind,
            rows: self.rows,
        })
    }
}
