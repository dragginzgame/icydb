//! Module: executor::mutation::constraint_scheduler
//! Responsibility: enforce accepted mutation-constraint phase ordering before
//! normal writes can enter commit preflight.
//! Does not own: row-program semantics, index proofs, relation proofs, or
//! commit-marker recovery.
//! Boundary: accepted final after-images -> constraint schedule -> commit batch.

use crate::{
    db::{
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
    error::{AcceptedConstraintFactContext, InternalError, MutationDiagnosticContext},
    types::EntityTag,
};
use std::collections::{BTreeMap, BTreeSet};

#[expect(
    clippy::too_many_arguments,
    reason = "the accepted scheduler keeps identity, mode, provenance, row contract, fingerprint, and compiled constraints explicit"
)]
fn validate_row_local_after_image(
    entity_path: &str,
    entity_tag: EntityTag,
    mode: MutationMode,
    _data_key: &RawDataStoreKey,
    row: &RawRow,
    provenance: &[Option<AcceptedFieldWriteProvenance>],
    accepted_row_decode_contract: AcceptedRowDecodeContract,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    fingerprint_method: u8,
    constraints: &CompiledAcceptedRowConstraints,
    mutation: MutationDiagnosticContext,
) -> Result<(), InternalError> {
    match constraints.unique_activation_write_blocker(mode, provenance) {
        Ok(Some(barrier)) => {
            return Err(InternalError::mutation_constraint_activation_write_blocked(
                AcceptedConstraintFactContext::write_admission(
                    fingerprint_method,
                    accepted_schema_fingerprint,
                    entity_tag.value(),
                    barrier.constraint_id().get(),
                    icydb_diagnostic_code::DiagnosticConstraintKind::Unique,
                    Some(mutation),
                    None,
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
    constraints
        .evaluate(accepted_schema_fingerprint, values.as_slice())
        .map_err(|error| {
            accepted_row_constraint_write_error(
                fingerprint_method,
                accepted_schema_fingerprint,
                entity_tag.value(),
                Some(mutation),
                error,
            )
        })
}

///
/// AcceptedMutationConstraintBatch
///
/// Opaque proof that one normal-write batch passed the logical constraint
/// phases required before storage-backed commit preflight.
///

pub(in crate::db) struct AcceptedMutationConstraintBatch {
    rows: Vec<CommitRowOp>,
    deleted_key_groups: Vec<(String, BTreeSet<RawDataStoreKey>)>,
}

impl AcceptedMutationConstraintBatch {
    /// Return whether this validated batch has no durable row transition.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(in crate::db::executor) fn into_parts(
        self,
    ) -> (Vec<CommitRowOp>, Vec<(String, BTreeSet<RawDataStoreKey>)>) {
        (self.rows, self.deleted_key_groups)
    }
}

/// Exact accepted authority for one entity participating in a structural
/// mutation batch.
pub(in crate::db) struct AcceptedMutationConstraintContext<'a> {
    pub(in crate::db) entity_path: &'a str,
    pub(in crate::db) entity_tag: EntityTag,
    pub(in crate::db) row_decode_contract: AcceptedRowDecodeContract,
    pub(in crate::db) schema_fingerprint: CommitSchemaFingerprint,
    pub(in crate::db) fingerprint_method: u8,
    pub(in crate::db) row_constraints: &'a CompiledAcceptedRowConstraints,
}

///
/// AcceptedMutationConstraintScheduler
///
/// Normal-write lifecycle owner for accepted constraint scheduling.
///
/// Save scheduling proves row-local constraints over policy-complete final
/// after-images while save and delete scheduling share one target-key set.
/// The opaque output carries the ordered row transitions and complete delete
/// set into final-overlay storage preflight before any commit marker is opened.
///

pub(in crate::db) struct AcceptedMutationConstraintScheduler {
    seen_keys: BTreeMap<RawDataStoreKey, u32>,
    deleted_key_groups: Vec<(String, BTreeSet<RawDataStoreKey>)>,
    rows: Vec<CommitRowOp>,
}

impl AcceptedMutationConstraintScheduler {
    /// Start one accepted mixed schedule that receives every final row intent
    /// after database-owned policy has resolved it.
    pub(in crate::db) fn new(row_capacity: usize) -> Self {
        Self {
            seen_keys: BTreeMap::new(),
            deleted_key_groups: Vec::new(),
            rows: Vec::with_capacity(row_capacity),
        }
    }

    /// Evaluate one logical save after-image and stage its optional physical
    /// transition for later storage-backed preflight.
    #[expect(
        clippy::too_many_arguments,
        reason = "the scheduler keeps accepted authority, mutation intent, final row, provenance, physical transition, and request position explicit"
    )]
    pub(in crate::db) fn schedule_save_after_image(
        &mut self,
        context: AcceptedMutationConstraintContext<'_>,
        mode: MutationMode,
        data_key: &DecodedDataStoreKey,
        row: &RawRow,
        provenance: &[Option<AcceptedFieldWriteProvenance>],
        row_op: Option<CommitRowOp>,
        batch_position: u32,
    ) -> Result<(), InternalError> {
        let raw_key = data_key.to_raw()?;
        self.record_target_key(&raw_key, context.entity_tag, batch_position)?;
        let mutation = MutationDiagnosticContext::new(
            context.entity_tag.value(),
            match mode {
                MutationMode::Insert => icydb_diagnostic_code::DiagnosticMutationOperation::Insert,
                MutationMode::Replace => {
                    icydb_diagnostic_code::DiagnosticMutationOperation::Replace
                }
                MutationMode::Update => icydb_diagnostic_code::DiagnosticMutationOperation::Update,
            },
            batch_position,
        );
        validate_row_local_after_image(
            context.entity_path,
            context.entity_tag,
            mode,
            &raw_key,
            row,
            provenance,
            context.row_decode_contract,
            context.schema_fingerprint,
            context.fingerprint_method,
            context.row_constraints,
            mutation,
        )?;

        if let Some(row_op) = row_op {
            if row_op.entity_path.as_ref() != context.entity_path
                || row_op.key != raw_key
                || row_op.schema_fingerprint != context.schema_fingerprint
                || row_op.after.as_deref() != Some(row.as_bytes())
            {
                return Err(InternalError::query_executor_invariant());
            }
            self.rows
                .push(row_op.with_mutation_diagnostic_context(mutation));
        }

        Ok(())
    }

    /// Stage one delete transition. Relation protection is deferred until the
    /// complete batch key set is known.
    pub(in crate::db) fn schedule_delete(
        &mut self,
        entity_path: &str,
        entity_tag: EntityTag,
        schema_fingerprint: CommitSchemaFingerprint,
        row_op: CommitRowOp,
        batch_position: u32,
    ) -> Result<(), InternalError> {
        if row_op.entity_path.as_ref() != entity_path
            || row_op.before.is_none()
            || row_op.after.is_some()
            || row_op.schema_fingerprint != schema_fingerprint
        {
            return Err(InternalError::query_executor_invariant());
        }
        let _ = DecodedDataStoreKey::try_from_raw(&row_op.key)
            .map_err(|_| InternalError::query_executor_invariant())?;
        self.record_target_key(&row_op.key, entity_tag, batch_position)?;
        let deleted_keys = if let Some((_, deleted_keys)) = self
            .deleted_key_groups
            .iter_mut()
            .find(|(path, _)| path == entity_path)
        {
            deleted_keys
        } else {
            self.deleted_key_groups
                .push((entity_path.to_string(), BTreeSet::new()));
            &mut self
                .deleted_key_groups
                .last_mut()
                .ok_or_else(InternalError::query_executor_invariant)?
                .1
        };
        deleted_keys.insert(row_op.key.clone());
        self.rows.push(
            row_op.with_mutation_diagnostic_context(MutationDiagnosticContext::new(
                entity_tag.value(),
                icydb_diagnostic_code::DiagnosticMutationOperation::Delete,
                batch_position,
            )),
        );
        Ok(())
    }

    fn record_target_key(
        &mut self,
        raw_key: &RawDataStoreKey,
        entity_tag: EntityTag,
        batch_position: u32,
    ) -> Result<(), InternalError> {
        if let Some(first_position) = self.seen_keys.get(raw_key).copied() {
            return Err(InternalError::mutation_atomic_save_duplicate_key(
                entity_tag.value(),
                first_position,
                batch_position,
            ));
        }
        self.seen_keys.insert(raw_key.clone(), batch_position);
        Ok(())
    }

    /// Complete logical scheduling and return the only batch shape accepted by
    /// normal commit-window entrypoints.
    pub(in crate::db) fn finish(self) -> AcceptedMutationConstraintBatch {
        AcceptedMutationConstraintBatch {
            rows: self.rows,
            deleted_key_groups: self.deleted_key_groups,
        }
    }
}
