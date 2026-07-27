//! Module: executor::mutation::save_validation
//! Responsibility: accepted structural after-image constraint enforcement.
//! Does not own: typed entity validation or commit-window application.
//! Boundary: validates one resolved structural row before commit planning.

use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        data::{
            AcceptedFieldWriteProvenance, RawDataStoreKey, RawRow, StructuralRowContract,
            StructuralSlotReader,
        },
        schema::{
            AcceptedRowConstraintEvaluationError, AcceptedRowConstraintViolationKind,
            AcceptedRowDecodeContract, CompiledAcceptedRowConstraints,
        },
        write_context::MutationMode,
    },
    error::{ConstraintDiagnostic, ConstraintDiagnosticKind, InternalError},
};

/// Validate activation gates and accepted row checks over one canonical
/// structural after-image.
#[expect(
    clippy::too_many_arguments,
    reason = "the accepted write boundary keeps identity, mode, provenance, row contract, fingerprint, and compiled constraints explicit"
)]
pub(in crate::db) fn validate_structural_accepted_after_image(
    entity_path: &'static str,
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
    let contract = StructuralRowContract::from_accepted_decode_contract(
        entity_path,
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
        .map_err(|error| match error {
            AcceptedRowConstraintEvaluationError::Violation {
                constraint_id,
                constraint_name,
                kind,
                field_paths,
            } => {
                InternalError::mutation_constraint_violation(ConstraintDiagnostic::write_violation(
                    constraint_id.get(),
                    constraint_name,
                    match kind {
                        AcceptedRowConstraintViolationKind::Check => {
                            ConstraintDiagnosticKind::Check
                        }
                        AcceptedRowConstraintViolationKind::NotNull => {
                            ConstraintDiagnosticKind::NotNull
                        }
                    },
                    entity_path.to_string(),
                    Some(primary_key.to_vec()),
                    field_paths,
                ))
            }
            AcceptedRowConstraintEvaluationError::InvalidExpression(_)
            | AcceptedRowConstraintEvaluationError::LiteralCorrupt
            | AcceptedRowConstraintEvaluationError::FingerprintMismatch
            | AcceptedRowConstraintEvaluationError::MissingSlot
            | AcceptedRowConstraintEvaluationError::RuntimeValueMismatch
            | AcceptedRowConstraintEvaluationError::WorkBudgetExceeded => {
                InternalError::accepted_row_constraint_program_corrupt()
            }
        })
}
