mod cardinality;
use super::generated_schema_finding_error;
use crate::error::ConstraintValidationFindingOutput;
use icydb_diagnostic_code::{DiagnosticFactTag, ErrorCode};

#[test]
fn generated_schema_finding_becomes_canonical_terminal_constraint_failure() {
    let finding = ConstraintValidationFindingOutput::new(
        [0x11; 16],
        7,
        9,
        vec![0x01],
        vec![2],
        None,
        ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION.raw(),
    );

    let error = generated_schema_finding_error(&finding)
        .expect("a generated check finding should map to E210");
    assert_eq!(
        error.diagnostic().error_code(),
        ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
    );
    assert_eq!(
        error
            .diagnostic_facts()
            .iter()
            .map(|(tag, _)| *tag)
            .collect::<Vec<_>>(),
        vec![
            DiagnosticFactTag::AcceptedSchemaFingerprintMethod,
            DiagnosticFactTag::AcceptedSchemaFingerprintHigh,
            DiagnosticFactTag::AcceptedSchemaFingerprintLow,
            DiagnosticFactTag::EntityTag,
            DiagnosticFactTag::ConstraintId,
            DiagnosticFactTag::ConstraintKind,
            DiagnosticFactTag::ConstraintContext,
        ],
    );
}
