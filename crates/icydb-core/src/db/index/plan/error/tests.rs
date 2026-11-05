use super::*;

#[test]
fn unique_violation_preserves_only_compact_accepted_identity() {
    let error = unique_violation(
        [0xAB; 16],
        Some(MutationDiagnosticContext::new(
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            [0xAB; 16],
            23,
            icydb_diagnostic_code::DiagnosticMutationOperation::Replace,
            4,
        )),
        17,
        23,
    );
    assert_eq!(error.origin(), crate::error::ErrorOrigin::Executor);
    assert_eq!(
        error.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
    );
    let facts = error.diagnostic_facts();
    assert!(facts.contains(&(icydb_diagnostic_code::DiagnosticFactTag::EntityTag, 23)));
    assert!(facts.contains(&(icydb_diagnostic_code::DiagnosticFactTag::ConstraintId, 17)));
    assert!(facts.contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::Unique.raw()
    )));
    assert!(facts.contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
        icydb_diagnostic_code::DiagnosticMutationOperation::Replace.raw(),
    )));
    assert!(facts.contains(&(icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 4,)));
    assert_eq!(facts.len(), 9);
}
