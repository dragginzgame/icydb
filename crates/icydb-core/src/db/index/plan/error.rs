//! Module: index::plan::error
//! Responsibility: construct accepted unique-constraint diagnostics for index planning.
//! Does not own: commit materialization or executor behavior.
//! Boundary: attach accepted identity to the canonical internal error.

use crate::{
    db::commit::CommitSchemaFingerprint,
    error::{AcceptedConstraintFactContext, InternalError, MutationDiagnosticContext},
};

/// Build one accepted unique-constraint violation with catalog identity.
#[must_use]
pub(super) fn unique_violation(
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    mutation: Option<MutationDiagnosticContext>,
    constraint_id: u32,
    entity_tag: u64,
) -> InternalError {
    InternalError::mutation_constraint_violation(AcceptedConstraintFactContext::write_admission(
        crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
        accepted_schema_fingerprint,
        entity_tag,
        constraint_id,
        icydb_diagnostic_code::DiagnosticConstraintKind::Unique,
        mutation,
        None,
    ))
}

#[cfg(test)]
mod tests {
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
}
