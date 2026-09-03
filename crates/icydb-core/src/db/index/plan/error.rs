//! Module: index::plan::error
//! Responsibility: preserve canonical internal errors across index planning.
//! Does not own: commit materialization or executor behavior.
//! Boundary: index planning wraps failures; callers recover the canonical error.

use crate::{
    db::commit::CommitSchemaFingerprint,
    error::{AcceptedConstraintFactContext, InternalError, MutationDiagnosticContext},
};

///
/// IndexPlanError
///
/// Internal planning error wrapper that preserves the canonical `InternalError`
/// taxonomy while carrying any side-effect signal the caller must observe at
/// the commit/executor boundary.
///

pub(in crate::db) struct IndexPlanError {
    error: InternalError,
}

impl IndexPlanError {
    /// Build one ordinary index-planning error without boundary side effects.
    #[must_use]
    pub(in crate::db) const fn new(error: InternalError) -> Self {
        Self { error }
    }

    /// Build one accepted unique-constraint violation with catalog identity.
    #[must_use]
    pub(in crate::db) fn unique_violation(
        accepted_schema_fingerprint: CommitSchemaFingerprint,
        mutation: Option<MutationDiagnosticContext>,
        constraint_id: u32,
        entity_tag: u64,
    ) -> Self {
        Self {
            error: InternalError::mutation_constraint_violation(
                AcceptedConstraintFactContext::write_admission(
                    crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                    accepted_schema_fingerprint,
                    entity_tag,
                    constraint_id,
                    icydb_diagnostic_code::DiagnosticConstraintKind::Unique,
                    mutation,
                    None,
                ),
            ),
        }
    }

    /// Consume this wrapper into the canonical internal error.
    #[must_use]
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        self.error
    }
}

impl From<InternalError> for IndexPlanError {
    fn from(error: InternalError) -> Self {
        Self::new(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_violation_preserves_only_compact_accepted_identity() {
        let error = IndexPlanError::unique_violation(
            [0xAB; 16],
            Some(MutationDiagnosticContext::new(
                23,
                icydb_diagnostic_code::DiagnosticMutationOperation::Replace,
                4,
            )),
            17,
            23,
        )
        .into_internal_error();
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
