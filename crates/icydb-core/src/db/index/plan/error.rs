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
mod tests;
