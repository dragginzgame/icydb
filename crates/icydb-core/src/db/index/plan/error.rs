//! Module: index::plan::error
//! Responsibility: carry index-planning errors plus boundary-observable signals.
//! Does not own: metrics emission, commit materialization, or executor behavior.
//! Boundary: index planning annotates outcomes; commit/executor boundaries observe them.

use crate::error::{ConstraintDiagnostic, ConstraintDiagnosticKind, InternalError};
use std::rc::Rc;

///
/// IndexPlanError
///
/// Internal planning error wrapper that preserves the canonical `InternalError`
/// taxonomy while carrying any side-effect signal the caller must observe at
/// the commit/executor boundary.
///

pub(in crate::db) struct IndexPlanError {
    error: InternalError,
    unique_violation_entity_path: Option<Rc<str>>,
}

impl IndexPlanError {
    /// Build one ordinary index-planning error without boundary side effects.
    #[must_use]
    pub(in crate::db) const fn new(error: InternalError) -> Self {
        Self {
            error,
            unique_violation_entity_path: None,
        }
    }

    /// Build one accepted unique-constraint violation with catalog identity.
    #[must_use]
    pub(in crate::db) fn unique_violation(
        constraint_id: u32,
        constraint_name: &str,
        entity_path: &str,
        primary_key: Vec<u8>,
        index_fields: Vec<String>,
    ) -> Self {
        Self {
            error: InternalError::mutation_constraint_violation(
                ConstraintDiagnostic::write_violation(
                    constraint_id,
                    constraint_name.to_string(),
                    ConstraintDiagnosticKind::Unique,
                    entity_path.to_string(),
                    Some(primary_key),
                    index_fields,
                ),
            ),
            unique_violation_entity_path: Some(entity_path.into()),
        }
    }

    /// Return the entity path for a unique-violation metric, when present.
    #[must_use]
    pub(in crate::db) fn unique_violation_entity_path(&self) -> Option<&str> {
        self.unique_violation_entity_path.as_deref()
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
    fn unique_violation_preserves_accepted_identity_and_fields() {
        let error = IndexPlanError::unique_violation(
            17,
            "unique_email",
            "tests::User",
            vec![0x01, 0x02],
            vec!["email".to_string()],
        )
        .into_internal_error();
        let diagnostic = error
            .constraint_diagnostic()
            .expect("unique violation should retain accepted diagnostic");

        assert_eq!(diagnostic.constraint_id(), 17);
        assert_eq!(diagnostic.constraint_name(), "unique_email");
        assert_eq!(
            diagnostic.constraint_kind(),
            ConstraintDiagnosticKind::Unique
        );
        assert_eq!(diagnostic.entity(), "tests::User");
        assert_eq!(diagnostic.primary_key(), Some([0x01, 0x02].as_slice()));
        assert_eq!(diagnostic.field_paths(), &["email".to_string()]);
    }
}
