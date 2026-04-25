//! Module: index::plan::error
//! Responsibility: carry index-planning errors plus boundary-observable signals.
//! Does not own: metrics emission, commit materialization, or executor behavior.
//! Boundary: index planning annotates outcomes; commit/executor boundaries observe them.

use crate::error::InternalError;

///
/// IndexPlanError
///
/// Internal planning error wrapper that preserves the canonical `InternalError`
/// taxonomy while carrying any side-effect signal the caller must observe at
/// the commit/executor boundary.
///

pub(in crate::db) struct IndexPlanError {
    error: InternalError,
    unique_violation_entity_path: Option<&'static str>,
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

    /// Build one unique-constraint violation while preserving the old error.
    #[must_use]
    pub(in crate::db) fn unique_violation(
        entity_path: &'static str,
        index_fields: &[&str],
    ) -> Self {
        Self {
            error: InternalError::index_violation(entity_path, index_fields),
            unique_violation_entity_path: Some(entity_path),
        }
    }

    /// Return the entity path for a unique-violation metric, when present.
    #[must_use]
    pub(in crate::db) const fn unique_violation_entity_path(&self) -> Option<&'static str> {
        self.unique_violation_entity_path
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
