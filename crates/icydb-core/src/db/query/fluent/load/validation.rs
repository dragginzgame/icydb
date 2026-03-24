//! Module: db::query::fluent::load::validation
//! Responsibility: module-local ownership and contracts for db::query::fluent::load::validation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::query::{
        fluent::load::FluentLoadQuery,
        intent::{IntentError, QueryError},
        plan::{
            FieldSlot, resolve_aggregate_target_field_slot, validate_fluent_non_paged_mode,
            validate_fluent_paged_mode,
        },
    },
    traits::EntityKind,
};

impl<E> FluentLoadQuery<'_, E>
where
    E: EntityKind,
{
    // Resolve one terminal field target through the planner field-slot boundary.
    // Unknown fields are rejected here so fluent terminal routing cannot bypass
    // planner slot resolution and drift back to runtime string lookups.
    pub(super) fn resolve_terminal_field_slot(field: &str) -> Result<FieldSlot, QueryError> {
        resolve_aggregate_target_field_slot(E::MODEL, field)
    }

    // Resolve one terminal field target, then delegate execution to the
    // provided closure so terminal methods can share the same slot lookup path.
    pub(super) fn with_slot<T>(
        field: impl AsRef<str>,
        f: impl FnOnce(FieldSlot) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let target_slot = Self::resolve_terminal_field_slot(field.as_ref())?;
        f(target_slot)
    }

    pub(super) fn non_paged_intent_error(&self) -> Option<IntentError> {
        validate_fluent_non_paged_mode(self.cursor_token.is_some(), self.query.has_grouping())
            .err()
            .map(IntentError::from)
    }

    pub(super) fn cursor_intent_error(&self) -> Option<IntentError> {
        self.cursor_token
            .as_ref()
            .and_then(|_| self.paged_intent_error())
    }

    pub(super) fn paged_intent_error(&self) -> Option<IntentError> {
        validate_fluent_paged_mode(
            self.query.has_grouping(),
            self.query.has_explicit_order(),
            self.query.load_spec(),
        )
        .err()
        .map(IntentError::from)
    }

    // Lift one optional fluent intent violation into the query-facing boundary
    // so builder and terminal surfaces share the same intent-error wrapper.
    pub(super) const fn ensure_intent_ready(error: Option<IntentError>) -> Result<(), QueryError> {
        if let Some(err) = error {
            return Err(QueryError::intent(err));
        }

        Ok(())
    }

    pub(super) fn ensure_cursor_mode_ready(&self) -> Result<(), QueryError> {
        Self::ensure_intent_ready(self.cursor_intent_error())
    }

    pub(super) fn ensure_paged_mode_ready(&self) -> Result<(), QueryError> {
        Self::ensure_intent_ready(self.paged_intent_error())
    }

    pub(super) fn ensure_non_paged_mode_ready(&self) -> Result<(), QueryError> {
        Self::ensure_intent_ready(self.non_paged_intent_error())
    }
}
