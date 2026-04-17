//! Module: db::query::fluent::load::validation
//! Shares fluent load validation helpers for terminal field resolution and
//! paged-vs-non-paged mode checks.

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

    // Enforce non-paged intent readiness before resolving one terminal slot so
    // field-based scalar terminals do not each repeat the same policy gate and
    // planner slot lookup shell.
    pub(super) fn with_non_paged_slot<T>(
        &self,
        field: impl AsRef<str>,
        f: impl FnOnce(FieldSlot) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.ensure_non_paged_mode_ready()?;
        let target_slot = Self::resolve_terminal_field_slot(field.as_ref())?;
        f(target_slot)
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
        let error = self
            .cursor_token
            .as_ref()
            .and_then(|_| {
                validate_fluent_paged_mode(
                    self.query.has_grouping(),
                    self.query.has_explicit_order(),
                    self.query.load_spec(),
                )
                .err()
            })
            .map(IntentError::from);

        Self::ensure_intent_ready(error)
    }

    pub(super) fn ensure_paged_mode_ready(&self) -> Result<(), QueryError> {
        let error = validate_fluent_paged_mode(
            self.query.has_grouping(),
            self.query.has_explicit_order(),
            self.query.load_spec(),
        )
        .err()
        .map(IntentError::from);

        Self::ensure_intent_ready(error)
    }

    pub(super) fn ensure_non_paged_mode_ready(&self) -> Result<(), QueryError> {
        let error =
            validate_fluent_non_paged_mode(self.cursor_token.is_some(), self.query.has_grouping())
                .err()
                .map(IntentError::from);

        Self::ensure_intent_ready(error)
    }
}
