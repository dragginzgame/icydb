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
    // Enforce non-paged intent readiness before resolving one terminal slot so
    // field-based scalar terminals do not each repeat the same policy gate and
    // planner slot lookup shell.
    pub(super) fn with_non_paged_slot<T>(
        &self,
        field: impl AsRef<str>,
        f: impl FnOnce(FieldSlot) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.ensure_non_paged_mode_ready()?;
        let target_slot = resolve_aggregate_target_field_slot(E::MODEL, field.as_ref())?;
        f(target_slot)
    }

    pub(super) fn ensure_cursor_mode_ready(&self) -> Result<(), QueryError> {
        // Cursor-mode fluent queries only need the paged policy gate when a
        // continuation token is actually present on the request boundary.
        if self.cursor_token.is_some() {
            validate_fluent_paged_mode(
                self.query.has_grouping(),
                self.query.has_explicit_order(),
                self.query.load_spec(),
            )
            .map_err(IntentError::from)
            .map_err(QueryError::intent)?;
        }

        Ok(())
    }

    pub(super) fn ensure_paged_mode_ready(&self) -> Result<(), QueryError> {
        validate_fluent_paged_mode(
            self.query.has_grouping(),
            self.query.has_explicit_order(),
            self.query.load_spec(),
        )
        .map_err(IntentError::from)
        .map_err(QueryError::intent)
    }

    pub(super) fn ensure_non_paged_mode_ready(&self) -> Result<(), QueryError> {
        validate_fluent_non_paged_mode(self.cursor_token.is_some(), self.query.has_grouping())
            .map_err(IntentError::from)
            .map_err(QueryError::intent)
    }
}
