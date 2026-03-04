use crate::{
    db::query::{
        fluent::load::FluentLoadQuery,
        intent::{IntentError, QueryError},
        plan::{FieldSlot, validate_fluent_non_paged_mode, validate_fluent_paged_mode},
    },
    error::InternalError,
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
        FieldSlot::resolve(E::MODEL, field).ok_or_else(|| {
            QueryError::execute(InternalError::executor_unsupported(format!(
                "unknown aggregate target field: {field}",
            )))
        })
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

    pub(super) fn ensure_paged_mode_ready(&self) -> Result<(), QueryError> {
        if let Some(err) = self.paged_intent_error() {
            return Err(QueryError::Intent(err));
        }

        Ok(())
    }

    pub(super) fn ensure_non_paged_mode_ready(&self) -> Result<(), QueryError> {
        if let Some(err) = self.non_paged_intent_error() {
            return Err(QueryError::Intent(err));
        }

        Ok(())
    }
}
