//! Module: db::query::fluent::load::validation
//! Shares fluent load validation helpers for terminal field resolution and
//! paged-vs-non-paged mode checks.

use crate::{
    db::query::{
        fluent::load::FluentLoadQuery,
        intent::{IntentError, QueryError},
        plan::{
            FieldSlot, resolve_aggregate_target_field_slot_with_schema,
            validate_fluent_non_paged_mode, validate_fluent_paged_mode,
        },
    },
    entity::EntityKind,
};

impl<E> FluentLoadQuery<'_, E>
where
    E: EntityKind,
{
    // Enforce non-paged intent readiness before resolving one terminal slot.
    // Terminal methods consume the resolved slot directly so execution and
    // explain helpers stay flat instead of nesting closures per field lane.
    pub(super) fn resolve_non_paged_slot(
        &self,
        field: impl AsRef<str>,
    ) -> Result<FieldSlot, QueryError> {
        self.ensure_non_paged_mode_ready()?;

        let schema = self
            .session
            .accepted_schema_info_for_entity::<E>()
            .map_err(QueryError::execute)?;

        resolve_aggregate_target_field_slot_with_schema(E::MODEL, &schema, field.as_ref())
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

    pub(super) const fn ensure_page_request_owns_cursor(&self) -> Result<(), QueryError> {
        if self.cursor_token.is_some() {
            return Err(QueryError::intent(
                IntentError::cursor_before_page_terminal(),
            ));
        }

        Ok(())
    }
}
