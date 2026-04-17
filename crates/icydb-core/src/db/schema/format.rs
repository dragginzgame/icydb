//! Module: db::schema::format
//! Responsibility: stable text formatting for runtime schema-introspection surfaces.
//! Does not own: schema DTO construction or query/session orchestration.
//! Boundary: converts `EntityModel` schema contracts into user-readable lines.

use crate::{db::IndexState, model::entity::EntityModel};

/// Build one stable SQL-style index listing for an entity model.
#[must_use]
pub(in crate::db) fn show_indexes_for_model(model: &EntityModel) -> Vec<String> {
    show_indexes_for_model_with_runtime_state(model, None)
}

// Build one stable SQL-style index listing for an entity model with one
// optional runtime lifecycle annotation.
#[must_use]
pub(in crate::db) fn show_indexes_for_model_with_runtime_state(
    model: &EntityModel,
    runtime_state: Option<IndexState>,
) -> Vec<String> {
    let mut indexes = Vec::with_capacity(model.indexes.len().saturating_add(1));
    let primary_key_line = format!("PRIMARY KEY ({})", model.primary_key.name);
    indexes.push(match runtime_state {
        Some(state) => format!("{primary_key_line} [state={}]", state.as_str()),
        None => primary_key_line,
    });

    for index in model.indexes {
        let kind = if index.is_unique() {
            "UNIQUE INDEX"
        } else {
            "INDEX"
        };
        let fields = index.fields().join(", ");
        let index_line = format!("{kind} {} ({fields})", index.name());
        indexes.push(match runtime_state {
            Some(state) => format!("{index_line} [state={}]", state.as_str()),
            None => index_line,
        });
    }

    indexes
}
