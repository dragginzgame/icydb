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
    indexes.push(formatted_index_line(
        format!("PRIMARY KEY ({})", model.primary_key.name),
        runtime_state,
    ));

    for index in model.indexes {
        let kind = if index.is_unique() {
            "UNIQUE INDEX"
        } else {
            "INDEX"
        };
        let fields = index.fields().join(", ");
        indexes.push(formatted_index_line(
            format!("{kind} {} ({fields})", index.name()),
            runtime_state,
        ));
    }

    indexes
}

// Append one optional runtime lifecycle annotation to one already-formatted
// SHOW INDEXES line.
fn formatted_index_line(line: String, runtime_state: Option<IndexState>) -> String {
    match runtime_state {
        Some(state) => format!("{line} [state={}]", state.as_str()),
        None => line,
    }
}
