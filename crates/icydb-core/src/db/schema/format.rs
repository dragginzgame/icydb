//! Module: db::schema::format
//! Responsibility: stable text formatting for runtime schema-introspection surfaces.
//! Does not own: schema DTO construction or query/session orchestration.
//! Boundary: converts `EntityModel` schema contracts into user-readable lines.

use crate::model::entity::EntityModel;

/// Build one stable SQL-style index listing for an entity model.
#[must_use]
pub(in crate::db) fn show_indexes_for_model(model: &EntityModel) -> Vec<String> {
    let mut indexes = Vec::with_capacity(model.indexes.len().saturating_add(1));
    indexes.push(format!("PRIMARY KEY ({})", model.primary_key.name));

    for index in model.indexes {
        let kind = if index.is_unique() {
            "UNIQUE INDEX"
        } else {
            "INDEX"
        };
        let fields = index.fields().join(", ");
        indexes.push(format!("{kind} {} ({fields})", index.name()));
    }

    indexes
}
