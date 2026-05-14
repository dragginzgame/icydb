//! Module: db::schema::format
//! Responsibility: stable text formatting for runtime schema-introspection surfaces.
//! Does not own: schema DTO construction or query/session orchestration.
//! Boundary: converts schema index contracts into user-readable lines.

use super::{SchemaExpressionIndexKeyItemInfo, SchemaInfo};
use crate::{db::IndexState, model::entity::EntityModel};
use std::fmt::Write;

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

    indexes.push(render_index_listing_line(
        "PRIMARY KEY",
        None,
        &[model.primary_key.name],
        runtime_state,
    ));

    for index in model.indexes {
        indexes.push(render_index_listing_line(
            if index.is_unique() {
                "UNIQUE INDEX"
            } else {
                "INDEX"
            },
            Some(index.name()),
            index.fields(),
            runtime_state,
        ));
    }

    indexes
}

// Build one stable SQL-style index listing for an accepted schema view with
// one optional runtime lifecycle annotation.
#[must_use]
pub(in crate::db) fn show_indexes_for_schema_info_with_runtime_state(
    schema: &SchemaInfo,
    runtime_state: Option<IndexState>,
) -> Vec<String> {
    let mut indexes = Vec::with_capacity(
        schema
            .field_path_indexes()
            .len()
            .saturating_add(schema.expression_indexes().len())
            .saturating_add(1),
    );

    if let Some(primary_key) = schema.primary_key_name() {
        indexes.push(render_index_listing_line(
            "PRIMARY KEY",
            None,
            &[primary_key],
            runtime_state,
        ));
    }

    for index in schema.field_path_indexes() {
        let fields: Vec<String> = index
            .fields()
            .iter()
            .map(|field| field.path().join("."))
            .collect();
        let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
        indexes.push(render_index_listing_line(
            if index.unique() {
                "UNIQUE INDEX"
            } else {
                "INDEX"
            },
            Some(index.name()),
            &field_refs,
            runtime_state,
        ));
    }

    for index in schema.expression_indexes() {
        let fields: Vec<String> = index
            .key_items()
            .iter()
            .map(|item| match item {
                SchemaExpressionIndexKeyItemInfo::FieldPath(field) => field.path().join("."),
                SchemaExpressionIndexKeyItemInfo::Expression(expression) => {
                    expression.canonical_text().to_string()
                }
            })
            .collect();
        let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
        indexes.push(render_index_listing_line(
            if index.unique() {
                "UNIQUE INDEX"
            } else {
                "INDEX"
            },
            Some(index.name()),
            &field_refs,
            runtime_state,
        ));
    }

    indexes
}

// Build one stable SQL-style index line without intermediate formatted strings
// so metadata surfaces keep their tiny payload cost tiny too.
fn render_index_listing_line(
    kind: &str,
    name: Option<&str>,
    fields: &[&str],
    runtime_state: Option<IndexState>,
) -> String {
    let mut rendered = String::with_capacity(48 + fields.len().saturating_mul(16));
    rendered.push_str(kind);

    if let Some(name) = name {
        rendered.push(' ');
        rendered.push_str(name);
    }

    rendered.push_str(" (");

    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            rendered.push_str(", ");
        }
        rendered.push_str(field);
    }

    rendered.push(')');

    if let Some(state) = runtime_state {
        let _ = write!(rendered, " [state={}]", state.as_str());
    }

    rendered
}
