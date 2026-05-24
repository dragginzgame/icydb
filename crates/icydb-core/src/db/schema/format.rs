//! Module: db::schema::format
//! Responsibility: stable text formatting for runtime schema-introspection surfaces.
//! Does not own: schema DTO construction or query/session orchestration.
//! Boundary: converts schema index contracts into user-readable lines.

use super::{SchemaExpressionIndexKeyItemInfo, SchemaInfo};
use crate::{
    db::IndexState,
    model::{entity::EntityModel, field::FieldModel},
};
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
        &primary_key_fields_from_model(model),
        None,
        runtime_state,
        Some("generated"),
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
            index.predicate(),
            runtime_state,
            Some("generated"),
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

    if !schema.primary_key_names().is_empty() {
        let primary_key_fields = primary_key_fields_from_schema(schema);
        indexes.push(render_index_listing_line(
            "PRIMARY KEY",
            None,
            &primary_key_fields,
            None,
            runtime_state,
            Some("generated"),
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
            index.predicate_sql(),
            runtime_state,
            Some(if index.generated() {
                "generated"
            } else {
                "ddl"
            }),
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
            index.predicate_sql(),
            runtime_state,
            Some(if index.generated() {
                "generated"
            } else {
                "ddl"
            }),
        ));
    }

    indexes
}

fn primary_key_fields_from_model(model: &EntityModel) -> Vec<&'static str> {
    model
        .primary_key_model()
        .fields()
        .iter()
        .map(FieldModel::name)
        .collect()
}

fn primary_key_fields_from_schema(schema: &SchemaInfo) -> Vec<&str> {
    schema
        .primary_key_names()
        .iter()
        .map(String::as_str)
        .collect()
}

// Build one stable SQL-style index line without intermediate formatted strings
// so metadata surfaces keep their tiny payload cost tiny too.
fn render_index_listing_line(
    kind: &str,
    name: Option<&str>,
    fields: &[&str],
    predicate_sql: Option<&str>,
    runtime_state: Option<IndexState>,
    origin: Option<&str>,
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

    if let Some(predicate_sql) = predicate_sql {
        let _ = write!(rendered, " WHERE {predicate_sql}");
    }

    if let Some(state) = runtime_state {
        let _ = write!(rendered, " [state={}]", state.as_str());
    }

    if let Some(origin) = origin {
        let _ = write!(rendered, " [origin={origin}]");
    }

    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::{EntityModel, FieldKind, FieldModel, IndexModel, PrimaryKeyModel},
        testing::entity_model_from_static,
    };

    static SCALAR_FIELDS: [FieldModel; 1] = [FieldModel::generated("id", FieldKind::Nat)];
    static COMPOSITE_FIELDS: [FieldModel; 3] = [
        FieldModel::generated("tenant_id", FieldKind::Nat),
        FieldModel::generated("local_id", FieldKind::Nat),
        FieldModel::generated("label", FieldKind::Text { max_len: None }),
    ];
    static EMPTY_INDEXES: [&IndexModel; 0] = [];
    static SCALAR_MODEL: EntityModel = entity_model_from_static(
        "schema::format::tests::ScalarEntity",
        "ScalarEntity",
        &SCALAR_FIELDS[0],
        0,
        &SCALAR_FIELDS,
        &EMPTY_INDEXES,
    );
    static COMPOSITE_PK_FIELDS: [&FieldModel; 2] = [&COMPOSITE_FIELDS[0], &COMPOSITE_FIELDS[1]];
    static COMPOSITE_MODEL: EntityModel = EntityModel::generated_with_primary_key_model(
        "schema::format::tests::CompositeEntity",
        "CompositeEntity",
        PrimaryKeyModel::ordered(&COMPOSITE_PK_FIELDS),
        0,
        &COMPOSITE_FIELDS,
        &EMPTY_INDEXES,
    );

    #[test]
    fn show_indexes_for_model_keeps_scalar_primary_key_format() {
        let indexes = show_indexes_for_model(&SCALAR_MODEL);

        assert_eq!(indexes[0], "PRIMARY KEY (id) [origin=generated]");
    }

    #[test]
    fn show_indexes_for_model_lists_composite_primary_key_fields() {
        let indexes = show_indexes_for_model(&COMPOSITE_MODEL);

        assert_eq!(
            indexes[0],
            "PRIMARY KEY (tenant_id, local_id) [origin=generated]",
        );
    }

    #[test]
    fn show_indexes_for_schema_info_lists_composite_primary_key_fields() {
        let schema = SchemaInfo::cached_for_generated_entity_model(&COMPOSITE_MODEL);
        let indexes = show_indexes_for_schema_info_with_runtime_state(schema, None);

        assert_eq!(
            indexes[0],
            "PRIMARY KEY (tenant_id, local_id) [origin=generated]",
        );
    }
}
