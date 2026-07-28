//! Module: db::schema::format
//! Responsibility: stable text formatting for runtime schema-introspection surfaces.
//! Does not own: schema DTO construction or query/session orchestration.
//! Boundary: converts schema index contracts into user-readable lines.

use super::{
    PersistedIndexSnapshot, PersistedSchemaSnapshot, SchemaExpressionIndexKeyItemInfo, SchemaInfo,
};
use crate::db::IndexState;
use std::fmt::Write;

// Build one stable SQL-style index listing for an accepted schema view with
// one optional runtime lifecycle annotation.
#[must_use]
pub(in crate::db) fn show_indexes_for_schema_info_with_runtime_state(
    schema: &SchemaInfo,
    snapshot: &PersistedSchemaSnapshot,
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
            index_origin(snapshot.indexes(), index.ordinal()),
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
            index_origin(snapshot.indexes(), index.ordinal()),
        ));
    }

    indexes
}

fn index_origin(indexes: &[PersistedIndexSnapshot], ordinal: u16) -> Option<&'static str> {
    indexes
        .iter()
        .find(|index| index.ordinal() == ordinal)
        .map(|index| {
            if index.generated() {
                "generated"
            } else {
                "ddl"
            }
        })
}

#[cfg(test)]
mod tests {
    use super::index_origin;
    use crate::db::schema::{PersistedIndexKeySnapshot, PersistedIndexSnapshot, SchemaIndexId};

    #[test]
    fn index_origin_reads_accepted_snapshot_instead_of_query_projection() {
        let generated = PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index ID should be non-zero"),
            2,
            "generated_index".to_string(),
            "GeneratedIndex".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(Vec::new()),
            None,
        );
        let ddl = PersistedIndexSnapshot::new_sql_ddl(
            SchemaIndexId::new(2).expect("test index ID should be non-zero"),
            3,
            "ddl_index".to_string(),
            "DdlIndex".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(Vec::new()),
            None,
        );
        let indexes = [generated, ddl];

        assert_eq!(index_origin(&indexes, 2), Some("generated"));
        assert_eq!(index_origin(&indexes, 3), Some("ddl"));
        assert_eq!(index_origin(&indexes, 4), None);
    }
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
