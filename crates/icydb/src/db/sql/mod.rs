//! Module: db::sql
//!
//! Responsibility: SQL-surface text rendering helpers for canister/CLI-facing endpoints.
//! Does not own: SQL parsing/lowering/execution semantics.
//! Boundary: consumes executed SQL projection/explain outputs and renders stable text payloads.

use candid::CandidType;
use serde::Deserialize;

use crate::{
    db::{EntityFieldDescription, EntitySchemaDescription},
    value::{Value, ValueEnum},
};

#[cfg_attr(doc, doc = "SqlProjectionRows\n\nRender-ready SQL projection rows.")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlProjectionRows {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    row_count: u32,
}

impl SqlProjectionRows {
    /// Construct one projection row payload.
    #[must_use]
    pub const fn new(columns: Vec<String>, rows: Vec<Vec<String>>, row_count: u32) -> Self {
        Self {
            columns,
            rows,
            row_count,
        }
    }

    /// Borrow projection column names.
    #[must_use]
    pub const fn columns(&self) -> &[String] {
        self.columns.as_slice()
    }

    /// Borrow rendered row values.
    #[must_use]
    pub const fn rows(&self) -> &[Vec<String>] {
        self.rows.as_slice()
    }

    /// Return projected row count.
    #[must_use]
    pub const fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Consume and return projection row parts.
    #[must_use]
    pub fn into_parts(self) -> (Vec<String>, Vec<Vec<String>>, u32) {
        (self.columns, self.rows, self.row_count)
    }
}

#[cfg_attr(doc, doc = "SqlQueryRowsOutput\n\nStructured SQL projection payload.")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlQueryRowsOutput {
    pub entity: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: u32,
}

impl SqlQueryRowsOutput {
    /// Build one endpoint-friendly rows payload from one projection result.
    #[must_use]
    pub fn from_projection(entity: String, projection: SqlProjectionRows) -> Self {
        let (columns, rows, row_count) = projection.into_parts();
        Self {
            entity,
            columns,
            rows,
            row_count,
        }
    }

    /// Borrow this output as one render-ready projection row payload.
    #[must_use]
    pub fn as_projection_rows(&self) -> SqlProjectionRows {
        SqlProjectionRows::new(self.columns.clone(), self.rows.clone(), self.row_count)
    }
}

#[cfg_attr(doc, doc = "SqlQueryResult\n\nUnified SQL endpoint result.")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlQueryResult {
    Projection(SqlQueryRowsOutput),
    Explain {
        entity: String,
        explain: String,
    },
    Describe(EntitySchemaDescription),
    ShowIndexes {
        entity: String,
        indexes: Vec<String>,
    },
    ShowColumns {
        entity: String,
        columns: Vec<EntityFieldDescription>,
    },
    ShowEntities {
        entities: Vec<String>,
    },
}

impl SqlQueryResult {
    /// Render this payload into deterministic shell-friendly lines.
    #[must_use]
    pub fn render_lines(&self) -> Vec<String> {
        match self {
            Self::Projection(rows) => {
                render_projection_lines(rows.entity.as_str(), &rows.as_projection_rows())
            }
            Self::Explain { explain, .. } => render_explain_lines(explain.as_str()),
            Self::Describe(description) => render_describe_lines(description),
            Self::ShowIndexes { entity, indexes } => {
                render_show_indexes_lines(entity.as_str(), indexes.as_slice())
            }
            Self::ShowColumns { entity, columns } => {
                render_show_columns_lines(entity.as_str(), columns.as_slice())
            }
            Self::ShowEntities { entities } => render_show_entities_lines(entities.as_slice()),
        }
    }

    /// Render this payload into one newline-separated display string.
    #[must_use]
    pub fn render_text(&self) -> String {
        self.render_lines().join("\n")
    }
}

#[cfg_attr(doc, doc = "Render one value into a shell-friendly stable text form.")]
#[must_use]
pub fn render_value_text(value: &Value) -> String {
    match value {
        Value::Account(v) => v.to_string(),
        Value::Blob(v) => render_blob_value(v),
        Value::Bool(v) => v.to_string(),
        Value::Date(v) => v.to_string(),
        Value::Decimal(v) => v.to_string(),
        Value::Duration(v) => render_duration_value(v.as_millis()),
        Value::Enum(v) => render_enum(v),
        Value::Float32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Int(v) => v.to_string(),
        Value::Int128(v) => v.to_string(),
        Value::IntBig(v) => v.to_string(),
        Value::List(items) => render_list_value(items.as_slice()),
        Value::Map(entries) => render_map_value(entries.as_slice()),
        Value::Null => "null".to_string(),
        Value::Principal(v) => v.to_string(),
        Value::Subaccount(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Timestamp(v) => v.as_millis().to_string(),
        Value::Uint(v) => v.to_string(),
        Value::Uint128(v) => v.to_string(),
        Value::UintBig(v) => v.to_string(),
        Value::Ulid(v) => v.to_string(),
        Value::Unit => "()".to_string(),
    }
}

fn render_blob_value(bytes: &[u8]) -> String {
    let mut rendered = String::from("0x");
    rendered.push_str(hex_encode(bytes).as_str());

    rendered
}

fn render_duration_value(millis: u64) -> String {
    let mut rendered = millis.to_string();
    rendered.push_str("ms");

    rendered
}

fn render_list_value(items: &[Value]) -> String {
    let mut rendered = String::from("[");

    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_value_text(item).as_str());
    }

    rendered.push(']');

    rendered
}

fn render_map_value(entries: &[(Value, Value)]) -> String {
    let mut rendered = String::from("{");

    for (index, (key, value)) in entries.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_value_text(key).as_str());
        rendered.push_str(": ");
        rendered.push_str(render_value_text(value).as_str());
    }

    rendered.push('}');

    rendered
}

#[cfg_attr(
    doc,
    doc = "Render one SQL EXPLAIN text payload as endpoint output lines."
)]
#[must_use]
pub fn render_explain_lines(explain: &str) -> Vec<String> {
    let mut lines = vec!["surface=explain".to_string()];
    lines.extend(explain.lines().map(ToString::to_string));

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one typed `DESCRIBE` payload into deterministic shell output lines."
)]
#[must_use]
pub fn render_describe_lines(description: &EntitySchemaDescription) -> Vec<String> {
    let mut lines = Vec::new();

    // Phase 1: emit top-level entity identity metadata.
    lines.push(format!("entity: {}", description.entity_name()));
    lines.push(format!("path: {}", description.entity_path()));
    lines.push(format!("primary_key: {}", description.primary_key()));

    // Phase 2: emit field descriptors in stable model order.
    lines.push("fields:".to_string());
    for field in description.fields() {
        lines.push(format!(
            "  - {}: {} (primary_key={}, queryable={})",
            field.name(),
            field.kind(),
            field.primary_key(),
            field.queryable(),
        ));
    }

    // Phase 3: emit index descriptors or explicit empty marker.
    if description.indexes().is_empty() {
        lines.push("indexes: []".to_string());
    } else {
        lines.push("indexes:".to_string());
        for index in description.indexes() {
            let unique = if index.unique() { ", unique" } else { "" };
            lines.push(format!(
                "  - {}({}){}",
                index.name(),
                index.fields().join(", "),
                unique,
            ));
        }
    }

    // Phase 4: emit relation descriptors or explicit empty marker.
    if description.relations().is_empty() {
        lines.push("relations: []".to_string());
    } else {
        lines.push("relations:".to_string());
        for relation in description.relations() {
            lines.push(format!(
                "  - {} -> {} ({:?}, {:?})",
                relation.field(),
                relation.target_entity_name(),
                relation.strength(),
                relation.cardinality(),
            ));
        }
    }

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one `SHOW INDEXES` payload into deterministic shell output lines."
)]
#[must_use]
pub fn render_show_indexes_lines(entity: &str, indexes: &[String]) -> Vec<String> {
    let mut lines = vec![format!(
        "surface=indexes entity={entity} index_count={}",
        indexes.len()
    )];
    lines.extend(indexes.iter().cloned());

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one `SHOW COLUMNS` payload into deterministic shell output lines."
)]
#[must_use]
pub fn render_show_columns_lines(entity: &str, columns: &[EntityFieldDescription]) -> Vec<String> {
    let mut lines = vec![format!(
        "surface=columns entity={entity} column_count={}",
        columns.len()
    )];
    lines.extend(columns.iter().map(|column| {
        format!(
            "{}: {} (primary_key={}, queryable={})",
            column.name(),
            column.kind(),
            column.primary_key(),
            column.queryable(),
        )
    }));

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one helper-level `SHOW ENTITIES` payload into deterministic lines."
)]
#[must_use]
pub fn render_show_entities_lines(entities: &[String]) -> Vec<String> {
    let mut lines = vec!["surface=entities".to_string()];
    lines.extend(entities.iter().map(|entity| format!("entity={entity}")));

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one SQL projection payload into pretty table lines for shell output."
)]
#[must_use]
pub fn render_projection_lines(entity: &str, projection: &SqlProjectionRows) -> Vec<String> {
    // Phase 1: seed surface header and handle empty-projection output.
    let mut lines = vec![format!(
        "surface=projection entity={entity} row_count={}",
        projection.row_count()
    )];
    if projection.columns().is_empty() {
        lines.push("(no projected columns)".to_string());
        return lines;
    }

    // Phase 2: compute per-column display widths from headers + row values.
    let mut widths = projection
        .columns()
        .iter()
        .map(String::len)
        .collect::<Vec<_>>();
    for row in projection.rows() {
        for (index, value) in row.iter().enumerate() {
            if index >= widths.len() {
                widths.push(value.len());
            } else {
                widths[index] = widths[index].max(value.len());
            }
        }
    }

    // Phase 3: render deterministic ASCII table surface.
    let separator = render_table_separator(widths.as_slice());
    lines.push(separator.clone());
    lines.push(render_table_row(projection.columns(), widths.as_slice()));
    lines.push(separator.clone());
    for row in projection.rows() {
        lines.push(render_table_row(row.as_slice(), widths.as_slice()));
    }
    lines.push(separator);

    lines
}

fn render_table_separator(widths: &[usize]) -> String {
    let segments = widths
        .iter()
        .map(|width| "-".repeat(width.saturating_add(2)))
        .collect::<Vec<_>>();

    format!("+{}+", segments.join("+"))
}

fn render_table_row(cells: &[String], widths: &[usize]) -> String {
    let mut parts = Vec::with_capacity(widths.len());
    for (index, width) in widths.iter().copied().enumerate() {
        let value = cells.get(index).map_or("", String::as_str);
        parts.push(format!("{value:<width$}"));
    }

    format!("| {} |", parts.join(" | "))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }

    out
}

fn render_enum(value: &ValueEnum) -> String {
    let mut rendered = String::new();
    if let Some(path) = value.path() {
        rendered.push_str(path);
        rendered.push_str("::");
    }
    rendered.push_str(value.variant());
    if let Some(payload) = value.payload() {
        rendered.push('(');
        rendered.push_str(render_value_text(payload).as_str());
        rendered.push(')');
    }

    rendered
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use crate::db::sql::{
        SqlQueryResult, SqlQueryRowsOutput, render_describe_lines, render_show_columns_lines,
        render_show_entities_lines, render_show_indexes_lines,
    };
    use crate::db::{
        EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
        EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
    };

    #[test]
    fn render_describe_lines_output_contract_vector_is_stable() {
        let description = EntitySchemaDescription::new(
            "schema.public.Character".to_string(),
            "Character".to_string(),
            "id".to_string(),
            vec![
                EntityFieldDescription::new("id".to_string(), "Ulid".to_string(), true, true),
                EntityFieldDescription::new("name".to_string(), "Text".to_string(), false, true),
            ],
            vec![
                EntityIndexDescription::new(
                    "character_name_idx".to_string(),
                    false,
                    vec!["name".to_string()],
                ),
                EntityIndexDescription::new(
                    "character_pk".to_string(),
                    true,
                    vec!["id".to_string()],
                ),
            ],
            vec![EntityRelationDescription::new(
                "mentor_id".to_string(),
                "schema.public.User".to_string(),
                "User".to_string(),
                "user_store".to_string(),
                EntityRelationStrength::Strong,
                EntityRelationCardinality::Single,
            )],
        );

        assert_eq!(
            render_describe_lines(&description),
            vec![
                "entity: Character".to_string(),
                "path: schema.public.Character".to_string(),
                "primary_key: id".to_string(),
                "fields:".to_string(),
                "  - id: Ulid (primary_key=true, queryable=true)".to_string(),
                "  - name: Text (primary_key=false, queryable=true)".to_string(),
                "indexes:".to_string(),
                "  - character_name_idx(name)".to_string(),
                "  - character_pk(id), unique".to_string(),
                "relations:".to_string(),
                "  - mentor_id -> User (Strong, Single)".to_string(),
            ],
            "describe shell output must remain contract-stable across release lines",
        );
    }

    #[test]
    fn render_show_indexes_lines_output_contract_vector_is_stable() {
        let indexes = vec![
            "PRIMARY KEY (id)".to_string(),
            "INDEX character_name_idx(name)".to_string(),
        ];

        assert_eq!(
            render_show_indexes_lines("Character", indexes.as_slice()),
            vec![
                "surface=indexes entity=Character index_count=2".to_string(),
                "PRIMARY KEY (id)".to_string(),
                "INDEX character_name_idx(name)".to_string(),
            ],
            "show-indexes shell output must remain contract-stable across release lines",
        );
    }

    #[test]
    fn render_show_columns_lines_output_contract_vector_is_stable() {
        let columns = vec![
            EntityFieldDescription::new("id".to_string(), "Ulid".to_string(), true, true),
            EntityFieldDescription::new("name".to_string(), "Text".to_string(), false, true),
        ];

        assert_eq!(
            render_show_columns_lines("Character", columns.as_slice()),
            vec![
                "surface=columns entity=Character column_count=2".to_string(),
                "id: Ulid (primary_key=true, queryable=true)".to_string(),
                "name: Text (primary_key=false, queryable=true)".to_string(),
            ],
            "show-columns shell output must remain contract-stable across release lines",
        );
    }

    #[test]
    fn render_show_entities_lines_output_contract_vector_is_stable() {
        let entities = vec![
            "Character".to_string(),
            "Order".to_string(),
            "User".to_string(),
        ];

        assert_eq!(
            render_show_entities_lines(entities.as_slice()),
            vec![
                "surface=entities".to_string(),
                "entity=Character".to_string(),
                "entity=Order".to_string(),
                "entity=User".to_string(),
            ],
            "show-entities shell output must remain contract-stable across release lines",
        );
    }

    #[test]
    fn sql_query_result_projection_render_lines_output_contract_vector_is_stable() {
        let projection = SqlQueryRowsOutput {
            entity: "User".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["alice".to_string()]],
            row_count: 1,
        };
        let result = SqlQueryResult::Projection(projection);

        assert_eq!(
            result.render_lines(),
            vec![
                "surface=projection entity=User row_count=1".to_string(),
                "+-------+".to_string(),
                "| name  |".to_string(),
                "+-------+".to_string(),
                "| alice |".to_string(),
                "+-------+".to_string(),
            ],
            "projection query-result rendering must remain contract-stable across release lines",
        );
    }
}
