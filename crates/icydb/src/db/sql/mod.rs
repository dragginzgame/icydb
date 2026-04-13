//! Defines the public SQL text/result payload types exposed by the facade crate.
//!
//! This module consumes already-executed SQL outputs and renders stable
//! endpoint-friendly row payloads; parsing and execution stay in `icydb-core`.

use candid::CandidType;
use icydb_core::db::{GroupedRow, SqlStatementResult};
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

#[cfg_attr(doc, doc = "SqlGroupedRowsOutput\n\nStructured grouped SQL payload.")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlGroupedRowsOutput {
    pub entity: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: u32,
    pub next_cursor: Option<String>,
}

#[cfg_attr(doc, doc = "SqlQueryResult\n\nUnified SQL endpoint result.")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlQueryResult {
    Count {
        entity: String,
        row_count: u32,
    },
    Projection(SqlQueryRowsOutput),
    Grouped(SqlGroupedRowsOutput),
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
            Self::Count { entity, row_count } => render_count_lines(entity.as_str(), *row_count),
            Self::Projection(rows) => {
                render_projection_lines(rows.entity.as_str(), &rows.as_projection_rows())
            }
            Self::Grouped(rows) => render_grouped_lines(rows),
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

pub(crate) fn sql_query_result_from_statement(
    result: SqlStatementResult,
    entity_name: String,
) -> SqlQueryResult {
    match result {
        SqlStatementResult::Count { row_count } => SqlQueryResult::Count {
            entity: entity_name,
            row_count,
        },
        SqlStatementResult::Projection {
            columns,
            rows,
            row_count,
        } => {
            let rows = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| render_value_text(&value))
                        .collect::<Vec<String>>()
                })
                .collect::<Vec<Vec<String>>>();

            SqlQueryResult::Projection(SqlQueryRowsOutput::from_projection(
                entity_name,
                SqlProjectionRows::new(columns, rows, row_count),
            ))
        }
        SqlStatementResult::ProjectionText {
            columns,
            rows,
            row_count,
        } => SqlQueryResult::Projection(SqlQueryRowsOutput::from_projection(
            entity_name,
            SqlProjectionRows::new(columns, rows, row_count),
        )),
        SqlStatementResult::Grouped {
            columns,
            rows,
            row_count,
            next_cursor,
        } => SqlQueryResult::Grouped(sql_grouped_rows_output(
            entity_name,
            columns,
            rows,
            row_count,
            next_cursor,
        )),
        SqlStatementResult::Explain(explain) => SqlQueryResult::Explain {
            entity: entity_name,
            explain,
        },
        SqlStatementResult::Describe(description) => SqlQueryResult::Describe(description),
        SqlStatementResult::ShowIndexes(indexes) => SqlQueryResult::ShowIndexes {
            entity: entity_name,
            indexes,
        },
        SqlStatementResult::ShowColumns(columns) => SqlQueryResult::ShowColumns {
            entity: entity_name,
            columns,
        },
        SqlStatementResult::ShowEntities(entities) => SqlQueryResult::ShowEntities { entities },
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

fn sql_grouped_rows_output(
    entity_name: String,
    columns: Vec<String>,
    rows: Vec<GroupedRow>,
    row_count: u32,
    next_cursor: Option<String>,
) -> SqlGroupedRowsOutput {
    let rows = rows
        .into_iter()
        .map(|row| {
            row.group_key()
                .iter()
                .chain(row.aggregate_values().iter())
                .map(render_value_text)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    SqlGroupedRowsOutput {
        entity: entity_name,
        columns,
        rows,
        row_count,
        next_cursor,
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
    doc = "Render one SQL count payload into deterministic shell output lines."
)]
#[must_use]
pub fn render_count_lines(entity: &str, row_count: u32) -> Vec<String> {
    vec![format!(
        "surface=count entity={entity} row_count={row_count}"
    )]
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

#[cfg_attr(
    doc,
    doc = "Render one grouped SQL payload into pretty table lines for shell output."
)]
#[must_use]
pub fn render_grouped_lines(grouped: &SqlGroupedRowsOutput) -> Vec<String> {
    // Phase 1: seed grouped header metadata and expose the outward continuation
    // cursor on its own line when grouped pagination has more rows.
    let mut lines = vec![format!(
        "surface=grouped entity={} row_count={}",
        grouped.entity, grouped.row_count
    )];
    if let Some(next_cursor) = &grouped.next_cursor {
        lines.push(format!("next_cursor={next_cursor}"));
    }
    if grouped.columns.is_empty() {
        lines.push("(no grouped columns)".to_string());
        return lines;
    }

    // Phase 2: compute per-column display widths from headers + grouped row values.
    let mut widths = grouped.columns.iter().map(String::len).collect::<Vec<_>>();
    for row in &grouped.rows {
        for (index, value) in row.iter().enumerate() {
            if index >= widths.len() {
                widths.push(value.len());
            } else {
                widths[index] = widths[index].max(value.len());
            }
        }
    }

    // Phase 3: render the grouped page as the same deterministic ASCII table
    // shape used by projection payloads.
    let separator = render_table_separator(widths.as_slice());
    lines.push(separator.clone());
    lines.push(render_table_row(
        grouped.columns.as_slice(),
        widths.as_slice(),
    ));
    lines.push(separator.clone());
    for row in &grouped.rows {
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
    use icydb_core::db::{GroupedRow, SqlStatementResult};

    use crate::db::sql::{
        SqlGroupedRowsOutput, SqlQueryResult, SqlQueryRowsOutput, render_describe_lines,
        render_show_columns_lines, render_show_entities_lines, render_show_indexes_lines,
        sql_query_result_from_statement,
    };
    use crate::db::{
        EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
        EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
    };
    use crate::value::Value;

    #[test]
    fn render_describe_lines_output_contract_vector_is_stable() {
        let description = EntitySchemaDescription::new(
            "schema.public.ExampleEntity".to_string(),
            "ExampleEntity".to_string(),
            "id".to_string(),
            vec![
                EntityFieldDescription::new("id".to_string(), "Ulid".to_string(), true, true),
                EntityFieldDescription::new("name".to_string(), "Text".to_string(), false, true),
            ],
            vec![
                EntityIndexDescription::new(
                    "example_entity_name_idx".to_string(),
                    false,
                    vec!["name".to_string()],
                ),
                EntityIndexDescription::new(
                    "example_entity_pk".to_string(),
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
                "entity: ExampleEntity".to_string(),
                "path: schema.public.ExampleEntity".to_string(),
                "primary_key: id".to_string(),
                "fields:".to_string(),
                "  - id: Ulid (primary_key=true, queryable=true)".to_string(),
                "  - name: Text (primary_key=false, queryable=true)".to_string(),
                "indexes:".to_string(),
                "  - example_entity_name_idx(name)".to_string(),
                "  - example_entity_pk(id), unique".to_string(),
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
            "INDEX example_entity_name_idx(name)".to_string(),
        ];

        assert_eq!(
            render_show_indexes_lines("ExampleEntity", indexes.as_slice()),
            vec![
                "surface=indexes entity=ExampleEntity index_count=2".to_string(),
                "PRIMARY KEY (id)".to_string(),
                "INDEX example_entity_name_idx(name)".to_string(),
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
            render_show_columns_lines("ExampleEntity", columns.as_slice()),
            vec![
                "surface=columns entity=ExampleEntity column_count=2".to_string(),
                "id: Ulid (primary_key=true, queryable=true)".to_string(),
                "name: Text (primary_key=false, queryable=true)".to_string(),
            ],
            "show-columns shell output must remain contract-stable across release lines",
        );
    }

    #[test]
    fn render_show_entities_lines_output_contract_vector_is_stable() {
        let entities = vec![
            "ExampleEntity".to_string(),
            "Order".to_string(),
            "User".to_string(),
        ];

        assert_eq!(
            render_show_entities_lines(entities.as_slice()),
            vec![
                "surface=entities".to_string(),
                "entity=ExampleEntity".to_string(),
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

    #[test]
    fn sql_query_result_from_statement_preserves_count_entity_and_row_count() {
        let result = sql_query_result_from_statement(
            SqlStatementResult::Count { row_count: 3 },
            "User".to_string(),
        );

        assert_eq!(
            result,
            SqlQueryResult::Count {
                entity: "User".to_string(),
                row_count: 3,
            },
            "public SQL packaging must preserve outward count payload identity",
        );
    }

    #[test]
    fn sql_query_result_from_statement_preserves_projection_text_rows() {
        let result = sql_query_result_from_statement(
            SqlStatementResult::ProjectionText {
                columns: vec!["lower(name)".to_string()],
                rows: vec![vec!["alice".to_string()], vec!["bob".to_string()]],
                row_count: 2,
            },
            "User".to_string(),
        );

        assert_eq!(
            result,
            SqlQueryResult::Projection(SqlQueryRowsOutput {
                entity: "User".to_string(),
                columns: vec!["lower(name)".to_string()],
                rows: vec![vec!["alice".to_string()], vec!["bob".to_string()]],
                row_count: 2,
            }),
            "public SQL packaging must preserve text projection payloads verbatim",
        );
    }

    #[test]
    fn sql_query_result_from_statement_preserves_grouped_rows_and_cursor() {
        let result = sql_query_result_from_statement(
            SqlStatementResult::Grouped {
                columns: vec!["age".to_string(), "count(*)".to_string()],
                rows: vec![
                    GroupedRow::new(vec![Value::Uint(24)], vec![Value::Uint(1)]),
                    GroupedRow::new(vec![Value::Uint(31)], vec![Value::Uint(2)]),
                ],
                row_count: 2,
                next_cursor: Some("cursor:age:31".to_string()),
            },
            "User".to_string(),
        );

        assert_eq!(
            result,
            SqlQueryResult::Grouped(SqlGroupedRowsOutput {
                entity: "User".to_string(),
                columns: vec!["age".to_string(), "count(*)".to_string()],
                rows: vec![
                    vec!["24".to_string(), "1".to_string()],
                    vec!["31".to_string(), "2".to_string()],
                ],
                row_count: 2,
                next_cursor: Some("cursor:age:31".to_string()),
            }),
            "public SQL packaging must preserve grouped rows and outward continuation cursor",
        );
    }
}
