use crate::db::{
    EntityFieldDescription, EntitySchemaDescription,
    sql::{SqlGroupedRowsOutput, SqlProjectionRows, SqlQueryRowsOutput},
};

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

    // Phase 2: emit field descriptors in stable model order using the same
    // padded ASCII table shape as shell query results.
    lines.push(String::new());
    lines.push("fields:".to_string());
    render_describe_field_section(&mut lines, description.fields());

    // Phase 3: emit index descriptors or explicit empty marker.
    lines.push(String::new());
    if description.indexes().is_empty() {
        lines.push("indexes: []".to_string());
    } else {
        lines.push("indexes:".to_string());
        let index_rows = description
            .indexes()
            .iter()
            .map(|index| {
                vec![
                    index.name().to_string(),
                    index.fields().join(", "),
                    if index.unique() {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    },
                ]
            })
            .collect::<Vec<_>>();
        render_describe_table_section(
            &mut lines,
            &[
                "name".to_string(),
                "fields".to_string(),
                "unique".to_string(),
            ],
            &index_rows,
        );
    }

    // Phase 4: emit relation descriptors or explicit empty marker.
    lines.push(String::new());
    if description.relations().is_empty() {
        lines.push("relations: []".to_string());
    } else {
        lines.push("relations:".to_string());
        let relation_rows = description
            .relations()
            .iter()
            .map(|relation| {
                vec![
                    relation.field().to_string(),
                    relation.target_entity_name().to_string(),
                    format!("{:?}", relation.strength()),
                    format!("{:?}", relation.cardinality()),
                ]
            })
            .collect::<Vec<_>>();
        render_describe_table_section(
            &mut lines,
            &[
                "field".to_string(),
                "target".to_string(),
                "strength".to_string(),
                "cardinality".to_string(),
            ],
            &relation_rows,
        );
    }

    lines
}

// Render the shared field table used by both full `DESCRIBE` output and the
// narrower `SHOW COLUMNS` surface. Keeping the table logic here prevents the
// two shell commands from drifting into different human-facing formats.
fn render_describe_field_section(lines: &mut Vec<String>, fields: &[EntityFieldDescription]) {
    let field_rows = fields
        .iter()
        .map(|field| {
            vec![
                field.name().to_string(),
                field
                    .slot()
                    .map_or_else(|| "-".to_string(), |slot| slot.to_string()),
                field.kind().to_string(),
                if field.primary_key() {
                    "yes".to_string()
                } else {
                    "no".to_string()
                },
                if field.queryable() {
                    "yes".to_string()
                } else {
                    "no".to_string()
                },
            ]
        })
        .collect::<Vec<_>>();
    render_describe_table_section(
        lines,
        &[
            "name".to_string(),
            "slot".to_string(),
            "type".to_string(),
            "pk".to_string(),
            "queryable".to_string(),
        ],
        &field_rows,
    );
}

// Render one `DESCRIBE` subsection as the same deterministic ASCII table shape
// used by shell-facing projection output.
fn render_describe_table_section(
    lines: &mut Vec<String>,
    headers: &[String],
    rows: &[Vec<String>],
) {
    let mut widths = headers.iter().map(String::len).collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }

    let separator = render_table_separator(widths.as_slice());
    lines.push(separator.clone());
    lines.push(render_table_row(headers, widths.as_slice()));
    lines.push(separator.clone());
    for row in rows {
        lines.push(render_table_row(row.as_slice(), widths.as_slice()));
    }
    if !rows.is_empty() {
        lines.push(separator);
    }
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

#[cfg_attr(doc, doc = "Render one SQL DDL payload into deterministic lines.")]
#[must_use]
pub fn render_sql_ddl_lines(
    entity: &str,
    mutation_kind: &str,
    target_index: &str,
    target_store: &str,
    field_path: &[String],
    status: &str,
) -> Vec<String> {
    vec![format!(
        "surface=ddl entity={entity} mutation_kind={mutation_kind} target_index={target_index} target_store={target_store} field_path={} status={status}",
        field_path.join("."),
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
    let mut lines = vec![
        format!("entity: {entity}"),
        String::new(),
        "fields:".to_string(),
    ];
    render_describe_field_section(&mut lines, columns);

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
pub fn render_projection_lines(_entity: &str, projection: &SqlProjectionRows) -> Vec<String> {
    render_projection_table(
        projection.columns(),
        projection.rows(),
        projection.row_count(),
    )
}

#[must_use]
pub(in crate::db::sql) fn render_query_rows_lines(projection: &SqlQueryRowsOutput) -> Vec<String> {
    render_projection_table(
        projection.columns.as_slice(),
        projection.rows.as_slice(),
        projection.row_count,
    )
}

fn render_projection_table(
    columns: &[String],
    rows: &[Vec<String>],
    row_count: u32,
) -> Vec<String> {
    // Phase 1: handle empty-projection output before table layout.
    let mut lines = Vec::new();
    if columns.is_empty() {
        lines.push("(no projected columns)".to_string());
        return lines;
    }

    // Phase 2: compute per-column display widths from headers + row values.
    let mut widths = columns.iter().map(String::len).collect::<Vec<_>>();
    for row in rows {
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
    lines.push(render_table_row(columns, widths.as_slice()));
    lines.push(separator.clone());
    for row in rows {
        lines.push(render_table_row(row.as_slice(), widths.as_slice()));
    }
    if !rows.is_empty() {
        lines.push(separator);
    }
    lines.push(String::new());
    lines.push(render_result_row_count_line(row_count));

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one grouped SQL payload into pretty table lines for shell output."
)]
#[must_use]
pub fn render_grouped_lines(grouped: &SqlGroupedRowsOutput) -> Vec<String> {
    // Phase 1: expose the outward continuation cursor on its own line when
    // grouped pagination has more rows.
    let mut lines = Vec::new();
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
    if !grouped.rows.is_empty() {
        lines.push(separator);
    }
    lines.push(String::new());
    lines.push(render_result_row_count_line(grouped.row_count));

    lines
}

fn render_result_row_count_line(row_count: u32) -> String {
    let noun = if row_count == 1 { "row" } else { "rows" };
    format!("{} {noun},", render_grouped_decimal_u32(row_count))
}

// Render one `u32` with ASCII thousands separators so shell row-count footers
// remain easy to scan on large result sets.
fn render_grouped_decimal_u32(value: u32) -> String {
    let digits = value.to_string();
    let mut rendered = String::with_capacity(digits.len().saturating_add(digits.len() / 3));
    let leading_group_len = digits.len().rem_euclid(3);

    for (index, ch) in digits.chars().enumerate() {
        if index > 0
            && (index == leading_group_len
                || (index > leading_group_len && (index - leading_group_len).rem_euclid(3) == 0))
        {
            rendered.push(',');
        }
        rendered.push(ch);
    }

    rendered
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
