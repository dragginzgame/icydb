//! Module: db::sql::table_render
//!
//! Responsibility: public SQL result and rendering facade.
//! Does not own: SQL parsing, lowering, planning, or execution.
//! Boundary: converts executed core SQL outputs into endpoint-friendly payloads.

use crate::ConstraintDiagnostic;
use crate::db::{
    EntityCatalogDescription, EntityConstraintDescription, EntityFieldDescription,
    EntitySchemaDescription, MemoryCatalogDescription, StoreCatalogDescription,
    response::RowProjectionOutput,
    sql::{SqlGroupedRowsOutput, value_render::render_projection_rows},
};
use std::fmt::Write as _;

#[cfg_attr(
    doc,
    doc = "Render one SQL EXPLAIN text payload as endpoint output lines."
)]
#[cfg(feature = "sql-explain")]
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
    lines.push(format!(
        "row layout: current={} history_floor={}",
        description.row_layout_current(),
        description.row_layout_history_floor(),
    ));

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
                    index.origin().to_string(),
                ]
            })
            .collect::<Vec<_>>();
        render_table_section(
            &mut lines,
            &[
                "name".to_string(),
                "fields".to_string(),
                "unique".to_string(),
                "origin".to_string(),
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
                    format!("{:?}", relation.cardinality()),
                ]
            })
            .collect::<Vec<_>>();
        render_table_section(
            &mut lines,
            &[
                "field".to_string(),
                "target".to_string(),
                "cardinality".to_string(),
            ],
            &relation_rows,
        );
    }

    // Phase 5: emit the accepted constraint registry. Structural semantics are
    // rendered from the catalog entry while fields/indexes/relations remain
    // the execution owners referenced by each row.
    lines.push(String::new());
    render_describe_constraint_section(&mut lines, description.constraints());

    lines
}

// Render accepted constraint identity without owning or reconstructing the
// field, index, or relation semantics referenced by each catalog row.
fn render_describe_constraint_section(
    lines: &mut Vec<String>,
    constraints: &[EntityConstraintDescription],
) {
    if constraints.is_empty() {
        lines.push("constraints: []".to_string());
    } else {
        lines.push("constraints:".to_string());
        let constraint_rows = constraints
            .iter()
            .map(|constraint| {
                vec![
                    constraint.id().to_string(),
                    constraint.name().to_string(),
                    constraint.kind().to_string(),
                    constraint.fields().join(", "),
                    constraint.origin().to_string(),
                    constraint.validation_state().to_string(),
                    constraint
                        .validation_progress()
                        .map_or_else(|| "-".to_string(), |progress| progress.phase().to_string()),
                    constraint.validation_progress().map_or_else(
                        || "-".to_string(),
                        |progress| progress.rows_scanned().to_string(),
                    ),
                    constraint.validation_progress().map_or_else(
                        || "-".to_string(),
                        |progress| progress.findings_seen().to_string(),
                    ),
                    constraint.validation_progress().map_or_else(
                        || "-".to_string(),
                        |progress| progress.restarts().to_string(),
                    ),
                    constraint.semantics().to_string(),
                    constraint.check_sql().unwrap_or("-").to_string(),
                ]
            })
            .collect::<Vec<_>>();
        render_table_section(
            lines,
            &[
                "id".to_string(),
                "name".to_string(),
                "kind".to_string(),
                "fields".to_string(),
                "origin".to_string(),
                "state".to_string(),
                "phase".to_string(),
                "rows_scanned".to_string(),
                "findings".to_string(),
                "restarts".to_string(),
                "semantics".to_string(),
                "check_sql".to_string(),
            ],
            &constraint_rows,
        );
    }
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
                if field.nullable() {
                    "yes".to_string()
                } else {
                    "no".to_string()
                },
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
                field.origin().to_string(),
                field.insert_omission().unwrap_or("-").to_string(),
                field.insert_default().unwrap_or("-").to_string(),
                field
                    .insert_default_bytes()
                    .map_or_else(|| "-".to_string(), |bytes| bytes.to_string()),
                field.insert_default_hash().unwrap_or("-").to_string(),
                field
                    .introduced_in_layout()
                    .map_or_else(|| "-".to_string(), |layout| layout.to_string()),
                field.historical_fill().unwrap_or("-").to_string(),
                field
                    .historical_fill_bytes()
                    .map_or_else(|| "-".to_string(), |bytes| bytes.to_string()),
                field.historical_fill_hash().unwrap_or("-").to_string(),
            ]
        })
        .collect::<Vec<_>>();
    render_table_section(
        lines,
        &[
            "name".to_string(),
            "slot".to_string(),
            "type".to_string(),
            "nullable".to_string(),
            "pk".to_string(),
            "queryable".to_string(),
            "origin".to_string(),
            "insert omission".to_string(),
            "insert default".to_string(),
            "default bytes".to_string(),
            "default hash".to_string(),
            "introduced layout".to_string(),
            "historical fill".to_string(),
            "fill bytes".to_string(),
            "fill hash".to_string(),
        ],
        &field_rows,
    );
}

// Render one subsection using the deterministic ASCII table shape shared by
// DESCRIBE, SHOW, projection, and grouped output.
fn render_table_section(lines: &mut Vec<String>, headers: &[String], rows: &[Vec<String>]) {
    let widths = render_table_widths(headers, rows);
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
pub(in crate::db::sql) fn render_sql_ddl_lines(input: SqlDdlRenderInput<'_>) -> Vec<String> {
    vec![format!(
        "surface=ddl entity={} mutation_kind={} target_index={} target_store={} field_path={} status={} rows_scanned={} index_keys_written={}",
        input.entity,
        input.mutation_kind,
        input.target_index,
        input.target_store,
        input.field_path.join("."),
        input.status,
        input.rows_scanned,
        input.index_keys_written,
    )]
}

/// Render one typed constraint finding without reopening schema authority.
pub(in crate::db::sql) fn render_constraint_diagnostic_line(
    diagnostic: &ConstraintDiagnostic,
) -> String {
    let primary_key = diagnostic
        .primary_key()
        .map_or_else(|| "-".to_string(), render_hex_bytes);

    format!(
        "constraint_finding id={} name={} kind={} entity={} primary_key={} fields={} context={} class={} code=E{}",
        diagnostic.constraint_id(),
        diagnostic.constraint_name(),
        diagnostic.constraint_kind().as_str(),
        diagnostic.entity(),
        primary_key,
        diagnostic.field_paths().join(","),
        diagnostic.context().as_str(),
        error_class_label(diagnostic.error_class()),
        diagnostic.error_code().raw(),
    )
}

fn render_hex_bytes(bytes: &[u8]) -> String {
    let mut rendered = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(rendered, "{byte:02x}");
    }
    rendered
}

const fn error_class_label(class: icydb_diagnostic_code::ErrorClass) -> &'static str {
    match class {
        icydb_diagnostic_code::ErrorClass::Conflict => "conflict",
        icydb_diagnostic_code::ErrorClass::Corruption => "corruption",
        icydb_diagnostic_code::ErrorClass::IncompatiblePersistedFormat => {
            "incompatible_persisted_format"
        }
        icydb_diagnostic_code::ErrorClass::Internal => "internal",
        icydb_diagnostic_code::ErrorClass::InvariantViolation => "invariant_violation",
        icydb_diagnostic_code::ErrorClass::NotFound => "not_found",
        icydb_diagnostic_code::ErrorClass::Query => "query",
        icydb_diagnostic_code::ErrorClass::Unsupported => "unsupported",
    }
}

pub(in crate::db::sql) struct SqlDdlRenderInput<'a> {
    pub(in crate::db::sql) entity: &'a str,
    pub(in crate::db::sql) mutation_kind: &'a str,
    pub(in crate::db::sql) target_index: &'a str,
    pub(in crate::db::sql) target_store: &'a str,
    pub(in crate::db::sql) field_path: &'a [String],
    pub(in crate::db::sql) status: &'a str,
    pub(in crate::db::sql) rows_scanned: u64,
    pub(in crate::db::sql) index_keys_written: u64,
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
    doc = "Render one `SHOW CONSTRAINTS` payload into deterministic shell output lines."
)]
#[must_use]
pub fn render_show_constraints_lines(
    entity: &str,
    constraints: &[EntityConstraintDescription],
) -> Vec<String> {
    let rows = constraints
        .iter()
        .map(|constraint| {
            vec![
                constraint.id().to_string(),
                constraint.name().to_string(),
                constraint.kind().to_string(),
                constraint.fields().join(", "),
                constraint.origin().to_string(),
                constraint.validation_state().to_string(),
                constraint
                    .validation_progress()
                    .map_or_else(|| "-".to_string(), |progress| progress.phase().to_string()),
                constraint.validation_progress().map_or_else(
                    || "-".to_string(),
                    |progress| progress.rows_scanned().to_string(),
                ),
                constraint.validation_progress().map_or_else(
                    || "-".to_string(),
                    |progress| progress.findings_seen().to_string(),
                ),
                constraint.validation_progress().map_or_else(
                    || "-".to_string(),
                    |progress| progress.restarts().to_string(),
                ),
                constraint.semantics().to_string(),
                constraint.check_sql().unwrap_or("-").to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let mut lines = vec![format!("entity: {entity}"), String::new()];
    if constraints.is_empty() {
        lines.push("constraints: []".to_string());
        return lines;
    }

    lines.push("constraints:".to_string());
    render_table_section(
        &mut lines,
        &[
            "id".to_string(),
            "name".to_string(),
            "kind".to_string(),
            "fields".to_string(),
            "origin".to_string(),
            "state".to_string(),
            "phase".to_string(),
            "rows_scanned".to_string(),
            "findings".to_string(),
            "restarts".to_string(),
            "semantics".to_string(),
            "check_sql".to_string(),
        ],
        rows.as_slice(),
    );
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
pub fn render_show_entities_lines(entities: &[EntityCatalogDescription]) -> Vec<String> {
    let rows = entities
        .iter()
        .map(|entity| {
            vec![
                entity.entity_name().to_string(),
                render_catalog_path_tail(entity.store_path()).to_string(),
                entity.storage().to_string(),
                entity.columns().to_string(),
                entity.indexes().to_string(),
                entity.relations().to_string(),
                entity.schema_version().to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let mut lines = Vec::new();
    render_table_section(
        &mut lines,
        &[
            "name".to_string(),
            "store".to_string(),
            "storage".to_string(),
            "cols".to_string(),
            "indexes".to_string(),
            "relations".to_string(),
            "sv".to_string(),
        ],
        rows.as_slice(),
    );
    lines.push(String::new());
    lines.push(render_result_entity_count_line(entities.len()));

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one verbose `SHOW ENTITIES` payload with full catalog paths."
)]
#[must_use]
pub fn render_show_entities_verbose_lines(entities: &[EntityCatalogDescription]) -> Vec<String> {
    let rows = entities
        .iter()
        .map(|entity| {
            vec![
                entity.entity_name().to_string(),
                entity.entity_path().to_string(),
                entity.store_path().to_string(),
                entity.storage().to_string(),
                entity.columns().to_string(),
                entity.indexes().to_string(),
                entity.relations().to_string(),
                entity.schema_version().to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let mut lines = Vec::new();
    render_table_section(
        &mut lines,
        &[
            "name".to_string(),
            "path".to_string(),
            "store".to_string(),
            "storage".to_string(),
            "cols".to_string(),
            "indexes".to_string(),
            "relations".to_string(),
            "sv".to_string(),
        ],
        rows.as_slice(),
    );
    lines.push(String::new());
    lines.push(render_result_entity_count_line(entities.len()));

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one helper-level `SHOW STORES` payload into deterministic lines."
)]
#[must_use]
pub fn render_show_stores_lines(stores: &[StoreCatalogDescription]) -> Vec<String> {
    let rows = stores
        .iter()
        .map(|store| {
            vec![
                render_catalog_path_tail(store.store_path()).to_string(),
                store.storage().to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let mut lines = Vec::new();
    render_table_section(
        &mut lines,
        &["store".to_string(), "storage".to_string()],
        rows.as_slice(),
    );
    lines.push(String::new());
    lines.push(render_result_store_count_line(stores.len()));

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one verbose `SHOW STORES` payload with full catalog paths."
)]
#[must_use]
pub fn render_show_stores_verbose_lines(stores: &[StoreCatalogDescription]) -> Vec<String> {
    let rows = stores
        .iter()
        .map(|store| vec![store.store_path().to_string(), store.storage().to_string()])
        .collect::<Vec<_>>();
    let mut lines = Vec::new();
    render_table_section(
        &mut lines,
        &["path".to_string(), "storage".to_string()],
        rows.as_slice(),
    );
    lines.push(String::new());
    lines.push(render_result_store_count_line(stores.len()));

    lines
}

#[cfg_attr(
    doc,
    doc = "Render one helper-level `SHOW MEMORY` payload into deterministic lines."
)]
#[must_use]
pub fn render_show_memory_lines(memory: &[MemoryCatalogDescription]) -> Vec<String> {
    let rows = memory
        .iter()
        .map(|entry| {
            vec![
                entry.tag().to_string(),
                entry.memory_id().to_string(),
                render_catalog_path_tail(entry.store_path()).to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let mut lines = Vec::new();
    render_table_section(
        &mut lines,
        &[
            "tag".to_string(),
            "memory_id".to_string(),
            "store".to_string(),
        ],
        rows.as_slice(),
    );
    lines.push(String::new());
    lines.push(render_result_memory_count_line(memory.len()));

    lines
}

fn render_catalog_path_tail(path: &str) -> &str {
    path.rsplit_once("::").map_or_else(
        || path.rsplit_once('.').map_or(path, |(_, tail)| tail),
        |(_, tail)| tail,
    )
}

#[must_use]
pub(in crate::db::sql) fn render_query_rows_lines(projection: &RowProjectionOutput) -> Vec<String> {
    let rows = render_projection_rows(projection.rows.as_slice());

    render_projection_display_rows_lines(
        projection.columns.as_slice(),
        rows.as_slice(),
        projection.row_count,
    )
}

/// Render one SQL projection payload whose values were already converted to
/// display text by the caller.
#[must_use]
pub fn render_projection_display_rows_lines(
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

    // Phase 2: render deterministic ASCII table surface.
    render_table_section(&mut lines, columns, rows);
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

    // Phase 2: render the grouped page as the same deterministic ASCII table
    // shape used by projection payloads.
    render_table_section(
        &mut lines,
        grouped.columns.as_slice(),
        grouped.rows.as_slice(),
    );
    lines.push(String::new());
    lines.push(render_result_row_count_line(grouped.row_count));

    lines
}

fn render_result_row_count_line(row_count: u32) -> String {
    let noun = if row_count == 1 { "row" } else { "rows" };
    format!(
        "{} {noun},",
        render_grouped_decimal_usize(row_count as usize)
    )
}

fn render_result_entity_count_line(entity_count: usize) -> String {
    let noun = if entity_count == 1 {
        "entity"
    } else {
        "entities"
    };
    format!("{} {noun},", render_grouped_decimal_usize(entity_count))
}

fn render_result_store_count_line(store_count: usize) -> String {
    let noun = if store_count == 1 { "store" } else { "stores" };
    format!("{} {noun},", render_grouped_decimal_usize(store_count))
}

fn render_result_memory_count_line(memory_count: usize) -> String {
    let noun = if memory_count == 1 {
        "memory"
    } else {
        "memories"
    };
    format!("{} {noun},", render_grouped_decimal_usize(memory_count))
}

// Render one count with ASCII thousands separators so shell count footers
// remain easy to scan on large result sets.
fn render_grouped_decimal_usize(value: usize) -> String {
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

fn render_table_widths(headers: &[String], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths = headers.iter().map(String::len).collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            if index >= widths.len() {
                widths.push(value.len());
            } else {
                widths[index] = widths[index].max(value.len());
            }
        }
    }

    widths
}

fn render_table_separator(widths: &[usize]) -> String {
    let segments = widths
        .iter()
        .map(|width| "-".repeat(width.saturating_add(2)))
        .collect::<Vec<_>>();

    format!("+{}+", segments.join("+"))
}

fn render_table_row(cells: &[String], widths: &[usize]) -> String {
    let mut padded_cells = Vec::with_capacity(widths.len());
    for (index, width) in widths.iter().copied().enumerate() {
        let value = cells.get(index).map_or("", String::as_str);
        padded_cells.push(format!("{value:<width$}"));
    }

    format!("| {} |", padded_cells.join(" | "))
}
