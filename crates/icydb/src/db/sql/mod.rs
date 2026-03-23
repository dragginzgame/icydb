//! Module: db::sql
//!
//! Responsibility: SQL-surface text rendering helpers for canister/CLI-facing endpoints.
//! Does not own: SQL parsing/lowering/execution semantics.
//! Boundary: consumes executed SQL projection/explain outputs and renders stable text payloads.

use candid::CandidType;
use serde::{Deserialize, Serialize};

use crate::{
    db::{
        DbSession, EntityAuthority, EntityFieldDescription, EntitySchemaDescription,
        SqlStatementRoute,
    },
    error::{Error, ErrorKind, ErrorOrigin, QueryErrorKind, RuntimeErrorKind},
    model::entity::EntityModel,
    traits::CanisterKind,
    value::{Value, ValueEnum},
};

///
/// SqlProjectionRows
///
/// Render-ready SQL projection row payload.
/// `columns` and each row vector are positionally aligned.
///

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

///
/// SqlQueryRowsOutput
///
/// Structured SQL projection payload for canister endpoint surfaces.
/// `columns` and each row vector are positionally aligned.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

///
/// SqlQueryResult
///
/// Unified SQL endpoint envelope for one executed statement.
/// Carries projection, explain, describe, index-listing, and entity-listing
/// payloads behind one canister-friendly return type.
///
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

/// Validate and normalize SQL endpoint input.
pub fn normalize_sql_input(sql: &str) -> Result<&str, Error> {
    let sql_trimmed = sql.trim();
    if sql_trimmed.is_empty() {
        return Err(Error::new(
            ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
            ErrorOrigin::Query,
            "query endpoint requires a non-empty SQL string",
        ));
    }

    Ok(sql_trimmed)
}

/// Return the wrapped SQL statement text inside one EXPLAIN surface statement.
///
/// If `sql` does not begin with EXPLAIN, this returns the trimmed input.
#[must_use]
pub fn explain_target_sql(sql: &str) -> &str {
    let mut rest = sql.trim_start();
    if let Some(next) = consume_keyword(rest, "EXPLAIN") {
        rest = next;
        if let Some(next) = consume_keyword(rest, "EXECUTION") {
            rest = next;
        } else if let Some(next) = consume_keyword(rest, "JSON") {
            rest = next;
        }
    }

    rest.trim_start()
}

/// Return whether two entity identifiers refer to the same logical entity.
///
/// Matching is case-insensitive and accepts one schema-qualified side by
/// comparing the final identifier segment (for example `public.User` vs `User`).
#[must_use]
pub fn identifiers_tail_match(left: &str, right: &str) -> bool {
    crate::db::identifiers_tail_match(left, right)
}

/// Build one stable list of SQL entity names from structural authority.
#[doc(hidden)]
#[must_use]
pub fn generated_sql_entities(authorities: &[EntityAuthority]) -> Vec<String> {
    authorities
        .iter()
        .map(|authority| authority.model().name().to_string())
        .collect()
}

/// Execute one generated canister SQL dispatch entrypoint from structural authority.
#[doc(hidden)]
pub fn execute_generated_sql_dispatch<C: CanisterKind>(
    session: &DbSession<C>,
    sql: &str,
    authorities: &[EntityAuthority],
) -> Result<SqlQueryResult, Error> {
    // Phase 1: normalize and parse the incoming SQL statement once.
    let sql_trimmed = normalize_sql_input(sql)?;
    let parsed = session.parse_sql_statement(sql_trimmed)?;
    let statement = parsed.route();

    // Phase 2: route the parsed statement through one shared structural helper.
    match statement {
        SqlStatementRoute::Query { .. } | SqlStatementRoute::Explain { .. } => {
            query_lane_result_for_statement(session, sql_trimmed, &parsed, statement, authorities)
        }
        SqlStatementRoute::Describe { .. } => {
            describe_result_for_statement(session, statement, authorities)
        }
        SqlStatementRoute::ShowIndexes { .. } => {
            show_indexes_result_for_statement(session, statement, authorities)
        }
        SqlStatementRoute::ShowColumns { .. } => {
            show_columns_result_for_statement(session, statement, authorities)
        }
        SqlStatementRoute::ShowEntities => Ok(show_entities_result_for_statement(session)),
    }
}

// Resolve one structural authority from an entity-scoped SQL statement.
fn authority_for_statement(
    statement: &SqlStatementRoute,
    authorities: &[EntityAuthority],
) -> Result<EntityAuthority, Error> {
    if statement.is_show_entities() {
        return Err(unsupported_entity_route_statement_error());
    }

    let sql_entity = statement.entity();
    authority_for_entity_name(sql_entity, authorities)
        .ok_or_else(|| unsupported_sql_entity_error(sql_entity, authorities))
}

// Resolve one structural authority from one SQL entity identifier.
fn authority_for_entity_name(
    entity_name: &str,
    authorities: &[EntityAuthority],
) -> Option<EntityAuthority> {
    authorities
        .iter()
        .copied()
        .find(|authority| identifiers_tail_match(entity_name, authority.model().name()))
}

// Execute one shared SQL query/explain lane after routing authority structurally.
fn query_lane_result_for_statement<C: CanisterKind>(
    session: &DbSession<C>,
    sql: &str,
    parsed: &crate::db::SqlParsedStatement,
    statement: &SqlStatementRoute,
    authorities: &[EntityAuthority],
) -> Result<SqlQueryResult, Error> {
    let authority = authority_for_statement(statement, authorities)?;
    let prepared = session.prepare_sql_dispatch_parsed(parsed, authority.model().name())?;
    let lowered = session.lower_sql_dispatch_query_lane_prepared(
        &prepared,
        authority.model().primary_key().name(),
    )?;
    let result = if statement.is_explain() {
        session.explain_lowered_sql_dispatch_for_model(&lowered, authority.model())
    } else {
        session.execute_lowered_sql_dispatch_query_for_authority(&lowered, authority)
    };

    if matches!(statement, SqlStatementRoute::Explain { .. }) {
        return result.map_err(|err| explain_surface_error(sql, authority.model(), err));
    }

    result
}

// Render one DESCRIBE result through one shared structural authority path.
fn describe_result_for_statement<C: CanisterKind>(
    session: &DbSession<C>,
    statement: &SqlStatementRoute,
    authorities: &[EntityAuthority],
) -> Result<SqlQueryResult, Error> {
    let authority = authority_for_statement(statement, authorities)?;
    let description = session.describe_entity_model(authority.model());

    Ok(SqlQueryResult::Describe(description))
}

// Render one SHOW INDEXES result through one shared structural authority path.
fn show_indexes_result_for_statement<C: CanisterKind>(
    session: &DbSession<C>,
    statement: &SqlStatementRoute,
    authorities: &[EntityAuthority],
) -> Result<SqlQueryResult, Error> {
    let authority = authority_for_statement(statement, authorities)?;
    let indexes = session.show_indexes_for_model(authority.model());

    Ok(SqlQueryResult::ShowIndexes {
        entity: authority.model().name().to_string(),
        indexes,
    })
}

// Render one SHOW COLUMNS result through one shared structural authority path.
fn show_columns_result_for_statement<C: CanisterKind>(
    session: &DbSession<C>,
    statement: &SqlStatementRoute,
    authorities: &[EntityAuthority],
) -> Result<SqlQueryResult, Error> {
    let authority = authority_for_statement(statement, authorities)?;
    let columns = session.show_columns_for_model(authority.model());

    Ok(SqlQueryResult::ShowColumns {
        entity: authority.model().name().to_string(),
        columns,
    })
}

// Render one SHOW ENTITIES result without per-entity authority resolution.
fn show_entities_result_for_statement<C: CanisterKind>(session: &DbSession<C>) -> SqlQueryResult {
    let entities = session.show_entities();

    SqlQueryResult::ShowEntities { entities }
}

// Build one stable unsupported-entity error using the generated authority table.
fn unsupported_sql_entity_error(entity_name: &str, authorities: &[EntityAuthority]) -> Error {
    let supported = generated_sql_entities(authorities).join(", ");

    Error::new(
        ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        ErrorOrigin::Query,
        format!("query endpoint does not support entity '{entity_name}'; supported: {supported}"),
    )
}

// Reject route resolution for non-entity-scoped statements.
fn unsupported_entity_route_statement_error() -> Error {
    Error::new(
        ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        ErrorOrigin::Query,
        "entity route resolution requires one entity-scoped SQL statement",
    )
}

// Rewrite unordered-pagination EXPLAIN failures into one actionable canister message.
fn explain_surface_error(sql: &str, model: &'static EntityModel, err: Error) -> Error {
    if !matches!(
        err.kind(),
        ErrorKind::Query(QueryErrorKind::UnorderedPagination)
    ) {
        return err;
    }

    let target_sql = explain_target_sql(sql);
    let suggestion = explain_order_hint_sql(target_sql, model.primary_key().name());
    let message = format!(
        "Cannot EXPLAIN this SQL statement.\n\nReason:\nThe wrapped query uses LIMIT or OFFSET without ORDER BY, so it is non-deterministic and not executable under IcyDB's ordering contract.\n\nSQL:\n{target_sql}\n\nHow to fix:\nAdd an explicit ORDER BY that produces a stable total order, for example:\n{suggestion}",
    );

    Error::new(
        ErrorKind::Query(QueryErrorKind::UnorderedPagination),
        err.origin(),
        message,
    )
}

// Suggest one stable ORDER BY rewrite for EXPLAIN pagination failures.
fn explain_order_hint_sql(target_sql: &str, order_field: &str) -> String {
    let trimmed = target_sql.trim().trim_end_matches(';').trim_end();
    let upper = trimmed.to_ascii_uppercase();

    if let Some(index) = upper.find(" LIMIT ") {
        return format!(
            "EXPLAIN {} ORDER BY {order_field} ASC{}",
            &trimmed[..index],
            &trimmed[index..]
        );
    }

    if let Some(index) = upper.find(" OFFSET ") {
        return format!(
            "EXPLAIN {} ORDER BY {order_field} ASC{}",
            &trimmed[..index],
            &trimmed[index..]
        );
    }

    format!("EXPLAIN {trimmed} ORDER BY {order_field} ASC")
}

/// Render one value into a shell-friendly stable text form.
#[must_use]
pub fn render_value_text(value: &Value) -> String {
    match value {
        Value::Account(v) => v.to_string(),
        Value::Blob(v) => format!("0x{}", hex_encode(v)),
        Value::Bool(v) => v.to_string(),
        Value::Date(v) => v.to_string(),
        Value::Decimal(v) => v.to_string(),
        Value::Duration(v) => format!("{}ms", v.as_millis()),
        Value::Enum(v) => render_enum(v),
        Value::Float32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Int(v) => v.to_string(),
        Value::Int128(v) => v.to_string(),
        Value::IntBig(v) => v.to_string(),
        Value::List(items) => {
            let rendered = items
                .iter()
                .map(render_value_text)
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{rendered}]")
        }
        Value::Map(entries) => {
            let rendered = entries
                .iter()
                .map(|(key, value)| {
                    format!("{}: {}", render_value_text(key), render_value_text(value))
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{rendered}}}")
        }
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

/// Build one rendered projection row payload from one already-materialized SQL
/// value grid.
#[must_use]
pub fn projection_rows_from_values(
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    row_count: u32,
) -> SqlProjectionRows {
    // Phase 1: render each projected row cell into stable text.
    let mut rendered_rows = Vec::new();
    let mut max_column_count = 0usize;

    for row in rows {
        let rendered_row = row.iter().map(render_value_text).collect::<Vec<_>>();
        max_column_count = max_column_count.max(rendered_row.len());
        rendered_rows.push(rendered_row);
    }

    // Phase 2: derive stable projection column labels from canonical core metadata.
    let columns = if max_column_count == 0 || columns.len() == max_column_count {
        columns
    } else {
        projection_columns(max_column_count)
    };

    SqlProjectionRows::new(columns, rendered_rows, row_count)
}

/// Render one SQL EXPLAIN text payload as endpoint output lines.
#[must_use]
pub fn render_explain_lines(explain: &str) -> Vec<String> {
    let mut lines = vec!["surface=explain".to_string()];
    lines.extend(explain.lines().map(ToString::to_string));

    lines
}

/// Render one typed `DESCRIBE` payload into deterministic shell output lines.
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

/// Render one `SHOW INDEXES` payload into deterministic shell output lines.
#[must_use]
pub fn render_show_indexes_lines(entity: &str, indexes: &[String]) -> Vec<String> {
    let mut lines = vec![format!(
        "surface=indexes entity={entity} index_count={}",
        indexes.len()
    )];
    lines.extend(indexes.iter().cloned());

    lines
}

/// Render one `SHOW COLUMNS` payload into deterministic shell output lines.
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

/// Render one helper-level `SHOW ENTITIES` payload into deterministic lines.
#[must_use]
pub fn render_show_entities_lines(entities: &[String]) -> Vec<String> {
    let mut lines = vec!["surface=entities".to_string()];
    lines.extend(entities.iter().map(|entity| format!("entity={entity}")));

    lines
}

/// Render one SQL projection payload into pretty table lines for shell output.
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

fn projection_columns(column_count: usize) -> Vec<String> {
    (0..column_count)
        .map(|index| format!("col_{index}"))
        .collect()
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

// Consume one keyword at the start of `input` (after optional leading whitespace).
fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if !starts_with_keyword(trimmed, keyword) {
        return None;
    }

    Some(&trimmed[keyword.len()..])
}

// Return whether `input` begins with one keyword token.
fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    let bytes = input.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    is_keyword_at(bytes, 0, keyword_bytes) && keyword_boundary_after(bytes, keyword_bytes.len())
}

// Return whether one keyword byte sequence matches at `index` (ASCII case-insensitive).
fn is_keyword_at(bytes: &[u8], index: usize, keyword: &[u8]) -> bool {
    let Some(window) = bytes.get(index..index.saturating_add(keyword.len())) else {
        return false;
    };

    window
        .iter()
        .zip(keyword.iter())
        .all(|(found, expected)| found.eq_ignore_ascii_case(expected))
}

// Return whether one keyword end index is token-delimited.
fn keyword_boundary_after(bytes: &[u8], end: usize) -> bool {
    bytes
        .get(end)
        .is_none_or(|byte| !is_identifier_continue(*byte))
}

// Return whether one byte may continue an unquoted SQL identifier segment.
const fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::sql::{
        SqlQueryResult, SqlQueryRowsOutput, explain_target_sql, identifiers_tail_match,
        render_describe_lines, render_show_columns_lines, render_show_entities_lines,
        render_show_indexes_lines,
    };
    use crate::db::{
        EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
        EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
    };

    #[test]
    fn explain_target_sql_strips_explain_wrappers() {
        assert_eq!(
            explain_target_sql("EXPLAIN SELECT * FROM FixtureOrder LIMIT 1"),
            "SELECT * FROM FixtureOrder LIMIT 1"
        );
        assert_eq!(
            explain_target_sql("EXPLAIN EXECUTION SELECT * FROM FixtureOrder LIMIT 1"),
            "SELECT * FROM FixtureOrder LIMIT 1"
        );
        assert_eq!(
            explain_target_sql("EXPLAIN JSON SELECT * FROM FixtureOrder LIMIT 1"),
            "SELECT * FROM FixtureOrder LIMIT 1"
        );
        assert_eq!(
            explain_target_sql("  SELECT * FROM FixtureOrder LIMIT 1"),
            "SELECT * FROM FixtureOrder LIMIT 1"
        );
    }

    #[test]
    fn identifiers_tail_match_accepts_schema_qualified_forms() {
        assert!(identifiers_tail_match("public.FixtureUser", "FixtureUser"));
        assert!(identifiers_tail_match("fixtureorder", "FixtureOrder"));
        assert!(!identifiers_tail_match("FixtureUser", "FixtureOrder"));
    }

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
