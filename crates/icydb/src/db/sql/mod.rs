//! Module: db::sql
//!
//! Responsibility: SQL-surface text rendering helpers for canister/CLI-facing endpoints.
//! Does not own: SQL parsing/lowering/execution semantics.
//! Boundary: consumes executed SQL projection/explain outputs and renders stable text payloads.

use candid::CandidType;
use serde::{Deserialize, Serialize};

use crate::{
    db::ProjectionResponse,
    error::{Error, ErrorKind, ErrorOrigin, RuntimeErrorKind},
    traits::{EntityKind, EntitySchema},
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

/// Return whether the input SQL statement is an EXPLAIN surface statement.
#[must_use]
pub fn is_explain_sql(sql: &str) -> bool {
    sql.to_ascii_uppercase().starts_with("EXPLAIN ")
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

/// Extract one entity identifier from a reduced SQL statement.
///
/// Supports `SELECT`, `DELETE`, and `EXPLAIN {PLAN|EXECUTION|JSON}` wrappers.
/// Returns the raw SQL entity token (for example `FixtureUser` or `public.FixtureUser`).
pub fn statement_entity_name(sql: &str) -> Result<String, Error> {
    // Phase 1: normalize optional EXPLAIN prefix.
    let mut rest = sql.trim_start();
    if let Some(next) = consume_keyword(rest, "EXPLAIN") {
        rest = next;
        if let Some(next) = consume_keyword(rest, "EXECUTION") {
            rest = next;
        } else if let Some(next) = consume_keyword(rest, "JSON") {
            rest = next;
        }
    }

    // Phase 2: extract the target entity from DELETE/SELECT statement heads.
    if let Some(next) = consume_keyword(rest, "DELETE") {
        let Some(next) = consume_keyword(next, "FROM") else {
            return Err(unsupported_sql_entity_error(
                "query endpoint expected DELETE FROM <entity>",
            ));
        };
        let Some((entity, _)) = parse_qualified_identifier(next) else {
            return Err(unsupported_sql_entity_error(
                "query endpoint expected DELETE FROM <entity>",
            ));
        };

        return Ok(entity.to_string());
    }

    if let Some(next) = consume_keyword(rest, "SELECT") {
        let Some(from_index) = find_top_level_keyword(next, "FROM") else {
            return Err(unsupported_sql_entity_error(
                "query endpoint expected SELECT ... FROM <entity>",
            ));
        };
        let after_from = &next[from_index + "FROM".len()..];
        let Some((entity, _)) = parse_qualified_identifier(after_from) else {
            return Err(unsupported_sql_entity_error(
                "query endpoint expected SELECT ... FROM <entity>",
            ));
        };

        return Ok(entity.to_string());
    }

    Err(unsupported_sql_entity_error(
        "query endpoint supports SELECT/DELETE statements only",
    ))
}

/// Return whether two entity identifiers refer to the same logical entity.
///
/// Matching is case-insensitive and accepts one schema-qualified side by
/// comparing the final identifier segment (for example `public.User` vs `User`).
#[must_use]
pub fn identifiers_tail_match(left: &str, right: &str) -> bool {
    if left.eq_ignore_ascii_case(right) {
        return true;
    }

    let left_last = identifier_last_segment(left);
    let right_last = identifier_last_segment(right);
    match (left_last, right_last) {
        (Some(lhs), Some(rhs)) => lhs.eq_ignore_ascii_case(rhs),
        _ => false,
    }
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

/// Build one rendered projection row payload from one SQL projection response.
#[must_use]
pub fn projection_rows_from_response<E>(
    sql: &str,
    projection: ProjectionResponse<E>,
) -> SqlProjectionRows
where
    E: EntityKind + EntitySchema,
{
    // Phase 1: render each projected row cell into stable text.
    let row_count = projection.count();
    let mut rows = Vec::new();
    let mut max_column_count = 0usize;

    for row in projection {
        let rendered_row = row
            .values()
            .iter()
            .map(render_value_text)
            .collect::<Vec<_>>();
        max_column_count = max_column_count.max(rendered_row.len());
        rows.push(rendered_row);
    }

    // Phase 2: derive stable projection column labels for the output surface.
    let columns = projection_columns_for_entity::<E>(sql, max_column_count);

    SqlProjectionRows::new(columns, rows, row_count)
}

/// Render one SQL EXPLAIN text payload as endpoint output lines.
#[must_use]
pub fn render_explain_lines(explain: &str) -> Vec<String> {
    let mut lines = vec!["surface=explain".to_string()];
    lines.extend(explain.lines().map(ToString::to_string));

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

fn split_projection_items(projection_sql: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;

    for (index, ch) in projection_sql.char_indices() {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                items.push(projection_sql[start..index].trim().to_string());
                start = index.saturating_add(1);
            }
            _ => {}
        }
    }

    let trailing = projection_sql[start..].trim();
    if !trailing.is_empty() {
        items.push(trailing.to_string());
    }

    items
}

fn projection_clause(sql: &str) -> Option<String> {
    // Parse one reduced SQL `SELECT ... FROM ...` projection segment while
    // respecting nested function parentheses.
    let sql_bytes = sql.as_bytes();
    let upper = sql.to_ascii_uppercase();
    let upper_bytes = upper.as_bytes();
    if !upper_bytes.starts_with(b"SELECT ") {
        return None;
    }

    let mut depth = 0usize;
    let mut from_index = None;
    let mut index = "SELECT ".len();

    while index < upper_bytes.len() {
        match upper_bytes[index] {
            b'(' => depth = depth.saturating_add(1),
            b')' => depth = depth.saturating_sub(1),
            b'F' if depth == 0 => {
                if upper_bytes[index..].starts_with(b"FROM")
                    && (index == 0 || upper_bytes[index - 1].is_ascii_whitespace())
                    && upper_bytes
                        .get(index.saturating_add(4))
                        .is_some_and(u8::is_ascii_whitespace)
                {
                    from_index = Some(index);
                    break;
                }
            }
            _ => {}
        }

        index = index.saturating_add(1);
    }

    let from_index = from_index?;
    let projection = std::str::from_utf8(&sql_bytes["SELECT ".len()..from_index]).ok()?;

    Some(projection.trim().to_string())
}

fn projection_columns_for_entity<E: EntitySchema>(
    sql: &str,
    observed_column_count: usize,
) -> Vec<String> {
    // Phase 1: extract projection clause from reduced SQL.
    let Some(mut projection) = projection_clause(sql) else {
        return projection_columns(observed_column_count);
    };

    // Phase 2: strip optional DISTINCT prefix.
    if projection
        .to_ascii_uppercase()
        .strip_prefix("DISTINCT ")
        .is_some()
    {
        projection = projection["DISTINCT ".len()..].trim_start().to_string();
    }

    // Phase 3: expand SELECT * from entity schema order.
    if projection == "*" {
        return E::MODEL
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
    }

    // Phase 4: normalize explicit projection items to output labels.
    let items = split_projection_items(projection.as_str());
    if items.is_empty() {
        return projection_columns(observed_column_count);
    }

    let columns = items
        .into_iter()
        .map(|item| {
            if item.contains('(') {
                return item;
            }

            item.rsplit('.').next().unwrap_or(item.as_str()).to_string()
        })
        .collect::<Vec<_>>();

    if observed_column_count == 0 || columns.len() == observed_column_count {
        columns
    } else {
        projection_columns(observed_column_count)
    }
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

// Build one stable unsupported-surface error for SQL endpoint helpers.
fn unsupported_sql_entity_error(message: &str) -> Error {
    Error::new(
        ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
        ErrorOrigin::Query,
        message,
    )
}

// Consume one keyword at the start of `input` (after optional leading whitespace).
fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if !starts_with_keyword(trimmed, keyword) {
        return None;
    }

    Some(&trimmed[keyword.len()..])
}

// Parse one dotted identifier (`schema.entity`) and return `(identifier, trailing)`.
fn parse_qualified_identifier(input: &str) -> Option<(&str, &str)> {
    let leading_ws = input.bytes().take_while(u8::is_ascii_whitespace).count();
    let bytes = input.as_bytes();
    let mut cursor = leading_ws;

    cursor = parse_identifier_segment(bytes, cursor)?;
    while bytes.get(cursor).is_some_and(|byte| *byte == b'.') {
        cursor = cursor.saturating_add(1);
        cursor = parse_identifier_segment(bytes, cursor)?;
    }

    Some((&input[leading_ws..cursor], &input[cursor..]))
}

// Return the first top-level keyword position while ignoring parenthesized/string scopes.
fn find_top_level_keyword(input: &str, keyword: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    let mut depth = 0usize;
    let mut index = 0usize;
    let mut in_string = false;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                if in_string
                    && bytes
                        .get(index.saturating_add(1))
                        .is_some_and(|byte| *byte == b'\'')
                {
                    index = index.saturating_add(2);
                    continue;
                }
                in_string = !in_string;
            }
            b'(' if !in_string => depth = depth.saturating_add(1),
            b')' if !in_string => depth = depth.saturating_sub(1),
            _ => {}
        }

        if depth == 0
            && !in_string
            && is_keyword_at(bytes, index, keyword_bytes)
            && keyword_boundary_before(bytes, index)
            && keyword_boundary_after(bytes, index.saturating_add(keyword_bytes.len()))
        {
            return Some(index);
        }

        index = index.saturating_add(1);
    }

    None
}

// Return one final dotted identifier segment.
fn identifier_last_segment(identifier: &str) -> Option<&str> {
    identifier.rsplit('.').next()
}

// Parse one SQL identifier segment and return its exclusive end index.
fn parse_identifier_segment(bytes: &[u8], start: usize) -> Option<usize> {
    let first = *bytes.get(start)?;
    if !is_identifier_start(first) {
        return None;
    }

    let mut end = start.saturating_add(1);
    while bytes
        .get(end)
        .is_some_and(|byte| is_identifier_continue(*byte))
    {
        end = end.saturating_add(1);
    }

    Some(end)
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

// Return whether one keyword start index is token-delimited.
fn keyword_boundary_before(bytes: &[u8], index: usize) -> bool {
    if index == 0 {
        return true;
    }

    bytes
        .get(index.saturating_sub(1))
        .is_none_or(|byte| !is_identifier_continue(*byte))
}

// Return whether one keyword end index is token-delimited.
fn keyword_boundary_after(bytes: &[u8], end: usize) -> bool {
    bytes
        .get(end)
        .is_none_or(|byte| !is_identifier_continue(*byte))
}

// Return whether one byte may start an unquoted SQL identifier segment.
const fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
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
    use crate::db::sql::{explain_target_sql, identifiers_tail_match, statement_entity_name};

    #[test]
    fn statement_entity_name_select_returns_entity() {
        let entity = statement_entity_name("SELECT * FROM FixtureUser ORDER BY name")
            .expect("entity should parse");
        assert_eq!(entity, "FixtureUser");
    }

    #[test]
    fn statement_entity_name_explain_json_returns_schema_qualified_entity() {
        let entity =
            statement_entity_name("EXPLAIN JSON SELECT name FROM public.FixtureOrder LIMIT 1")
                .expect("entity should parse");
        assert_eq!(entity, "public.FixtureOrder");
    }

    #[test]
    fn statement_entity_name_delete_returns_entity() {
        let entity = statement_entity_name("DELETE FROM FixtureOrder WHERE status = 'paid'")
            .expect("entity should parse");
        assert_eq!(entity, "FixtureOrder");
    }

    #[test]
    fn statement_entity_name_ignores_from_inside_string_literals() {
        let entity = statement_entity_name(
            "SELECT name FROM FixtureUser WHERE note = 'before from after' ORDER BY name",
        )
        .expect("entity should parse");
        assert_eq!(entity, "FixtureUser");
    }

    #[test]
    fn statement_entity_name_rejects_unsupported_statement_kind() {
        let error =
            statement_entity_name("INSERT INTO FixtureUser VALUES ('alice')").expect_err("reject");
        assert!(
            error
                .message()
                .contains("query endpoint supports SELECT/DELETE statements only"),
            "error should explain supported statement kinds: {error:?}"
        );
    }

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
}
