//! Module: db::sql
//! Responsibility: SQL frontend parsing contracts for reduced SQL entrypoints.
//! Does not own: schema validation, access planning, or executor behavior.
//! Boundary: parses SQL text into deterministic statement AST used by planner-facing layers.

pub(in crate::db) mod ddl;
pub(crate) mod identifier;
pub(crate) mod lowering;
pub(crate) mod parser;

pub(in crate::db) use parser::SqlIntegrityStatement;

/// Parse one bounded integrity-administration statement.
///
/// This SQL-root boundary keeps parser internals private while allowing the
/// trusted session frontend to share their canonical grammar.
pub(in crate::db) fn parse_integrity_sql(
    sql: &str,
) -> Result<SqlIntegrityStatement, parser::SqlParseError> {
    parser::parse_integrity_sql(sql)
}
