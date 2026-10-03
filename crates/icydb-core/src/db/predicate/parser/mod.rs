//! Module: predicate::parser
//! Responsibility: reduced SQL predicate parsing for core predicate semantics.
//! Does not own: statement routing, SQL frontend dispatch, or executor behavior.
//! Boundary: SQL index intake binds this frontend syntax into accepted typed
//! metadata. Runtime consumers never parse persisted predicate text.

mod expression;
mod lowering;
mod operand;
#[cfg(test)]
mod tests;

use crate::db::{
    predicate::{CompareOp, Predicate},
    sql_shared::{SqlExpectedToken, SqlParseError, SqlTokenCursor, TokenKind, tokenize_sql},
};
use icydb_diagnostic_code::SqlFeatureCode;

/// Parse one SQL predicate expression.
///
/// This is the core predicate parsing boundary used by schema/index contracts
/// that need predicate semantics without a full SQL statement wrapper.
pub(in crate::db) fn parse_sql_predicate(sql: &str) -> Result<Predicate, SqlParseError> {
    let tokens = tokenize_sql(sql)?;
    let mut cursor = SqlTokenCursor::new(tokens);
    let predicate = expression::parse_predicate_from_cursor(&mut cursor)?;

    if cursor.eat_semicolon() && !cursor.is_eof() {
        return Err(SqlParseError::unsupported_feature(
            SqlFeatureCode::MultiStatementSql,
        ));
    }

    if !cursor.is_eof() {
        if let Some(feature) = SqlParseError::trailing_unsupported_feature(cursor.peek_kind()) {
            return Err(SqlParseError::unsupported_feature(feature));
        }

        return Err(SqlParseError::expected_end_of_input(cursor.peek_kind()));
    }

    Ok(predicate)
}

// Parse one predicate comparison operator at the predicate boundary so the
// shared SQL token cursor does not depend on predicate semantics.
pub(in crate::db::predicate::parser) fn parse_compare_operator(
    cursor: &mut SqlTokenCursor,
) -> Result<CompareOp, SqlParseError> {
    let op = match cursor.peek_kind() {
        Some(TokenKind::Eq) => CompareOp::Eq,
        Some(TokenKind::Ne) => CompareOp::Ne,
        Some(TokenKind::Lt) => CompareOp::Lt,
        Some(TokenKind::Lte) => CompareOp::Lte,
        Some(TokenKind::Gt) => CompareOp::Gt,
        Some(TokenKind::Gte) => CompareOp::Gte,
        _ => {
            return Err(SqlParseError::expected(
                SqlExpectedToken::CompareOperator,
                cursor.peek_kind(),
            ));
        }
    };

    cursor.advance();

    Ok(op)
}
