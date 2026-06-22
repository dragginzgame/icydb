//! Module: predicate::parser
//! Responsibility: reduced SQL predicate parsing for core predicate semantics.
//! Does not own: statement routing, SQL frontend dispatch, or executor behavior.
//! Boundary: schema/index/core code consumes this standalone generated-index
//! predicate DSL separately from the main SQL statement parser path.

mod expression;
mod lowering;
mod operand;
#[cfg(test)]
mod tests;

use crate::db::{
    predicate::{CompareOp, Predicate},
    sql_shared::{
        Keyword, SqlExpectedToken, SqlParseError, SqlTokenCursor, TokenKind, tokenize_sql,
    },
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
        if let Some(feature) = predicate_unsupported_feature(cursor.peek_kind()) {
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

// Map trailing reduced-SQL tokens to the same unsupported feature codes as the
// statement parser while keeping feature policy out of sql_shared.
const fn predicate_unsupported_feature(kind: Option<&TokenKind>) -> Option<SqlFeatureCode> {
    match kind {
        Some(TokenKind::Keyword(Keyword::As)) => Some(SqlFeatureCode::ColumnAlias),
        Some(TokenKind::Keyword(Keyword::Describe)) => Some(SqlFeatureCode::DescribeModifier),
        Some(TokenKind::Keyword(Keyword::Having)) => Some(SqlFeatureCode::Having),
        Some(TokenKind::Keyword(Keyword::Insert)) => Some(SqlFeatureCode::Insert),
        Some(TokenKind::Keyword(Keyword::Join)) => Some(SqlFeatureCode::Join),
        Some(TokenKind::Keyword(Keyword::Filter)) => Some(SqlFeatureCode::AggregateFilterClause),
        Some(TokenKind::Keyword(Keyword::Over)) => Some(SqlFeatureCode::WindowFunction),
        Some(TokenKind::Keyword(Keyword::Returning)) => {
            Some(SqlFeatureCode::ReturningUnsupportedShape)
        }
        Some(TokenKind::Keyword(Keyword::Show)) => Some(SqlFeatureCode::ShowUnsupportedCommand),
        Some(TokenKind::Keyword(Keyword::With)) => Some(SqlFeatureCode::With),
        Some(TokenKind::Keyword(Keyword::Union | Keyword::Intersect | Keyword::Except)) => {
            Some(SqlFeatureCode::UnionIntersectExcept)
        }
        Some(TokenKind::Keyword(Keyword::Update)) => Some(SqlFeatureCode::Update),
        _ => None,
    }
}
