//! Module: predicate::parser
//! Responsibility: reduced SQL predicate parsing for core predicate semantics.
//! Does not own: statement routing, SQL frontend dispatch, or executor behavior.
//! Boundary: schema/index/core code consumes standalone predicate parsing here.

mod expression;
mod lowering;
mod operand;
#[cfg(test)]
mod tests;

use crate::db::{
    predicate::{CompareOp, Predicate},
    sql_shared::{Keyword, SqlParseError, SqlTokenCursor, TokenKind, tokenize_sql},
};

pub(in crate::db) use crate::db::predicate::parser::expression::parse_predicate_from_cursor;

/// Parse one SQL predicate expression.
///
/// This is the core predicate parsing boundary used by schema/index contracts
/// that need predicate semantics without a full SQL statement wrapper.
pub(crate) fn parse_sql_predicate(sql: &str) -> Result<Predicate, SqlParseError> {
    let tokens = tokenize_sql(sql)?;
    let mut cursor = SqlTokenCursor::new(tokens);
    let predicate = parse_predicate_from_cursor(&mut cursor)?;

    if cursor.eat_semicolon() && !cursor.is_eof() {
        return Err(SqlParseError::unsupported_feature(
            "multi-statement SQL input",
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
                "one of =, !=, <>, <, <=, >, >=",
                cursor.peek_kind(),
            ));
        }
    };

    cursor.advance();

    Ok(op)
}

// Map trailing reduced-SQL tokens to the same user-facing unsupported feature
// labels as the statement parser while keeping feature policy out of sql_shared.
const fn predicate_unsupported_feature(kind: Option<&TokenKind>) -> Option<&'static str> {
    match kind {
        Some(TokenKind::Keyword(Keyword::As)) => Some("column/expression aliases"),
        Some(TokenKind::Keyword(Keyword::Describe)) => Some("DESCRIBE modifiers"),
        Some(TokenKind::Keyword(Keyword::Having)) => Some("HAVING"),
        Some(TokenKind::Keyword(Keyword::Insert)) => Some("INSERT"),
        Some(TokenKind::Keyword(Keyword::Join)) => Some("JOIN"),
        Some(TokenKind::Keyword(Keyword::Filter)) => Some("aggregate FILTER clauses"),
        Some(TokenKind::Keyword(Keyword::Over)) => Some("window functions / OVER"),
        Some(TokenKind::Keyword(Keyword::Returning)) => Some("RETURNING"),
        Some(TokenKind::Keyword(Keyword::Show)) => {
            Some("SHOW commands beyond SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES")
        }
        Some(TokenKind::Keyword(Keyword::With)) => Some("WITH"),
        Some(TokenKind::Keyword(Keyword::Union | Keyword::Intersect | Keyword::Except)) => {
            Some("UNION/INTERSECT/EXCEPT")
        }
        Some(TokenKind::Keyword(Keyword::Update)) => Some("UPDATE"),
        _ => None,
    }
}
