//! Module: db::sql_shared
//! Responsibility: shared SQL lexical utilities: tokenization, parse errors, and token-cursor primitives.
//! Does not own: predicate semantics, statement policy, AST lowering, command DTOs, or executor behavior.
//! Boundary: predicate parsing and the feature-gated SQL frontend both build on
//! this ungated lexical layer.

mod cursor;
mod lexer;
#[cfg(test)]
mod tests;
pub(in crate::db::sql_shared) mod types;

pub(crate) use cursor::SqlTokenCursor;
pub(crate) use lexer::tokenize_sql;
pub(crate) use types::{Keyword, SqlExpectedToken, SqlParseError, SqlSyntaxErrorKind, TokenKind};
#[cfg(feature = "sql")]
pub(crate) use types::{SqlClauseOrderRule, SqlIntegerLiteralClause};

pub(crate) const MAX_SQL_INPUT_BYTES: usize = (2 * 1024 * 1024) + 4096;
pub(crate) const MAX_SQL_TOKENS: usize = 32_768;
pub(crate) const MAX_SQL_EXPR_DEPTH: usize = 128;

pub(crate) const fn sql_expr_depth_limit_error() -> SqlParseError {
    SqlParseError::invalid_syntax(SqlSyntaxErrorKind::ExpressionDepthLimit {
        max_depth: MAX_SQL_EXPR_DEPTH,
    })
}

pub(crate) const fn sql_token_limit_error() -> SqlParseError {
    SqlParseError::invalid_syntax(SqlSyntaxErrorKind::TokenLimit {
        max_tokens: MAX_SQL_TOKENS,
    })
}

pub(crate) fn validate_sql_input_bytes(sql: &str) -> Result<(), SqlParseError> {
    if sql.len() > MAX_SQL_INPUT_BYTES {
        return Err(SqlParseError::invalid_syntax(
            SqlSyntaxErrorKind::InputTooLong {
                max_bytes: MAX_SQL_INPUT_BYTES,
            },
        ));
    }

    Ok(())
}
