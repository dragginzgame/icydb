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
pub(crate) use types::{Keyword, SqlParseError, TokenKind};
