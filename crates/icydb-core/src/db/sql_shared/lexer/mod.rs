mod keywords;
mod scan;
mod token_body;

use crate::db::sql_shared::{SqlParseError, types::Token};

///
/// Lexer
///
/// Tracks one reduced-SQL byte cursor so the shared lexer can scan tokens
/// without exposing mutable lexical state outside this boundary.
///
struct Lexer<'a> {
    bytes: &'a [u8],
    pos: usize,
}

pub(crate) fn tokenize_sql(sql: &str) -> Result<Vec<Token>, SqlParseError> {
    Lexer::tokenize(sql)
}
