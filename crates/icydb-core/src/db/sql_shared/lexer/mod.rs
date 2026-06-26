mod keywords;
mod scan;
mod token_body;

use crate::db::sql_shared::{SqlParseError, types::Token, validate_sql_input_bytes};

///
/// Lexer
///
/// Tracks one reduced-SQL byte cursor so the shared lexer can scan tokens
/// without exposing mutable lexical state outside this boundary.
///
struct Lexer<'a> {
    source: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

pub(crate) fn tokenize_sql(sql: &str) -> Result<Vec<Token>, SqlParseError> {
    validate_sql_input_bytes(sql)?;

    Lexer::tokenize(sql)
}
