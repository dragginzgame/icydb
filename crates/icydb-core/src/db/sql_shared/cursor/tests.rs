use super::SqlTokenCursor;
use crate::{
    db::sql_shared::{SqlExpectedToken, SqlParseError, TokenKind, tokenize_sql},
    value::Value,
};

#[test]
fn moved_literals_and_identifiers_preserve_contents_and_cursor_order() {
    let text = "ab'cd".repeat(1024);
    let sql = format!("'{}' X'00fe80' identifier", text.replace('\'', "''"));
    let mut cursor = SqlTokenCursor::new(tokenize_sql(&sql).expect("valid mixed tokens"));
    assert_eq!(cursor.take_string_literal(), Ok(Value::Text(text)));
    assert_eq!(
        cursor.take_blob_literal(),
        Ok(Value::Blob(vec![0, 0xfe, 0x80]))
    );
    assert_eq!(cursor.take_identifier_segment(), Ok("identifier".into()));
    assert!(cursor.peek_kind().is_none());
}

#[test]
fn literal_and_identifier_mismatches_preserve_the_unconsumed_token() {
    let mut cursor = SqlTokenCursor::new(tokenize_sql("name X'80' 'text'").expect("valid tokens"));
    assert_eq!(
        cursor.take_string_literal(),
        Err(SqlParseError::expected(
            SqlExpectedToken::Literal,
            Some(&TokenKind::Identifier("name".into()))
        ))
    );
    assert_eq!(
        cursor.take_blob_literal(),
        Err(SqlParseError::expected(
            SqlExpectedToken::Literal,
            Some(&TokenKind::Identifier("name".into()))
        ))
    );
    assert_eq!(cursor.take_identifier_segment(), Ok("name".into()));
    assert_eq!(
        cursor.take_identifier_segment(),
        Err(SqlParseError::expected(
            SqlExpectedToken::Identifier,
            Some(&TokenKind::BlobLiteral(vec![0x80]))
        ))
    );
    assert_eq!(cursor.take_blob_literal(), Ok(Value::Blob(vec![0x80])));
    assert_eq!(
        cursor.take_blob_literal(),
        Err(SqlParseError::expected(
            SqlExpectedToken::Literal,
            Some(&TokenKind::StringLiteral("text".into()))
        ))
    );
    assert_eq!(cursor.take_string_literal(), Ok(Value::Text("text".into())));
    assert_eq!(
        cursor.take_string_literal(),
        Err(SqlParseError::expected(SqlExpectedToken::Literal, None))
    );
    assert_eq!(
        cursor.take_blob_literal(),
        Err(SqlParseError::expected(SqlExpectedToken::Literal, None))
    );
    assert_eq!(
        cursor.take_identifier_segment(),
        Err(SqlParseError::expected(SqlExpectedToken::Identifier, None))
    );
}
