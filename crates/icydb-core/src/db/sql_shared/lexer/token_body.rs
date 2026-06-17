use crate::db::sql_shared::{
    SqlSyntaxErrorKind, TokenKind,
    lexer::{
        Lexer,
        keywords::{is_identifier_continue, keyword_from_ident_bytes},
    },
};

const MAX_SQL_BLOB_LITERAL_BYTES: usize = 1_048_576;

impl Lexer<'_> {
    pub(super) fn lex_blob_literal(
        &mut self,
    ) -> Result<Vec<u8>, crate::db::sql_shared::SqlParseError> {
        self.pos += 1;
        self.expect_byte(b'\'')?;
        let start = self.pos;

        while let Some(byte) = self.peek_byte() {
            self.pos += 1;
            if byte == b'\'' {
                let hex = &self.bytes[start..self.pos - 1];
                return decode_hex_blob_literal(hex);
            }
            if !byte.is_ascii_hexdigit() {
                return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
                    SqlSyntaxErrorKind::BlobLiteralNonHexDigit,
                ));
            }
        }

        Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
            SqlSyntaxErrorKind::BlobLiteralUnterminated,
        ))
    }

    pub(super) fn lex_string_literal(
        &mut self,
    ) -> Result<String, crate::db::sql_shared::SqlParseError> {
        self.expect_byte(b'\'')?;
        let mut segment_start = self.pos;
        let mut out = None;
        while let Some(byte) = self.peek_byte() {
            self.pos += 1;
            if byte == b'\'' {
                if self.peek_byte() == Some(b'\'') {
                    let out = out.get_or_insert_with(String::new);
                    out.push_str(string_literal_slice(
                        self.bytes,
                        segment_start,
                        self.pos - 1,
                    ));
                    self.pos += 1;
                    out.push('\'');
                    segment_start = self.pos;
                    continue;
                }

                if let Some(mut out) = out {
                    out.push_str(string_literal_slice(
                        self.bytes,
                        segment_start,
                        self.pos - 1,
                    ));
                    return Ok(out);
                }

                return Ok(
                    string_literal_slice(self.bytes, segment_start, self.pos - 1).to_owned(),
                );
            }
        }

        Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
            SqlSyntaxErrorKind::StringLiteralUnterminated,
        ))
    }

    pub(super) fn lex_number(&mut self) -> String {
        let start = self.pos;
        let len = self.bytes.len();

        while self.pos < len && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos + 1 < len
            && self.bytes[self.pos] == b'.'
            && self.bytes[self.pos + 1].is_ascii_digit()
        {
            self.pos += 1;
            while self.pos < len && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }

        std::str::from_utf8(&self.bytes[start..self.pos])
            .expect("numeric token bytes must remain utf-8")
            .to_owned()
    }

    pub(super) fn lex_identifier_or_keyword(&mut self) -> TokenKind {
        let start = self.pos;
        let len = self.bytes.len();
        self.pos += 1;
        while self.pos < len && is_identifier_continue(self.bytes[self.pos]) {
            self.pos += 1;
        }
        let ident_bytes = &self.bytes[start..self.pos];

        match keyword_from_ident_bytes(ident_bytes) {
            Some(keyword) => TokenKind::Keyword(keyword),
            None => TokenKind::Identifier(
                std::str::from_utf8(ident_bytes)
                    .expect("identifier token bytes must remain utf-8")
                    .to_owned(),
            ),
        }
    }
}

fn string_literal_slice(bytes: &[u8], start: usize, end: usize) -> &str {
    std::str::from_utf8(&bytes[start..end]).expect("SQL source bytes must remain utf-8")
}

// Decode the SQL-standard-ish `X'ABCD'` blob surface at the lexical boundary so
// downstream parsers see one ordinary literal token instead of reinterpreting
// identifier/string token pairs.
fn decode_hex_blob_literal(hex: &[u8]) -> Result<Vec<u8>, crate::db::sql_shared::SqlParseError> {
    if !hex.len().is_multiple_of(2) {
        return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
            SqlSyntaxErrorKind::BlobLiteralOddHexLength,
        ));
    }
    if hex.len() / 2 > MAX_SQL_BLOB_LITERAL_BYTES {
        return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
            SqlSyntaxErrorKind::BlobLiteralTooLarge {
                max_decoded_bytes: MAX_SQL_BLOB_LITERAL_BYTES,
            },
        ));
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.chunks_exact(2) {
        let high = hex_nibble(pair[0]).expect("sql lexer invariant");
        let low = hex_nibble(pair[1]).expect("sql lexer invariant");
        bytes.push((high << 4) | low);
    }

    Ok(bytes)
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
