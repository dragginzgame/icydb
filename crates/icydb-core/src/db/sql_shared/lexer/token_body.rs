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

        loop {
            let Some(relative_quote) = self.bytes[self.pos..]
                .iter()
                .position(|byte| *byte == b'\'')
            else {
                return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
                    SqlSyntaxErrorKind::StringLiteralUnterminated,
                ));
            };
            let quote_pos = self.pos + relative_quote;
            self.pos = quote_pos + 1;

            if self.peek_byte() == Some(b'\'') {
                let out = out.get_or_insert_with(|| {
                    String::with_capacity(quote_pos.saturating_sub(segment_start) + 1)
                });
                out.push_str(self.source_slice(segment_start, quote_pos));
                self.pos += 1;
                out.push('\'');
                segment_start = self.pos;
                continue;
            }

            if let Some(mut out) = out {
                out.push_str(self.source_slice(segment_start, quote_pos));
                return Ok(out);
            }

            return Ok(self.source_slice(segment_start, quote_pos).to_owned());
        }
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

        self.source_slice(start, self.pos).to_owned()
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
            None => TokenKind::Identifier(self.source_slice(start, self.pos).to_owned()),
        }
    }

    fn source_slice(&self, start: usize, end: usize) -> &str {
        &self.source[start..end]
    }
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
        let Some(high) = hex_nibble(pair[0]) else {
            return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
                SqlSyntaxErrorKind::BlobLiteralNonHexDigit,
            ));
        };
        let Some(low) = hex_nibble(pair[1]) else {
            return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
                SqlSyntaxErrorKind::BlobLiteralNonHexDigit,
            ));
        };
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
