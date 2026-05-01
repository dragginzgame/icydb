use crate::db::sql_shared::{
    TokenKind,
    lexer::{
        Lexer,
        keywords::{is_identifier_continue, keyword_from_ident_bytes},
    },
};

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
                    "blob literal must contain only hexadecimal digits",
                ));
            }
        }

        Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
            "unterminated blob literal",
        ))
    }

    pub(super) fn lex_string_literal(
        &mut self,
    ) -> Result<String, crate::db::sql_shared::SqlParseError> {
        self.expect_byte(b'\'')?;
        let mut out = String::new();
        while let Some(byte) = self.peek_byte() {
            self.pos += 1;
            if byte == b'\'' {
                if self.peek_byte() == Some(b'\'') {
                    self.pos += 1;
                    out.push('\'');
                    continue;
                }

                return Ok(out);
            }
            out.push(byte as char);
        }

        Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
            "unterminated string literal",
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

// Decode the SQL-standard-ish `X'ABCD'` blob surface at the lexical boundary so
// downstream parsers see one ordinary literal token instead of reinterpreting
// identifier/string token pairs.
fn decode_hex_blob_literal(hex: &[u8]) -> Result<Vec<u8>, crate::db::sql_shared::SqlParseError> {
    if !hex.len().is_multiple_of(2) {
        return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
            "blob literal must contain an even number of hex digits",
        ));
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.chunks_exact(2) {
        let high = hex_nibble(pair[0]).expect("blob literal hex digits are validated while lexing");
        let low = hex_nibble(pair[1]).expect("blob literal hex digits are validated while lexing");
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
