use crate::db::sql_shared::{
    TokenKind,
    lexer::{
        Lexer,
        keywords::{is_identifier_continue, keyword_from_ident_bytes},
    },
};

impl Lexer<'_> {
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
