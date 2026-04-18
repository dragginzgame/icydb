use crate::db::sql_shared::{
    SqlParseError, TokenKind,
    lexer::{Lexer, keywords::is_identifier_start},
    types::Token,
};

impl<'a> Lexer<'a> {
    pub(super) fn tokenize(sql: &'a str) -> Result<Vec<Token>, SqlParseError> {
        let mut lexer = Self {
            bytes: sql.as_bytes(),
            pos: 0,
        };
        let mut tokens = Vec::with_capacity(sql.len().saturating_div(4).saturating_add(1));

        while let Some(token) = lexer.next_token()? {
            tokens.push(token);
        }

        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Option<Token>, SqlParseError> {
        self.skip_whitespace();
        let Some(byte) = self.peek_byte() else {
            return Ok(None);
        };

        let kind = if let Some(kind) = self.lex_single_char_token(byte) {
            kind
        } else if matches!(byte, b'!' | b'<' | b'>') {
            self.lex_comparison_operator(byte)?
        } else {
            match byte {
                b'\'' => TokenKind::StringLiteral(self.lex_string_literal()?),
                b'"' | b'`' => {
                    return Err(SqlParseError::unsupported_feature("quoted identifiers"));
                }
                next if next.is_ascii_digit() => TokenKind::Number(self.lex_number()),
                next if is_identifier_start(next) => self.lex_identifier_or_keyword(),
                other => {
                    return Err(SqlParseError::invalid_syntax(format!(
                        "unexpected character '{}'; reduced SQL supports bare identifiers, strings, numbers, and simple operators",
                        other as char
                    )));
                }
            }
        };

        Ok(Some(Token { kind }))
    }

    // Scan the punctuation tokens that always map one byte onto one token kind.
    const fn lex_single_char_token(&mut self, byte: u8) -> Option<TokenKind> {
        let kind = match byte {
            b',' => TokenKind::Comma,
            b'.' => TokenKind::Dot,
            b'+' => TokenKind::Plus,
            b'-' => TokenKind::Minus,
            b'/' => TokenKind::Slash,
            b'(' => TokenKind::LParen,
            b')' => TokenKind::RParen,
            b';' => TokenKind::Semicolon,
            b'*' => TokenKind::Star,
            b'=' => TokenKind::Eq,
            _ => return None,
        };
        self.pos += 1;

        Some(kind)
    }

    // Scan the reduced comparison operator surface while keeping unsupported
    // punctuation fail-closed at the lexical boundary.
    fn lex_comparison_operator(&mut self, byte: u8) -> Result<TokenKind, SqlParseError> {
        self.pos += 1;

        match byte {
            b'!' => {
                if self.consume_if(b'=') {
                    Ok(TokenKind::Ne)
                } else {
                    Err(SqlParseError::invalid_syntax("unexpected '!'"))
                }
            }
            b'<' => Ok(if self.consume_if(b'=') {
                TokenKind::Lte
            } else if self.consume_if(b'>') {
                TokenKind::Ne
            } else {
                TokenKind::Lt
            }),
            b'>' => Ok(if self.consume_if(b'=') {
                TokenKind::Gte
            } else {
                TokenKind::Gt
            }),
            _ => unreachable!("comparison operator entry is guarded by the caller"),
        }
    }

    fn skip_whitespace(&mut self) {
        let len = self.bytes.len();
        while self.pos < len && self.bytes[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    pub(super) fn peek_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn consume_if(&mut self, expected: u8) -> bool {
        if self.peek_byte() != Some(expected) {
            return false;
        }

        self.pos += 1;
        true
    }

    pub(super) fn expect_byte(&mut self, expected: u8) -> Result<(), SqlParseError> {
        match self.peek_byte() {
            Some(found) if found == expected => {
                self.pos += 1;
                Ok(())
            }
            Some(found) => Err(SqlParseError::invalid_syntax(format!(
                "expected '{}', found '{}'",
                expected as char, found as char
            ))),
            None => Err(SqlParseError::invalid_syntax(format!(
                "expected '{}', found end of input",
                expected as char
            ))),
        }
    }
}
