use crate::db::reduced_sql::{Keyword, SqlParseError, Token, TokenKind};

pub(crate) fn tokenize_sql(sql: &str) -> Result<Vec<Token>, SqlParseError> {
    Lexer::tokenize(sql)
}

struct Lexer<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn tokenize(sql: &'a str) -> Result<Vec<Token>, SqlParseError> {
        let mut lexer = Self {
            bytes: sql.as_bytes(),
            pos: 0,
        };
        let mut tokens = Vec::new();

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

        let kind = match byte {
            b',' => {
                self.pos += 1;
                TokenKind::Comma
            }
            b'.' => {
                self.pos += 1;
                TokenKind::Dot
            }
            b'+' => {
                self.pos += 1;
                TokenKind::Plus
            }
            b'-' => {
                self.pos += 1;
                TokenKind::Minus
            }
            b'/' => {
                self.pos += 1;
                TokenKind::Slash
            }
            b'(' => {
                self.pos += 1;
                TokenKind::LParen
            }
            b')' => {
                self.pos += 1;
                TokenKind::RParen
            }
            b';' => {
                self.pos += 1;
                TokenKind::Semicolon
            }
            b'*' => {
                self.pos += 1;
                TokenKind::Star
            }
            b'=' => {
                self.pos += 1;
                TokenKind::Eq
            }
            b'!' => {
                self.pos += 1;
                if self.consume_if(b'=') {
                    TokenKind::Ne
                } else {
                    return Err(SqlParseError::invalid_syntax("unexpected '!'"));
                }
            }
            b'<' => {
                self.pos += 1;
                if self.consume_if(b'=') {
                    TokenKind::Lte
                } else if self.consume_if(b'>') {
                    TokenKind::Ne
                } else {
                    TokenKind::Lt
                }
            }
            b'>' => {
                self.pos += 1;
                if self.consume_if(b'=') {
                    TokenKind::Gte
                } else {
                    TokenKind::Gt
                }
            }
            b'\'' => TokenKind::StringLiteral(self.lex_string_literal()?),
            b'"' | b'`' => return Err(SqlParseError::unsupported_feature("quoted identifiers")),
            next if next.is_ascii_digit() => TokenKind::Number(self.lex_number()),
            next if is_identifier_start(next) => self.lex_identifier_or_keyword(),
            other => {
                return Err(SqlParseError::invalid_syntax(format!(
                    "unexpected character '{}'; reduced SQL supports bare identifiers, strings, numbers, and simple operators",
                    other as char
                )));
            }
        };

        Ok(Some(Token { kind }))
    }

    fn skip_whitespace(&mut self) {
        while self
            .peek_byte()
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            self.pos += 1;
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn peek_second_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos + 1).copied()
    }

    fn lex_string_literal(&mut self) -> Result<String, SqlParseError> {
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

        Err(SqlParseError::invalid_syntax("unterminated string literal"))
    }

    fn lex_number(&mut self) -> String {
        let start = self.pos;

        while self.peek_byte().is_some_and(|byte| byte.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.peek_byte() == Some(b'.')
            && self
                .peek_second_byte()
                .is_some_and(|byte| byte.is_ascii_digit())
        {
            self.pos += 1;
            while self.peek_byte().is_some_and(|byte| byte.is_ascii_digit()) {
                self.pos += 1;
            }
        }

        std::str::from_utf8(&self.bytes[start..self.pos])
            .expect("numeric token bytes must remain utf-8")
            .to_owned()
    }

    fn lex_identifier_or_keyword(&mut self) -> TokenKind {
        let start = self.pos;
        self.pos += 1;
        while self.peek_byte().is_some_and(is_identifier_continue) {
            self.pos += 1;
        }
        let out = std::str::from_utf8(&self.bytes[start..self.pos])
            .expect("identifier token bytes must remain utf-8")
            .to_owned();
        match keyword_from_ident(out.as_str()) {
            Some(keyword) => TokenKind::Keyword(keyword),
            None => TokenKind::Identifier(out),
        }
    }

    fn consume_if(&mut self, expected: u8) -> bool {
        if self.peek_byte() != Some(expected) {
            return false;
        }

        self.pos += 1;
        true
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), SqlParseError> {
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

const fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

const fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

const fn keyword_from_ident(value: &str) -> Option<Keyword> {
    match value.len() {
        2 if value.eq_ignore_ascii_case("AS") => Some(Keyword::As),
        2 if value.eq_ignore_ascii_case("BY") => Some(Keyword::By),
        2 if value.eq_ignore_ascii_case("IN") => Some(Keyword::In),
        2 if value.eq_ignore_ascii_case("IS") => Some(Keyword::Is),
        2 if value.eq_ignore_ascii_case("OR") => Some(Keyword::Or),
        3 if value.eq_ignore_ascii_case("AND") => Some(Keyword::And),
        3 if value.eq_ignore_ascii_case("ASC") => Some(Keyword::Asc),
        3 if value.eq_ignore_ascii_case("AVG") => Some(Keyword::Avg),
        3 if value.eq_ignore_ascii_case("MAX") => Some(Keyword::Max),
        3 if value.eq_ignore_ascii_case("MIN") => Some(Keyword::Min),
        3 if value.eq_ignore_ascii_case("NOT") => Some(Keyword::Not),
        3 if value.eq_ignore_ascii_case("SUM") => Some(Keyword::Sum),
        4 if value.eq_ignore_ascii_case("DESC") => Some(Keyword::Desc),
        4 if value.eq_ignore_ascii_case("FROM") => Some(Keyword::From),
        4 if value.eq_ignore_ascii_case("JOIN") => Some(Keyword::Join),
        4 if value.eq_ignore_ascii_case("JSON") => Some(Keyword::Json),
        4 if value.eq_ignore_ascii_case("NULL") => Some(Keyword::Null),
        4 if value.eq_ignore_ascii_case("SHOW") => Some(Keyword::Show),
        4 if value.eq_ignore_ascii_case("TRUE") => Some(Keyword::True),
        4 if value.eq_ignore_ascii_case("WITH") => Some(Keyword::With),
        5 if value.eq_ignore_ascii_case("COUNT") => Some(Keyword::Count),
        5 if value.eq_ignore_ascii_case("FALSE") => Some(Keyword::False),
        5 if value.eq_ignore_ascii_case("GROUP") => Some(Keyword::Group),
        5 if value.eq_ignore_ascii_case("LIMIT") => Some(Keyword::Limit),
        5 if value.eq_ignore_ascii_case("ORDER") => Some(Keyword::Order),
        5 if value.eq_ignore_ascii_case("UNION") => Some(Keyword::Union),
        5 if value.eq_ignore_ascii_case("WHERE") => Some(Keyword::Where),
        9 if value.eq_ignore_ascii_case("RETURNING") => Some(Keyword::Returning),
        6 if value.eq_ignore_ascii_case("DELETE") => Some(Keyword::Delete),
        6 if value.eq_ignore_ascii_case("EXCEPT") => Some(Keyword::Except),
        6 if value.eq_ignore_ascii_case("HAVING") => Some(Keyword::Having),
        6 if value.eq_ignore_ascii_case("INSERT") => Some(Keyword::Insert),
        6 if value.eq_ignore_ascii_case("OFFSET") => Some(Keyword::Offset),
        6 if value.eq_ignore_ascii_case("SELECT") => Some(Keyword::Select),
        6 if value.eq_ignore_ascii_case("UPDATE") => Some(Keyword::Update),
        7 if value.eq_ignore_ascii_case("BETWEEN") => Some(Keyword::Between),
        7 if value.eq_ignore_ascii_case("COLUMNS") => Some(Keyword::Columns),
        7 if value.eq_ignore_ascii_case("EXPLAIN") => Some(Keyword::Explain),
        7 if value.eq_ignore_ascii_case("INDEXES") => Some(Keyword::Indexes),
        8 if value.eq_ignore_ascii_case("DESCRIBE") => Some(Keyword::Describe),
        8 if value.eq_ignore_ascii_case("DISTINCT") => Some(Keyword::Distinct),
        8 if value.eq_ignore_ascii_case("ENTITIES") => Some(Keyword::Entities),
        6 if value.eq_ignore_ascii_case("TABLES") => Some(Keyword::Tables),
        9 if value.eq_ignore_ascii_case("EXECUTION") => Some(Keyword::Execution),
        9 if value.eq_ignore_ascii_case("INTERSECT") => Some(Keyword::Intersect),
        _ => None,
    }
}
