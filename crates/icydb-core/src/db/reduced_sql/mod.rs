//! Module: db::reduced_sql
//! Responsibility: reduced SQL tokenization, shared parse errors, and token-cursor primitives.
//! Does not own: predicate semantics, statement AST lowering, or executor behavior.
//! Boundary: predicate parsing and the SQL frontend both build on this shared lexical layer.

use crate::{db::predicate::CompareOp, types::Decimal, value::Value};
use std::str::FromStr;
use thiserror::Error as ThisError;

///
/// SqlParseError
///
/// Reduced SQL parser errors shared by standalone predicate parsing and the
/// statement-level SQL frontend.
///
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(crate) enum SqlParseError {
    #[cfg(feature = "sql")]
    #[error("empty SQL input")]
    EmptyInput,

    #[error("unsupported SQL feature: {feature}")]
    UnsupportedFeature { feature: &'static str },

    #[error("invalid SQL syntax: {message}")]
    InvalidSyntax { message: String },
}

impl SqlParseError {
    pub(in crate::db) const fn unsupported_feature(feature: &'static str) -> Self {
        Self::UnsupportedFeature { feature }
    }

    pub(in crate::db) fn invalid_syntax(message: impl Into<String>) -> Self {
        Self::InvalidSyntax {
            message: message.into(),
        }
    }

    pub(crate) fn expected(expected: &str, found: Option<&TokenKind>) -> Self {
        let found = found.map_or_else(|| "end of input".to_string(), token_kind_label);

        Self::invalid_syntax(format!("expected {expected}, found {found}"))
    }

    pub(in crate::db) fn expected_end_of_input(found: Option<&TokenKind>) -> Self {
        let found = found.map_or_else(|| "end of input".to_string(), token_kind_label);

        Self::invalid_syntax(format!("expected end of input, found {found}"))
    }

    pub(in crate::db) fn invalid_numeric_literal(raw: &str) -> Self {
        Self::invalid_syntax(format!("invalid numeric literal: {raw}"))
    }
}

///
/// Keyword
///
/// Reduced SQL keyword taxonomy shared by predicate parsing and statement parsing.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Keyword {
    And,
    As,
    Asc,
    Avg,
    Between,
    By,
    Columns,
    Count,
    Delete,
    Describe,
    Desc,
    Distinct,
    Except,
    Execution,
    Explain,
    Entities,
    False,
    From,
    Group,
    Having,
    In,
    Indexes,
    Insert,
    Intersect,
    Is,
    Join,
    Json,
    Limit,
    Max,
    Min,
    Not,
    Null,
    Offset,
    Or,
    Order,
    Select,
    Show,
    Sum,
    Tables,
    True,
    Union,
    Update,
    Where,
    With,
}

impl Keyword {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::Avg => "AVG",
            Self::Between => "BETWEEN",
            Self::By => "BY",
            Self::Columns => "COLUMNS",
            Self::Count => "COUNT",
            Self::Delete => "DELETE",
            Self::Describe => "DESCRIBE",
            Self::Desc => "DESC",
            Self::Distinct => "DISTINCT",
            Self::Except => "EXCEPT",
            Self::Execution => "EXECUTION",
            Self::Explain => "EXPLAIN",
            Self::Entities => "ENTITIES",
            Self::False => "FALSE",
            Self::From => "FROM",
            Self::Group => "GROUP",
            Self::Having => "HAVING",
            Self::In => "IN",
            Self::Indexes => "INDEXES",
            Self::Insert => "INSERT",
            Self::Intersect => "INTERSECT",
            Self::Is => "IS",
            Self::Join => "JOIN",
            Self::Json => "JSON",
            Self::Limit => "LIMIT",
            Self::Max => "MAX",
            Self::Min => "MIN",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::Offset => "OFFSET",
            Self::Or => "OR",
            Self::Order => "ORDER",
            Self::Select => "SELECT",
            Self::Show => "SHOW",
            Self::Sum => "SUM",
            Self::Tables => "TABLES",
            Self::True => "TRUE",
            Self::Union => "UNION",
            Self::Update => "UPDATE",
            Self::Where => "WHERE",
            Self::With => "WITH",
        }
    }
}

// One reduced SQL token kind shared by predicate and statement parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TokenKind {
    Identifier(String),
    Number(String),
    StringLiteral(String),
    Keyword(Keyword),
    Comma,
    Dot,
    LParen,
    RParen,
    Semicolon,
    Star,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
}

pub(crate) fn token_kind_label(kind: &TokenKind) -> String {
    match kind {
        TokenKind::Identifier(name) => format!("identifier({name})"),
        TokenKind::Number(number) => format!("number({number})"),
        TokenKind::StringLiteral(_) => "string literal".to_string(),
        TokenKind::Keyword(keyword) => keyword.as_str().to_string(),
        TokenKind::Comma => ",".to_string(),
        TokenKind::Dot => ".".to_string(),
        TokenKind::LParen => "(".to_string(),
        TokenKind::RParen => ")".to_string(),
        TokenKind::Semicolon => ";".to_string(),
        TokenKind::Star => "*".to_string(),
        TokenKind::Eq => "=".to_string(),
        TokenKind::Ne => "!=".to_string(),
        TokenKind::Lt => "<".to_string(),
        TokenKind::Lte => "<=".to_string(),
        TokenKind::Gt => ">".to_string(),
        TokenKind::Gte => ">=".to_string(),
    }
}

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
            b'-' => {
                if self
                    .peek_second_byte()
                    .is_some_and(|byte| byte.is_ascii_digit())
                {
                    self.pos += 1;
                    TokenKind::Number(self.lex_number(true))
                } else {
                    return Err(SqlParseError::invalid_syntax("unexpected '-'"));
                }
            }
            byte if byte.is_ascii_digit() => TokenKind::Number(self.lex_number(false)),
            byte if is_identifier_start(byte) => self.lex_identifier_or_keyword(),
            other => {
                return Err(SqlParseError::invalid_syntax(format!(
                    "unexpected character '{}'; reduced SQL only supports unquoted identifiers, string literals, numbers, and simple operators",
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

    fn lex_number(&mut self, negative: bool) -> String {
        let start = if negative { self.pos - 1 } else { self.pos };

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

        String::from_utf8(self.bytes[start..self.pos].to_vec())
            .expect("numeric token bytes must remain utf-8")
    }

    fn lex_identifier_or_keyword(&mut self) -> TokenKind {
        let start = self.pos;
        self.pos += 1;
        while self.peek_byte().is_some_and(is_identifier_continue) {
            self.pos += 1;
        }
        let out = String::from_utf8(self.bytes[start..self.pos].to_vec())
            .expect("identifier token bytes must remain utf-8");
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

fn keyword_from_ident(value: &str) -> Option<Keyword> {
    match value.to_ascii_uppercase().as_str() {
        "AND" => Some(Keyword::And),
        "AS" => Some(Keyword::As),
        "ASC" => Some(Keyword::Asc),
        "AVG" => Some(Keyword::Avg),
        "BETWEEN" => Some(Keyword::Between),
        "BY" => Some(Keyword::By),
        "COLUMNS" => Some(Keyword::Columns),
        "COUNT" => Some(Keyword::Count),
        "DELETE" => Some(Keyword::Delete),
        "DESCRIBE" => Some(Keyword::Describe),
        "DESC" => Some(Keyword::Desc),
        "DISTINCT" => Some(Keyword::Distinct),
        "EXCEPT" => Some(Keyword::Except),
        "EXECUTION" => Some(Keyword::Execution),
        "EXPLAIN" => Some(Keyword::Explain),
        "ENTITIES" => Some(Keyword::Entities),
        "FALSE" => Some(Keyword::False),
        "FROM" => Some(Keyword::From),
        "GROUP" => Some(Keyword::Group),
        "HAVING" => Some(Keyword::Having),
        "IN" => Some(Keyword::In),
        "INDEXES" => Some(Keyword::Indexes),
        "INSERT" => Some(Keyword::Insert),
        "INTERSECT" => Some(Keyword::Intersect),
        "IS" => Some(Keyword::Is),
        "JOIN" => Some(Keyword::Join),
        "JSON" => Some(Keyword::Json),
        "LIMIT" => Some(Keyword::Limit),
        "MAX" => Some(Keyword::Max),
        "MIN" => Some(Keyword::Min),
        "NOT" => Some(Keyword::Not),
        "NULL" => Some(Keyword::Null),
        "OFFSET" => Some(Keyword::Offset),
        "OR" => Some(Keyword::Or),
        "ORDER" => Some(Keyword::Order),
        "SELECT" => Some(Keyword::Select),
        "SHOW" => Some(Keyword::Show),
        "SUM" => Some(Keyword::Sum),
        "TABLES" => Some(Keyword::Tables),
        "TRUE" => Some(Keyword::True),
        "UNION" => Some(Keyword::Union),
        "UPDATE" => Some(Keyword::Update),
        "WHERE" => Some(Keyword::Where),
        "WITH" => Some(Keyword::With),
        _ => None,
    }
}

///
/// SqlTokenCursor
///
/// Shared reduced-SQL token cursor used by standalone predicate parsing and
/// feature-gated statement parsing.
///
#[derive(Clone, Debug)]
pub(crate) struct SqlTokenCursor {
    tokens: Vec<Token>,
    pos: usize,
}

impl SqlTokenCursor {
    pub(crate) const fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    pub(crate) fn parse_compare_operator(&mut self) -> Result<CompareOp, SqlParseError> {
        let op = match self.peek_kind() {
            Some(TokenKind::Eq) => CompareOp::Eq,
            Some(TokenKind::Ne) => CompareOp::Ne,
            Some(TokenKind::Lt) => CompareOp::Lt,
            Some(TokenKind::Lte) => CompareOp::Lte,
            Some(TokenKind::Gt) => CompareOp::Gt,
            Some(TokenKind::Gte) => CompareOp::Gte,
            _ => {
                return Err(SqlParseError::expected(
                    "one of =, !=, <, <=, >, >=",
                    self.peek_kind(),
                ));
            }
        };

        self.bump();
        Ok(op)
    }

    pub(crate) fn parse_literal(&mut self) -> Result<Value, SqlParseError> {
        match self.bump() {
            Some(TokenKind::StringLiteral(value)) => Ok(Value::Text(value)),
            Some(TokenKind::Number(value)) => parse_number_literal(value.as_str()),
            Some(TokenKind::Keyword(Keyword::Null)) => Ok(Value::Null),
            Some(TokenKind::Keyword(Keyword::True)) => Ok(Value::Bool(true)),
            Some(TokenKind::Keyword(Keyword::False)) => Ok(Value::Bool(false)),
            _ => Err(SqlParseError::expected("literal", self.peek_kind())),
        }
    }

    pub(crate) fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), SqlParseError> {
        if self.eat_keyword(keyword) {
            return Ok(());
        }

        Err(SqlParseError::expected(keyword.as_str(), self.peek_kind()))
    }

    pub(crate) fn expect_identifier(&mut self) -> Result<String, SqlParseError> {
        let Some(TokenKind::Identifier(mut name)) = self.bump() else {
            return Err(SqlParseError::expected("identifier", self.peek_kind()));
        };

        while self.eat_dot() {
            let Some(TokenKind::Identifier(part)) = self.bump() else {
                return Err(SqlParseError::expected(
                    "identifier after '.'",
                    self.peek_kind(),
                ));
            };
            name.push('.');
            name.push_str(part.as_str());
        }

        Ok(name)
    }

    pub(crate) fn expect_lparen(&mut self) -> Result<(), SqlParseError> {
        if self.eat_lparen() {
            return Ok(());
        }

        Err(SqlParseError::expected("(", self.peek_kind()))
    }

    pub(in crate::db) fn expect_rparen(&mut self) -> Result<(), SqlParseError> {
        if self.eat_rparen() {
            return Ok(());
        }

        Err(SqlParseError::expected(")", self.peek_kind()))
    }

    pub(in crate::db) fn eat_keyword(&mut self, keyword: Keyword) -> bool {
        if !self.peek_keyword(keyword) {
            return false;
        }

        self.pos += 1;
        true
    }

    pub(in crate::db) fn eat_identifier_keyword(&mut self, keyword: &str) -> bool {
        if !self.peek_identifier_keyword(keyword) {
            return false;
        }

        self.pos += 1;
        true
    }

    pub(crate) fn peek_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek_kind(), Some(TokenKind::Keyword(found)) if *found == keyword)
    }

    pub(in crate::db) fn peek_identifier_keyword(&self, keyword: &str) -> bool {
        matches!(
            self.peek_kind(),
            Some(TokenKind::Identifier(value)) if value.eq_ignore_ascii_case(keyword)
        )
    }

    pub(in crate::db) fn eat_comma(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Comma)) {
            return false;
        }

        self.pos += 1;
        true
    }

    pub(crate) fn eat_dot(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Dot)) {
            return false;
        }

        self.pos += 1;
        true
    }

    pub(in crate::db) fn eat_lparen(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::LParen)) {
            return false;
        }

        self.pos += 1;
        true
    }

    pub(in crate::db) fn eat_rparen(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::RParen)) {
            return false;
        }

        self.pos += 1;
        true
    }

    pub(in crate::db) fn eat_semicolon(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Semicolon)) {
            return false;
        }

        self.pos += 1;
        true
    }

    #[cfg(feature = "sql")]
    pub(crate) fn eat_star(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Star)) {
            return false;
        }

        self.pos += 1;
        true
    }

    #[cfg(feature = "sql")]
    pub(crate) fn peek_lparen(&self) -> bool {
        matches!(self.peek_kind(), Some(TokenKind::LParen))
    }

    pub(in crate::db) fn peek_unsupported_feature(&self) -> Option<&'static str> {
        match self.peek_kind() {
            Some(TokenKind::Keyword(Keyword::As)) => Some("column/expression aliases"),
            Some(TokenKind::Keyword(Keyword::Describe)) => Some("DESCRIBE modifiers"),
            Some(TokenKind::Keyword(Keyword::Having)) => Some("HAVING"),
            Some(TokenKind::Keyword(Keyword::Insert)) => Some("INSERT"),
            Some(TokenKind::Keyword(Keyword::Join)) => Some("JOIN"),
            Some(TokenKind::Keyword(Keyword::Show)) => {
                Some("SHOW commands beyond SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES/SHOW TABLES")
            }
            Some(TokenKind::Keyword(Keyword::With)) => Some("WITH"),
            Some(TokenKind::Keyword(Keyword::Union | Keyword::Intersect | Keyword::Except)) => {
                Some("UNION/INTERSECT/EXCEPT")
            }
            Some(TokenKind::Keyword(Keyword::Update)) => Some("UPDATE"),
            _ => None,
        }
    }

    pub(crate) fn bump(&mut self) -> Option<TokenKind> {
        let token = self.tokens.get(self.pos)?;
        self.pos += 1;
        Some(token.kind.clone())
    }

    pub(in crate::db) fn peek_kind(&self) -> Option<&TokenKind> {
        self.tokens.get(self.pos).map(|token| &token.kind)
    }

    pub(in crate::db) fn peek_next_kind(&self) -> Option<&TokenKind> {
        self.tokens.get(self.pos + 1).map(|token| &token.kind)
    }

    pub(in crate::db) const fn is_eof(&self) -> bool {
        self.pos >= self.tokens.len()
    }
}

fn parse_number_literal(raw: &str) -> Result<Value, SqlParseError> {
    if raw.contains('.') {
        let decimal =
            Decimal::from_str(raw).map_err(|_| SqlParseError::invalid_numeric_literal(raw))?;
        return Ok(Value::Decimal(decimal));
    }

    if let Ok(value) = raw.parse::<i64>() {
        return Ok(Value::Int(value));
    }
    if let Ok(value) = raw.parse::<u64>() {
        return Ok(Value::Uint(value));
    }
    if let Ok(value) = Decimal::from_str(raw) {
        return Ok(Value::Decimal(value));
    }

    Err(SqlParseError::invalid_numeric_literal(raw))
}
