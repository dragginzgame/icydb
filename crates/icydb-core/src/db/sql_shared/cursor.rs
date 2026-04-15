use crate::{
    db::{
        predicate::CompareOp,
        sql_shared::{
            Keyword, SqlParseError, TokenKind,
            types::{Token, parse_number_literal},
        },
    },
    value::Value,
};

#[cfg_attr(
    doc,
    doc = "SqlTokenCursor\n\nShared SQL token cursor used by standalone predicate parsing and feature-gated statement parsing."
)]
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
                    "one of =, !=, <>, <, <=, >, >=",
                    self.peek_kind(),
                ));
            }
        };

        self.advance();
        Ok(op)
    }

    pub(crate) fn parse_literal(&mut self) -> Result<Value, SqlParseError> {
        if matches!(self.peek_kind(), Some(TokenKind::Minus)) {
            self.advance();

            let Some(TokenKind::Number(value)) = self.peek_kind() else {
                return Err(SqlParseError::expected(
                    "numeric literal after '-'",
                    self.peek_kind(),
                ));
            };
            let literal = parse_number_literal(format!("-{value}").as_str())?;
            self.advance();

            return Ok(literal);
        }

        let literal = match self.peek_kind() {
            Some(TokenKind::StringLiteral(value)) => Value::Text(value.clone()),
            Some(TokenKind::Number(value)) => parse_number_literal(value.as_str())?,
            Some(TokenKind::Keyword(Keyword::Null)) => Value::Null,
            Some(TokenKind::Keyword(Keyword::True)) => Value::Bool(true),
            Some(TokenKind::Keyword(Keyword::False)) => Value::Bool(false),
            _ => return Err(SqlParseError::expected("literal", self.peek_kind())),
        };

        self.advance();

        Ok(literal)
    }

    pub(crate) fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), SqlParseError> {
        if self.eat_keyword(keyword) {
            return Ok(());
        }

        Err(SqlParseError::expected(keyword.as_str(), self.peek_kind()))
    }

    pub(crate) fn expect_identifier(&mut self) -> Result<String, SqlParseError> {
        let Some(TokenKind::Identifier(name)) = self.peek_kind() else {
            return Err(SqlParseError::expected("identifier", self.peek_kind()));
        };
        let mut name = name.clone();
        self.advance();

        while self.eat_dot() {
            let Some(TokenKind::Identifier(part)) = self.peek_kind() else {
                return Err(SqlParseError::expected(
                    "identifier after '.'",
                    self.peek_kind(),
                ));
            };
            let part = part.clone();
            self.advance();
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

    pub(in crate::db) fn eat_plus(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Plus)) {
            return false;
        }

        self.pos += 1;
        true
    }

    pub(in crate::db) fn eat_minus(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Minus)) {
            return false;
        }

        self.pos += 1;
        true
    }

    pub(in crate::db) fn eat_slash(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Slash)) {
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
            Some(TokenKind::Keyword(Keyword::Returning)) => Some("RETURNING"),
            Some(TokenKind::Keyword(Keyword::Show)) => {
                Some("SHOW commands beyond SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES")
            }
            Some(TokenKind::Keyword(Keyword::With)) => Some("WITH"),
            Some(TokenKind::Keyword(Keyword::Union | Keyword::Intersect | Keyword::Except)) => {
                Some("UNION/INTERSECT/EXCEPT")
            }
            Some(TokenKind::Keyword(Keyword::Update)) => Some("UPDATE"),
            _ => None,
        }
    }

    pub(in crate::db) const fn advance(&mut self) -> bool {
        if self.is_eof() {
            return false;
        }

        self.pos += 1;
        true
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
