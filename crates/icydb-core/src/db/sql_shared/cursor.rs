use crate::{
    db::sql_shared::{
        Keyword, SqlParseError, TokenKind,
        types::{Token, parse_number_literal},
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
        if matches!(self.peek_kind(), Some(TokenKind::BlobLiteral(_))) {
            return self.take_blob_literal();
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

    // Move large SQL blob literal payloads out of the token buffer instead of
    // cloning bytes at the parser boundary. Consumed tokens are never revisited,
    // so replacing the slot with punctuation preserves cursor invariants.
    fn take_blob_literal(&mut self) -> Result<Value, SqlParseError> {
        let Some(token) = self.tokens.get_mut(self.pos) else {
            return Err(SqlParseError::expected("literal", self.peek_kind()));
        };
        let TokenKind::BlobLiteral(bytes) = std::mem::replace(&mut token.kind, TokenKind::Comma)
        else {
            unreachable!("blob literal guard should make the replacement shape exact");
        };
        self.pos += 1;

        Ok(Value::Blob(bytes))
    }

    pub(crate) fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), SqlParseError> {
        if self.eat_keyword(keyword) {
            return Ok(());
        }

        Err(SqlParseError::expected(keyword.as_str(), self.peek_kind()))
    }

    pub(crate) fn expect_identifier(&mut self) -> Result<String, SqlParseError> {
        let mut name = self.take_identifier_segment()?;

        while self.eat_dot() {
            let part = self
                .take_identifier_segment()
                .map_err(|_| SqlParseError::expected("identifier after '.'", self.peek_kind()))?;
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

    // Read one future token kind without cloning the cursor state when parser
    // lookahead only needs local postfix disambiguation.
    pub(in crate::db) fn peek_kind_at(&self, offset: usize) -> Option<&TokenKind> {
        self.tokens
            .get(self.pos.saturating_add(offset))
            .map(|token| &token.kind)
    }

    // Reuse the shared token slice for one-token keyword lookahead so postfix
    // parsers do not clone the whole cursor just to inspect the next token.
    pub(in crate::db) fn peek_keyword_at(&self, offset: usize, keyword: Keyword) -> bool {
        matches!(
            self.peek_kind_at(offset),
            Some(TokenKind::Keyword(found)) if *found == keyword
        )
    }

    pub(in crate::db) fn peek_identifier_keyword(&self, keyword: &str) -> bool {
        matches!(
            self.peek_kind(),
            Some(TokenKind::Identifier(value)) if value.eq_ignore_ascii_case(keyword)
        )
    }

    // Mirror `peek_identifier_keyword` for fixed-offset lookahead so the
    // parser can probe `NOT LIKE` / `NOT ILIKE` without cloning the cursor.
    pub(in crate::db) fn peek_identifier_keyword_at(&self, offset: usize, keyword: &str) -> bool {
        matches!(
            self.peek_kind_at(offset),
            Some(TokenKind::Identifier(value)) if value.eq_ignore_ascii_case(keyword)
        )
    }

    // Move one consumed identifier token out of the cursor buffer so parser
    // hot paths do not clone field and entity names on every successful read.
    fn take_identifier_segment(&mut self) -> Result<String, SqlParseError> {
        let Some(token) = self.tokens.get_mut(self.pos) else {
            return Err(SqlParseError::expected("identifier", self.peek_kind()));
        };
        if !matches!(token.kind, TokenKind::Identifier(_)) {
            return Err(SqlParseError::expected("identifier", self.peek_kind()));
        }

        // The parser never revisits consumed tokens, so a cheap punctuation
        // placeholder is enough to move the owned identifier out safely.
        let TokenKind::Identifier(name) = std::mem::replace(&mut token.kind, TokenKind::Comma)
        else {
            unreachable!("identifier guard should make the replacement shape exact");
        };
        self.pos += 1;

        Ok(name)
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

    pub(in crate::db) fn eat_question(&mut self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Question)) {
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
