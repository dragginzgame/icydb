//! Module: db::sql::parser
//! Responsibility: reduced SQL statement parsing for deterministic frontend normalization.
//! Does not own: standalone predicate parsing semantics, planner policy, or execution semantics.
//! Boundary: parses one SQL statement into frontend-neutral statement contracts on top of the shared reduced-SQL token cursor.

mod clauses;
mod model;
mod projection;
mod statement;

#[cfg(test)]
mod tests;

use crate::{
    db::{
        predicate::{CompareOp, Predicate, parse_predicate_from_cursor},
        reduced_sql::{Keyword, SqlTokenCursor, TokenKind, tokenize_sql},
    },
    value::Value,
};

pub(crate) use crate::db::reduced_sql::SqlParseError;
pub(crate) use model::{
    SqlAggregateCall, SqlAggregateKind, SqlDeleteStatement, SqlDescribeStatement, SqlExplainMode,
    SqlExplainStatement, SqlExplainTarget, SqlHavingClause, SqlHavingSymbol, SqlOrderDirection,
    SqlOrderTerm, SqlProjection, SqlSelectItem, SqlSelectStatement, SqlShowColumnsStatement,
    SqlShowEntitiesStatement, SqlShowIndexesStatement, SqlStatement, SqlTextFunction,
    SqlTextFunctionCall,
};

/// Parse one reduced SQL statement.
///
/// Parsing is deterministic and normalization-insensitive for keyword casing,
/// insignificant whitespace, and optional one-statement terminator (`;`).
pub(crate) fn parse_sql(sql: &str) -> Result<SqlStatement, SqlParseError> {
    let tokens = tokenize_sql(sql)?;
    if tokens.is_empty() {
        return Err(SqlParseError::EmptyInput);
    }

    let mut parser = Parser::new(SqlTokenCursor::new(tokens));
    let statement = parser.parse_statement()?;

    if parser.eat_semicolon() && !parser.is_eof() {
        return Err(SqlParseError::unsupported_feature(
            "multi-statement SQL input",
        ));
    }

    if !parser.is_eof() {
        if let Some(err) = parser.trailing_clause_order_error(&statement) {
            return Err(err);
        }

        if let Some(feature) = parser.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        }

        return Err(SqlParseError::expected_end_of_input(parser.peek_kind()));
    }

    Ok(statement)
}

// Parser state over one pre-tokenized SQL statement.
struct Parser {
    cursor: SqlTokenCursor,
}

impl Parser {
    const fn new(cursor: SqlTokenCursor) -> Self {
        Self { cursor }
    }

    fn parse_predicate(&mut self) -> Result<Predicate, SqlParseError> {
        parse_predicate_from_cursor(&mut self.cursor)
    }

    fn parse_compare_operator(&mut self) -> Result<CompareOp, SqlParseError> {
        self.cursor.parse_compare_operator()
    }

    fn parse_literal(&mut self) -> Result<Value, SqlParseError> {
        self.cursor.parse_literal()
    }

    fn parse_u32_literal(&mut self, clause: &str) -> Result<u32, SqlParseError> {
        let token = self.bump();
        let Some(TokenKind::Number(value)) = token else {
            return Err(SqlParseError::expected(
                &format!("integer literal after {clause}"),
                self.peek_kind(),
            ));
        };

        if value.contains('.') || value.starts_with('-') {
            return Err(SqlParseError::invalid_syntax(format!(
                "{clause} requires a non-negative integer literal"
            )));
        }

        value.parse::<u32>().map_err(|_| {
            SqlParseError::invalid_syntax(format!("{clause} value exceeds supported u32 bound"))
        })
    }

    fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), SqlParseError> {
        self.cursor.expect_keyword(keyword)
    }

    fn expect_identifier(&mut self) -> Result<String, SqlParseError> {
        self.cursor.expect_identifier()
    }

    fn expect_lparen(&mut self) -> Result<(), SqlParseError> {
        self.cursor.expect_lparen()
    }

    fn expect_rparen(&mut self) -> Result<(), SqlParseError> {
        self.cursor.expect_rparen()
    }

    fn eat_keyword(&mut self, keyword: Keyword) -> bool {
        self.cursor.eat_keyword(keyword)
    }

    fn eat_comma(&mut self) -> bool {
        self.cursor.eat_comma()
    }

    fn eat_semicolon(&mut self) -> bool {
        self.cursor.eat_semicolon()
    }

    fn eat_star(&mut self) -> bool {
        self.cursor.eat_star()
    }

    fn peek_keyword(&self, keyword: Keyword) -> bool {
        self.cursor.peek_keyword(keyword)
    }

    fn peek_lparen(&self) -> bool {
        self.cursor.peek_lparen()
    }

    fn peek_unsupported_feature(&self) -> Option<&'static str> {
        self.cursor.peek_unsupported_feature()
    }

    fn bump(&mut self) -> Option<TokenKind> {
        self.cursor.bump()
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.cursor.peek_kind()
    }

    const fn is_eof(&self) -> bool {
        self.cursor.is_eof()
    }
}
