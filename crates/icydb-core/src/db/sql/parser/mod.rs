//! Module: db::sql::parser
//! Responsibility: reduced SQL statement parsing for deterministic frontend normalization.
//! Does not own: standalone predicate parsing semantics, planner policy, or execution semantics.
//! Boundary: parses one SQL statement into frontend-neutral statement contracts on top of the shared SQL token cursor.

mod clauses;
mod model;
#[cfg(test)]
mod order_expr;
mod projection;
mod statement;

#[cfg(test)]
mod tests;

use crate::{
    db::{
        diagnostics::measure_local_instruction_delta as measure_parse_stage,
        sql_shared::{Keyword, SqlTokenCursor, TokenKind, tokenize_sql},
    },
    value::Value,
};

pub(crate) use crate::db::sql_shared::SqlParseError;
pub(crate) use model::{
    SqlAggregateCall, SqlAggregateKind, SqlAssignment, SqlCaseArm, SqlDeleteStatement,
    SqlDescribeStatement, SqlExplainMode, SqlExplainStatement, SqlExplainTarget, SqlExpr,
    SqlExprBinaryOp, SqlExprUnaryOp, SqlInsertSource, SqlInsertStatement, SqlOrderDirection,
    SqlOrderTerm, SqlProjection, SqlReturningProjection, SqlScalarFunction,
    SqlScalarFunctionCallShape, SqlSelectItem, SqlSelectStatement, SqlShowColumnsStatement,
    SqlShowEntitiesStatement, SqlShowIndexesStatement, SqlStatement, SqlUpdateStatement,
};
#[cfg(test)]
pub(in crate::db) use order_expr::{
    parse_grouped_post_aggregate_order_expr_ast, parse_supported_order_expr_ast,
};

///
/// SqlParsePhaseAttribution
///
/// SqlParsePhaseAttribution records the parser-owned reduced SQL front-end
/// split beneath the top-level compile parse bucket.
/// The statement-shell bucket keeps clause sequencing and trailing validation
/// separate from tokenization and the heavier expression/predicate roots.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SqlParsePhaseAttribution {
    pub tokenize: u64,
    pub statement_shell: u64,
    pub expr: u64,
    pub predicate: u64,
}

/// Parse one reduced SQL statement.
///
/// Parsing is deterministic and normalization-insensitive for keyword casing,
/// insignificant whitespace, and optional one-statement terminator (`;`).
#[cfg(test)]
pub(crate) fn parse_sql(sql: &str) -> Result<SqlStatement, SqlParseError> {
    let (statement, _) = parse_sql_with_attribution(sql)?;

    Ok(statement)
}

/// Parse one reduced SQL statement while reporting the parser-owned
/// tokenization, statement-shell, expression-root, and predicate-root split.
pub(crate) fn parse_sql_with_attribution(
    sql: &str,
) -> Result<(SqlStatement, SqlParsePhaseAttribution), SqlParseError> {
    let (tokenize, tokens) = measure_parse_stage(|| tokenize_sql(sql));
    let tokens = tokens?;
    if tokens.is_empty() {
        return Err(SqlParseError::EmptyInput);
    }

    let mut parser = Parser::new(SqlTokenCursor::new(tokens));
    let (statement_total, statement) = measure_parse_stage(|| {
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
    });
    let statement = statement?;

    let statement_shell = statement_total
        .saturating_sub(parser.attribution.expr)
        .saturating_sub(parser.attribution.predicate);

    Ok((
        statement,
        SqlParsePhaseAttribution {
            tokenize,
            statement_shell,
            expr: parser.attribution.expr,
            predicate: parser.attribution.predicate,
        },
    ))
}

// Parser state over one pre-tokenized SQL statement.
struct Parser {
    cursor: SqlTokenCursor,
    attribution: SqlParsePhaseAttribution,
    next_param_index: usize,
}

impl Parser {
    const fn new(cursor: SqlTokenCursor) -> Self {
        Self {
            cursor,
            attribution: SqlParsePhaseAttribution {
                tokenize: 0,
                statement_shell: 0,
                expr: 0,
                predicate: 0,
            },
            next_param_index: 0,
        }
    }

    fn parse_literal(&mut self) -> Result<Value, SqlParseError> {
        self.cursor.parse_literal()
    }

    fn parse_u32_literal(&mut self, clause: &str) -> Result<u32, SqlParseError> {
        let Some(TokenKind::Number(value)) = self.peek_kind() else {
            return Err(SqlParseError::expected(
                &format!("integer literal after {clause}"),
                self.peek_kind(),
            ));
        };
        let value = value.as_str();

        if value.contains('.') || value.starts_with('-') {
            return Err(SqlParseError::invalid_syntax(format!(
                "{clause} requires a non-negative integer literal"
            )));
        }

        let parsed = value.parse::<u32>().map_err(|_| {
            SqlParseError::invalid_syntax(format!("{clause} value exceeds supported u32 bound"))
        })?;
        self.cursor.advance();

        Ok(parsed)
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

    fn eat_identifier_keyword(&mut self, keyword: &str) -> bool {
        self.cursor.eat_identifier_keyword(keyword)
    }

    fn eat_comma(&mut self) -> bool {
        self.cursor.eat_comma()
    }

    fn eat_plus(&mut self) -> bool {
        self.cursor.eat_plus()
    }

    fn eat_question(&mut self) -> bool {
        self.cursor.eat_question()
    }

    fn eat_minus(&mut self) -> bool {
        self.cursor.eat_minus()
    }

    fn eat_slash(&mut self) -> bool {
        self.cursor.eat_slash()
    }

    fn eat_semicolon(&mut self) -> bool {
        self.cursor.eat_semicolon()
    }

    fn eat_star(&mut self) -> bool {
        self.cursor.eat_star()
    }

    const fn take_param_index(&mut self) -> usize {
        let index = self.next_param_index;
        self.next_param_index = self.next_param_index.saturating_add(1);

        index
    }

    fn peek_keyword(&self, keyword: Keyword) -> bool {
        self.cursor.peek_keyword(keyword)
    }

    fn peek_lparen(&self) -> bool {
        self.cursor.peek_lparen()
    }

    fn peek_unsupported_feature(&self) -> Option<&'static str> {
        sql_unsupported_feature(self.cursor.peek_kind())
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.cursor.peek_kind()
    }

    fn expect_identifier_keyword(&mut self, keyword: &str) -> Result<(), SqlParseError> {
        if self.eat_identifier_keyword(keyword) {
            return Ok(());
        }

        Err(SqlParseError::expected(keyword, self.peek_kind()))
    }

    const fn is_eof(&self) -> bool {
        self.cursor.is_eof()
    }

    fn record_expr_parse_stage<T>(
        &mut self,
        run: impl FnOnce(&mut Self) -> Result<T, SqlParseError>,
    ) -> Result<T, SqlParseError> {
        let (delta, result) = measure_parse_stage(|| run(self));
        self.attribution.expr = self.attribution.expr.saturating_add(delta);

        result
    }

    fn record_predicate_parse_stage<T>(
        &mut self,
        run: impl FnOnce(&mut Self) -> Result<T, SqlParseError>,
    ) -> Result<T, SqlParseError> {
        let (delta, result) = measure_parse_stage(|| run(self));
        self.attribution.predicate = self.attribution.predicate.saturating_add(delta);

        result
    }
}

// Keep reduced-SQL feature-policy labels at the statement parser boundary so
// sql_shared remains a lexical token/cursor utility.
const fn sql_unsupported_feature(kind: Option<&TokenKind>) -> Option<&'static str> {
    match kind {
        Some(TokenKind::Keyword(Keyword::As)) => Some("column/expression aliases"),
        Some(TokenKind::Keyword(Keyword::Describe)) => Some("DESCRIBE modifiers"),
        Some(TokenKind::Keyword(Keyword::Having)) => Some("HAVING"),
        Some(TokenKind::Keyword(Keyword::Insert)) => Some("INSERT"),
        Some(TokenKind::Keyword(Keyword::Join)) => Some("JOIN"),
        Some(TokenKind::Keyword(Keyword::Filter)) => Some("aggregate FILTER clauses"),
        Some(TokenKind::Keyword(Keyword::Over)) => Some("window functions / OVER"),
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
