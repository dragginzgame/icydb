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
        sql_shared::{
            Keyword, MAX_SQL_EXPR_DEPTH, SqlExpectedToken, SqlIntegerLiteralClause,
            SqlSyntaxErrorKind, SqlTokenCursor, TokenKind, sql_expr_depth_limit_error,
            tokenize_sql,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::SqlFeatureCode;

pub(crate) use crate::db::sql_shared::SqlParseError;
pub(crate) use model::{
    SqlAggregateCall, SqlAggregateKind, SqlAlterColumnAction,
    SqlAlterTableAddCheckConstraintStatement, SqlAlterTableAddColumnStatement,
    SqlAlterTableAlterColumnStatement, SqlAlterTableDropColumnStatement,
    SqlAlterTableDropConstraintStatement, SqlAlterTableRenameColumnStatement,
    SqlAlterTableValidateConstraintStatement, SqlAssignment, SqlCaseArm,
    SqlCreateIndexExpressionFunction, SqlCreateIndexExpressionKey, SqlCreateIndexKeyItem,
    SqlCreateIndexStatement, SqlCreateIndexUniqueness, SqlDdlSchemaVersionContract,
    SqlDdlStatement, SqlDeleteStatement, SqlDescribeStatement, SqlDropIndexStatement, SqlExpr,
    SqlExprBinaryOp, SqlExprUnaryOp, SqlInsertSource, SqlInsertStatement, SqlIntegrityStatement,
    SqlOrderDirection, SqlOrderTerm, SqlProjection, SqlReturningProjection, SqlScalarFunction,
    SqlScalarFunctionCallShape, SqlSelectItem, SqlSelectStatement, SqlShowColumnsStatement,
    SqlShowConstraintsStatement, SqlShowEntitiesStatement, SqlShowIndexesStatement,
    SqlShowMemoryStatement, SqlShowStoresStatement, SqlStatement, SqlUpdateStatement,
    SqlWriteValue,
};
#[cfg(feature = "sql-explain")]
pub(crate) use model::{SqlExplainMode, SqlExplainStatement, SqlExplainTarget};
#[cfg(test)]
pub(in crate::db::sql) use order_expr::{
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
                SqlFeatureCode::MultiStatementSql,
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

/// Parse one bounded `CHECK INTEGRITY` statement.
///
/// The dedicated entry keeps this administrative capability outside ordinary
/// query/mutation/DDL dispatch while sharing the canonical lexer and parser
/// grammar. Generated endpoint routing may call this same owner directly.
pub(in crate::db) fn parse_integrity_sql(
    sql: &str,
) -> Result<SqlIntegrityStatement, SqlParseError> {
    let tokens = tokenize_sql(sql)?;
    if tokens.is_empty() {
        return Err(SqlParseError::EmptyInput);
    }

    let mut parser = Parser::new(SqlTokenCursor::new(tokens));
    let statement = parser.parse_integrity_statement()?;

    if parser.eat_semicolon() && !parser.is_eof() {
        return Err(SqlParseError::unsupported_feature(
            SqlFeatureCode::MultiStatementSql,
        ));
    }
    if !parser.is_eof() {
        return Err(SqlParseError::expected_end_of_input(parser.peek_kind()));
    }

    Ok(statement)
}

// Parser state over one pre-tokenized SQL statement.
struct Parser {
    cursor: SqlTokenCursor,
    attribution: SqlParsePhaseAttribution,
    next_param_index: usize,
    expr_depth: usize,
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
            expr_depth: 0,
        }
    }

    fn parse_literal(&mut self) -> Result<Value, SqlParseError> {
        self.cursor.parse_literal()
    }

    fn expect_string_literal(&mut self) -> Result<String, SqlParseError> {
        if !matches!(self.peek_kind(), Some(TokenKind::StringLiteral(_))) {
            return Err(SqlParseError::expected(
                SqlExpectedToken::StringLiteral,
                self.peek_kind(),
            ));
        }

        match self.parse_literal()? {
            Value::Text(value) => Ok(value),
            _ => Err(SqlParseError::expected(
                SqlExpectedToken::StringLiteral,
                self.peek_kind(),
            )),
        }
    }

    fn parse_u32_literal(&mut self, clause: SqlIntegerLiteralClause) -> Result<u32, SqlParseError> {
        let Some(TokenKind::Number(value)) = self.peek_kind() else {
            return Err(SqlParseError::expected(
                SqlExpectedToken::IntegerLiteral { clause },
                self.peek_kind(),
            ));
        };
        let value = value.as_str();

        if value.contains('.') || value.starts_with('-') {
            return Err(SqlParseError::invalid_syntax(
                SqlSyntaxErrorKind::IntegerLiteralRequiresNonNegative { clause },
            ));
        }

        let parsed = value.parse::<u32>().map_err(|_| {
            SqlParseError::invalid_syntax(SqlSyntaxErrorKind::IntegerLiteralU32Overflow { clause })
        })?;
        self.cursor.advance();

        Ok(parsed)
    }

    fn parse_u64_literal(&mut self, clause: SqlIntegerLiteralClause) -> Result<u64, SqlParseError> {
        let Some(TokenKind::Number(value)) = self.peek_kind() else {
            return Err(SqlParseError::expected(
                SqlExpectedToken::IntegerLiteral { clause },
                self.peek_kind(),
            ));
        };
        let value = value.as_str();

        if value.contains('.') || value.starts_with('-') {
            return Err(SqlParseError::invalid_syntax(
                SqlSyntaxErrorKind::IntegerLiteralRequiresNonNegative { clause },
            ));
        }

        let parsed = value.parse::<u64>().map_err(|_| {
            SqlParseError::invalid_syntax(SqlSyntaxErrorKind::IntegerLiteralU64Overflow { clause })
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

    const fn enter_sql_expr_depth(&mut self) -> Result<(), SqlParseError> {
        if self.expr_depth >= MAX_SQL_EXPR_DEPTH {
            return Err(sql_expr_depth_limit_error());
        }

        self.expr_depth = self.expr_depth.saturating_add(1);

        Ok(())
    }

    const fn leave_sql_expr_depth(&mut self) {
        self.expr_depth = self.expr_depth.saturating_sub(1);
    }

    fn peek_keyword(&self, keyword: Keyword) -> bool {
        self.cursor.peek_keyword(keyword)
    }

    fn peek_lparen(&self) -> bool {
        self.cursor.peek_lparen()
    }

    fn peek_unsupported_feature(&self) -> Option<SqlFeatureCode> {
        sql_unsupported_feature(self.cursor.peek_kind())
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.cursor.peek_kind()
    }

    fn expect_identifier_keyword(&mut self, keyword: &str) -> Result<(), SqlParseError> {
        if self.eat_identifier_keyword(keyword) {
            return Ok(());
        }

        Err(SqlParseError::expected(
            SqlExpectedToken::identifier_keyword(keyword),
            self.peek_kind(),
        ))
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

// Keep reduced-SQL feature-policy codes at the statement parser boundary so
// sql_shared remains a lexical token/cursor utility.
const fn sql_unsupported_feature(kind: Option<&TokenKind>) -> Option<SqlFeatureCode> {
    match kind {
        Some(TokenKind::Keyword(Keyword::As)) => Some(SqlFeatureCode::ColumnAlias),
        Some(TokenKind::Keyword(Keyword::Describe)) => Some(SqlFeatureCode::DescribeModifier),
        Some(TokenKind::Keyword(Keyword::Having)) => Some(SqlFeatureCode::Having),
        Some(TokenKind::Keyword(Keyword::Insert)) => Some(SqlFeatureCode::Insert),
        Some(TokenKind::Keyword(Keyword::Join)) => Some(SqlFeatureCode::Join),
        Some(TokenKind::Keyword(Keyword::Filter)) => Some(SqlFeatureCode::AggregateFilterClause),
        Some(TokenKind::Keyword(Keyword::Over)) => Some(SqlFeatureCode::WindowFunction),
        Some(TokenKind::Keyword(Keyword::Returning)) => {
            Some(SqlFeatureCode::ReturningUnsupportedShape)
        }
        Some(TokenKind::Keyword(Keyword::Show)) => Some(SqlFeatureCode::ShowUnsupportedCommand),
        Some(TokenKind::Keyword(Keyword::With)) => Some(SqlFeatureCode::With),
        Some(TokenKind::Keyword(Keyword::Union | Keyword::Intersect | Keyword::Except)) => {
            Some(SqlFeatureCode::UnionIntersectExcept)
        }
        Some(TokenKind::Keyword(Keyword::Update)) => Some(SqlFeatureCode::Update),
        _ => None,
    }
}
