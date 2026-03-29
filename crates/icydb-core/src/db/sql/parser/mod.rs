//! Module: db::sql::parser
//! Responsibility: reduced SQL statement parsing for deterministic frontend normalization.
//! Does not own: standalone predicate parsing semantics, planner policy, or execution semantics.
//! Boundary: parses one SQL statement into frontend-neutral statement contracts on top of the shared reduced-SQL token cursor.

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

///
/// SqlStatement
///
/// Reduced SQL statement contract accepted by the current parser baseline.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlStatement {
    Select(SqlSelectStatement),
    Delete(SqlDeleteStatement),
    Explain(SqlExplainStatement),
    Describe(SqlDescribeStatement),
    ShowIndexes(SqlShowIndexesStatement),
    ShowColumns(SqlShowColumnsStatement),
    ShowEntities(SqlShowEntitiesStatement),
}

///
/// SqlProjection
///
/// Projection shape parsed from one `SELECT` statement.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlProjection {
    All,
    Items(Vec<SqlSelectItem>),
}

///
/// SqlSelectItem
///
/// One projection item parsed from one `SELECT` list.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlSelectItem {
    Field(String),
    Aggregate(SqlAggregateCall),
    TextFunction(SqlTextFunctionCall),
}

///
/// SqlHavingSymbol
///
/// One grouped HAVING symbol reference (`group_field` or aggregate terminal).
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlHavingSymbol {
    Field(String),
    Aggregate(SqlAggregateCall),
}

///
/// SqlHavingClause
///
/// One reduced grouped HAVING compare clause.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlHavingClause {
    pub(crate) symbol: SqlHavingSymbol,
    pub(crate) op: CompareOp,
    pub(crate) value: Value,
}

///
/// SqlAggregateKind
///
/// Aggregate operator taxonomy accepted by the reduced parser.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlAggregateKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

///
/// SqlAggregateCall
///
/// Parsed aggregate call projection item.
/// `field = None` is only valid for `COUNT(*)`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlAggregateCall {
    pub(crate) kind: SqlAggregateKind,
    pub(crate) field: Option<String>,
}

///
/// SqlTextFunction
///
/// Reduced text-function taxonomy accepted in scalar SQL projection position.
/// This remains intentionally narrow and only carries the small staged `0.66`
/// projection batches.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlTextFunction {
    Trim,
    Ltrim,
    Rtrim,
    Lower,
    Upper,
    Length,
    Left,
    Right,
    StartsWith,
    EndsWith,
    Contains,
    Position,
    Replace,
    Substring,
}

impl SqlTextFunction {
    /// Resolve one reduced SQL function identifier into one supported unary text function.
    #[must_use]
    const fn from_identifier(identifier: &str) -> Option<Self> {
        if identifier.eq_ignore_ascii_case("trim") {
            return Some(Self::Trim);
        }
        if identifier.eq_ignore_ascii_case("ltrim") {
            return Some(Self::Ltrim);
        }
        if identifier.eq_ignore_ascii_case("rtrim") {
            return Some(Self::Rtrim);
        }
        if identifier.eq_ignore_ascii_case("lower") {
            return Some(Self::Lower);
        }
        if identifier.eq_ignore_ascii_case("upper") {
            return Some(Self::Upper);
        }
        if identifier.eq_ignore_ascii_case("length") {
            return Some(Self::Length);
        }
        if identifier.eq_ignore_ascii_case("left") {
            return Some(Self::Left);
        }
        if identifier.eq_ignore_ascii_case("right") {
            return Some(Self::Right);
        }
        if identifier.eq_ignore_ascii_case("starts_with") {
            return Some(Self::StartsWith);
        }
        if identifier.eq_ignore_ascii_case("ends_with") {
            return Some(Self::EndsWith);
        }
        if identifier.eq_ignore_ascii_case("contains") {
            return Some(Self::Contains);
        }
        if identifier.eq_ignore_ascii_case("position") {
            return Some(Self::Position);
        }
        if identifier.eq_ignore_ascii_case("replace") {
            return Some(Self::Replace);
        }
        if identifier.eq_ignore_ascii_case("substring") {
            return Some(Self::Substring);
        }
        None
    }
}

///
/// SqlTextFunctionCall
///
/// Parsed narrow text-function projection item.
/// Reduced SQL keeps this to one field plus a small fixed literal envelope so
/// the parser does not open a broad nested expression surface.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlTextFunctionCall {
    pub(crate) function: SqlTextFunction,
    pub(crate) field: String,
    pub(crate) literal: Option<Value>,
    pub(crate) literal2: Option<Value>,
    pub(crate) literal3: Option<Value>,
}

///
/// SqlOrderDirection
///
/// Parsed order direction for one `ORDER BY` item.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlOrderDirection {
    Asc,
    Desc,
}

///
/// SqlOrderTerm
///
/// Parsed `ORDER BY` field/direction pair.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlOrderTerm {
    pub(crate) field: String,
    pub(crate) direction: SqlOrderDirection,
}

///
/// SqlSelectStatement
///
/// Canonical parsed `SELECT` statement shape for reduced SQL.
///
/// This contract is frontend-only and intentionally schema-agnostic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlSelectStatement {
    pub(crate) entity: String,
    pub(crate) projection: SqlProjection,
    pub(crate) predicate: Option<Predicate>,
    pub(crate) distinct: bool,
    pub(crate) group_by: Vec<String>,
    pub(crate) having: Vec<SqlHavingClause>,
    pub(crate) order_by: Vec<SqlOrderTerm>,
    pub(crate) limit: Option<u32>,
    pub(crate) offset: Option<u32>,
}

///
/// SqlDeleteStatement
///
/// Canonical parsed `DELETE` statement shape for reduced SQL.
///
/// This contract keeps delete-mode clause policy explicit.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlDeleteStatement {
    pub(crate) entity: String,
    pub(crate) predicate: Option<Predicate>,
    pub(crate) order_by: Vec<SqlOrderTerm>,
    pub(crate) limit: Option<u32>,
}

///
/// SqlExplainMode
///
/// Reduced EXPLAIN render mode selector.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlExplainMode {
    Plan,
    Execution,
    Json,
}

///
/// SqlExplainTarget
///
/// Statement forms accepted behind one `EXPLAIN` prefix.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlExplainTarget {
    Select(SqlSelectStatement),
    Delete(SqlDeleteStatement),
}

///
/// SqlExplainStatement
///
/// Canonical parsed `EXPLAIN` statement.
///
/// Explain remains a wrapper over one executable reduced SQL statement.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlExplainStatement {
    pub(crate) mode: SqlExplainMode,
    pub(crate) statement: SqlExplainTarget,
}

///
/// SqlDescribeStatement
///
/// Canonical parsed `DESCRIBE` statement.
/// Carries one schema entity identifier for typed session introspection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlDescribeStatement {
    pub(crate) entity: String,
}

///
/// SqlShowIndexesStatement
///
/// Canonical parsed `SHOW INDEXES` statement.
/// Carries one schema entity identifier for typed session introspection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlShowIndexesStatement {
    pub(crate) entity: String,
}

///
/// SqlShowColumnsStatement
///
/// Canonical parsed `SHOW COLUMNS` statement.
/// Carries one schema entity identifier for typed session introspection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlShowColumnsStatement {
    pub(crate) entity: String,
}

///
/// SqlShowEntitiesStatement
///
/// Canonical parsed `SHOW ENTITIES` statement.
/// This lane carries no entity identifier and targets SQL helper introspection.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlShowEntitiesStatement;

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

    fn parse_statement(&mut self) -> Result<SqlStatement, SqlParseError> {
        if self.eat_keyword(Keyword::Select) {
            return Ok(SqlStatement::Select(self.parse_select_statement()?));
        }
        if self.eat_keyword(Keyword::Delete) {
            return Ok(SqlStatement::Delete(self.parse_delete_statement()?));
        }
        if self.eat_keyword(Keyword::Explain) {
            return Ok(SqlStatement::Explain(self.parse_explain_statement()?));
        }
        if self.eat_keyword(Keyword::Describe) {
            return Ok(SqlStatement::Describe(self.parse_describe_statement()?));
        }
        if self.eat_keyword(Keyword::Show) {
            return self.parse_show_statement();
        }

        if let Some(feature) = self.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        }

        Err(SqlParseError::expected(
            "one of SELECT, DELETE, EXPLAIN, DESCRIBE, SHOW",
            self.peek_kind(),
        ))
    }

    fn parse_show_statement(&mut self) -> Result<SqlStatement, SqlParseError> {
        if self.eat_keyword(Keyword::Indexes) {
            return Ok(SqlStatement::ShowIndexes(
                self.parse_show_indexes_statement()?,
            ));
        }
        if self.eat_keyword(Keyword::Columns) {
            return Ok(SqlStatement::ShowColumns(
                self.parse_show_columns_statement()?,
            ));
        }
        if self.eat_keyword(Keyword::Entities) {
            return Ok(SqlStatement::ShowEntities(SqlShowEntitiesStatement));
        }

        Err(SqlParseError::unsupported_feature(
            "SHOW commands beyond SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES",
        ))
    }

    fn parse_explain_statement(&mut self) -> Result<SqlExplainStatement, SqlParseError> {
        let mode = if self.eat_keyword(Keyword::Execution) {
            SqlExplainMode::Execution
        } else if self.eat_keyword(Keyword::Json) {
            SqlExplainMode::Json
        } else {
            SqlExplainMode::Plan
        };

        let statement = if self.eat_keyword(Keyword::Select) {
            SqlExplainTarget::Select(self.parse_select_statement()?)
        } else if self.eat_keyword(Keyword::Delete) {
            SqlExplainTarget::Delete(self.parse_delete_statement()?)
        } else if let Some(feature) = self.peek_unsupported_feature() {
            return Err(SqlParseError::unsupported_feature(feature));
        } else {
            return Err(SqlParseError::expected(
                "one of SELECT, DELETE",
                self.peek_kind(),
            ));
        };

        Ok(SqlExplainStatement { mode, statement })
    }

    // Classify one trailing token as a likely out-of-order clause mistake so
    // callers get an actionable parser diagnostic instead of generic EOI.
    fn trailing_clause_order_error(&self, statement: &SqlStatement) -> Option<SqlParseError> {
        match statement {
            SqlStatement::Select(select) => self.select_clause_order_error(select),
            SqlStatement::Delete(delete) => self.delete_clause_order_error(delete),
            SqlStatement::Explain(explain) => match &explain.statement {
                SqlExplainTarget::Select(select) => self.select_clause_order_error(select),
                SqlExplainTarget::Delete(delete) => self.delete_clause_order_error(delete),
            },
            SqlStatement::Describe(_) => {
                Some(SqlParseError::unsupported_feature("DESCRIBE modifiers"))
            }
            SqlStatement::ShowIndexes(_) => {
                Some(SqlParseError::unsupported_feature("SHOW INDEXES modifiers"))
            }
            SqlStatement::ShowColumns(_) => {
                Some(SqlParseError::unsupported_feature("SHOW COLUMNS modifiers"))
            }
            SqlStatement::ShowEntities(_) => Some(SqlParseError::unsupported_feature(
                "SHOW ENTITIES modifiers",
            )),
        }
    }

    fn select_clause_order_error(&self, statement: &SqlSelectStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order)
            && (statement.limit.is_some() || statement.offset.is_some())
        {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT/OFFSET; \
                 try: SELECT ... ORDER BY <field> [ASC|DESC] LIMIT <n> [OFFSET <n>]",
            ));
        }

        None
    }

    fn delete_clause_order_error(&self, statement: &SqlDeleteStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order) && statement.limit.is_some() {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT in DELETE statements; \
                 try: DELETE ... ORDER BY <field> [ASC|DESC] LIMIT <n>",
            ));
        }

        None
    }

    fn parse_select_statement(&mut self) -> Result<SqlSelectStatement, SqlParseError> {
        let distinct = self.eat_keyword(Keyword::Distinct);
        let projection = self.parse_projection()?;
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        self.reject_table_alias_if_present()?;

        // Phase 1: parse predicate and grouping clauses in canonical sequence.
        let predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        let group_by = if self.eat_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By)?;
            self.parse_identifier_list()?
        } else {
            Vec::new()
        };

        let having = if self.eat_keyword(Keyword::Having) {
            self.parse_having_clauses()?
        } else {
            Vec::new()
        };

        // Phase 2: parse ordering and window clauses.
        let order_by = if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_terms()?
        } else {
            Vec::new()
        };

        let limit = if self.eat_keyword(Keyword::Limit) {
            Some(self.parse_u32_literal("LIMIT")?)
        } else {
            None
        };

        let offset = if self.eat_keyword(Keyword::Offset) {
            Some(self.parse_u32_literal("OFFSET")?)
        } else {
            None
        };

        Ok(SqlSelectStatement {
            entity,
            projection,
            predicate,
            distinct,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_delete_statement(&mut self) -> Result<SqlDeleteStatement, SqlParseError> {
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        self.reject_table_alias_if_present()?;

        let predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        let order_by = if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_terms()?
        } else {
            Vec::new()
        };

        let limit = if self.eat_keyword(Keyword::Limit) {
            Some(self.parse_u32_literal("LIMIT")?)
        } else {
            None
        };

        if self.eat_keyword(Keyword::Offset) {
            return Err(SqlParseError::unsupported_feature("DELETE ... OFFSET"));
        }

        Ok(SqlDeleteStatement {
            entity,
            predicate,
            order_by,
            limit,
        })
    }

    fn parse_describe_statement(&mut self) -> Result<SqlDescribeStatement, SqlParseError> {
        let entity = self.expect_identifier()?;

        Ok(SqlDescribeStatement { entity })
    }

    fn parse_show_indexes_statement(&mut self) -> Result<SqlShowIndexesStatement, SqlParseError> {
        let entity = self.expect_identifier()?;

        Ok(SqlShowIndexesStatement { entity })
    }

    fn parse_show_columns_statement(&mut self) -> Result<SqlShowColumnsStatement, SqlParseError> {
        let entity = self.expect_identifier()?;

        Ok(SqlShowColumnsStatement { entity })
    }

    fn parse_projection(&mut self) -> Result<SqlProjection, SqlParseError> {
        if self.eat_star() {
            return Ok(SqlProjection::All);
        }

        let mut items = Vec::new();
        loop {
            items.push(self.parse_select_item()?);

            if self.eat_keyword(Keyword::As) {
                return Err(SqlParseError::unsupported_feature(
                    "column/expression aliases",
                ));
            }
            if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
                return Err(SqlParseError::unsupported_feature(
                    "column/expression aliases",
                ));
            }

            if self.eat_comma() {
                continue;
            }

            break;
        }

        if items.is_empty() {
            return Err(SqlParseError::expected(
                "one projection item",
                self.peek_kind(),
            ));
        }

        Ok(SqlProjection::Items(items))
    }

    fn parse_select_item(&mut self) -> Result<SqlSelectItem, SqlParseError> {
        if let Some(kind) = self.parse_aggregate_kind() {
            return Ok(SqlSelectItem::Aggregate(self.parse_aggregate_call(kind)?));
        }

        let field = self.expect_identifier()?;
        if self.peek_lparen() {
            let Some(function) = SqlTextFunction::from_identifier(field.as_str()) else {
                return Err(SqlParseError::unsupported_feature(
                    "SQL function namespace beyond supported aggregate or scalar text projection forms",
                ));
            };

            return Ok(SqlSelectItem::TextFunction(
                self.parse_text_function_call(function)?,
            ));
        }

        Ok(SqlSelectItem::Field(field))
    }

    fn parse_aggregate_kind(&self) -> Option<SqlAggregateKind> {
        match self.peek_kind() {
            Some(TokenKind::Keyword(Keyword::Count)) => Some(SqlAggregateKind::Count),
            Some(TokenKind::Keyword(Keyword::Sum)) => Some(SqlAggregateKind::Sum),
            Some(TokenKind::Keyword(Keyword::Avg)) => Some(SqlAggregateKind::Avg),
            Some(TokenKind::Keyword(Keyword::Min)) => Some(SqlAggregateKind::Min),
            Some(TokenKind::Keyword(Keyword::Max)) => Some(SqlAggregateKind::Max),
            _ => None,
        }
    }

    fn parse_aggregate_call(
        &mut self,
        kind: SqlAggregateKind,
    ) -> Result<SqlAggregateCall, SqlParseError> {
        self.bump();
        self.expect_lparen()?;

        if self.eat_keyword(Keyword::Distinct) {
            return Err(SqlParseError::unsupported_feature(
                "DISTINCT aggregate qualifiers",
            ));
        }

        let field = if kind == SqlAggregateKind::Count && self.eat_star() {
            None
        } else {
            Some(self.expect_identifier()?)
        };

        self.expect_rparen()?;

        Ok(SqlAggregateCall { kind, field })
    }

    fn parse_text_function_call(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlTextFunctionCall, SqlParseError> {
        self.expect_lparen()?;
        let (field, literal, literal2, literal3) = match function {
            SqlTextFunction::Trim
            | SqlTextFunction::Ltrim
            | SqlTextFunction::Rtrim
            | SqlTextFunction::Lower
            | SqlTextFunction::Upper
            | SqlTextFunction::Length => (self.expect_identifier()?, None, None, None),
            SqlTextFunction::Left
            | SqlTextFunction::Right
            | SqlTextFunction::StartsWith
            | SqlTextFunction::EndsWith
            | SqlTextFunction::Contains => {
                let field = self.expect_identifier()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(
                        "',' between text function arguments",
                        self.peek_kind(),
                    ));
                }

                (field, Some(self.parse_literal()?), None, None)
            }
            SqlTextFunction::Position => {
                let literal = self.parse_literal()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(
                        "',' between text function arguments",
                        self.peek_kind(),
                    ));
                }

                (self.expect_identifier()?, Some(literal), None, None)
            }
            SqlTextFunction::Replace => {
                let field = self.expect_identifier()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(
                        "',' between text function arguments",
                        self.peek_kind(),
                    ));
                }
                let from = self.parse_literal()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(
                        "',' between text function arguments",
                        self.peek_kind(),
                    ));
                }

                (field, Some(from), Some(self.parse_literal()?), None)
            }
            SqlTextFunction::Substring => {
                let field = self.expect_identifier()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(
                        "',' between text function arguments",
                        self.peek_kind(),
                    ));
                }
                let start = self.parse_literal()?;
                if !self.eat_comma() {
                    self.expect_rparen()?;

                    return Ok(SqlTextFunctionCall {
                        function,
                        field,
                        literal: Some(start),
                        literal2: None,
                        literal3: None,
                    });
                }

                (field, Some(start), Some(self.parse_literal()?), None)
            }
        };
        self.expect_rparen()?;

        Ok(SqlTextFunctionCall {
            function,
            field,
            literal,
            literal2,
            literal3,
        })
    }

    fn parse_order_terms(&mut self) -> Result<Vec<SqlOrderTerm>, SqlParseError> {
        let mut terms = Vec::new();
        loop {
            let field = self.expect_identifier()?;
            let direction = if self.eat_keyword(Keyword::Desc) {
                SqlOrderDirection::Desc
            } else {
                self.eat_keyword(Keyword::Asc);
                SqlOrderDirection::Asc
            };

            terms.push(SqlOrderTerm { field, direction });
            if !self.eat_comma() {
                break;
            }
        }

        Ok(terms)
    }

    fn parse_having_clauses(&mut self) -> Result<Vec<SqlHavingClause>, SqlParseError> {
        let mut clauses = vec![self.parse_having_clause()?];
        while self.eat_keyword(Keyword::And) {
            clauses.push(self.parse_having_clause()?);
        }

        if self.peek_keyword(Keyword::Or) || self.peek_keyword(Keyword::Not) {
            return Err(SqlParseError::unsupported_feature(
                "HAVING boolean operators beyond AND",
            ));
        }

        Ok(clauses)
    }

    fn parse_having_clause(&mut self) -> Result<SqlHavingClause, SqlParseError> {
        let symbol = self.parse_having_symbol()?;

        if self.eat_keyword(Keyword::Is) {
            let is_not = self.eat_keyword(Keyword::Not);
            self.expect_keyword(Keyword::Null)?;

            return Ok(SqlHavingClause {
                symbol,
                op: if is_not { CompareOp::Ne } else { CompareOp::Eq },
                value: Value::Null,
            });
        }

        let op = self.parse_compare_operator()?;
        let value = self.parse_literal()?;

        Ok(SqlHavingClause { symbol, op, value })
    }

    fn parse_having_symbol(&mut self) -> Result<SqlHavingSymbol, SqlParseError> {
        if let Some(kind) = self.parse_aggregate_kind() {
            return Ok(SqlHavingSymbol::Aggregate(self.parse_aggregate_call(kind)?));
        }

        let field = self.expect_identifier()?;
        if self.peek_lparen() {
            return Err(SqlParseError::unsupported_feature(
                "SQL function namespace beyond supported aggregate forms",
            ));
        }

        Ok(SqlHavingSymbol::Field(field))
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, SqlParseError> {
        let mut fields = vec![self.expect_identifier()?];
        while self.eat_comma() {
            fields.push(self.expect_identifier()?);
        }

        Ok(fields)
    }

    // Keep reduced-parser table ownership explicit: aliases are intentionally
    // unsupported in this baseline and must fail closed.
    fn reject_table_alias_if_present(&self) -> Result<(), SqlParseError> {
        if self.peek_keyword(Keyword::As)
            || matches!(self.peek_kind(), Some(TokenKind::Identifier(_)))
        {
            return Err(SqlParseError::unsupported_feature("table aliases"));
        }

        Ok(())
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
