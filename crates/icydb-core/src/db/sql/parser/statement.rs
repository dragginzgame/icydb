//! Module: db::sql::parser::statement
//! Responsibility: reduced SQL statement-shell parsing and clause-order diagnostics.
//! Does not own: projection item parsing, clause helper internals, or execution semantics.
//! Boundary: keeps statement entry routing and statement-local clause sequencing out of the parser root.

use crate::db::{
    reduced_sql::{Keyword, SqlParseError},
    sql::parser::{
        Parser, SqlAssignment, SqlDeleteStatement, SqlDescribeStatement, SqlExplainMode,
        SqlExplainStatement, SqlExplainTarget, SqlInsertStatement, SqlSelectStatement,
        SqlShowColumnsStatement, SqlShowEntitiesStatement, SqlShowIndexesStatement, SqlStatement,
        SqlUpdateStatement,
    },
};

impl Parser {
    pub(super) fn parse_statement(&mut self) -> Result<SqlStatement, SqlParseError> {
        if self.eat_keyword(Keyword::Select) {
            return Ok(SqlStatement::Select(self.parse_select_statement()?));
        }
        if self.eat_keyword(Keyword::Delete) {
            return Ok(SqlStatement::Delete(self.parse_delete_statement()?));
        }
        if self.eat_keyword(Keyword::Insert) {
            return Ok(SqlStatement::Insert(self.parse_insert_statement()?));
        }
        if self.eat_keyword(Keyword::Update) {
            return Ok(SqlStatement::Update(self.parse_update_statement()?));
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
            "one of SELECT, DELETE, INSERT, UPDATE, EXPLAIN, DESCRIBE, SHOW",
            self.peek_kind(),
        ))
    }

    // Classify one trailing token as a likely out-of-order clause mistake so
    // callers get an actionable parser diagnostic instead of generic EOI.
    pub(super) fn trailing_clause_order_error(
        &self,
        statement: &SqlStatement,
    ) -> Option<SqlParseError> {
        match statement {
            SqlStatement::Select(select) => self.select_clause_order_error(select),
            SqlStatement::Delete(delete) => self.delete_clause_order_error(delete),
            SqlStatement::Insert(_) | SqlStatement::Update(_) => None,
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

    fn select_clause_order_error(&self, statement: &SqlSelectStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order)
            && (statement.limit.is_some() || statement.offset.is_some())
        {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT/OFFSET",
            ));
        }

        None
    }

    fn delete_clause_order_error(&self, statement: &SqlDeleteStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order) && statement.limit.is_some() {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT in DELETE",
            ));
        }

        None
    }

    fn parse_select_statement(&mut self) -> Result<SqlSelectStatement, SqlParseError> {
        let distinct = self.eat_keyword(Keyword::Distinct);
        let (projection, projection_aliases) = self.parse_projection()?;
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
            projection_aliases,
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

        let offset = if self.eat_keyword(Keyword::Offset) {
            Some(self.parse_u32_literal("OFFSET")?)
        } else {
            None
        };

        Ok(SqlDeleteStatement {
            entity,
            predicate,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_insert_statement(&mut self) -> Result<SqlInsertStatement, SqlParseError> {
        self.expect_identifier_keyword("INTO")?;
        let entity = self.expect_identifier()?;
        self.reject_insert_table_alias_if_present()?;

        if !self.peek_lparen() {
            return Err(SqlParseError::unsupported_feature(
                "INSERT without explicit column list",
            ));
        }

        self.expect_lparen()?;
        let columns = self.parse_identifier_list()?;
        self.expect_rparen()?;
        self.expect_identifier_keyword("VALUES")?;
        self.expect_lparen()?;

        let mut values = Vec::new();
        loop {
            values.push(self.parse_literal()?);

            if self.eat_comma() {
                continue;
            }

            break;
        }

        self.expect_rparen()?;

        if columns.len() != values.len() {
            return Err(SqlParseError::invalid_syntax(
                "INSERT column list and VALUES tuple length must match",
            ));
        }

        Ok(SqlInsertStatement {
            entity,
            columns,
            values,
        })
    }

    fn reject_insert_table_alias_if_present(&self) -> Result<(), SqlParseError> {
        if self.peek_keyword(Keyword::As) {
            return Err(SqlParseError::unsupported_feature("table aliases"));
        }

        if matches!(
            self.peek_kind(),
            Some(crate::db::reduced_sql::TokenKind::Identifier(value))
                if !value.eq_ignore_ascii_case("VALUES")
        ) {
            return Err(SqlParseError::unsupported_feature("table aliases"));
        }

        Ok(())
    }

    fn parse_update_statement(&mut self) -> Result<SqlUpdateStatement, SqlParseError> {
        let entity = self.expect_identifier()?;
        self.reject_update_table_alias_if_present()?;
        self.expect_identifier_keyword("SET")?;
        let assignments = self.parse_update_assignments()?;
        let predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        Ok(SqlUpdateStatement {
            entity,
            assignments,
            predicate,
        })
    }

    fn parse_update_assignments(&mut self) -> Result<Vec<SqlAssignment>, SqlParseError> {
        let mut assignments = Vec::new();
        loop {
            let field = self.expect_identifier()?;
            self.expect_assignment_eq()?;
            let value = self.parse_literal()?;
            assignments.push(SqlAssignment { field, value });

            if self.eat_comma() {
                continue;
            }

            break;
        }

        if assignments.is_empty() {
            return Err(SqlParseError::expected(
                "one UPDATE assignment",
                self.peek_kind(),
            ));
        }

        Ok(assignments)
    }

    fn reject_update_table_alias_if_present(&self) -> Result<(), SqlParseError> {
        if self.peek_keyword(Keyword::As) {
            return Err(SqlParseError::unsupported_feature("table aliases"));
        }

        if matches!(
            self.peek_kind(),
            Some(crate::db::reduced_sql::TokenKind::Identifier(value))
                if !value.eq_ignore_ascii_case("SET")
        ) {
            return Err(SqlParseError::unsupported_feature("table aliases"));
        }

        Ok(())
    }

    fn expect_assignment_eq(&mut self) -> Result<(), SqlParseError> {
        if matches!(
            self.peek_kind(),
            Some(crate::db::reduced_sql::TokenKind::Eq)
        ) {
            let _ = self.cursor.advance();
            return Ok(());
        }

        Err(SqlParseError::expected(
            "'=' in UPDATE assignment",
            self.peek_kind(),
        ))
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
}
