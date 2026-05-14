//! Module: db::sql::parser::statement
//! Responsibility: reduced SQL statement-shell parsing and clause-order diagnostics.
//! Does not own: projection item parsing, clause helper internals, or execution semantics.
//! Boundary: keeps statement entry routing and statement-local clause sequencing out of the parser root.

mod delete;
mod insert;
mod select;
mod update;

use crate::db::{
    sql::parser::{
        Parser, SqlCreateIndexStatement, SqlCreateIndexUniqueness, SqlDdlStatement,
        SqlDeleteStatement, SqlDescribeStatement, SqlExplainMode, SqlExplainStatement,
        SqlExplainTarget, SqlSelectStatement, SqlShowColumnsStatement, SqlShowEntitiesStatement,
        SqlShowIndexesStatement, SqlStatement, SqlUpdateStatement,
    },
    sql_shared::{Keyword, SqlParseError, TokenKind},
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
        if self.eat_keyword(Keyword::Create) {
            return Ok(SqlStatement::Ddl(self.parse_create_statement()?));
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
            "one of SELECT, DELETE, INSERT, UPDATE, CREATE, EXPLAIN, DESCRIBE, SHOW",
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
            SqlStatement::Insert(_) => None,
            SqlStatement::Update(update) => self.update_clause_order_error(update),
            SqlStatement::Ddl(ddl) => Some(self.ddl_clause_order_error(ddl)),
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

    fn parse_create_statement(&mut self) -> Result<SqlDdlStatement, SqlParseError> {
        if self.eat_keyword(Keyword::Unique) {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL CREATE UNIQUE INDEX",
            ));
        }
        if !self.eat_keyword(Keyword::Index) {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL CREATE statements beyond CREATE INDEX",
            ));
        }

        Ok(SqlDdlStatement::CreateIndex(
            self.parse_create_index_statement()?,
        ))
    }

    fn parse_create_index_statement(&mut self) -> Result<SqlCreateIndexStatement, SqlParseError> {
        let name = self.expect_identifier()?;
        self.expect_keyword(Keyword::On)?;
        let entity = self.expect_identifier()?;
        self.expect_lparen()?;
        let field_path = self.expect_identifier()?;

        if self.peek_lparen() {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL expression index keys",
            ));
        }
        if self.eat_comma() {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL multi-field CREATE INDEX keys",
            ));
        }

        self.expect_rparen()?;

        if self.peek_keyword(Keyword::Where) {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL filtered CREATE INDEX",
            ));
        }

        Ok(SqlCreateIndexStatement {
            name,
            entity,
            field_path,
            uniqueness: SqlCreateIndexUniqueness::NonUnique,
        })
    }

    fn ddl_clause_order_error(&self, statement: &SqlDdlStatement) -> SqlParseError {
        match statement {
            SqlDdlStatement::CreateIndex(_) if self.peek_keyword(Keyword::Where) => {
                SqlParseError::unsupported_feature("SQL DDL filtered CREATE INDEX")
            }
            SqlDdlStatement::CreateIndex(_) => {
                SqlParseError::unsupported_feature("CREATE INDEX modifiers")
            }
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
        if self.eat_keyword(Keyword::Tables) {
            return Ok(SqlStatement::ShowEntities(SqlShowEntitiesStatement));
        }

        Err(SqlParseError::unsupported_feature(
            "SHOW commands beyond SHOW INDEXES/SHOW COLUMNS/SHOW ENTITIES/SHOW TABLES",
        ))
    }

    fn parse_explain_statement(&mut self) -> Result<SqlExplainStatement, SqlParseError> {
        let (mode, verbose) = if self.eat_keyword(Keyword::Execution) {
            (
                SqlExplainMode::Execution,
                self.eat_keyword(Keyword::Verbose),
            )
        } else if self.eat_keyword(Keyword::Json) {
            (SqlExplainMode::Json, false)
        } else {
            (SqlExplainMode::Plan, false)
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

        Ok(SqlExplainStatement {
            mode,
            verbose,
            statement,
        })
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

    fn update_clause_order_error(&self, statement: &SqlUpdateStatement) -> Option<SqlParseError> {
        if self.peek_keyword(Keyword::Order)
            && (statement.limit.is_some() || statement.offset.is_some())
        {
            return Some(SqlParseError::invalid_syntax(
                "ORDER BY must appear before LIMIT/OFFSET in UPDATE",
            ));
        }
        if self.peek_keyword(Keyword::Limit) && statement.offset.is_some() {
            return Some(SqlParseError::invalid_syntax(
                "LIMIT must appear before OFFSET in UPDATE",
            ));
        }

        None
    }

    pub(super) fn parse_optional_table_alias(&mut self) -> Result<Option<String>, SqlParseError> {
        if self.eat_keyword(Keyword::As) {
            return self.expect_identifier().map(Some);
        }

        if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            let Some(TokenKind::Identifier(value)) = self.peek_kind() else {
                unreachable!();
            };
            if matches!(
                value.as_str().to_ascii_uppercase().as_str(),
                "SET" | "VALUES"
            ) {
                return Ok(None);
            }

            return self.expect_identifier().map(Some);
        }

        Ok(None)
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
