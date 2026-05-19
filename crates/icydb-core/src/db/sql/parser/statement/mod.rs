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
        Parser, SqlAlterColumnAction, SqlCreateIndexExpressionFunction,
        SqlCreateIndexExpressionKey, SqlCreateIndexKeyItem, SqlCreateIndexStatement,
        SqlCreateIndexUniqueness, SqlDdlStatement, SqlDeleteStatement, SqlDescribeStatement,
        SqlDropIndexStatement, SqlExplainMode, SqlExplainStatement, SqlExplainTarget,
        SqlSelectStatement, SqlShowColumnsStatement, SqlShowEntitiesStatement,
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
        if self.eat_keyword(Keyword::Drop) {
            return Ok(SqlStatement::Ddl(self.parse_drop_statement()?));
        }
        if self.eat_identifier_keyword("ALTER") {
            return Ok(SqlStatement::Ddl(self.parse_alter_statement()?));
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
            "one of SELECT, DELETE, INSERT, UPDATE, CREATE, DROP, ALTER, EXPLAIN, DESCRIBE, SHOW",
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
            SqlStatement::Ddl(ddl) => Some(Self::ddl_clause_order_error(ddl)),
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
        let uniqueness = if self.eat_keyword(Keyword::Unique) {
            SqlCreateIndexUniqueness::Unique
        } else {
            SqlCreateIndexUniqueness::NonUnique
        };
        if !self.eat_keyword(Keyword::Index) {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL CREATE statements beyond CREATE INDEX",
            ));
        }

        Ok(SqlDdlStatement::CreateIndex(
            self.parse_create_index_statement(uniqueness)?,
        ))
    }

    fn parse_create_index_statement(
        &mut self,
        uniqueness: SqlCreateIndexUniqueness,
    ) -> Result<SqlCreateIndexStatement, SqlParseError> {
        let if_not_exists = self.parse_create_index_if_not_exists()?;
        let name = self.expect_identifier()?;
        self.expect_keyword(Keyword::On)?;
        let entity = self.expect_identifier()?;
        self.expect_lparen()?;
        let key_items = self.parse_create_index_key_items()?;
        self.expect_rparen()?;
        let predicate_sql = self.parse_create_index_predicate_sql()?;

        Ok(SqlCreateIndexStatement {
            name,
            entity,
            key_items,
            predicate_sql,
            uniqueness,
            if_not_exists,
        })
    }

    fn parse_create_index_predicate_sql(&mut self) -> Result<Option<String>, SqlParseError> {
        if !self.eat_keyword(Keyword::Where) {
            return Ok(None);
        }
        let predicate_sql = self.cursor.remaining_sql_until_semicolon();
        let _ = self.parse_where_expr()?;

        Ok(Some(predicate_sql))
    }

    fn parse_create_index_key_items(
        &mut self,
    ) -> Result<Vec<SqlCreateIndexKeyItem>, SqlParseError> {
        let mut key_items = Vec::new();
        loop {
            let key_item = self.parse_create_index_key_item()?;
            if self.eat_keyword(Keyword::Asc) {
                // ASC is the current physical key default; keep it as syntax
                // sugar rather than a stored DDL contract.
            } else if self.peek_keyword(Keyword::Desc) {
                return Err(SqlParseError::unsupported_feature(
                    "SQL DDL CREATE INDEX key ordering modifiers",
                ));
            }
            key_items.push(key_item);

            if !self.eat_comma() {
                break;
            }
        }

        Ok(key_items)
    }

    fn parse_create_index_key_item(&mut self) -> Result<SqlCreateIndexKeyItem, SqlParseError> {
        let head = self.expect_identifier()?;
        if !self.peek_lparen() {
            return Ok(SqlCreateIndexKeyItem::FieldPath(head));
        }

        let Some(function) = SqlCreateIndexExpressionFunction::parse(head.as_str()) else {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL expression index functions beyond LOWER, UPPER, TRIM",
            ));
        };
        self.expect_lparen()?;
        let field_path = self.expect_identifier()?;
        self.expect_rparen()?;

        Ok(SqlCreateIndexKeyItem::Expression(
            SqlCreateIndexExpressionKey {
                function,
                field_path,
            },
        ))
    }

    fn parse_create_index_if_not_exists(&mut self) -> Result<bool, SqlParseError> {
        if self.eat_identifier_keyword("IF") {
            if !self.eat_keyword(Keyword::Not) || !self.eat_identifier_keyword("EXISTS") {
                return Err(SqlParseError::unsupported_feature(
                    "CREATE INDEX IF NOT EXISTS",
                ));
            }

            return Ok(true);
        }

        Ok(false)
    }

    fn parse_drop_statement(&mut self) -> Result<SqlDdlStatement, SqlParseError> {
        if !self.eat_keyword(Keyword::Index) {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL DROP statements beyond DROP INDEX",
            ));
        }

        Ok(SqlDdlStatement::DropIndex(
            self.parse_drop_index_statement()?,
        ))
    }

    fn parse_drop_index_statement(&mut self) -> Result<SqlDropIndexStatement, SqlParseError> {
        let if_exists = self.parse_drop_index_if_exists()?;
        let name = self.expect_identifier()?;
        let entity = if self.eat_keyword(Keyword::On) {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        Ok(SqlDropIndexStatement {
            name,
            entity,
            if_exists,
        })
    }

    fn parse_drop_index_if_exists(&mut self) -> Result<bool, SqlParseError> {
        if self.eat_identifier_keyword("IF") {
            if !self.eat_identifier_keyword("EXISTS") {
                return Err(SqlParseError::unsupported_feature("DROP INDEX IF EXISTS"));
            }

            return Ok(true);
        }

        Ok(false)
    }

    fn parse_alter_statement(&mut self) -> Result<SqlDdlStatement, SqlParseError> {
        if !self.eat_identifier_keyword("TABLE") {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL ALTER statements beyond ALTER TABLE",
            ));
        }

        let entity = self.expect_identifier()?;
        if self.eat_identifier_keyword("ADD") {
            return Ok(SqlDdlStatement::AlterTableAddColumn(
                self.parse_alter_table_add_column_statement(entity)?,
            ));
        }
        if self.eat_keyword(Keyword::Drop) {
            return Ok(SqlDdlStatement::AlterTableDropColumn(
                self.parse_alter_table_drop_column_statement(entity)?,
            ));
        }
        if self.eat_identifier_keyword("ALTER") {
            return Ok(SqlDdlStatement::AlterTableAlterColumn(
                self.parse_alter_table_alter_column_statement(entity)?,
            ));
        }
        if self.eat_identifier_keyword("RENAME") {
            return Ok(SqlDdlStatement::AlterTableRenameColumn(
                self.parse_alter_table_rename_column_statement(entity)?,
            ));
        }

        Err(SqlParseError::unsupported_feature(
            "SQL DDL ALTER TABLE statements beyond ADD COLUMN, ALTER COLUMN, DROP COLUMN, and RENAME COLUMN",
        ))
    }

    fn parse_alter_table_add_column_statement(
        &mut self,
        entity: String,
    ) -> Result<crate::db::sql::parser::SqlAlterTableAddColumnStatement, SqlParseError> {
        if !self.eat_identifier_keyword("COLUMN") {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL ALTER TABLE ADD statements beyond ADD COLUMN",
            ));
        }
        let column_name = self.expect_identifier()?;
        let column_type = self.expect_identifier()?;
        let mut nullable = true;
        let mut default = None;

        loop {
            if self.eat_identifier_keyword("DEFAULT") {
                if default.is_some() {
                    return Err(SqlParseError::unsupported_feature(
                        "ALTER TABLE ADD COLUMN duplicate DEFAULT clauses",
                    ));
                }
                default = Some(self.parse_literal()?);
            } else if self.eat_keyword(Keyword::Not) {
                self.expect_keyword(Keyword::Null)?;
                nullable = false;
            } else if self.eat_keyword(Keyword::Null) {
                nullable = true;
            } else {
                break;
            }
        }

        Ok(crate::db::sql::parser::SqlAlterTableAddColumnStatement {
            entity,
            column_name,
            column_type,
            nullable,
            default,
        })
    }

    fn parse_alter_table_alter_column_statement(
        &mut self,
        entity: String,
    ) -> Result<crate::db::sql::parser::SqlAlterTableAlterColumnStatement, SqlParseError> {
        if !self.eat_identifier_keyword("COLUMN") {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL ALTER TABLE ALTER statements beyond ALTER COLUMN",
            ));
        }
        let column_name = self.expect_identifier()?;
        let action = if self.eat_identifier_keyword("SET") {
            if self.eat_identifier_keyword("DEFAULT") {
                SqlAlterColumnAction::SetDefault(self.parse_literal()?)
            } else if self.eat_keyword(Keyword::Not) {
                self.expect_keyword(Keyword::Null)?;
                SqlAlterColumnAction::SetNotNull
            } else {
                return Err(SqlParseError::unsupported_feature(
                    "ALTER TABLE ALTER COLUMN SET actions beyond DEFAULT and NOT NULL",
                ));
            }
        } else if self.eat_keyword(Keyword::Drop) {
            if self.eat_identifier_keyword("DEFAULT") {
                SqlAlterColumnAction::DropDefault
            } else if self.eat_keyword(Keyword::Not) {
                self.expect_keyword(Keyword::Null)?;
                SqlAlterColumnAction::DropNotNull
            } else {
                return Err(SqlParseError::unsupported_feature(
                    "ALTER TABLE ALTER COLUMN DROP actions beyond DEFAULT and NOT NULL",
                ));
            }
        } else {
            return Err(SqlParseError::unsupported_feature(
                "ALTER TABLE ALTER COLUMN actions beyond SET/DROP DEFAULT and SET/DROP NOT NULL",
            ));
        };

        Ok(crate::db::sql::parser::SqlAlterTableAlterColumnStatement {
            entity,
            column_name,
            action,
        })
    }

    fn parse_alter_table_drop_column_statement(
        &mut self,
        entity: String,
    ) -> Result<crate::db::sql::parser::SqlAlterTableDropColumnStatement, SqlParseError> {
        if !self.eat_identifier_keyword("COLUMN") {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL ALTER TABLE DROP statements beyond DROP COLUMN",
            ));
        }
        let if_exists = self.parse_drop_column_if_exists()?;
        let column_name = self.expect_identifier()?;

        Ok(crate::db::sql::parser::SqlAlterTableDropColumnStatement {
            entity,
            column_name,
            if_exists,
        })
    }

    fn parse_drop_column_if_exists(&mut self) -> Result<bool, SqlParseError> {
        if self.eat_identifier_keyword("IF") {
            if !self.eat_identifier_keyword("EXISTS") {
                return Err(SqlParseError::unsupported_feature(
                    "ALTER TABLE DROP COLUMN IF EXISTS",
                ));
            }

            return Ok(true);
        }

        Ok(false)
    }

    fn parse_alter_table_rename_column_statement(
        &mut self,
        entity: String,
    ) -> Result<crate::db::sql::parser::SqlAlterTableRenameColumnStatement, SqlParseError> {
        if !self.eat_identifier_keyword("COLUMN") {
            return Err(SqlParseError::unsupported_feature(
                "SQL DDL ALTER TABLE RENAME statements beyond RENAME COLUMN",
            ));
        }
        let old_column_name = self.expect_identifier()?;
        if !self.eat_identifier_keyword("TO") {
            return Err(SqlParseError::unsupported_feature(
                "ALTER TABLE RENAME COLUMN without TO",
            ));
        }
        let new_column_name = self.expect_identifier()?;

        Ok(crate::db::sql::parser::SqlAlterTableRenameColumnStatement {
            entity,
            old_column_name,
            new_column_name,
        })
    }

    const fn ddl_clause_order_error(statement: &SqlDdlStatement) -> SqlParseError {
        match statement {
            SqlDdlStatement::CreateIndex(_) => {
                SqlParseError::unsupported_feature("CREATE INDEX modifiers")
            }
            SqlDdlStatement::DropIndex(_) => {
                SqlParseError::unsupported_feature("DROP INDEX modifiers")
            }
            SqlDdlStatement::AlterTableAddColumn(_) => {
                SqlParseError::unsupported_feature("ALTER TABLE ADD COLUMN modifiers")
            }
            SqlDdlStatement::AlterTableAlterColumn(_) => {
                SqlParseError::unsupported_feature("ALTER TABLE ALTER COLUMN modifiers")
            }
            SqlDdlStatement::AlterTableDropColumn(_) => {
                SqlParseError::unsupported_feature("ALTER TABLE DROP COLUMN modifiers")
            }
            SqlDdlStatement::AlterTableRenameColumn(_) => {
                SqlParseError::unsupported_feature("ALTER TABLE RENAME COLUMN modifiers")
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
        if !self.eat_keyword(Keyword::From) && !self.eat_keyword(Keyword::In) {
            return Err(SqlParseError::expected("FROM or IN", self.peek_kind()));
        }
        let entity = self.expect_identifier()?;

        Ok(SqlShowIndexesStatement { entity })
    }

    fn parse_show_columns_statement(&mut self) -> Result<SqlShowColumnsStatement, SqlParseError> {
        let entity = self.expect_identifier()?;

        Ok(SqlShowColumnsStatement { entity })
    }
}
