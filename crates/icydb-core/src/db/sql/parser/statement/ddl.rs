use crate::db::{
    sql::parser::{
        Parser, SqlAlterColumnAction, SqlAlterTableAddColumnStatement,
        SqlAlterTableAlterColumnStatement, SqlAlterTableDropColumnStatement,
        SqlAlterTableRenameColumnStatement, SqlCreateIndexExpressionFunction,
        SqlCreateIndexExpressionKey, SqlCreateIndexKeyItem, SqlCreateIndexStatement,
        SqlCreateIndexUniqueness, SqlDdlSchemaVersionContract, SqlDdlStatement,
        SqlDropIndexStatement,
    },
    sql_shared::{Keyword, SqlParseError, TokenKind},
};
use icydb_diagnostic_code::SqlFeatureCode;

impl Parser {
    pub(super) fn parse_create_statement(&mut self) -> Result<SqlDdlStatement, SqlParseError> {
        let uniqueness = if self.eat_keyword(Keyword::Unique) {
            SqlCreateIndexUniqueness::Unique
        } else {
            SqlCreateIndexUniqueness::NonUnique
        };
        if !self.eat_keyword(Keyword::Index) {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::CreateStatementBeyondCreateIndex,
            ));
        }

        Ok(SqlDdlStatement::CreateIndex(
            self.parse_create_index_statement(uniqueness)?,
        ))
    }

    pub(super) fn parse_drop_statement(&mut self) -> Result<SqlDdlStatement, SqlParseError> {
        if !self.eat_keyword(Keyword::Index) {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::DropStatementBeyondDropIndex,
            ));
        }

        Ok(SqlDdlStatement::DropIndex(
            self.parse_drop_index_statement()?,
        ))
    }

    pub(super) fn parse_alter_statement(&mut self) -> Result<SqlDdlStatement, SqlParseError> {
        if !self.eat_identifier_keyword("TABLE") {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::AlterStatementBeyondAlterTable,
            ));
        }

        let entity = self.expect_identifier()?;
        let prefix_contract = self.parse_optional_ddl_schema_version_contract()?;
        if self.eat_identifier_keyword("ADD") {
            let mut statement = self.parse_alter_table_add_column_statement(entity)?;
            statement.schema_version_contract = Self::merge_ddl_schema_version_contracts(
                prefix_contract,
                self.parse_optional_ddl_schema_version_contract()?,
            )?;

            return Ok(SqlDdlStatement::AlterTableAddColumn(statement));
        }
        if self.eat_keyword(Keyword::Drop) {
            let mut statement = self.parse_alter_table_drop_column_statement(entity)?;
            statement.schema_version_contract = Self::merge_ddl_schema_version_contracts(
                prefix_contract,
                self.parse_optional_ddl_schema_version_contract()?,
            )?;

            return Ok(SqlDdlStatement::AlterTableDropColumn(statement));
        }
        if self.eat_identifier_keyword("ALTER") {
            let mut statement = self.parse_alter_table_alter_column_statement(entity)?;
            statement.schema_version_contract = Self::merge_ddl_schema_version_contracts(
                prefix_contract,
                self.parse_optional_ddl_schema_version_contract()?,
            )?;

            return Ok(SqlDdlStatement::AlterTableAlterColumn(statement));
        }
        if self.eat_identifier_keyword("RENAME") {
            let mut statement = self.parse_alter_table_rename_column_statement(entity)?;
            statement.schema_version_contract = Self::merge_ddl_schema_version_contracts(
                prefix_contract,
                self.parse_optional_ddl_schema_version_contract()?,
            )?;

            return Ok(SqlDdlStatement::AlterTableRenameColumn(statement));
        }

        Err(SqlParseError::unsupported_feature(
            SqlFeatureCode::AlterTableUnsupportedOperation,
        ))
    }

    pub(super) const fn ddl_clause_order_error(statement: &SqlDdlStatement) -> SqlParseError {
        match statement {
            SqlDdlStatement::CreateIndex(_) => {
                SqlParseError::unsupported_feature(SqlFeatureCode::CreateIndexModifiers)
            }
            SqlDdlStatement::DropIndex(_) => {
                SqlParseError::unsupported_feature(SqlFeatureCode::DropIndexModifiers)
            }
            SqlDdlStatement::AlterTableAddColumn(_) => {
                SqlParseError::unsupported_feature(SqlFeatureCode::AlterTableAddColumnModifiers)
            }
            SqlDdlStatement::AlterTableAlterColumn(_) => {
                SqlParseError::unsupported_feature(SqlFeatureCode::AlterTableAlterColumnModifiers)
            }
            SqlDdlStatement::AlterTableDropColumn(_) => {
                SqlParseError::unsupported_feature(SqlFeatureCode::AlterTableDropColumnModifiers)
            }
            SqlDdlStatement::AlterTableRenameColumn(_) => {
                SqlParseError::unsupported_feature(SqlFeatureCode::AlterTableRenameColumnModifiers)
            }
        }
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
        let schema_version_contract = self.parse_optional_ddl_schema_version_contract()?;
        let predicate_sql = self.parse_create_index_predicate_sql()?;

        Ok(SqlCreateIndexStatement {
            name,
            entity,
            key_items,
            predicate_sql,
            uniqueness,
            if_not_exists,
            schema_version_contract,
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
                    SqlFeatureCode::CreateIndexKeyOrderingModifiers,
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
                SqlFeatureCode::ExpressionIndexUnsupportedFunction,
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
                    SqlFeatureCode::CreateIndexIfNotExistsSyntax,
                ));
            }

            return Ok(true);
        }

        Ok(false)
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
            schema_version_contract: self.parse_optional_ddl_schema_version_contract()?,
        })
    }

    fn parse_drop_index_if_exists(&mut self) -> Result<bool, SqlParseError> {
        if self.eat_identifier_keyword("IF") {
            if !self.eat_identifier_keyword("EXISTS") {
                return Err(SqlParseError::unsupported_feature(
                    SqlFeatureCode::DropIndexIfExistsSyntax,
                ));
            }

            return Ok(true);
        }

        Ok(false)
    }

    fn parse_alter_table_add_column_statement(
        &mut self,
        entity: String,
    ) -> Result<SqlAlterTableAddColumnStatement, SqlParseError> {
        if !self.eat_identifier_keyword("COLUMN") {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::AlterTableAddStatementBeyondAddColumn,
            ));
        }
        let column_name = self.expect_identifier()?;
        let column_type = self.parse_alter_table_add_column_type()?;
        let mut nullable = true;
        let mut default = None;

        loop {
            if self.eat_identifier_keyword("DEFAULT") {
                if default.is_some() {
                    return Err(SqlParseError::unsupported_feature(
                        SqlFeatureCode::AlterTableAddColumnDuplicateDefault,
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

        Ok(SqlAlterTableAddColumnStatement {
            entity,
            column_name,
            column_type,
            nullable,
            default,
            schema_version_contract: SqlDdlSchemaVersionContract::default(),
        })
    }

    fn parse_alter_table_add_column_type(&mut self) -> Result<String, SqlParseError> {
        let head = self.expect_identifier()?;
        if !self.peek_lparen() {
            return Ok(head);
        }

        self.expect_lparen()?;
        self.expect_identifier_keyword("max_bytes")?;
        if !matches!(self.peek_kind(), Some(TokenKind::Eq)) {
            return Err(SqlParseError::expected("=", self.peek_kind()));
        }
        self.cursor.advance();
        let max_bytes = self.parse_u32_literal("max_bytes")?;
        self.expect_rparen()?;

        Ok(format!("{head}(max_bytes={max_bytes})"))
    }

    fn parse_alter_table_alter_column_statement(
        &mut self,
        entity: String,
    ) -> Result<SqlAlterTableAlterColumnStatement, SqlParseError> {
        if !self.eat_identifier_keyword("COLUMN") {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::AlterTableAlterStatementBeyondAlterColumn,
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
                    SqlFeatureCode::AlterTableAlterColumnSetUnsupportedAction,
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
                    SqlFeatureCode::AlterTableAlterColumnDropUnsupportedAction,
                ));
            }
        } else {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::AlterTableAlterColumnUnsupportedAction,
            ));
        };

        Ok(SqlAlterTableAlterColumnStatement {
            entity,
            column_name,
            action,
            schema_version_contract: SqlDdlSchemaVersionContract::default(),
        })
    }

    fn parse_alter_table_drop_column_statement(
        &mut self,
        entity: String,
    ) -> Result<SqlAlterTableDropColumnStatement, SqlParseError> {
        if !self.eat_identifier_keyword("COLUMN") {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::AlterTableDropStatementBeyondDropColumn,
            ));
        }
        let if_exists = self.parse_drop_column_if_exists()?;
        let column_name = self.expect_identifier()?;

        Ok(SqlAlterTableDropColumnStatement {
            entity,
            column_name,
            if_exists,
            schema_version_contract: SqlDdlSchemaVersionContract::default(),
        })
    }

    fn parse_drop_column_if_exists(&mut self) -> Result<bool, SqlParseError> {
        if self.eat_identifier_keyword("IF") {
            if !self.eat_identifier_keyword("EXISTS") {
                return Err(SqlParseError::unsupported_feature(
                    SqlFeatureCode::AlterTableDropColumnIfExistsSyntax,
                ));
            }

            return Ok(true);
        }

        Ok(false)
    }

    fn parse_alter_table_rename_column_statement(
        &mut self,
        entity: String,
    ) -> Result<SqlAlterTableRenameColumnStatement, SqlParseError> {
        if !self.eat_identifier_keyword("COLUMN") {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::AlterTableRenameStatementBeyondRenameColumn,
            ));
        }
        let old_column_name = self.expect_identifier()?;
        if !self.eat_identifier_keyword("TO") {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::AlterTableRenameColumnMissingTo,
            ));
        }
        let new_column_name = self.expect_identifier()?;

        Ok(SqlAlterTableRenameColumnStatement {
            entity,
            old_column_name,
            new_column_name,
            schema_version_contract: SqlDdlSchemaVersionContract::default(),
        })
    }

    fn parse_optional_ddl_schema_version_contract(
        &mut self,
    ) -> Result<SqlDdlSchemaVersionContract, SqlParseError> {
        let mut contract = SqlDdlSchemaVersionContract::default();

        loop {
            if self.eat_identifier_keyword("EXPECT") {
                if contract.expected_schema_version.is_some() {
                    return Err(SqlParseError::unsupported_feature(
                        SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause,
                    ));
                }
                self.expect_identifier_keyword("SCHEMA")?;
                self.expect_identifier_keyword("VERSION")?;
                contract.expected_schema_version =
                    Some(self.parse_u32_literal("EXPECT SCHEMA VERSION")?);
            } else if self.eat_identifier_keyword("SET") {
                if contract.next_schema_version.is_some() {
                    return Err(SqlParseError::unsupported_feature(
                        SqlFeatureCode::DdlSchemaVersionDuplicateSetClause,
                    ));
                }
                self.expect_identifier_keyword("SCHEMA")?;
                self.expect_identifier_keyword("VERSION")?;
                contract.next_schema_version = Some(self.parse_u32_literal("SET SCHEMA VERSION")?);
            } else {
                break;
            }
        }

        Ok(contract)
    }

    fn merge_ddl_schema_version_contracts(
        prefix: SqlDdlSchemaVersionContract,
        suffix: SqlDdlSchemaVersionContract,
    ) -> Result<SqlDdlSchemaVersionContract, SqlParseError> {
        if prefix.expected_schema_version.is_some() && suffix.expected_schema_version.is_some() {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause,
            ));
        }
        if prefix.next_schema_version.is_some() && suffix.next_schema_version.is_some() {
            return Err(SqlParseError::unsupported_feature(
                SqlFeatureCode::DdlSchemaVersionDuplicateSetClause,
            ));
        }

        Ok(SqlDdlSchemaVersionContract {
            expected_schema_version: prefix
                .expected_schema_version
                .or(suffix.expected_schema_version),
            next_schema_version: prefix.next_schema_version.or(suffix.next_schema_version),
        })
    }
}
