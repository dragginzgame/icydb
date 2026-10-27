//! Module: sql::parser::statement::delete
//! Responsibility: DELETE statement clause parsing and statement assembly.
//! Does not own: mutation lowering or execution semantics.
//! Boundary: converts DELETE tokens into the shared parsed statement model.

use crate::db::{
    sql::parser::{Parser, SqlDeleteStatement},
    sql_shared::{Keyword, SqlParseError},
};

impl Parser {
    pub(super) fn parse_delete_statement(&mut self) -> Result<SqlDeleteStatement, SqlParseError> {
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        let table_alias = self.parse_optional_table_alias()?;

        let (predicate, window, returning) = self.parse_write_tail()?;

        Ok(SqlDeleteStatement {
            entity,
            table_alias,
            predicate,
            order_by: window.order_by,
            limit: window.limit,
            offset: window.offset,
            returning,
        })
    }
}
