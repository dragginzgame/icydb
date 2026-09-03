use crate::db::{
    sql::parser::{Parser, SqlDeleteStatement},
    sql_shared::{Keyword, SqlParseError},
};

impl Parser {
    pub(super) fn parse_delete_statement(&mut self) -> Result<SqlDeleteStatement, SqlParseError> {
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        let table_alias = self.parse_optional_table_alias()?;

        let predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_where_expr()?)
        } else {
            None
        };

        let window = self.parse_order_limit_offset_clauses()?;
        let returning = if self.eat_keyword(Keyword::Returning) {
            Some(self.parse_returning_projection()?)
        } else {
            None
        };

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
