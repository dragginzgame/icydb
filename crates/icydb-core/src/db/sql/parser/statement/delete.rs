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
        let returning = if self.eat_keyword(Keyword::Returning) {
            Some(self.parse_returning_projection()?)
        } else {
            None
        };

        Ok(SqlDeleteStatement {
            entity,
            table_alias,
            predicate,
            order_by,
            limit,
            offset,
            returning,
        })
    }
}
