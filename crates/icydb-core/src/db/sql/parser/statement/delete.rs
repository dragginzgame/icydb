use crate::db::{
    reduced_sql::{Keyword, SqlParseError},
    sql::parser::{Parser, SqlDeleteStatement},
};

impl Parser {
    pub(super) fn parse_delete_statement(&mut self) -> Result<SqlDeleteStatement, SqlParseError> {
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        let table_alias = self.parse_optional_table_alias()?;

        let mut predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        let mut order_by = if self.eat_keyword(Keyword::Order) {
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
        let mut returning = if self.eat_keyword(Keyword::Returning) {
            Some(self.parse_returning_projection()?)
        } else {
            None
        };

        if let Some(alias) = table_alias.as_deref() {
            predicate = predicate.map(|predicate| {
                crate::db::sql::parser::statement::normalize_predicate_for_table_alias(
                    predicate,
                    entity.as_str(),
                    alias,
                )
            });
            order_by = crate::db::sql::parser::statement::normalize_order_terms_for_table_alias(
                order_by,
                entity.as_str(),
                alias,
            );
            returning = returning.map(|returning| {
                crate::db::sql::parser::statement::normalize_returning_projection_for_table_alias(
                    returning,
                    entity.as_str(),
                    alias,
                )
            });
        }

        Ok(SqlDeleteStatement {
            entity,
            predicate,
            order_by,
            limit,
            offset,
            returning,
        })
    }
}
