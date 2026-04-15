use crate::db::{
    sql::parser::{Parser, SqlSelectStatement},
    sql_shared::{Keyword, SqlParseError},
};

impl Parser {
    pub(super) fn parse_select_statement(&mut self) -> Result<SqlSelectStatement, SqlParseError> {
        let distinct = self.eat_keyword(Keyword::Distinct);
        let (projection, projection_aliases) = self.parse_projection()?;
        self.expect_keyword(Keyword::From)?;
        let entity = self.expect_identifier()?;
        let table_alias = self.parse_optional_table_alias()?;

        // Phase 1: parse predicate and grouping clauses in canonical sequence.
        let mut predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        let mut group_by = if self.eat_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By)?;
            self.parse_identifier_list()?
        } else {
            Vec::new()
        };

        let mut having = if self.eat_keyword(Keyword::Having) {
            self.parse_having_clauses()?
        } else {
            Vec::new()
        };

        // Phase 2: parse ordering and window clauses.
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

        // Phase 3: collapse one admitted single-table alias back onto the
        // canonical entity field namespace so downstream lowering stays
        // alias-neutral.
        let projection = match table_alias.as_deref() {
            Some(alias) => crate::db::sql::parser::statement::normalize_projection_for_table_alias(
                projection,
                entity.as_str(),
                alias,
            ),
            None => projection,
        };
        if let Some(alias) = table_alias.as_deref() {
            predicate = predicate.map(|predicate| {
                crate::db::sql::parser::statement::normalize_predicate_for_table_alias(
                    predicate,
                    entity.as_str(),
                    alias,
                )
            });
            group_by = crate::db::sql::parser::statement::normalize_identifier_list_for_table_alias(
                group_by,
                entity.as_str(),
                alias,
            );
            having = crate::db::sql::parser::statement::normalize_having_for_table_alias(
                having,
                entity.as_str(),
                alias,
            );
            order_by = crate::db::sql::parser::statement::normalize_order_terms_for_table_alias(
                order_by,
                entity.as_str(),
                alias,
            );
        }

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
}
