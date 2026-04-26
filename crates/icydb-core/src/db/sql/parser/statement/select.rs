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

        // Phase 1: parse predicate and grouping clauses in source syntax.
        let predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_where_expr()?)
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
            let clause = self.record_predicate_parse_stage(|parser| {
                parser.parse_sql_expr(
                    crate::db::sql::parser::projection::SqlExprParseSurface::HavingCondition,
                    0,
                )
            })?;

            vec![clause]
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
            table_alias,
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
