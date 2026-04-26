use crate::db::{
    sql::parser::{Parser, SqlAssignment, SqlUpdateStatement},
    sql_shared::{Keyword, SqlParseError, TokenKind},
};

impl Parser {
    pub(super) fn parse_update_statement(&mut self) -> Result<SqlUpdateStatement, SqlParseError> {
        let entity = self.expect_identifier()?;
        let table_alias = self.parse_optional_table_alias()?;
        self.expect_identifier_keyword("SET")?;
        let assignments = self.parse_update_assignments()?;

        // Phase 1: parse the reduced predicate before any bounded windowing.
        let predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_where_expr()?)
        } else {
            None
        };

        // Phase 2: parse the bounded ordered window admitted on the narrowed
        // SQL UPDATE lane.
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

        Ok(SqlUpdateStatement {
            entity,
            table_alias,
            assignments,
            predicate,
            order_by,
            limit,
            offset,
            returning,
        })
    }

    fn parse_update_assignments(&mut self) -> Result<Vec<SqlAssignment>, SqlParseError> {
        let mut assignments = Vec::new();
        loop {
            let field = self.expect_identifier()?;
            if matches!(self.peek_kind(), Some(TokenKind::Eq)) {
                let _ = self.cursor.advance();
            } else {
                return Err(SqlParseError::expected(
                    "'=' in UPDATE assignment",
                    self.peek_kind(),
                ));
            }
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
}
