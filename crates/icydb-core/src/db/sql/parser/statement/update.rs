use crate::db::{
    reduced_sql::{Keyword, SqlParseError, TokenKind},
    sql::parser::{Parser, SqlAssignment, SqlUpdateStatement},
};

impl Parser {
    pub(super) fn parse_update_statement(&mut self) -> Result<SqlUpdateStatement, SqlParseError> {
        let entity = self.expect_identifier()?;
        let table_alias = self.parse_optional_table_alias()?;
        self.expect_identifier_keyword("SET")?;
        let mut assignments = self.parse_update_assignments()?;

        // Phase 1: parse the reduced predicate before any bounded windowing.
        let mut predicate = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        // Phase 2: parse the bounded ordered window admitted on the narrowed
        // SQL UPDATE lane.
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

        // Phase 3: collapse the admitted single-table alias back onto the
        // canonical entity field namespace so the write selector stays
        // alias-neutral downstream.
        if let Some(alias) = table_alias.as_deref() {
            assignments = crate::db::sql::parser::statement::normalize_assignments_for_table_alias(
                assignments,
                entity.as_str(),
                alias,
            );
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

        Ok(SqlUpdateStatement {
            entity,
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
            self.expect_assignment_eq()?;
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

    fn expect_assignment_eq(&mut self) -> Result<(), SqlParseError> {
        if matches!(self.peek_kind(), Some(TokenKind::Eq)) {
            let _ = self.cursor.advance();
            return Ok(());
        }

        Err(SqlParseError::expected(
            "'=' in UPDATE assignment",
            self.peek_kind(),
        ))
    }
}
