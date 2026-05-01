use crate::{
    db::{
        sql::parser::{
            Parser, SqlExpr, SqlExprBinaryOp, SqlExprUnaryOp, projection::SqlExprParseSurface,
        },
        sql_shared::{Keyword, SqlParseError, TokenKind},
    },
    value::Value,
};

impl Parser {
    pub(super) fn try_parse_where_postfix_expr(
        &mut self,
        left: SqlExpr,
        surface: SqlExprParseSurface,
    ) -> Result<Option<SqlExpr>, SqlParseError> {
        debug_assert!(surface.allows_predicate_postfix());

        if self.eat_keyword(Keyword::Is) {
            let negated = self.eat_keyword(Keyword::Not);
            if self.peek_keyword(Keyword::Null) {
                let _ = self.cursor.advance();

                return Ok(Some(SqlExpr::NullTest {
                    expr: Box::new(left),
                    negated,
                }));
            }
            if self.peek_keyword(Keyword::True) || self.peek_keyword(Keyword::False) {
                let value = if self.eat_keyword(Keyword::True) {
                    Value::Bool(true)
                } else {
                    let _ = self.cursor.advance();
                    Value::Bool(false)
                };
                let expr = SqlExpr::Binary {
                    op: SqlExprBinaryOp::Eq,
                    left: Box::new(left),
                    right: Box::new(SqlExpr::Literal(value)),
                };

                return Ok(Some(if negated {
                    SqlExpr::Unary {
                        op: SqlExprUnaryOp::Not,
                        expr: Box::new(expr),
                    }
                } else {
                    expr
                }));
            }

            return Err(SqlParseError::expected(
                "NULL/TRUE/FALSE after IS/IS NOT",
                self.peek_kind(),
            ));
        }

        if self.eat_identifier_keyword("LIKE") {
            return self.parse_where_like_expr(left, false, false).map(Some);
        }
        if self.eat_identifier_keyword("ILIKE") {
            return self.parse_where_like_expr(left, false, true).map(Some);
        }
        if self.cursor.peek_keyword(Keyword::Not) {
            if self.cursor.peek_identifier_keyword_at(1, "LIKE") {
                let _ = self.cursor.advance();
                let _ = self.cursor.eat_identifier_keyword("LIKE");

                return self.parse_where_like_expr(left, true, false).map(Some);
            }
            if self.cursor.peek_identifier_keyword_at(1, "ILIKE") {
                let _ = self.cursor.advance();
                let _ = self.cursor.eat_identifier_keyword("ILIKE");

                return self.parse_where_like_expr(left, true, true).map(Some);
            }
            if self.cursor.peek_keyword_at(1, Keyword::In) {
                let _ = self.cursor.advance();
                let _ = self.cursor.advance();

                return self.parse_where_in_expr(left, true).map(Some);
            }
            if self.cursor.peek_keyword_at(1, Keyword::Between) {
                let _ = self.cursor.advance();
                let _ = self.cursor.advance();

                return self.parse_where_between_expr(left, true, surface).map(Some);
            }
        }

        if self.eat_keyword(Keyword::In) {
            return self.parse_where_in_expr(left, false).map(Some);
        }
        if self.eat_keyword(Keyword::Between) {
            return self
                .parse_where_between_expr(left, false, surface)
                .map(Some);
        }

        Ok(None)
    }

    fn parse_where_like_expr(
        &mut self,
        left: SqlExpr,
        negated: bool,
        casefold: bool,
    ) -> Result<SqlExpr, SqlParseError> {
        let Value::Text(pattern) = self.parse_literal()? else {
            return Err(SqlParseError::expected(
                "string literal pattern after LIKE",
                self.peek_kind(),
            ));
        };

        Ok(SqlExpr::Like {
            expr: Box::new(left),
            pattern,
            negated,
            casefold,
        })
    }

    fn parse_where_in_expr(
        &mut self,
        left: SqlExpr,
        negated: bool,
    ) -> Result<SqlExpr, SqlParseError> {
        self.expect_lparen()?;
        let mut values = Vec::new();
        loop {
            values.push(self.parse_literal()?);
            if !self.eat_comma() {
                break;
            }
            if matches!(self.peek_kind(), Some(TokenKind::RParen)) {
                break;
            }
        }
        self.expect_rparen()?;

        if values.is_empty() {
            return Err(SqlParseError::invalid_syntax(
                "IN requires at least one literal",
            ));
        }

        Ok(SqlExpr::Membership {
            expr: Box::new(left),
            values,
            negated,
        })
    }

    fn parse_where_between_expr(
        &mut self,
        left: SqlExpr,
        negated: bool,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, SqlParseError> {
        let lower = self.parse_sql_expr_prefix(surface)?;
        self.expect_keyword(Keyword::And)?;
        let upper = self.parse_sql_expr_prefix(surface)?;

        Ok(if negated {
            SqlExpr::Binary {
                op: SqlExprBinaryOp::Or,
                left: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Lt,
                    left: Box::new(left.clone()),
                    right: Box::new(lower),
                }),
                right: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Gt,
                    left: Box::new(left),
                    right: Box::new(upper),
                }),
            }
        } else {
            SqlExpr::Binary {
                op: SqlExprBinaryOp::And,
                left: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Gte,
                    left: Box::new(left.clone()),
                    right: Box::new(lower),
                }),
                right: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Lte,
                    left: Box::new(left),
                    right: Box::new(upper),
                }),
            }
        })
    }
}
