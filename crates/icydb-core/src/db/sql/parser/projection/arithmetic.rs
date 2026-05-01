use crate::db::{
    sql::parser::{Parser, SqlExpr, SqlExprBinaryOp},
    sql_shared::{SqlParseError, TokenKind},
};

impl Parser {
    fn parse_projection_arithmetic_expr(
        &mut self,
        min_precedence: u8,
    ) -> Result<SqlExpr, SqlParseError> {
        let left = self.parse_projection_arithmetic_leaf()?;

        self.parse_projection_arithmetic_expr_tail(left, min_precedence)
    }

    fn parse_projection_arithmetic_expr_tail(
        &mut self,
        mut left: SqlExpr,
        min_precedence: u8,
    ) -> Result<SqlExpr, SqlParseError> {
        while let Some(op) = self.peek_arithmetic_projection_op() {
            let precedence = arithmetic_projection_op_precedence(op);
            if precedence < min_precedence {
                break;
            }

            let _ = self.eat_arithmetic_projection_op();
            let right = self.parse_projection_arithmetic_expr(precedence.saturating_add(1))?;
            left = SqlExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    pub(in crate::db::sql::parser) fn parse_projection_arithmetic_from_left(
        &mut self,
        left: SqlExpr,
        op: SqlExprBinaryOp,
    ) -> Result<SqlExpr, SqlParseError> {
        let right =
            self.parse_projection_arithmetic_expr(arithmetic_projection_op_precedence(op) + 1)?;

        Ok(SqlExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn parse_projection_arithmetic_leaf(&mut self) -> Result<SqlExpr, SqlParseError> {
        if self.peek_lparen() {
            self.expect_lparen()?;
            let expr = self.parse_projection_arithmetic_expr(0)?;
            self.expect_rparen()?;

            return Ok(expr);
        }
        if let Some(kind) = self.parse_aggregate_kind() {
            return self.parse_aggregate_call(kind).map(SqlExpr::Aggregate);
        }
        if self.eat_question() {
            return Ok(SqlExpr::Param {
                index: self.take_param_index(),
            });
        }
        if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            let field = self.expect_identifier()?;
            if self.peek_lparen() {
                return Err(SqlParseError::unsupported_feature(
                    "nested projection functions inside arithmetic expressions",
                ));
            }

            return Ok(SqlExpr::from_field_identifier(field));
        }

        self.parse_literal().map(SqlExpr::Literal)
    }

    fn peek_arithmetic_projection_op(&self) -> Option<SqlExprBinaryOp> {
        match self.peek_kind() {
            Some(TokenKind::Plus) => Some(SqlExprBinaryOp::Add),
            Some(TokenKind::Minus) => Some(SqlExprBinaryOp::Sub),
            Some(TokenKind::Star) => Some(SqlExprBinaryOp::Mul),
            Some(TokenKind::Slash) => Some(SqlExprBinaryOp::Div),
            _ => None,
        }
    }

    fn eat_arithmetic_projection_op(&mut self) -> Option<SqlExprBinaryOp> {
        let op = self.peek_arithmetic_projection_op()?;
        let _ = self.cursor.advance();

        Some(op)
    }
}

const fn arithmetic_projection_op_precedence(op: SqlExprBinaryOp) -> u8 {
    match op {
        SqlExprBinaryOp::Add | SqlExprBinaryOp::Sub => 1,
        SqlExprBinaryOp::Mul | SqlExprBinaryOp::Div => 2,
        SqlExprBinaryOp::Or
        | SqlExprBinaryOp::And
        | SqlExprBinaryOp::Eq
        | SqlExprBinaryOp::Ne
        | SqlExprBinaryOp::Lt
        | SqlExprBinaryOp::Lte
        | SqlExprBinaryOp::Gt
        | SqlExprBinaryOp::Gte => 0,
    }
}
