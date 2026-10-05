use crate::db::{
    sql::parser::{Parser, SqlExpr, SqlExprBinaryOp},
    sql_shared::{MAX_SQL_EXPR_DEPTH, SqlParseError, TokenKind, sql_expr_depth_limit_error},
};
use icydb_diagnostic_code::SqlFeatureCode;

impl Parser {
    fn parse_projection_arithmetic_expr(
        &mut self,
        min_precedence: u8,
    ) -> Result<(SqlExpr, usize), SqlParseError> {
        // Direct ORDER BY arithmetic shares the parser's depth authority, even
        // when parentheses disappear from the resulting expression tree.
        self.enter_sql_expr_depth()?;
        let result = self.parse_projection_arithmetic_expr_at_current_depth(min_precedence);
        self.leave_sql_expr_depth();

        result
    }

    fn parse_projection_arithmetic_expr_at_current_depth(
        &mut self,
        min_precedence: u8,
    ) -> Result<(SqlExpr, usize), SqlParseError> {
        let left = self.parse_projection_arithmetic_leaf()?;

        self.parse_projection_arithmetic_expr_tail(left, min_precedence)
    }

    fn parse_projection_arithmetic_expr_tail(
        &mut self,
        (mut left, mut left_depth): (SqlExpr, usize),
        min_precedence: u8,
    ) -> Result<(SqlExpr, usize), SqlParseError> {
        while let Some(op) = self.peek_arithmetic_projection_op() {
            let precedence = arithmetic_projection_op_precedence(op);
            if precedence < min_precedence {
                break;
            }

            let _ = self.eat_arithmetic_projection_op();
            let (right, right_depth) =
                self.parse_projection_arithmetic_expr(precedence.saturating_add(1))?;
            // Carry arithmetic height through parentheses, so individually
            // short chains cannot compose an unbounded retained tree.
            let next_depth = left_depth.max(right_depth).saturating_add(1);
            if next_depth > MAX_SQL_EXPR_DEPTH {
                return Err(sql_expr_depth_limit_error());
            }
            left_depth = next_depth;
            left = SqlExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok((left, left_depth))
    }

    pub(in crate::db::sql::parser) fn parse_projection_arithmetic_from_left(
        &mut self,
        left: SqlExpr,
        op: SqlExprBinaryOp,
    ) -> Result<SqlExpr, SqlParseError> {
        let (right, right_depth) =
            self.parse_projection_arithmetic_expr(arithmetic_projection_op_precedence(op) + 1)?;
        // The direct field/aggregate target is one arithmetic leaf. Its own
        // aggregate children remain guarded by their expression parser.
        if right_depth.saturating_add(1) > MAX_SQL_EXPR_DEPTH {
            return Err(sql_expr_depth_limit_error());
        }

        Ok(SqlExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn parse_projection_arithmetic_leaf(&mut self) -> Result<(SqlExpr, usize), SqlParseError> {
        if self.peek_lparen() {
            self.expect_lparen()?;
            let expr = self.parse_projection_arithmetic_expr(0)?;
            self.expect_rparen()?;

            return Ok(expr);
        }
        if let Some(kind) = self.parse_aggregate_kind() {
            return self
                .parse_aggregate_call(kind)
                .map(|aggregate| (SqlExpr::Aggregate(aggregate), 1));
        }
        if self.eat_question() {
            return Ok((
                SqlExpr::Param {
                    index: self.take_param_index(),
                },
                1,
            ));
        }
        if self.cursor.peek_u256_literal() {
            return self
                .parse_literal()
                .map(|value| (SqlExpr::Literal(value), 1));
        }
        if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            let field = self.expect_identifier()?;
            if self.peek_lparen() {
                return Err(SqlParseError::unsupported_feature(
                    SqlFeatureCode::NestedProjectionFunctionInArithmetic,
                ));
            }

            return Ok((SqlExpr::from_field_identifier(field), 1));
        }

        self.parse_literal()
            .map(|value| (SqlExpr::Literal(value), 1))
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
