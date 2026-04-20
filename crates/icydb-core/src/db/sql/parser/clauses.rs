//! Module: db::sql::parser::clauses
//! Responsibility: reduced SQL clause parsing shared by statement shells.
//! Does not own: statement routing, projection parsing, or predicate semantics.
//! Boundary: keeps ordering/grouping/HAVING helpers out of the parser root.

use crate::db::sql::parser::projection::SqlExprParseSurface;
use crate::db::{
    sql::parser::{
        Parser, SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlOrderTerm, SqlScalarFunction,
    },
    sql_shared::{Keyword, SqlParseError},
};

const ORDER_BY_UNSUPPORTED_FEATURE: &str = "ORDER BY terms beyond supported fields, bounded arithmetic, or supported scalar-function forms";

impl Parser {
    pub(super) fn parse_order_terms(&mut self) -> Result<Vec<SqlOrderTerm>, SqlParseError> {
        let mut terms = Vec::new();
        loop {
            let field = self.record_expr_parse_stage(Self::parse_order_term_target)?;
            let direction = if self.eat_keyword(Keyword::Desc) {
                SqlOrderDirection::Desc
            } else {
                self.eat_keyword(Keyword::Asc);
                SqlOrderDirection::Asc
            };

            terms.push(SqlOrderTerm { field, direction });
            if !self.eat_comma() {
                break;
            }
        }

        Ok(terms)
    }

    fn parse_order_term_target(&mut self) -> Result<SqlExpr, SqlParseError> {
        if let Some(kind) = self.parse_aggregate_kind() {
            let aggregate = self.parse_aggregate_call(kind)?;
            if let Some(op) = self.parse_direct_order_arithmetic_op() {
                return self
                    .parse_projection_arithmetic_from_left(SqlExpr::Aggregate(aggregate), op);
            }

            return Ok(SqlExpr::Aggregate(aggregate));
        }

        let field = self.expect_identifier()?;
        if let Some(op) = self.parse_direct_order_arithmetic_op() {
            return self.parse_projection_arithmetic_from_left(SqlExpr::Field(field), op);
        }
        if !self.peek_lparen() {
            return Ok(SqlExpr::Field(field));
        }

        if field.eq_ignore_ascii_case("ROUND") {
            return self.parse_round_function_call(
                SqlScalarFunction::Round,
                SqlExprParseSurface::Projection,
            );
        }

        let Some(function) = SqlScalarFunction::from_identifier(field.as_str()) else {
            return Err(SqlParseError::unsupported_feature(
                ORDER_BY_UNSUPPORTED_FEATURE,
            ));
        };

        self.parse_supported_scalar_function_order_term(function)
    }

    fn parse_direct_order_arithmetic_op(&mut self) -> Option<SqlExprBinaryOp> {
        if self.eat_plus() {
            return Some(SqlExprBinaryOp::Add);
        }
        if self.eat_minus() {
            return Some(SqlExprBinaryOp::Sub);
        }
        if self.eat_star() {
            return Some(SqlExprBinaryOp::Mul);
        }
        if self.eat_slash() {
            return Some(SqlExprBinaryOp::Div);
        }

        None
    }

    // Parse one direct scalar-function `ORDER BY` target on the same bounded
    // scalar-expression family already admitted in projection-style positions.
    fn parse_supported_scalar_function_order_term(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, SqlParseError> {
        if matches!(function, SqlScalarFunction::Round) {
            return self.parse_round_function_call(function, SqlExprParseSurface::Projection);
        }
        self.parse_scalar_function_call(function, SqlExprParseSurface::Projection)
    }

    pub(super) fn parse_having_clauses(&mut self) -> Result<Vec<SqlExpr>, SqlParseError> {
        let clause = self.record_predicate_parse_stage(|parser| {
            parser.parse_sql_expr(SqlExprParseSurface::HavingCondition, 0)
        })?;

        Ok(vec![clause])
    }

    pub(super) fn parse_identifier_list(&mut self) -> Result<Vec<String>, SqlParseError> {
        let mut fields = vec![self.expect_identifier()?];
        while self.eat_comma() {
            fields.push(self.expect_identifier()?);
        }

        Ok(fields)
    }
}
