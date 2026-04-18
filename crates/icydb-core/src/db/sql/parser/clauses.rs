//! Module: db::sql::parser::clauses
//! Responsibility: reduced SQL clause parsing shared by statement shells.
//! Does not own: statement routing, projection parsing, or predicate semantics.
//! Boundary: keeps ordering/grouping/HAVING helpers out of the parser root.

use crate::db::sql::parser::projection::SqlExprParseSurface;
use crate::db::{
    sql::parser::{
        Parser, SqlArithmeticProjectionOp, SqlExpr, SqlOrderDirection, SqlOrderTerm,
        SqlProjectionOperand, SqlTextFunction, SqlTextFunctionCall,
    },
    sql_shared::{Keyword, SqlParseError},
};

const ORDER_BY_UNSUPPORTED_FEATURE: &str = "ORDER BY terms beyond supported field, supported scalar text functions, bounded arithmetic, or ROUND(...) forms";

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
                return Ok(SqlExpr::from_projection_operand(
                    &SqlProjectionOperand::Arithmetic(Box::new(
                        self.parse_arithmetic_projection_call(
                            SqlProjectionOperand::Aggregate(aggregate),
                            op,
                        )?,
                    )),
                ));
            }

            return Ok(SqlExpr::Aggregate(aggregate));
        }

        let field = self.expect_identifier()?;
        if let Some(op) = self.parse_direct_order_arithmetic_op() {
            return Ok(SqlExpr::from_projection_operand(
                &SqlProjectionOperand::Arithmetic(Box::new(
                    self.parse_arithmetic_projection_call(SqlProjectionOperand::Field(field), op)?,
                )),
            ));
        }
        if !self.peek_lparen() {
            return Ok(SqlExpr::Field(field));
        }

        if field.eq_ignore_ascii_case("ROUND") {
            return Ok(SqlExpr::Round(self.parse_round_projection_call()?));
        }

        let Some(function) = SqlTextFunction::from_identifier(field.as_str()) else {
            return Err(SqlParseError::unsupported_feature(
                ORDER_BY_UNSUPPORTED_FEATURE,
            ));
        };

        self.parse_supported_scalar_text_order_term(function)
    }

    fn parse_direct_order_arithmetic_op(&mut self) -> Option<SqlArithmeticProjectionOp> {
        if self.eat_plus() {
            return Some(SqlArithmeticProjectionOp::Add);
        }
        if self.eat_minus() {
            return Some(SqlArithmeticProjectionOp::Sub);
        }
        if self.eat_star() {
            return Some(SqlArithmeticProjectionOp::Mul);
        }
        if self.eat_slash() {
            return Some(SqlArithmeticProjectionOp::Div);
        }

        None
    }

    // Parse one direct scalar text-function `ORDER BY` target on the same
    // bounded field-plus-literal family already admitted in projection.
    fn parse_supported_scalar_text_order_term(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlExpr, SqlParseError> {
        self.expect_lparen()?;

        let call = match function {
            SqlTextFunction::Trim
            | SqlTextFunction::Ltrim
            | SqlTextFunction::Rtrim
            | SqlTextFunction::Lower
            | SqlTextFunction::Upper
            | SqlTextFunction::Length => {
                let field = self.expect_identifier()?;
                SqlTextFunctionCall {
                    function,
                    field,
                    literal: None,
                    literal2: None,
                    literal3: None,
                }
            }
            SqlTextFunction::Left
            | SqlTextFunction::Right
            | SqlTextFunction::StartsWith
            | SqlTextFunction::EndsWith
            | SqlTextFunction::Contains => {
                let field = self.expect_identifier()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(",", self.peek_kind()));
                }
                let literal = self.parse_literal()?;

                SqlTextFunctionCall {
                    function,
                    field,
                    literal: Some(literal),
                    literal2: None,
                    literal3: None,
                }
            }
            SqlTextFunction::Position => {
                let literal = self.parse_literal()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(",", self.peek_kind()));
                }
                let field = self.expect_identifier()?;

                SqlTextFunctionCall {
                    function,
                    field,
                    literal: Some(literal),
                    literal2: None,
                    literal3: None,
                }
            }
            SqlTextFunction::Replace => {
                let field = self.expect_identifier()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(",", self.peek_kind()));
                }
                let literal = self.parse_literal()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(",", self.peek_kind()));
                }
                let literal2 = self.parse_literal()?;

                SqlTextFunctionCall {
                    function,
                    field,
                    literal: Some(literal),
                    literal2: Some(literal2),
                    literal3: None,
                }
            }
            SqlTextFunction::Substring => {
                let field = self.expect_identifier()?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(",", self.peek_kind()));
                }
                let literal = self.parse_literal()?;
                let literal2 = if self.eat_comma() {
                    Some(self.parse_literal()?)
                } else {
                    None
                };

                SqlTextFunctionCall {
                    function,
                    field,
                    literal: Some(literal),
                    literal2,
                    literal3: None,
                }
            }
        };

        self.expect_rparen()?;

        Ok(SqlExpr::TextFunction(call))
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
