//! Module: db::sql::parser::clauses
//! Responsibility: reduced SQL clause parsing shared by statement shells.
//! Does not own: statement routing, projection parsing, or predicate semantics.
//! Boundary: keeps ordering/grouping/HAVING helpers out of the parser root.

use crate::{
    db::{
        predicate::CompareOp,
        sql::parser::{
            Parser, SqlAggregateInputExpr, SqlArithmeticProjectionCall, SqlArithmeticProjectionOp,
            SqlHavingClause, SqlHavingValueExpr, SqlOrderDirection, SqlOrderTerm,
            SqlProjectionOperand, SqlRoundProjectionCall, SqlRoundProjectionInput, SqlTextFunction,
        },
        sql_shared::{Keyword, SqlParseError},
    },
    value::Value,
};

const ORDER_BY_UNSUPPORTED_FEATURE: &str = "ORDER BY terms beyond supported field, unary text functions, bounded arithmetic, or ROUND(...) forms";

impl Parser {
    pub(super) fn parse_order_terms(&mut self) -> Result<Vec<SqlOrderTerm>, SqlParseError> {
        let mut terms = Vec::new();
        loop {
            let field = self.parse_order_term_target()?;
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

    fn parse_order_term_target(&mut self) -> Result<String, SqlParseError> {
        if let Some(kind) = self.parse_aggregate_kind() {
            let aggregate = self.parse_aggregate_call(kind)?;
            if let Some(op) = self.parse_direct_order_arithmetic_op() {
                return Ok(render_order_arithmetic_term(
                    self.parse_arithmetic_projection_call(
                        SqlProjectionOperand::Aggregate(aggregate),
                        op,
                    )?,
                ));
            }

            return Ok(render_order_aggregate_call(aggregate));
        }

        let field = self.expect_identifier()?;
        if let Some(op) = self.parse_direct_order_arithmetic_op() {
            return Ok(render_order_arithmetic_term(
                self.parse_arithmetic_projection_call(SqlProjectionOperand::Field(field), op)?,
            ));
        }
        if !self.peek_lparen() {
            return Ok(field);
        }

        if field.eq_ignore_ascii_case("ROUND") {
            return Ok(render_order_round_term(self.parse_round_projection_call()?));
        }

        let Some(function) = SqlTextFunction::from_identifier(field.as_str()) else {
            return Err(SqlParseError::unsupported_feature(
                ORDER_BY_UNSUPPORTED_FEATURE,
            ));
        };

        match function {
            SqlTextFunction::Trim
            | SqlTextFunction::Ltrim
            | SqlTextFunction::Rtrim
            | SqlTextFunction::Lower
            | SqlTextFunction::Upper
            | SqlTextFunction::Length => {
                self.expect_lparen()?;
                let field = self.expect_identifier()?;
                self.expect_rparen()?;

                Ok(format!(
                    "{}({field})",
                    match function {
                        SqlTextFunction::Trim => "TRIM",
                        SqlTextFunction::Ltrim => "LTRIM",
                        SqlTextFunction::Rtrim => "RTRIM",
                        SqlTextFunction::Lower => "LOWER",
                        SqlTextFunction::Upper => "UPPER",
                        SqlTextFunction::Length => "LENGTH",
                        SqlTextFunction::Left
                        | SqlTextFunction::Right
                        | SqlTextFunction::StartsWith
                        | SqlTextFunction::EndsWith
                        | SqlTextFunction::Contains
                        | SqlTextFunction::Position
                        | SqlTextFunction::Replace
                        | SqlTextFunction::Substring => unreachable!(),
                    }
                ))
            }
            SqlTextFunction::Left
            | SqlTextFunction::Right
            | SqlTextFunction::StartsWith
            | SqlTextFunction::EndsWith
            | SqlTextFunction::Contains
            | SqlTextFunction::Position
            | SqlTextFunction::Replace
            | SqlTextFunction::Substring => Err(SqlParseError::unsupported_feature(
                ORDER_BY_UNSUPPORTED_FEATURE,
            )),
        }
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

    pub(super) fn parse_having_clauses(&mut self) -> Result<Vec<SqlHavingClause>, SqlParseError> {
        let mut clauses = vec![self.parse_having_clause()?];
        while self.eat_keyword(Keyword::And) {
            clauses.push(self.parse_having_clause()?);
        }

        if self.peek_keyword(Keyword::Or) || self.peek_keyword(Keyword::Not) {
            return Err(SqlParseError::unsupported_feature(
                "HAVING boolean operators beyond AND",
            ));
        }

        Ok(clauses)
    }

    pub(super) fn parse_identifier_list(&mut self) -> Result<Vec<String>, SqlParseError> {
        let mut fields = vec![self.expect_identifier()?];
        while self.eat_comma() {
            fields.push(self.expect_identifier()?);
        }

        Ok(fields)
    }

    fn parse_having_clause(&mut self) -> Result<SqlHavingClause, SqlParseError> {
        let left = self.parse_having_value_expr()?;

        if self.eat_keyword(Keyword::Is) {
            let is_not = self.eat_keyword(Keyword::Not);
            self.expect_keyword(Keyword::Null)?;

            return Ok(SqlHavingClause {
                left,
                op: if is_not { CompareOp::Ne } else { CompareOp::Eq },
                right: SqlHavingValueExpr::Literal(Value::Null),
            });
        }

        let op = self.parse_compare_operator()?;
        let right = self.parse_having_value_expr()?;

        Ok(SqlHavingClause { left, op, right })
    }

    fn parse_having_value_expr(&mut self) -> Result<SqlHavingValueExpr, SqlParseError> {
        if !matches!(
            self.peek_kind(),
            Some(crate::db::sql_shared::TokenKind::Identifier(_))
        ) && self.parse_aggregate_kind().is_none()
        {
            return self.parse_literal().map(SqlHavingValueExpr::Literal);
        }

        let left = if let Some(kind) = self.parse_aggregate_kind() {
            SqlProjectionOperand::Aggregate(self.parse_aggregate_call(kind)?)
        } else {
            let field = self.expect_identifier()?;
            if self.peek_lparen() {
                if field.eq_ignore_ascii_case("ROUND") {
                    return Ok(SqlHavingValueExpr::Round(
                        self.parse_round_projection_call()?,
                    ));
                }

                return Err(SqlParseError::unsupported_feature(
                    "SQL function namespace beyond supported aggregate, ROUND, or grouped HAVING forms",
                ));
            }

            SqlProjectionOperand::Field(field)
        };

        if self.eat_plus() {
            return Ok(SqlHavingValueExpr::Arithmetic(
                self.parse_arithmetic_projection_call(left, SqlArithmeticProjectionOp::Add)?,
            ));
        }
        if self.eat_minus() {
            return Ok(SqlHavingValueExpr::Arithmetic(
                self.parse_arithmetic_projection_call(left, SqlArithmeticProjectionOp::Sub)?,
            ));
        }
        if self.eat_star() {
            return Ok(SqlHavingValueExpr::Arithmetic(
                self.parse_arithmetic_projection_call(left, SqlArithmeticProjectionOp::Mul)?,
            ));
        }
        if self.eat_slash() {
            return Ok(SqlHavingValueExpr::Arithmetic(
                self.parse_arithmetic_projection_call(left, SqlArithmeticProjectionOp::Div)?,
            ));
        }

        Ok(match left {
            SqlProjectionOperand::Field(field) => SqlHavingValueExpr::Field(field),
            SqlProjectionOperand::Aggregate(aggregate) => SqlHavingValueExpr::Aggregate(aggregate),
            SqlProjectionOperand::Literal(literal) => SqlHavingValueExpr::Literal(literal),
            SqlProjectionOperand::Arithmetic(call) => SqlHavingValueExpr::Arithmetic(*call),
        })
    }
}

fn render_order_arithmetic_term(term: SqlArithmeticProjectionCall) -> String {
    let left = render_order_arithmetic_operand(term.left);
    let right = render_order_arithmetic_operand(term.right);
    let op = match term.op {
        SqlArithmeticProjectionOp::Add => "+",
        SqlArithmeticProjectionOp::Sub => "-",
        SqlArithmeticProjectionOp::Mul => "*",
        SqlArithmeticProjectionOp::Div => "/",
    };

    format!("{left} {op} {right}")
}

fn render_order_round_term(term: SqlRoundProjectionCall) -> String {
    let input = match term.input {
        SqlRoundProjectionInput::Operand(operand) => render_order_projection_operand(operand),
        SqlRoundProjectionInput::Arithmetic(arithmetic) => render_order_arithmetic_term(arithmetic),
    };

    format!("ROUND({input}, {})", render_order_literal(term.scale))
}

fn render_order_arithmetic_operand(operand: SqlProjectionOperand) -> String {
    match operand {
        SqlProjectionOperand::Arithmetic(call) => {
            format!("({})", render_order_arithmetic_term(*call))
        }
        other => render_order_projection_operand(other),
    }
}

fn render_order_projection_operand(operand: SqlProjectionOperand) -> String {
    match operand {
        SqlProjectionOperand::Field(field) => field,
        SqlProjectionOperand::Aggregate(aggregate) => render_order_aggregate_call(aggregate),
        SqlProjectionOperand::Literal(literal) => render_order_literal(literal),
        SqlProjectionOperand::Arithmetic(call) => render_order_arithmetic_term(*call),
    }
}

fn render_order_aggregate_call(aggregate: crate::db::sql::parser::SqlAggregateCall) -> String {
    let function = match aggregate.kind {
        crate::db::sql::parser::SqlAggregateKind::Count => "COUNT",
        crate::db::sql::parser::SqlAggregateKind::Sum => "SUM",
        crate::db::sql::parser::SqlAggregateKind::Avg => "AVG",
        crate::db::sql::parser::SqlAggregateKind::Min => "MIN",
        crate::db::sql::parser::SqlAggregateKind::Max => "MAX",
    };
    let distinct = if aggregate.distinct { "DISTINCT " } else { "" };
    let inner = match aggregate.input {
        Some(input) => render_order_aggregate_input_expr(*input, false),
        None => "*".to_string(),
    };

    format!("{function}({distinct}{inner})")
}

fn render_order_aggregate_input_expr(expr: SqlAggregateInputExpr, nested: bool) -> String {
    match expr {
        SqlAggregateInputExpr::Field(field) => field,
        SqlAggregateInputExpr::Literal(literal) => render_order_literal(literal),
        SqlAggregateInputExpr::Arithmetic(call) => {
            let rendered = render_order_aggregate_input_arithmetic_term(call);

            if nested {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        SqlAggregateInputExpr::Round(call) => render_order_round_term(call),
    }
}

fn render_order_aggregate_input_arithmetic_term(term: SqlArithmeticProjectionCall) -> String {
    let left = render_order_aggregate_input_operand(term.left);
    let right = render_order_aggregate_input_operand(term.right);
    let op = match term.op {
        SqlArithmeticProjectionOp::Add => "+",
        SqlArithmeticProjectionOp::Sub => "-",
        SqlArithmeticProjectionOp::Mul => "*",
        SqlArithmeticProjectionOp::Div => "/",
    };

    format!("{left} {op} {right}")
}

fn render_order_aggregate_input_operand(operand: SqlProjectionOperand) -> String {
    match operand {
        SqlProjectionOperand::Aggregate(aggregate) => render_order_aggregate_call(aggregate),
        SqlProjectionOperand::Field(field) => field,
        SqlProjectionOperand::Literal(literal) => render_order_literal(literal),
        SqlProjectionOperand::Arithmetic(call) => {
            format!("({})", render_order_aggregate_input_arithmetic_term(*call))
        }
    }
}

fn render_order_literal(value: Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Text(text) => format!("'{}'", text.replace('\'', "''")),
        Value::Int(value) => value.to_string(),
        Value::Int128(value) => value.to_string(),
        Value::IntBig(value) => value.to_string(),
        Value::Uint(value) => value.to_string(),
        Value::Uint128(value) => value.to_string(),
        Value::UintBig(value) => value.to_string(),
        Value::Decimal(value) => value.to_string(),
        Value::Float32(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Bool(value) => value.to_string().to_uppercase(),
        other => format!("{other:?}"),
    }
}
