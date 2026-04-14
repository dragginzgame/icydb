//! Module: db::sql::parser::clauses
//! Responsibility: reduced SQL clause parsing shared by statement shells.
//! Does not own: statement routing, projection parsing, or predicate semantics.
//! Boundary: keeps ordering/grouping/HAVING helpers out of the parser root.

use crate::{
    db::{
        predicate::CompareOp,
        reduced_sql::{Keyword, SqlParseError},
        sql::parser::{
            Parser, SqlArithmeticProjectionCall, SqlArithmeticProjectionOp,
            SqlArithmeticProjectionOperand, SqlHavingClause, SqlHavingSymbol, SqlOrderDirection,
            SqlOrderTerm, SqlRoundProjectionCall, SqlRoundProjectionInput, SqlTextFunction,
        },
    },
    value::Value,
};

const ORDER_BY_UNSUPPORTED_FEATURE: &str = "ORDER BY terms beyond supported field, LOWER/UPPER(...), bounded arithmetic, or ROUND(...) forms";

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
        let field = self.expect_identifier()?;
        if let Some(op) = self.parse_direct_order_arithmetic_op() {
            return Ok(render_order_arithmetic_term(
                self.parse_arithmetic_projection_call(field, op)?,
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
            SqlTextFunction::Lower | SqlTextFunction::Upper => {
                self.expect_lparen()?;
                let field = self.expect_identifier()?;
                self.expect_rparen()?;

                Ok(match function {
                    SqlTextFunction::Lower => format!("LOWER({field})"),
                    SqlTextFunction::Upper => format!("UPPER({field})"),
                    SqlTextFunction::Trim
                    | SqlTextFunction::Ltrim
                    | SqlTextFunction::Rtrim
                    | SqlTextFunction::Length
                    | SqlTextFunction::Left
                    | SqlTextFunction::Right
                    | SqlTextFunction::StartsWith
                    | SqlTextFunction::EndsWith
                    | SqlTextFunction::Contains
                    | SqlTextFunction::Position
                    | SqlTextFunction::Replace
                    | SqlTextFunction::Substring => unreachable!(),
                })
            }
            SqlTextFunction::Trim
            | SqlTextFunction::Ltrim
            | SqlTextFunction::Rtrim
            | SqlTextFunction::Length
            | SqlTextFunction::Left
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
        let symbol = self.parse_having_symbol()?;

        if self.eat_keyword(Keyword::Is) {
            let is_not = self.eat_keyword(Keyword::Not);
            self.expect_keyword(Keyword::Null)?;

            return Ok(SqlHavingClause {
                symbol,
                op: if is_not { CompareOp::Ne } else { CompareOp::Eq },
                value: Value::Null,
            });
        }

        let op = self.parse_compare_operator()?;
        let value = self.parse_literal()?;

        Ok(SqlHavingClause { symbol, op, value })
    }

    fn parse_having_symbol(&mut self) -> Result<SqlHavingSymbol, SqlParseError> {
        if let Some(kind) = self.parse_aggregate_kind() {
            return Ok(SqlHavingSymbol::Aggregate(self.parse_aggregate_call(kind)?));
        }

        let field = self.expect_identifier()?;
        if self.peek_lparen() {
            return Err(SqlParseError::unsupported_feature(
                "SQL function namespace beyond supported aggregate forms",
            ));
        }

        Ok(SqlHavingSymbol::Field(field))
    }
}

fn render_order_arithmetic_term(term: SqlArithmeticProjectionCall) -> String {
    let rhs = match term.rhs {
        SqlArithmeticProjectionOperand::Field(field) => field,
        SqlArithmeticProjectionOperand::Literal(literal) => render_order_literal(literal),
    };
    let op = match term.op {
        SqlArithmeticProjectionOp::Add => "+",
        SqlArithmeticProjectionOp::Sub => "-",
        SqlArithmeticProjectionOp::Mul => "*",
        SqlArithmeticProjectionOp::Div => "/",
    };

    format!("{} {op} {rhs}", term.field)
}

fn render_order_round_term(term: SqlRoundProjectionCall) -> String {
    let input = match term.input {
        SqlRoundProjectionInput::Field(field) => field,
        SqlRoundProjectionInput::Arithmetic(arithmetic) => render_order_arithmetic_term(arithmetic),
    };

    format!("ROUND({input}, {})", render_order_literal(term.scale))
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
