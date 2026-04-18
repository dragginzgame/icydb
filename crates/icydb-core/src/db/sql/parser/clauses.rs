//! Module: db::sql::parser::clauses
//! Responsibility: reduced SQL clause parsing shared by statement shells.
//! Does not own: statement routing, projection parsing, or predicate semantics.
//! Boundary: keeps ordering/grouping/HAVING helpers out of the parser root.

use crate::db::sql::parser::projection::SqlExprParseSurface;
use crate::{
    db::{
        sql::parser::{
            Parser, SqlAggregateInputExpr, SqlArithmeticProjectionCall, SqlArithmeticProjectionOp,
            SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlOrderTerm, SqlProjectionOperand,
            SqlRoundProjectionCall, SqlRoundProjectionInput, SqlTextFunction, SqlTextFunctionCall,
        },
        sql_shared::{Keyword, SqlParseError},
    },
    value::Value,
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
    ) -> Result<String, SqlParseError> {
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

        Ok(render_order_text_function_term(call))
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

fn render_order_text_function_term(call: SqlTextFunctionCall) -> String {
    let field = call.field;

    match call.function {
        SqlTextFunction::Trim
        | SqlTextFunction::Ltrim
        | SqlTextFunction::Rtrim
        | SqlTextFunction::Lower
        | SqlTextFunction::Upper
        | SqlTextFunction::Length => {
            format!("{}({field})", order_text_function_sql_label(call.function))
        }
        SqlTextFunction::Left
        | SqlTextFunction::Right
        | SqlTextFunction::StartsWith
        | SqlTextFunction::EndsWith
        | SqlTextFunction::Contains => format!(
            "{}({field}, {})",
            order_text_function_sql_label(call.function),
            render_order_literal(
                call.literal
                    .expect("field-literal text function should keep one literal")
            ),
        ),
        SqlTextFunction::Position => format!(
            "{}({}, {field})",
            order_text_function_sql_label(call.function),
            render_order_literal(
                call.literal
                    .expect("position text function should keep one literal")
            ),
        ),
        SqlTextFunction::Replace => format!(
            "{}({field}, {}, {})",
            order_text_function_sql_label(call.function),
            render_order_literal(
                call.literal
                    .expect("replace text function should keep from literal")
            ),
            render_order_literal(
                call.literal2
                    .expect("replace text function should keep to literal")
            ),
        ),
        SqlTextFunction::Substring => match call.literal2 {
            Some(length) => format!(
                "{}({field}, {}, {})",
                order_text_function_sql_label(call.function),
                render_order_literal(call.literal.expect("substring should keep start literal")),
                render_order_literal(length),
            ),
            None => format!(
                "{}({field}, {})",
                order_text_function_sql_label(call.function),
                render_order_literal(call.literal.expect("substring should keep start literal")),
            ),
        },
    }
}

const fn order_text_function_sql_label(function: SqlTextFunction) -> &'static str {
    match function {
        SqlTextFunction::Trim => "TRIM",
        SqlTextFunction::Ltrim => "LTRIM",
        SqlTextFunction::Rtrim => "RTRIM",
        SqlTextFunction::Lower => "LOWER",
        SqlTextFunction::Upper => "UPPER",
        SqlTextFunction::Length => "LENGTH",
        SqlTextFunction::Left => "LEFT",
        SqlTextFunction::Right => "RIGHT",
        SqlTextFunction::StartsWith => "STARTS_WITH",
        SqlTextFunction::EndsWith => "ENDS_WITH",
        SqlTextFunction::Contains => "CONTAINS",
        SqlTextFunction::Position => "POSITION",
        SqlTextFunction::Replace => "REPLACE",
        SqlTextFunction::Substring => "SUBSTRING",
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
        SqlAggregateInputExpr::Expr(expr) => {
            let rendered = render_order_sql_expr(expr);

            if nested {
                format!("({rendered})")
            } else {
                rendered
            }
        }
    }
}

fn render_order_sql_expr(expr: SqlExpr) -> String {
    match expr {
        SqlExpr::Field(field) => field,
        SqlExpr::Aggregate(aggregate) => render_order_aggregate_call(aggregate),
        SqlExpr::Literal(literal) => render_order_literal(literal),
        SqlExpr::TextFunction(call) => render_order_text_function_term(call),
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => format!(
            "{} {}IN ({})",
            render_order_sql_expr(*expr),
            if negated { "NOT " } else { "" },
            values
                .into_iter()
                .map(render_order_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlExpr::NullTest { expr, negated } => format!(
            "{} IS{} NULL",
            render_order_sql_expr(*expr),
            if negated { " NOT" } else { "" }
        ),
        SqlExpr::FunctionCall { function, args } => format!(
            "{}({})",
            function_name(function),
            args.into_iter()
                .map(render_order_sql_expr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlExpr::Round(call) => render_order_round_term(call),
        SqlExpr::Unary {
            op: crate::db::sql::parser::SqlExprUnaryOp::Not,
            expr,
        } => format!("NOT {}", render_order_sql_expr(*expr)),
        SqlExpr::Binary { op, left, right } => {
            let op = match op {
                SqlExprBinaryOp::Or => "OR",
                SqlExprBinaryOp::And => "AND",
                SqlExprBinaryOp::Eq => "=",
                SqlExprBinaryOp::Ne => "!=",
                SqlExprBinaryOp::Lt => "<",
                SqlExprBinaryOp::Lte => "<=",
                SqlExprBinaryOp::Gt => ">",
                SqlExprBinaryOp::Gte => ">=",
                SqlExprBinaryOp::Add => "+",
                SqlExprBinaryOp::Sub => "-",
                SqlExprBinaryOp::Mul => "*",
                SqlExprBinaryOp::Div => "/",
            };

            format!(
                "{} {} {}",
                render_order_sql_expr(*left),
                op,
                render_order_sql_expr(*right)
            )
        }
        SqlExpr::Case { arms, else_expr } => {
            let mut rendered = String::from("CASE");
            for arm in arms {
                rendered.push_str(" WHEN ");
                rendered.push_str(render_order_sql_expr(arm.condition).as_str());
                rendered.push_str(" THEN ");
                rendered.push_str(render_order_sql_expr(arm.result).as_str());
            }
            if let Some(else_expr) = else_expr {
                rendered.push_str(" ELSE ");
                rendered.push_str(render_order_sql_expr(*else_expr).as_str());
            }
            rendered.push_str(" END");

            rendered
        }
    }
}

const fn function_name(function: crate::db::sql::parser::SqlTextFunction) -> &'static str {
    match function {
        crate::db::sql::parser::SqlTextFunction::Trim => "TRIM",
        crate::db::sql::parser::SqlTextFunction::Ltrim => "LTRIM",
        crate::db::sql::parser::SqlTextFunction::Rtrim => "RTRIM",
        crate::db::sql::parser::SqlTextFunction::Lower => "LOWER",
        crate::db::sql::parser::SqlTextFunction::Upper => "UPPER",
        crate::db::sql::parser::SqlTextFunction::Length => "LENGTH",
        crate::db::sql::parser::SqlTextFunction::Left => "LEFT",
        crate::db::sql::parser::SqlTextFunction::Right => "RIGHT",
        crate::db::sql::parser::SqlTextFunction::StartsWith => "STARTS_WITH",
        crate::db::sql::parser::SqlTextFunction::EndsWith => "ENDS_WITH",
        crate::db::sql::parser::SqlTextFunction::Contains => "CONTAINS",
        crate::db::sql::parser::SqlTextFunction::Position => "POSITION",
        crate::db::sql::parser::SqlTextFunction::Replace => "REPLACE",
        crate::db::sql::parser::SqlTextFunction::Substring => "SUBSTRING",
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
