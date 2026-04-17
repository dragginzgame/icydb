//! Module: db::sql::parser::projection
//! Responsibility: reduced SQL projection, aggregate call, and narrow text-function parsing.
//! Does not own: statement-level clause ordering, predicate semantics, or execution behavior.
//! Boundary: keeps projection-specific syntax branching out of the statement parser shell.

use crate::{
    db::{
        sql::parser::{
            Parser, SqlAggregateCall, SqlAggregateInputExpr, SqlAggregateKind,
            SqlArithmeticProjectionCall, SqlArithmeticProjectionOp, SqlProjection,
            SqlProjectionOperand, SqlRoundProjectionCall, SqlRoundProjectionInput, SqlSelectItem,
            SqlTextFunction, SqlTextFunctionCall,
        },
        sql_shared::{Keyword, TokenKind},
    },
    value::Value,
};

impl Parser {
    pub(super) fn parse_projection(
        &mut self,
    ) -> Result<(SqlProjection, Vec<Option<String>>), crate::db::sql_shared::SqlParseError> {
        if self.eat_star() {
            return Ok((SqlProjection::All, Vec::new()));
        }

        let mut items = Vec::new();
        let mut aliases = Vec::new();
        loop {
            items.push(self.parse_select_item()?);
            aliases.push(self.parse_projection_alias_if_present()?);

            if self.eat_comma() {
                continue;
            }

            break;
        }

        if items.is_empty() {
            return Err(crate::db::sql_shared::SqlParseError::expected(
                "one projection item",
                self.peek_kind(),
            ));
        }

        Ok((SqlProjection::Items(items), aliases))
    }

    fn parse_select_item(&mut self) -> Result<SqlSelectItem, crate::db::sql_shared::SqlParseError> {
        if matches!(
            self.peek_kind(),
            Some(
                TokenKind::StringLiteral(_)
                    | TokenKind::Number(_)
                    | TokenKind::Keyword(
                        crate::db::sql_shared::Keyword::Null
                            | crate::db::sql_shared::Keyword::True
                            | crate::db::sql_shared::Keyword::False,
                    )
                    | TokenKind::LParen
            )
        ) {
            let expr = self.parse_projection_arithmetic_expr(0)?;

            return match expr {
                SqlProjectionOperand::Arithmetic(call) => Ok(SqlSelectItem::Arithmetic(*call)),
                SqlProjectionOperand::Field(field) => Ok(SqlSelectItem::Field(field)),
                SqlProjectionOperand::Aggregate(aggregate) => {
                    Ok(SqlSelectItem::Aggregate(aggregate))
                }
                SqlProjectionOperand::Literal(_) => {
                    Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                        "standalone literal projection items are not supported",
                    ))
                }
            };
        }

        if let Some(kind) = self.parse_aggregate_kind() {
            let aggregate = self.parse_aggregate_call(kind)?;
            let expr = self.parse_projection_arithmetic_expr_tail(
                SqlProjectionOperand::Aggregate(aggregate),
                0,
            )?;

            return Self::select_item_from_projection_expr(expr);
        }

        let field = self.expect_identifier()?;
        if self.peek_lparen() {
            if field.eq_ignore_ascii_case("ROUND") {
                return Ok(SqlSelectItem::Round(self.parse_round_projection_call()?));
            }

            let Some(function) = SqlTextFunction::from_identifier(field.as_str()) else {
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "SQL function namespace beyond supported aggregate or scalar text projection forms",
                ));
            };

            return Ok(SqlSelectItem::TextFunction(
                self.parse_text_function_call(function)?,
            ));
        }

        let expr =
            self.parse_projection_arithmetic_expr_tail(SqlProjectionOperand::Field(field), 0)?;

        Self::select_item_from_projection_expr(expr)
    }

    pub(super) fn parse_aggregate_kind(&self) -> Option<SqlAggregateKind> {
        match self.peek_kind() {
            Some(TokenKind::Keyword(Keyword::Count)) => Some(SqlAggregateKind::Count),
            Some(TokenKind::Keyword(Keyword::Sum)) => Some(SqlAggregateKind::Sum),
            Some(TokenKind::Keyword(Keyword::Avg)) => Some(SqlAggregateKind::Avg),
            Some(TokenKind::Keyword(Keyword::Min)) => Some(SqlAggregateKind::Min),
            Some(TokenKind::Keyword(Keyword::Max)) => Some(SqlAggregateKind::Max),
            _ => None,
        }
    }

    pub(super) fn parse_aggregate_call(
        &mut self,
        kind: SqlAggregateKind,
    ) -> Result<SqlAggregateCall, crate::db::sql_shared::SqlParseError> {
        let _ = self.cursor.advance();
        self.expect_lparen()?;
        let distinct = self.eat_keyword(Keyword::Distinct);

        let input = if kind == SqlAggregateKind::Count && self.eat_star() {
            None
        } else {
            Some(self.parse_aggregate_input_expr()?)
        };

        self.expect_rparen()?;

        Ok(SqlAggregateCall {
            kind,
            input: input.map(Box::new),
            distinct,
        })
    }

    fn parse_aggregate_input_expr(
        &mut self,
    ) -> Result<SqlAggregateInputExpr, crate::db::sql_shared::SqlParseError> {
        if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            let field = self.expect_identifier()?;
            if self.peek_lparen() {
                if !field.eq_ignore_ascii_case("ROUND") {
                    return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                        "aggregate input functions beyond supported ROUND(...) forms",
                    ));
                }

                return Ok(SqlAggregateInputExpr::Round(
                    self.parse_aggregate_input_round_call()?,
                ));
            }

            let expr = self.parse_aggregate_input_arithmetic_expr_tail(
                SqlProjectionOperand::Field(field),
                0,
            )?;

            return Self::aggregate_input_expr_from_projection_expr(expr);
        }

        let expr = self.parse_aggregate_input_arithmetic_expr(0)?;

        Self::aggregate_input_expr_from_projection_expr(expr)
    }

    fn parse_text_function_call(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlTextFunctionCall, crate::db::sql_shared::SqlParseError> {
        self.expect_lparen()?;

        let call = match function {
            SqlTextFunction::Trim
            | SqlTextFunction::Ltrim
            | SqlTextFunction::Rtrim
            | SqlTextFunction::Lower
            | SqlTextFunction::Upper
            | SqlTextFunction::Length => self.parse_unary_text_function_call(function)?,
            SqlTextFunction::Left
            | SqlTextFunction::Right
            | SqlTextFunction::StartsWith
            | SqlTextFunction::EndsWith
            | SqlTextFunction::Contains => {
                self.parse_field_plus_literal_text_function_call(function)?
            }
            SqlTextFunction::Position => self.parse_position_text_function_call(function)?,
            SqlTextFunction::Replace => self.parse_replace_text_function_call(function)?,
            SqlTextFunction::Substring => self.parse_substring_text_function_call(function)?,
        };

        self.expect_rparen()?;

        Ok(call)
    }

    // Parse one optional projection alias while keeping alias ownership at the
    // parser/session boundary instead of widening planner semantics.
    fn parse_projection_alias_if_present(
        &mut self,
    ) -> Result<Option<String>, crate::db::sql_shared::SqlParseError> {
        if self.eat_keyword(Keyword::As) {
            return self.expect_identifier().map(Some);
        }

        if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            return self.expect_identifier().map(Some);
        }

        Ok(None)
    }

    fn parse_unary_text_function_call(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlTextFunctionCall, crate::db::sql_shared::SqlParseError> {
        let field = self.expect_identifier()?;

        Ok(Self::text_function_call(function, field, None, None, None))
    }

    fn parse_field_plus_literal_text_function_call(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlTextFunctionCall, crate::db::sql_shared::SqlParseError> {
        let field = self.expect_identifier()?;
        self.expect_text_function_argument_comma()?;
        let literal = self.parse_literal()?;

        Ok(Self::text_function_call(
            function,
            field,
            Some(literal),
            None,
            None,
        ))
    }

    fn parse_position_text_function_call(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlTextFunctionCall, crate::db::sql_shared::SqlParseError> {
        let literal = self.parse_literal()?;
        self.expect_text_function_argument_comma()?;
        let field = self.expect_identifier()?;

        Ok(Self::text_function_call(
            function,
            field,
            Some(literal),
            None,
            None,
        ))
    }

    fn parse_replace_text_function_call(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlTextFunctionCall, crate::db::sql_shared::SqlParseError> {
        let field = self.expect_identifier()?;
        self.expect_text_function_argument_comma()?;
        let from = self.parse_literal()?;
        self.expect_text_function_argument_comma()?;
        let to = self.parse_literal()?;

        Ok(Self::text_function_call(
            function,
            field,
            Some(from),
            Some(to),
            None,
        ))
    }

    fn parse_substring_text_function_call(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlTextFunctionCall, crate::db::sql_shared::SqlParseError> {
        let field = self.expect_identifier()?;
        self.expect_text_function_argument_comma()?;
        let start = self.parse_literal()?;

        if !self.eat_comma() {
            return Ok(Self::text_function_call(
                function,
                field,
                Some(start),
                None,
                None,
            ));
        }

        let length = self.parse_literal()?;

        Ok(Self::text_function_call(
            function,
            field,
            Some(start),
            Some(length),
            None,
        ))
    }

    fn expect_text_function_argument_comma(
        &mut self,
    ) -> Result<(), crate::db::sql_shared::SqlParseError> {
        if self.eat_comma() {
            return Ok(());
        }

        Err(crate::db::sql_shared::SqlParseError::expected(
            "',' between text function arguments",
            self.peek_kind(),
        ))
    }

    pub(super) fn parse_projection_operand(
        &mut self,
    ) -> Result<SqlProjectionOperand, crate::db::sql_shared::SqlParseError> {
        if self.cursor.eat_lparen() {
            let expr = self.parse_projection_arithmetic_expr(0)?;
            self.expect_rparen()?;

            return Ok(expr);
        }
        if let Some(kind) = self.parse_aggregate_kind() {
            return self
                .parse_aggregate_call(kind)
                .map(SqlProjectionOperand::Aggregate);
        }

        self.expect_identifier().map(SqlProjectionOperand::Field)
    }

    pub(super) fn parse_projection_operand_or_literal(
        &mut self,
    ) -> Result<SqlProjectionOperand, crate::db::sql_shared::SqlParseError> {
        if self.peek_lparen() {
            self.expect_lparen()?;
            let expr = self.parse_projection_arithmetic_expr(0)?;
            self.expect_rparen()?;

            return Ok(expr);
        }
        if self.parse_aggregate_kind().is_some() {
            return self.parse_projection_operand();
        }
        if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            return self.parse_projection_operand();
        }

        self.parse_literal().map(SqlProjectionOperand::Literal)
    }

    fn parse_aggregate_input_operand_or_literal(
        &mut self,
    ) -> Result<SqlProjectionOperand, crate::db::sql_shared::SqlParseError> {
        if self.peek_lparen() {
            self.expect_lparen()?;
            let expr = self.parse_aggregate_input_arithmetic_expr(0)?;
            self.expect_rparen()?;

            return Ok(expr);
        }
        if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            return self.parse_aggregate_input_operand();
        }

        self.parse_literal().map(SqlProjectionOperand::Literal)
    }

    fn parse_aggregate_input_operand(
        &mut self,
    ) -> Result<SqlProjectionOperand, crate::db::sql_shared::SqlParseError> {
        let field = self.expect_identifier()?;
        if self.peek_lparen() {
            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "nested aggregate input functions inside arithmetic expressions",
            ));
        }

        Ok(SqlProjectionOperand::Field(field))
    }

    fn select_item_from_projection_expr(
        expr: SqlProjectionOperand,
    ) -> Result<SqlSelectItem, crate::db::sql_shared::SqlParseError> {
        match expr {
            SqlProjectionOperand::Field(field) => Ok(SqlSelectItem::Field(field)),
            SqlProjectionOperand::Aggregate(aggregate) => Ok(SqlSelectItem::Aggregate(aggregate)),
            SqlProjectionOperand::Arithmetic(call) => Ok(SqlSelectItem::Arithmetic(*call)),
            SqlProjectionOperand::Literal(_) => {
                Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "standalone literal projection items are not supported",
                ))
            }
        }
    }

    fn aggregate_input_expr_from_projection_expr(
        expr: SqlProjectionOperand,
    ) -> Result<SqlAggregateInputExpr, crate::db::sql_shared::SqlParseError> {
        match expr {
            SqlProjectionOperand::Field(field) => Ok(SqlAggregateInputExpr::Field(field)),
            SqlProjectionOperand::Literal(literal) => Ok(SqlAggregateInputExpr::Literal(literal)),
            SqlProjectionOperand::Arithmetic(call) => Ok(SqlAggregateInputExpr::Arithmetic(*call)),
            SqlProjectionOperand::Aggregate(_) => {
                Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "nested aggregate references inside aggregate input expressions",
                ))
            }
        }
    }

    fn parse_projection_arithmetic_expr(
        &mut self,
        min_precedence: u8,
    ) -> Result<SqlProjectionOperand, crate::db::sql_shared::SqlParseError> {
        let left = self.parse_projection_operand_or_literal()?;

        self.parse_projection_arithmetic_expr_tail(left, min_precedence)
    }

    fn parse_projection_arithmetic_expr_tail(
        &mut self,
        mut left: SqlProjectionOperand,
        min_precedence: u8,
    ) -> Result<SqlProjectionOperand, crate::db::sql_shared::SqlParseError> {
        while let Some(op) = self.peek_arithmetic_projection_op() {
            let precedence = arithmetic_projection_op_precedence(op);
            if precedence < min_precedence {
                break;
            }

            let _ = self.eat_arithmetic_projection_op();
            let right = self.parse_projection_arithmetic_expr(precedence.saturating_add(1))?;
            left = SqlProjectionOperand::Arithmetic(Box::new(SqlArithmeticProjectionCall {
                left,
                op,
                right,
            }));
        }

        Ok(left)
    }

    fn parse_aggregate_input_arithmetic_expr(
        &mut self,
        min_precedence: u8,
    ) -> Result<SqlProjectionOperand, crate::db::sql_shared::SqlParseError> {
        let left = self.parse_aggregate_input_operand_or_literal()?;

        self.parse_aggregate_input_arithmetic_expr_tail(left, min_precedence)
    }

    fn parse_aggregate_input_arithmetic_expr_tail(
        &mut self,
        mut left: SqlProjectionOperand,
        min_precedence: u8,
    ) -> Result<SqlProjectionOperand, crate::db::sql_shared::SqlParseError> {
        while let Some(op) = self.peek_arithmetic_projection_op() {
            let precedence = arithmetic_projection_op_precedence(op);
            if precedence < min_precedence {
                break;
            }

            let _ = self.eat_arithmetic_projection_op();
            let right = self.parse_aggregate_input_arithmetic_expr(precedence.saturating_add(1))?;
            left = SqlProjectionOperand::Arithmetic(Box::new(SqlArithmeticProjectionCall {
                left,
                op,
                right,
            }));
        }

        Ok(left)
    }

    fn parse_aggregate_input_round_call(
        &mut self,
    ) -> Result<SqlRoundProjectionCall, crate::db::sql_shared::SqlParseError> {
        self.expect_lparen()?;

        let operand = self.parse_aggregate_input_arithmetic_expr(0)?;
        let input = match operand {
            SqlProjectionOperand::Arithmetic(call) => SqlRoundProjectionInput::Arithmetic(*call),
            other => SqlRoundProjectionInput::Operand(other),
        };

        self.expect_round_projection_argument_comma()?;
        let scale = self.parse_literal()?;
        self.expect_rparen()?;

        Ok(SqlRoundProjectionCall { input, scale })
    }

    pub(super) fn parse_arithmetic_projection_call(
        &mut self,
        left: SqlProjectionOperand,
        op: SqlArithmeticProjectionOp,
    ) -> Result<SqlArithmeticProjectionCall, crate::db::sql_shared::SqlParseError> {
        let right = self.parse_projection_arithmetic_expr(
            arithmetic_projection_op_precedence(op).saturating_add(1),
        )?;

        Ok(SqlArithmeticProjectionCall { left, op, right })
    }

    pub(super) fn parse_round_projection_call(
        &mut self,
    ) -> Result<SqlRoundProjectionCall, crate::db::sql_shared::SqlParseError> {
        self.expect_lparen()?;

        let operand = self.parse_projection_arithmetic_expr(0)?;
        let input = match operand {
            SqlProjectionOperand::Arithmetic(call) => SqlRoundProjectionInput::Arithmetic(*call),
            other => SqlRoundProjectionInput::Operand(other),
        };

        self.expect_round_projection_argument_comma()?;
        let scale = self.parse_literal()?;
        self.expect_rparen()?;

        Ok(SqlRoundProjectionCall { input, scale })
    }

    fn expect_round_projection_argument_comma(
        &mut self,
    ) -> Result<(), crate::db::sql_shared::SqlParseError> {
        if self.eat_comma() {
            return Ok(());
        }

        Err(crate::db::sql_shared::SqlParseError::expected(
            "',' between ROUND arguments",
            self.peek_kind(),
        ))
    }

    const fn text_function_call(
        function: SqlTextFunction,
        field: String,
        literal: Option<Value>,
        literal2: Option<Value>,
        literal3: Option<Value>,
    ) -> SqlTextFunctionCall {
        SqlTextFunctionCall {
            function,
            field,
            literal,
            literal2,
            literal3,
        }
    }

    fn peek_arithmetic_projection_op(&self) -> Option<SqlArithmeticProjectionOp> {
        match self.peek_kind() {
            Some(TokenKind::Plus) => Some(SqlArithmeticProjectionOp::Add),
            Some(TokenKind::Minus) => Some(SqlArithmeticProjectionOp::Sub),
            Some(TokenKind::Star) => Some(SqlArithmeticProjectionOp::Mul),
            Some(TokenKind::Slash) => Some(SqlArithmeticProjectionOp::Div),
            _ => None,
        }
    }

    fn eat_arithmetic_projection_op(&mut self) -> Option<SqlArithmeticProjectionOp> {
        let op = self.peek_arithmetic_projection_op()?;
        let _ = self.cursor.advance();

        Some(op)
    }
}

const fn arithmetic_projection_op_precedence(op: SqlArithmeticProjectionOp) -> u8 {
    match op {
        SqlArithmeticProjectionOp::Add | SqlArithmeticProjectionOp::Sub => 1,
        SqlArithmeticProjectionOp::Mul | SqlArithmeticProjectionOp::Div => 2,
    }
}
