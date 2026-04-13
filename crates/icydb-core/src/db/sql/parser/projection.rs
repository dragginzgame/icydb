//! Module: db::sql::parser::projection
//! Responsibility: reduced SQL projection, aggregate call, and narrow text-function parsing.
//! Does not own: statement-level clause ordering, predicate semantics, or execution behavior.
//! Boundary: keeps projection-specific syntax branching out of the statement parser shell.

use crate::{
    db::{
        reduced_sql::{Keyword, TokenKind},
        sql::parser::{
            Parser, SqlAggregateCall, SqlAggregateKind, SqlArithmeticProjectionCall,
            SqlArithmeticProjectionOp, SqlProjection, SqlRoundProjectionCall,
            SqlRoundProjectionInput, SqlSelectItem, SqlTextFunction, SqlTextFunctionCall,
        },
    },
    value::Value,
};

impl Parser {
    pub(super) fn parse_projection(
        &mut self,
    ) -> Result<(SqlProjection, Vec<Option<String>>), crate::db::reduced_sql::SqlParseError> {
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
            return Err(crate::db::reduced_sql::SqlParseError::expected(
                "one projection item",
                self.peek_kind(),
            ));
        }

        Ok((SqlProjection::Items(items), aliases))
    }

    fn parse_select_item(
        &mut self,
    ) -> Result<SqlSelectItem, crate::db::reduced_sql::SqlParseError> {
        if let Some(kind) = self.parse_aggregate_kind() {
            return Ok(SqlSelectItem::Aggregate(self.parse_aggregate_call(kind)?));
        }

        let field = self.expect_identifier()?;
        if self.peek_lparen() {
            if field.eq_ignore_ascii_case("ROUND") {
                return Ok(SqlSelectItem::Round(self.parse_round_projection_call()?));
            }

            let Some(function) = SqlTextFunction::from_identifier(field.as_str()) else {
                return Err(crate::db::reduced_sql::SqlParseError::unsupported_feature(
                    "SQL function namespace beyond supported aggregate or scalar text projection forms",
                ));
            };

            return Ok(SqlSelectItem::TextFunction(
                self.parse_text_function_call(function)?,
            ));
        }

        if self.eat_plus() {
            return Ok(SqlSelectItem::Arithmetic(
                self.parse_arithmetic_projection_call(field, SqlArithmeticProjectionOp::Add)?,
            ));
        }
        if self.eat_minus() {
            return Ok(SqlSelectItem::Arithmetic(
                self.parse_arithmetic_projection_call(field, SqlArithmeticProjectionOp::Sub)?,
            ));
        }
        if self.eat_star() {
            return Ok(SqlSelectItem::Arithmetic(
                self.parse_arithmetic_projection_call(field, SqlArithmeticProjectionOp::Mul)?,
            ));
        }
        if self.eat_slash() {
            return Ok(SqlSelectItem::Arithmetic(
                self.parse_arithmetic_projection_call(field, SqlArithmeticProjectionOp::Div)?,
            ));
        }

        Ok(SqlSelectItem::Field(field))
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
    ) -> Result<SqlAggregateCall, crate::db::reduced_sql::SqlParseError> {
        let _ = self.cursor.advance();
        self.expect_lparen()?;
        let distinct = self.eat_keyword(Keyword::Distinct);

        let field = if kind == SqlAggregateKind::Count && self.eat_star() {
            None
        } else {
            Some(self.expect_identifier()?)
        };

        self.expect_rparen()?;

        Ok(SqlAggregateCall {
            kind,
            field,
            distinct,
        })
    }

    fn parse_text_function_call(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlTextFunctionCall, crate::db::reduced_sql::SqlParseError> {
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
    ) -> Result<Option<String>, crate::db::reduced_sql::SqlParseError> {
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
    ) -> Result<SqlTextFunctionCall, crate::db::reduced_sql::SqlParseError> {
        let field = self.expect_identifier()?;

        Ok(Self::text_function_call(function, field, None, None, None))
    }

    fn parse_field_plus_literal_text_function_call(
        &mut self,
        function: SqlTextFunction,
    ) -> Result<SqlTextFunctionCall, crate::db::reduced_sql::SqlParseError> {
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
    ) -> Result<SqlTextFunctionCall, crate::db::reduced_sql::SqlParseError> {
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
    ) -> Result<SqlTextFunctionCall, crate::db::reduced_sql::SqlParseError> {
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
    ) -> Result<SqlTextFunctionCall, crate::db::reduced_sql::SqlParseError> {
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
    ) -> Result<(), crate::db::reduced_sql::SqlParseError> {
        if self.eat_comma() {
            return Ok(());
        }

        Err(crate::db::reduced_sql::SqlParseError::expected(
            "',' between text function arguments",
            self.peek_kind(),
        ))
    }

    fn parse_arithmetic_projection_call(
        &mut self,
        field: String,
        op: SqlArithmeticProjectionOp,
    ) -> Result<SqlArithmeticProjectionCall, crate::db::reduced_sql::SqlParseError> {
        let literal = self.parse_literal()?;

        Ok(SqlArithmeticProjectionCall { field, op, literal })
    }

    fn parse_round_projection_call(
        &mut self,
    ) -> Result<SqlRoundProjectionCall, crate::db::reduced_sql::SqlParseError> {
        self.expect_lparen()?;

        let field = self.expect_identifier()?;
        let input = if self.eat_plus() {
            SqlRoundProjectionInput::Arithmetic(
                self.parse_arithmetic_projection_call(field, SqlArithmeticProjectionOp::Add)?,
            )
        } else if self.eat_minus() {
            SqlRoundProjectionInput::Arithmetic(
                self.parse_arithmetic_projection_call(field, SqlArithmeticProjectionOp::Sub)?,
            )
        } else if self.eat_star() {
            SqlRoundProjectionInput::Arithmetic(
                self.parse_arithmetic_projection_call(field, SqlArithmeticProjectionOp::Mul)?,
            )
        } else if self.eat_slash() {
            SqlRoundProjectionInput::Arithmetic(
                self.parse_arithmetic_projection_call(field, SqlArithmeticProjectionOp::Div)?,
            )
        } else {
            SqlRoundProjectionInput::Field(field)
        };

        self.expect_round_projection_argument_comma()?;
        let scale = self.parse_literal()?;
        self.expect_rparen()?;

        Ok(SqlRoundProjectionCall { input, scale })
    }

    fn expect_round_projection_argument_comma(
        &mut self,
    ) -> Result<(), crate::db::reduced_sql::SqlParseError> {
        if self.eat_comma() {
            return Ok(());
        }

        Err(crate::db::reduced_sql::SqlParseError::expected(
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
}
