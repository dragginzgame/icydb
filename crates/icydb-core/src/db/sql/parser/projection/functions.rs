use crate::{
    db::{
        sql::parser::{
            Parser, SqlExpr, SqlScalarFunction, SqlScalarFunctionCallShape,
            projection::SqlExprParseSurface,
        },
        sql_shared::{Keyword, SqlParseError, TokenKind},
    },
    value::Value,
};

impl Parser {
    pub(in crate::db::sql::parser) fn parse_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, SqlParseError> {
        let expr = match function.non_where_call_shape() {
            SqlScalarFunctionCallShape::NumericScaleSpecial => {
                self.parse_numeric_scale_function_call(function, surface)?
            }
            SqlScalarFunctionCallShape::VariadicExprArgs => {
                self.expect_lparen()?;
                let expr = self.parse_coalesce_scalar_function_call(surface)?;
                self.expect_rparen()?;

                expr
            }
            SqlScalarFunctionCallShape::BinaryExprArgs => {
                self.expect_lparen()?;
                let expr = self.parse_binary_expr_scalar_function_call(function, surface)?;
                self.expect_rparen()?;

                expr
            }
            SqlScalarFunctionCallShape::UnaryExpr => {
                self.expect_lparen()?;
                let expr = SqlExpr::FunctionCall {
                    function,
                    args: vec![self.parse_sql_expr(surface, 0)?],
                };
                self.expect_rparen()?;

                expr
            }
            SqlScalarFunctionCallShape::FieldPlusLiteral => {
                self.expect_lparen()?;
                let expr = self.parse_field_plus_literal_scalar_function_call(function)?;
                self.expect_rparen()?;

                expr
            }
            SqlScalarFunctionCallShape::Position => {
                self.expect_lparen()?;
                let expr = self.parse_position_scalar_function_call(function)?;
                self.expect_rparen()?;

                expr
            }
            SqlScalarFunctionCallShape::Replace => {
                self.expect_lparen()?;
                let expr = self.parse_replace_scalar_function_call(function)?;
                self.expect_rparen()?;

                expr
            }
            SqlScalarFunctionCallShape::Substring => {
                self.expect_lparen()?;
                let expr = self.parse_substring_scalar_function_call(function)?;
                self.expect_rparen()?;

                expr
            }
            SqlScalarFunctionCallShape::SharedScalarCall
            | SqlScalarFunctionCallShape::WherePredicateExprPair => {
                unreachable!("non-WHERE scalar parser should not request WHERE-only call shapes")
            }
        };

        Ok(expr)
    }

    // Detect one function-call shell followed immediately by an unsupported
    // keyword so parser diagnostics can report the real feature family instead
    // of a generic unknown-function namespace error.
    pub(super) fn function_call_is_followed_by_keyword(&self, keyword: Keyword) -> bool {
        if !matches!(self.cursor.peek_kind_at(0), Some(TokenKind::LParen)) {
            return false;
        }

        let mut offset = 1usize;
        let mut depth = 1usize;
        while let Some(kind) = self.cursor.peek_kind_at(offset) {
            match kind {
                TokenKind::LParen => {
                    depth = depth.saturating_add(1);
                    offset = offset.saturating_add(1);
                }
                TokenKind::RParen => {
                    depth = depth.saturating_sub(1);
                    offset = offset.saturating_add(1);
                    if depth == 0 {
                        return self.cursor.peek_keyword_at(offset, keyword);
                    }
                }
                _ => {
                    offset = offset.saturating_add(1);
                }
            }
        }

        false
    }

    fn parse_field_plus_literal_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, SqlParseError> {
        let field = self.expect_identifier()?;
        self.expect_scalar_function_argument_comma()?;
        let literal = self.parse_literal()?;

        Ok(Self::scalar_function_call(function, field, vec![literal]))
    }

    fn parse_position_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, SqlParseError> {
        let literal = self.parse_literal()?;
        self.expect_scalar_function_argument_comma()?;
        let field = self.expect_identifier()?;

        Ok(SqlExpr::FunctionCall {
            function,
            args: vec![
                SqlExpr::Literal(literal),
                SqlExpr::from_field_identifier(field),
            ],
        })
    }

    fn parse_replace_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, SqlParseError> {
        let field = self.expect_identifier()?;
        self.expect_scalar_function_argument_comma()?;
        let from = self.parse_literal()?;
        self.expect_scalar_function_argument_comma()?;
        let to = self.parse_literal()?;

        Ok(Self::scalar_function_call(function, field, vec![from, to]))
    }

    fn parse_substring_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, SqlParseError> {
        let field = self.expect_identifier()?;
        self.expect_scalar_function_argument_comma()?;
        let start = self.parse_literal()?;

        if !self.eat_comma() {
            return Ok(Self::scalar_function_call(function, field, vec![start]));
        }

        let length = self.parse_literal()?;

        Ok(Self::scalar_function_call(
            function,
            field,
            vec![start, length],
        ))
    }

    // Parse one variadic COALESCE(...) call on the shared projection
    // expression parser so nested arguments can reuse the current expression
    // grammar instead of a function-local mini grammar.
    fn parse_coalesce_scalar_function_call(
        &mut self,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, SqlParseError> {
        let mut args = vec![self.parse_sql_expr(surface, 0)?];
        while self.eat_comma() {
            args.push(self.parse_sql_expr(surface, 0)?);
        }

        if args.len() < 2 {
            return Err(SqlParseError::invalid_syntax(
                "COALESCE requires at least two arguments",
            ));
        }

        Ok(SqlExpr::FunctionCall {
            function: SqlScalarFunction::Coalesce,
            args,
        })
    }

    // Parse one two-argument scalar call on the shared projection expression
    // parser while preserving the function identity chosen by the parser
    // surface.
    fn parse_binary_expr_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, SqlParseError> {
        let left = self.parse_sql_expr(surface, 0)?;
        self.expect_scalar_function_argument_comma()?;
        let right = self.parse_sql_expr(surface, 0)?;

        Ok(SqlExpr::FunctionCall {
            function,
            args: vec![left, right],
        })
    }

    fn expect_scalar_function_argument_comma(&mut self) -> Result<(), SqlParseError> {
        if self.eat_comma() {
            return Ok(());
        }

        Err(SqlParseError::expected(
            "',' between scalar function arguments",
            self.peek_kind(),
        ))
    }

    pub(super) fn parse_where_function_expr(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, SqlParseError> {
        match function.where_call_shape() {
            SqlScalarFunctionCallShape::NumericScaleSpecial => {
                self.parse_numeric_scale_function_call(function, SqlExprParseSurface::Where)
            }
            SqlScalarFunctionCallShape::VariadicExprArgs => {
                self.expect_lparen()?;
                let mut args = vec![self.parse_sql_expr(SqlExprParseSurface::Where, 0)?];
                while self.eat_comma() {
                    args.push(self.parse_sql_expr(SqlExprParseSurface::Where, 0)?);
                }
                self.expect_rparen()?;

                if args.len() < 2 {
                    return Err(SqlParseError::invalid_syntax(
                        "COALESCE requires at least two arguments",
                    ));
                }

                Ok(SqlExpr::FunctionCall { function, args })
            }
            SqlScalarFunctionCallShape::BinaryExprArgs
            | SqlScalarFunctionCallShape::WherePredicateExprPair => {
                self.expect_lparen()?;
                let left = self.parse_sql_expr(SqlExprParseSurface::Where, 0)?;
                if !self.eat_comma() {
                    return Err(SqlParseError::expected(",", self.peek_kind()));
                }
                let right = self.parse_sql_expr(SqlExprParseSurface::Where, 0)?;
                self.expect_rparen()?;

                Ok(SqlExpr::FunctionCall {
                    function,
                    args: vec![left, right],
                })
            }
            SqlScalarFunctionCallShape::SharedScalarCall => {
                self.parse_scalar_function_call(function, SqlExprParseSurface::Where)
            }
            SqlScalarFunctionCallShape::UnaryExpr
            | SqlScalarFunctionCallShape::FieldPlusLiteral
            | SqlScalarFunctionCallShape::Position
            | SqlScalarFunctionCallShape::Replace
            | SqlScalarFunctionCallShape::Substring => {
                unreachable!("WHERE scalar parser should only request WHERE call shapes")
            }
        }
    }

    fn scalar_function_call(
        function: SqlScalarFunction,
        field: String,
        literals: Vec<Value>,
    ) -> SqlExpr {
        let mut args = Vec::with_capacity(1 + literals.len());
        args.push(SqlExpr::from_field_identifier(field));
        args.extend(literals.into_iter().map(SqlExpr::Literal));

        SqlExpr::FunctionCall { function, args }
    }

    fn parse_numeric_scale_function_call(
        &mut self,
        function: SqlScalarFunction,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, SqlParseError> {
        self.expect_lparen()?;

        let input = self.parse_sql_expr(surface, 0)?;

        let mut args = vec![input];
        if self.eat_comma() {
            let scale = SqlExpr::Literal(self.parse_literal()?);
            let SqlExpr::Literal(Value::Int(_) | Value::Uint(_)) = scale else {
                return Err(SqlParseError::invalid_syntax(
                    "ROUND scale must be an integer literal",
                ));
            };
            args.push(scale);
        }
        self.expect_rparen()?;

        Ok(SqlExpr::FunctionCall { function, args })
    }
}
