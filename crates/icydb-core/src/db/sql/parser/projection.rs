//! Module: db::sql::parser::projection
//! Responsibility: reduced SQL projection, aggregate call, and narrow scalar-function parsing.
//! Does not own: statement-level clause ordering, predicate semantics, or execution behavior.
//! Boundary: keeps projection-specific syntax branching out of the statement parser shell.

use crate::{
    db::{
        sql::parser::{
            Parser, SqlAggregateCall, SqlAggregateKind, SqlCaseArm, SqlExpr, SqlExprBinaryOp,
            SqlExprUnaryOp, SqlProjection, SqlScalarFunction, SqlSelectItem,
        },
        sql_shared::{Keyword, TokenKind},
    },
    value::Value,
};

#[derive(Clone, Copy)]
pub(super) enum SqlExprParseSurface {
    Projection,
    ProjectionCondition,
    AggregateInput,
    AggregateInputCondition,
    HavingCondition,
    Where,
}

impl SqlExprParseSurface {
    const fn allows_aggregates(self) -> bool {
        matches!(
            self,
            Self::Projection | Self::ProjectionCondition | Self::HavingCondition
        )
    }

    const fn allows_general_scalar_functions(self) -> bool {
        matches!(
            self,
            Self::Projection
                | Self::ProjectionCondition
                | Self::AggregateInput
                | Self::AggregateInputCondition
                | Self::HavingCondition
                | Self::Where
        )
    }

    const fn allows_round_function(self) -> bool {
        matches!(
            self,
            Self::Projection
                | Self::ProjectionCondition
                | Self::AggregateInput
                | Self::HavingCondition
                | Self::Where
        )
    }

    const fn allows_predicate_postfix(self) -> bool {
        matches!(
            self,
            Self::ProjectionCondition
                | Self::AggregateInputCondition
                | Self::HavingCondition
                | Self::Where
        )
    }

    const fn canonicalizes_where_compare(self) -> bool {
        matches!(self, Self::Where)
    }

    // Searched CASE conditions reuse the owning clause's aggregate/scalar-function
    // authority, but they also need the postfix predicate family so `WHEN x IS NULL`
    // and similar condition forms do not stop at the shared infix parser.
    const fn case_condition_surface(self) -> Self {
        match self {
            Self::Projection | Self::ProjectionCondition => Self::ProjectionCondition,
            Self::AggregateInput | Self::AggregateInputCondition => Self::AggregateInputCondition,
            Self::HavingCondition => Self::HavingCondition,
            Self::Where => Self::Where,
        }
    }
}

impl Parser {
    pub(super) fn parse_where_expr(
        &mut self,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        self.record_predicate_parse_stage(|parser| {
            parser.parse_sql_expr(SqlExprParseSurface::Where, 0)
        })
    }

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
        if self.projection_item_is_simple_field() {
            return self.expect_identifier().map(SqlSelectItem::Field);
        }

        let expr = self.record_expr_parse_stage(|parser| {
            parser.parse_sql_expr(SqlExprParseSurface::Projection, 0)
        })?;

        Self::select_item_from_sql_expr(expr)
    }

    // Fast-path the common `SELECT field [, field ...] FROM ...` shape so the
    // shared floor does not pay the full projection expression parser for
    // simple field items and optional bare aliases.
    fn projection_item_is_simple_field(&self) -> bool {
        if !matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            return false;
        }

        let mut offset = 0usize;
        loop {
            if !matches!(
                self.cursor.peek_kind_at(offset),
                Some(TokenKind::Identifier(_))
            ) {
                return false;
            }

            if !matches!(self.cursor.peek_kind_at(offset + 1), Some(TokenKind::Dot)) {
                break;
            }
            if !matches!(
                self.cursor.peek_kind_at(offset + 2),
                Some(TokenKind::Identifier(_))
            ) {
                return false;
            }

            offset = offset.saturating_add(2);
        }

        matches!(
            self.cursor.peek_kind_at(offset + 1),
            Some(
                TokenKind::Comma
                    | TokenKind::Keyword(Keyword::From | Keyword::As)
                    | TokenKind::Identifier(_)
            )
        )
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
        let filter_expr = self.parse_aggregate_filter_clause()?;

        Ok(SqlAggregateCall {
            kind,
            input: input.map(Box::new),
            filter_expr: filter_expr.map(Box::new),
            distinct,
        })
    }

    // Parse one aggregate-owned FILTER predicate directly onto the aggregate
    // call instead of rewriting it through CASE or a clause-local wrapper.
    fn parse_aggregate_filter_clause(
        &mut self,
    ) -> Result<Option<SqlExpr>, crate::db::sql_shared::SqlParseError> {
        if !self.eat_keyword(Keyword::Filter) {
            return Ok(None);
        }

        self.expect_lparen()?;
        self.expect_keyword(Keyword::Where)?;
        let expr = self.record_predicate_parse_stage(|parser| {
            parser.parse_sql_expr(SqlExprParseSurface::Where, 0)
        })?;
        self.expect_rparen()?;

        Ok(Some(expr))
    }

    fn parse_aggregate_input_expr(
        &mut self,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let expr = self.parse_sql_expr(SqlExprParseSurface::AggregateInput, 0)?;

        if matches!(expr, SqlExpr::Aggregate(_)) {
            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "nested aggregate references inside aggregate input expressions",
            ));
        }

        Ok(expr)
    }

    pub(super) fn parse_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        self.expect_lparen()?;

        let expr = match function {
            SqlScalarFunction::Round => {
                unreachable!("ROUND should use the generic scalar-function parse path")
            }
            SqlScalarFunction::Coalesce => self.parse_coalesce_scalar_function_call(surface)?,
            SqlScalarFunction::NullIf => self.parse_nullif_scalar_function_call(surface)?,
            SqlScalarFunction::Trim
            | SqlScalarFunction::Ltrim
            | SqlScalarFunction::Rtrim
            | SqlScalarFunction::Abs
            | SqlScalarFunction::Ceil
            | SqlScalarFunction::Ceiling
            | SqlScalarFunction::Floor
            | SqlScalarFunction::Lower
            | SqlScalarFunction::Upper
            | SqlScalarFunction::Length => SqlExpr::FunctionCall {
                function,
                args: vec![self.parse_sql_expr(surface, 0)?],
            },
            SqlScalarFunction::Left
            | SqlScalarFunction::Right
            | SqlScalarFunction::StartsWith
            | SqlScalarFunction::EndsWith
            | SqlScalarFunction::Contains => {
                self.parse_field_plus_literal_scalar_function_call(function)?
            }
            SqlScalarFunction::Position => self.parse_position_scalar_function_call(function)?,
            SqlScalarFunction::Replace => self.parse_replace_scalar_function_call(function)?,
            SqlScalarFunction::Substring => self.parse_substring_scalar_function_call(function)?,
        };

        self.expect_rparen()?;

        Ok(expr)
    }

    // Parse one unary scalar function over the shared expression seam used by
    // projection-style clause positions. This keeps the function family on the
    // same parser-owned expression surface instead of hardcoding one bare-field
    // grammar for text functions and another expression grammar for numerics.
    pub(super) fn parse_expr_unary_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        self.expect_lparen()?;
        let input = self.parse_sql_expr(surface, 0)?;
        self.expect_rparen()?;

        Ok(SqlExpr::FunctionCall {
            function,
            args: vec![input],
        })
    }

    // Detect one function-call shell followed immediately by an unsupported
    // keyword so parser diagnostics can report the real feature family instead
    // of a generic unknown-function namespace error.
    fn function_call_is_followed_by_keyword(&self, keyword: Keyword) -> bool {
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

    fn parse_field_plus_literal_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let field = self.expect_identifier()?;
        self.expect_scalar_function_argument_comma()?;
        let literal = self.parse_literal()?;

        Ok(Self::scalar_function_call(function, field, vec![literal]))
    }

    fn parse_position_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let literal = self.parse_literal()?;
        self.expect_scalar_function_argument_comma()?;
        let field = self.expect_identifier()?;

        Ok(SqlExpr::FunctionCall {
            function,
            args: vec![SqlExpr::Literal(literal), SqlExpr::Field(field)],
        })
    }

    fn parse_replace_scalar_function_call(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
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
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
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
    // expression seam so nested arguments can reuse the current expression
    // parser instead of a function-local mini grammar.
    fn parse_coalesce_scalar_function_call(
        &mut self,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let mut args = vec![self.parse_sql_expr(surface, 0)?];
        while self.eat_comma() {
            args.push(self.parse_sql_expr(surface, 0)?);
        }

        if args.len() < 2 {
            return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
                "COALESCE requires at least two arguments",
            ));
        }

        Ok(SqlExpr::FunctionCall {
            function: SqlScalarFunction::Coalesce,
            args,
        })
    }

    // Parse one NULLIF(...) call on the shared projection expression seam
    // while preserving the current two-argument contract directly in the
    // parser surface.
    fn parse_nullif_scalar_function_call(
        &mut self,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let left = self.parse_sql_expr(surface, 0)?;
        self.expect_scalar_function_argument_comma()?;
        let right = self.parse_sql_expr(surface, 0)?;

        Ok(SqlExpr::FunctionCall {
            function: SqlScalarFunction::NullIf,
            args: vec![left, right],
        })
    }

    fn expect_scalar_function_argument_comma(
        &mut self,
    ) -> Result<(), crate::db::sql_shared::SqlParseError> {
        if self.eat_comma() {
            return Ok(());
        }

        Err(crate::db::sql_shared::SqlParseError::expected(
            "',' between scalar function arguments",
            self.peek_kind(),
        ))
    }

    fn select_item_from_sql_expr(
        expr: SqlExpr,
    ) -> Result<SqlSelectItem, crate::db::sql_shared::SqlParseError> {
        match expr {
            SqlExpr::Field(field) => Ok(SqlSelectItem::Field(field)),
            SqlExpr::Aggregate(aggregate) => Ok(SqlSelectItem::Aggregate(aggregate)),
            SqlExpr::Literal(_) => Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "standalone literal projection items are not supported",
            )),
            other => Ok(SqlSelectItem::Expr(other)),
        }
    }

    pub(super) fn parse_sql_expr(
        &mut self,
        surface: SqlExprParseSurface,
        min_precedence: u8,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let mut left = self.parse_sql_expr_prefix(surface)?;

        loop {
            if surface.allows_predicate_postfix()
                && let Some(expr) = self.try_parse_where_postfix_expr(left.clone(), surface)?
            {
                left = expr;
                continue;
            }

            let Some((op, precedence)) = self.peek_sql_expr_binary_op() else {
                break;
            };
            if precedence < min_precedence {
                break;
            }

            self.advance_sql_expr_binary_op();
            let right = self.parse_sql_expr(surface, precedence.saturating_add(1))?;
            left = if surface.canonicalizes_where_compare() {
                Self::canonicalize_where_compare_expr(op, left, right)
            } else {
                SqlExpr::Binary {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                }
            };
        }

        Ok(left)
    }

    fn parse_sql_expr_prefix(
        &mut self,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        if self.eat_keyword(Keyword::Case) {
            return self.parse_searched_case_expr(surface);
        }
        if self.eat_keyword(Keyword::Not) {
            return Ok(SqlExpr::Unary {
                op: SqlExprUnaryOp::Not,
                expr: Box::new(self.parse_sql_expr_prefix(surface)?),
            });
        }
        if self.peek_lparen() {
            self.expect_lparen()?;
            let expr = self.parse_sql_expr(surface, 0)?;
            self.expect_rparen()?;

            return Ok(expr);
        }
        if self.eat_question() {
            return Ok(SqlExpr::Param {
                index: self.take_param_index(),
            });
        }
        if matches!(
            self.peek_kind(),
            Some(
                TokenKind::StringLiteral(_)
                    | TokenKind::Number(_)
                    | TokenKind::Keyword(Keyword::Null | Keyword::True | Keyword::False)
                    | TokenKind::Minus
            )
        ) {
            return self.parse_literal().map(SqlExpr::Literal);
        }
        if surface.allows_aggregates()
            && let Some(kind) = self.parse_aggregate_kind()
        {
            let aggregate = self.parse_aggregate_call(kind)?;
            if self.peek_keyword(Keyword::Over) {
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "window functions / OVER",
                ));
            }

            return Ok(SqlExpr::Aggregate(aggregate));
        }

        let field = self.expect_identifier()?;
        if !self.peek_lparen() {
            return Ok(SqlExpr::Field(field));
        }

        let Some(function) = SqlScalarFunction::from_identifier(field.as_str()) else {
            if self.function_call_is_followed_by_keyword(Keyword::Over) {
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "window functions / OVER",
                ));
            }

            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "SQL function namespace beyond supported aggregate or scalar function forms",
            ));
        };

        if matches!(function, SqlScalarFunction::Round) {
            if !surface.allows_round_function() {
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "ROUND(...) is not supported in this expression position",
                ));
            }
        } else if !surface.allows_general_scalar_functions() {
            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "functions beyond supported ROUND(...) forms are not supported in this expression position",
            ));
        }

        if matches!(surface, SqlExprParseSurface::Where) {
            return self.parse_where_function_expr(function);
        }

        let call = if matches!(function, SqlScalarFunction::Round) {
            self.parse_round_function_call(function, surface)?
        } else if matches!(
            function,
            SqlScalarFunction::Trim
                | SqlScalarFunction::Ltrim
                | SqlScalarFunction::Rtrim
                | SqlScalarFunction::Lower
                | SqlScalarFunction::Upper
                | SqlScalarFunction::Length
                | SqlScalarFunction::Abs
                | SqlScalarFunction::Ceil
                | SqlScalarFunction::Ceiling
                | SqlScalarFunction::Floor
        ) {
            self.parse_expr_unary_scalar_function_call(function, surface)?
        } else {
            self.parse_scalar_function_call(function, surface)?
        };
        if self.peek_keyword(Keyword::Over) {
            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "window functions / OVER",
            ));
        }

        Ok(call)
    }

    fn parse_searched_case_expr(
        &mut self,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        if !self.eat_keyword(Keyword::When) {
            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "simple CASE expressions",
            ));
        }

        let mut arms = Vec::new();
        loop {
            let condition = self.parse_sql_expr(surface.case_condition_surface(), 0)?;
            self.expect_keyword(Keyword::Then)?;
            let result = self.parse_sql_expr(surface, 0)?;
            arms.push(SqlCaseArm { condition, result });

            if !self.eat_keyword(Keyword::When) {
                break;
            }
        }

        let else_expr = if self.eat_keyword(Keyword::Else) {
            Some(Box::new(self.parse_sql_expr(surface, 0)?))
        } else {
            None
        };

        self.expect_keyword(Keyword::End)?;

        Ok(SqlExpr::Case { arms, else_expr })
    }

    fn try_parse_where_postfix_expr(
        &mut self,
        left: SqlExpr,
        surface: SqlExprParseSurface,
    ) -> Result<Option<SqlExpr>, crate::db::sql_shared::SqlParseError> {
        debug_assert!(surface.allows_predicate_postfix());

        if self.eat_keyword(Keyword::Is) {
            let negated = self.eat_keyword(Keyword::Not);
            if self.peek_keyword(Keyword::Null) {
                let _ = self.cursor.advance();

                return Ok(Some(SqlExpr::NullTest {
                    expr: Box::new(left),
                    negated,
                }));
            }
            if self.peek_keyword(Keyword::True) || self.peek_keyword(Keyword::False) {
                let value = if self.eat_keyword(Keyword::True) {
                    Value::Bool(true)
                } else {
                    let _ = self.cursor.advance();
                    Value::Bool(false)
                };
                let expr = SqlExpr::Binary {
                    op: SqlExprBinaryOp::Eq,
                    left: Box::new(left),
                    right: Box::new(SqlExpr::Literal(value)),
                };

                return Ok(Some(if negated {
                    SqlExpr::Unary {
                        op: SqlExprUnaryOp::Not,
                        expr: Box::new(expr),
                    }
                } else {
                    expr
                }));
            }

            return Err(crate::db::sql_shared::SqlParseError::expected(
                "NULL/TRUE/FALSE after IS/IS NOT",
                self.peek_kind(),
            ));
        }

        if self.eat_identifier_keyword("LIKE") {
            return self.parse_where_like_expr(left, false, false).map(Some);
        }
        if self.eat_identifier_keyword("ILIKE") {
            return self.parse_where_like_expr(left, false, true).map(Some);
        }
        if self.cursor.peek_keyword(Keyword::Not) {
            if self.cursor.peek_identifier_keyword_at(1, "LIKE") {
                let _ = self.cursor.advance();
                let _ = self.cursor.eat_identifier_keyword("LIKE");

                return self.parse_where_like_expr(left, true, false).map(Some);
            }
            if self.cursor.peek_identifier_keyword_at(1, "ILIKE") {
                let _ = self.cursor.advance();
                let _ = self.cursor.eat_identifier_keyword("ILIKE");

                return self.parse_where_like_expr(left, true, true).map(Some);
            }
            if self.cursor.peek_keyword_at(1, Keyword::In) {
                let _ = self.cursor.advance();
                let _ = self.cursor.advance();

                return self.parse_where_in_expr(left, true).map(Some);
            }
            if self.cursor.peek_keyword_at(1, Keyword::Between) {
                let _ = self.cursor.advance();
                let _ = self.cursor.advance();

                return self.parse_where_between_expr(left, true, surface).map(Some);
            }
        }

        if self.eat_keyword(Keyword::In) {
            return self.parse_where_in_expr(left, false).map(Some);
        }
        if self.eat_keyword(Keyword::Between) {
            return self
                .parse_where_between_expr(left, false, surface)
                .map(Some);
        }

        Ok(None)
    }

    fn parse_where_like_expr(
        &mut self,
        left: SqlExpr,
        negated: bool,
        casefold: bool,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let Value::Text(pattern) = self.parse_literal()? else {
            return Err(crate::db::sql_shared::SqlParseError::expected(
                "string literal pattern after LIKE",
                self.peek_kind(),
            ));
        };
        let Some(prefix) = pattern.strip_suffix('%') else {
            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "LIKE patterns beyond trailing '%' prefix form",
            ));
        };

        let left = match left {
            SqlExpr::Field(field) if casefold => SqlExpr::FunctionCall {
                function: SqlScalarFunction::Lower,
                args: vec![SqlExpr::Field(field)],
            },
            SqlExpr::FunctionCall { function, args }
                if matches!(
                    function,
                    SqlScalarFunction::Lower | SqlScalarFunction::Upper
                ) && matches!(args.as_slice(), [SqlExpr::Field(_)]) =>
            {
                Self::canonicalize_where_casefold_text_function(args)
            }
            SqlExpr::Field(field) => SqlExpr::Field(field),
            _ => {
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "LIKE left-hand expression forms beyond plain or LOWER/UPPER field wrappers",
                ));
            }
        };

        let expr = SqlExpr::FunctionCall {
            function: SqlScalarFunction::StartsWith,
            args: vec![left, SqlExpr::Literal(Value::Text(prefix.to_string()))],
        };

        Ok(if negated {
            SqlExpr::Unary {
                op: SqlExprUnaryOp::Not,
                expr: Box::new(expr),
            }
        } else {
            expr
        })
    }

    fn parse_where_in_expr(
        &mut self,
        left: SqlExpr,
        negated: bool,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        self.expect_lparen()?;
        let mut values = Vec::new();
        loop {
            values.push(self.parse_literal()?);
            if !self.eat_comma() {
                break;
            }
            if matches!(self.peek_kind(), Some(TokenKind::RParen)) {
                break;
            }
        }
        self.expect_rparen()?;

        if values.is_empty() {
            return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
                "IN requires at least one literal",
            ));
        }

        Ok(SqlExpr::Membership {
            expr: Box::new(left),
            values,
            negated,
        })
    }

    fn parse_where_between_expr(
        &mut self,
        left: SqlExpr,
        negated: bool,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let lower = self.parse_sql_expr_prefix(surface)?;
        self.expect_keyword(Keyword::And)?;
        let upper = self.parse_sql_expr_prefix(surface)?;

        Ok(if negated {
            SqlExpr::Binary {
                op: SqlExprBinaryOp::Or,
                left: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Lt,
                    left: Box::new(left.clone()),
                    right: Box::new(lower),
                }),
                right: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Gt,
                    left: Box::new(left),
                    right: Box::new(upper),
                }),
            }
        } else {
            SqlExpr::Binary {
                op: SqlExprBinaryOp::And,
                left: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Gte,
                    left: Box::new(left.clone()),
                    right: Box::new(lower),
                }),
                right: Box::new(SqlExpr::Binary {
                    op: SqlExprBinaryOp::Lte,
                    left: Box::new(left),
                    right: Box::new(upper),
                }),
            }
        })
    }

    fn parse_where_function_expr(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        match function {
            SqlScalarFunction::Round => {
                self.parse_round_function_call(function, SqlExprParseSurface::Where)
            }
            SqlScalarFunction::Coalesce => {
                self.expect_lparen()?;
                let mut args = vec![self.parse_sql_expr(SqlExprParseSurface::Where, 0)?];
                while self.eat_comma() {
                    args.push(self.parse_sql_expr(SqlExprParseSurface::Where, 0)?);
                }
                self.expect_rparen()?;

                if args.len() < 2 {
                    return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
                        "COALESCE requires at least two arguments",
                    ));
                }

                Ok(SqlExpr::FunctionCall { function, args })
            }
            SqlScalarFunction::NullIf => {
                self.expect_lparen()?;
                let left = self.parse_sql_expr(SqlExprParseSurface::Where, 0)?;
                if !self.eat_comma() {
                    return Err(crate::db::sql_shared::SqlParseError::expected(
                        ",",
                        self.peek_kind(),
                    ));
                }
                let right = self.parse_sql_expr(SqlExprParseSurface::Where, 0)?;
                self.expect_rparen()?;

                Ok(SqlExpr::FunctionCall {
                    function,
                    args: vec![left, right],
                })
            }
            SqlScalarFunction::Trim
            | SqlScalarFunction::Ltrim
            | SqlScalarFunction::Rtrim
            | SqlScalarFunction::Abs
            | SqlScalarFunction::Ceil
            | SqlScalarFunction::Ceiling
            | SqlScalarFunction::Floor
            | SqlScalarFunction::Lower
            | SqlScalarFunction::Upper
            | SqlScalarFunction::Length
            | SqlScalarFunction::Left
            | SqlScalarFunction::Right
            | SqlScalarFunction::Position
            | SqlScalarFunction::Replace
            | SqlScalarFunction::Substring => {
                self.parse_scalar_function_call(function, SqlExprParseSurface::Where)
            }
            SqlScalarFunction::StartsWith
            | SqlScalarFunction::EndsWith
            | SqlScalarFunction::Contains => {
                self.expect_lparen()?;
                let left = self.parse_sql_expr(SqlExprParseSurface::Where, 0)?;
                if !self.eat_comma() {
                    return Err(crate::db::sql_shared::SqlParseError::expected(
                        ",",
                        self.peek_kind(),
                    ));
                }
                let literal = SqlExpr::Literal(self.parse_literal()?);
                self.expect_rparen()?;

                if !matches!(left, SqlExpr::Field(_)) && !sql_expr_is_casefold_field_wrapper(&left)
                {
                    return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                        match function {
                            SqlScalarFunction::StartsWith => {
                                "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
                            }
                            SqlScalarFunction::EndsWith => {
                                "ENDS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
                            }
                            SqlScalarFunction::Contains => {
                                "CONTAINS first argument forms beyond plain or LOWER/UPPER field wrappers"
                            }
                            _ => unreachable!(
                                "bounded WHERE function matcher called with unsupported function"
                            ),
                        },
                    ));
                }

                let left = match left {
                    SqlExpr::FunctionCall { function, args }
                        if matches!(
                            function,
                            SqlScalarFunction::Lower | SqlScalarFunction::Upper
                        ) && matches!(args.as_slice(), [SqlExpr::Field(_)]) =>
                    {
                        Self::canonicalize_where_casefold_text_function(args)
                    }
                    other => other,
                };

                Ok(SqlExpr::FunctionCall {
                    function,
                    args: vec![left, literal],
                })
            }
        }
    }

    fn canonicalize_where_casefold_text_function(args: Vec<SqlExpr>) -> SqlExpr {
        let [SqlExpr::Field(field)] = <[SqlExpr; 1]>::try_from(args)
            .expect("WHERE casefold canonicalization requires exactly one field argument")
        else {
            unreachable!("WHERE casefold canonicalization requires exactly one field argument");
        };

        SqlExpr::FunctionCall {
            function: SqlScalarFunction::Lower,
            args: vec![SqlExpr::Field(field)],
        }
    }

    // Keep the shared WHERE expression seam aligned with the older predicate
    // surface by preserving the same field-first and symmetric-equality
    // canonical forms the predicate parser had already shipped.
    fn canonicalize_where_compare_expr(
        op: SqlExprBinaryOp,
        left: SqlExpr,
        right: SqlExpr,
    ) -> SqlExpr {
        match (&left, &right) {
            (SqlExpr::Literal(_), right_expr)
                if matches!(right_expr, SqlExpr::Field(_))
                    || sql_expr_is_casefold_field_wrapper(right_expr) =>
            {
                SqlExpr::Binary {
                    op: flip_sql_compare_op(op),
                    left: Box::new(right),
                    right: Box::new(left),
                }
            }
            (SqlExpr::Field(left_field), SqlExpr::Field(right_field))
                if matches!(op, SqlExprBinaryOp::Eq | SqlExprBinaryOp::Ne)
                    && left_field < right_field =>
            {
                SqlExpr::Binary {
                    op,
                    left: Box::new(right),
                    right: Box::new(left),
                }
            }
            _ => SqlExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            },
        }
    }

    fn scalar_function_call(
        function: SqlScalarFunction,
        field: String,
        literals: Vec<Value>,
    ) -> SqlExpr {
        let mut args = Vec::with_capacity(1 + literals.len());
        args.push(SqlExpr::Field(field));
        args.extend(literals.into_iter().map(SqlExpr::Literal));

        SqlExpr::FunctionCall { function, args }
    }

    fn peek_sql_expr_binary_op(&self) -> Option<(SqlExprBinaryOp, u8)> {
        let op = match self.peek_kind() {
            Some(TokenKind::Keyword(Keyword::Or)) => SqlExprBinaryOp::Or,
            Some(TokenKind::Keyword(Keyword::And)) => SqlExprBinaryOp::And,
            Some(TokenKind::Eq) => SqlExprBinaryOp::Eq,
            Some(TokenKind::Ne) => SqlExprBinaryOp::Ne,
            Some(TokenKind::Lt) => SqlExprBinaryOp::Lt,
            Some(TokenKind::Lte) => SqlExprBinaryOp::Lte,
            Some(TokenKind::Gt) => SqlExprBinaryOp::Gt,
            Some(TokenKind::Gte) => SqlExprBinaryOp::Gte,
            Some(TokenKind::Plus) => SqlExprBinaryOp::Add,
            Some(TokenKind::Minus) => SqlExprBinaryOp::Sub,
            Some(TokenKind::Star) => SqlExprBinaryOp::Mul,
            Some(TokenKind::Slash) => SqlExprBinaryOp::Div,
            _ => return None,
        };

        Some((op, sql_expr_binary_op_precedence(op)))
    }

    const fn advance_sql_expr_binary_op(&mut self) {
        let _ = self.cursor.advance();
    }

    fn parse_projection_arithmetic_expr(
        &mut self,
        min_precedence: u8,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let left = self.parse_projection_arithmetic_leaf()?;

        self.parse_projection_arithmetic_expr_tail(left, min_precedence)
    }

    fn parse_projection_arithmetic_expr_tail(
        &mut self,
        mut left: SqlExpr,
        min_precedence: u8,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
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

    pub(super) fn parse_round_function_call(
        &mut self,
        function: SqlScalarFunction,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        self.expect_lparen()?;

        let input = self.parse_sql_expr(surface, 0)?;

        let mut args = vec![input];
        if self.eat_comma() {
            let scale = SqlExpr::Literal(self.parse_literal()?);
            let SqlExpr::Literal(Value::Int(_) | Value::Uint(_)) = scale else {
                return Err(crate::db::sql_shared::SqlParseError::invalid_syntax(
                    "ROUND scale must be an integer literal",
                ));
            };
            args.push(scale);
        }
        self.expect_rparen()?;

        Ok(SqlExpr::FunctionCall { function, args })
    }

    pub(super) fn parse_projection_arithmetic_from_left(
        &mut self,
        left: SqlExpr,
        op: SqlExprBinaryOp,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        let right =
            self.parse_projection_arithmetic_expr(arithmetic_projection_op_precedence(op) + 1)?;

        Ok(SqlExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn parse_projection_arithmetic_leaf(
        &mut self,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
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
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "nested projection functions inside arithmetic expressions",
                ));
            }

            return Ok(SqlExpr::Field(field));
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

const fn flip_sql_compare_op(op: SqlExprBinaryOp) -> SqlExprBinaryOp {
    match op {
        SqlExprBinaryOp::Eq => SqlExprBinaryOp::Eq,
        SqlExprBinaryOp::Ne => SqlExprBinaryOp::Ne,
        SqlExprBinaryOp::Lt => SqlExprBinaryOp::Gt,
        SqlExprBinaryOp::Lte => SqlExprBinaryOp::Gte,
        SqlExprBinaryOp::Gt => SqlExprBinaryOp::Lt,
        SqlExprBinaryOp::Gte => SqlExprBinaryOp::Lte,
        SqlExprBinaryOp::Or
        | SqlExprBinaryOp::And
        | SqlExprBinaryOp::Add
        | SqlExprBinaryOp::Sub
        | SqlExprBinaryOp::Mul
        | SqlExprBinaryOp::Div => op,
    }
}

fn sql_expr_is_casefold_field_wrapper(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::FunctionCall {
            function: SqlScalarFunction::Lower | SqlScalarFunction::Upper,
            args,
        } if matches!(args.as_slice(), [SqlExpr::Field(_)])
    )
}

const fn sql_expr_binary_op_precedence(op: SqlExprBinaryOp) -> u8 {
    match op {
        SqlExprBinaryOp::Or => 1,
        SqlExprBinaryOp::And => 2,
        SqlExprBinaryOp::Eq
        | SqlExprBinaryOp::Ne
        | SqlExprBinaryOp::Lt
        | SqlExprBinaryOp::Lte
        | SqlExprBinaryOp::Gt
        | SqlExprBinaryOp::Gte => 3,
        SqlExprBinaryOp::Add | SqlExprBinaryOp::Sub => 4,
        SqlExprBinaryOp::Mul | SqlExprBinaryOp::Div => 5,
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
