//! Module: db::sql::parser::projection
//! Responsibility: reduced SQL projection, aggregate call, and narrow text-function parsing.
//! Does not own: statement-level clause ordering, predicate semantics, or execution behavior.
//! Boundary: keeps projection-specific syntax branching out of the statement parser shell.

use crate::{
    db::{
        sql::parser::{
            Parser, SqlAggregateCall, SqlAggregateInputExpr, SqlAggregateKind,
            SqlArithmeticProjectionCall, SqlArithmeticProjectionOp, SqlCaseArm, SqlExpr,
            SqlExprBinaryOp, SqlExprUnaryOp, SqlProjection, SqlProjectionOperand,
            SqlRoundProjectionCall, SqlRoundProjectionInput, SqlSelectItem, SqlTextFunction,
            SqlTextFunctionCall,
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

    const fn allows_text_functions(self) -> bool {
        matches!(
            self,
            Self::Projection | Self::ProjectionCondition | Self::Where
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

    // Searched CASE conditions reuse the owning clause's aggregate/text-function
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
        self.parse_sql_expr(SqlExprParseSurface::Where, 0)
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
        let expr = self.parse_sql_expr(SqlExprParseSurface::Projection, 0)?;

        Self::select_item_from_sql_expr(expr)
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
        let expr = self.parse_sql_expr(SqlExprParseSurface::AggregateInput, 0)?;

        Self::aggregate_input_expr_from_sql_expr(expr)
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

    // Detect one function-call shell followed immediately by an unsupported
    // keyword so parser diagnostics can report the real feature family instead
    // of a generic unknown-function namespace error.
    fn function_call_is_followed_by_keyword(&self, keyword: Keyword) -> bool {
        let mut cursor = self.cursor.clone();
        if !cursor.eat_lparen() {
            return false;
        }

        let mut depth = 1usize;
        while let Some(kind) = cursor.peek_kind() {
            match kind {
                TokenKind::LParen => {
                    depth = depth.saturating_add(1);
                    cursor.advance();
                }
                TokenKind::RParen => {
                    depth = depth.saturating_sub(1);
                    cursor.advance();
                    if depth == 0 {
                        return cursor.peek_keyword(keyword);
                    }
                }
                _ => {
                    cursor.advance();
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

    fn select_item_from_sql_expr(
        expr: SqlExpr,
    ) -> Result<SqlSelectItem, crate::db::sql_shared::SqlParseError> {
        if let Some(operand) = Self::projection_operand_from_sql_expr(&expr) {
            return Self::select_item_from_projection_expr(operand);
        }

        match expr {
            SqlExpr::TextFunction(call) => Ok(SqlSelectItem::TextFunction(call)),
            SqlExpr::Round(call) => Ok(SqlSelectItem::Round(call)),
            SqlExpr::Literal(_) => Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "standalone literal projection items are not supported",
            )),
            other => Ok(SqlSelectItem::Expr(other)),
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

    fn aggregate_input_expr_from_sql_expr(
        expr: SqlExpr,
    ) -> Result<SqlAggregateInputExpr, crate::db::sql_shared::SqlParseError> {
        if let Some(operand) = Self::projection_operand_from_sql_expr(&expr) {
            return Self::aggregate_input_expr_from_projection_expr(operand);
        }

        match expr {
            SqlExpr::Round(call) => Ok(SqlAggregateInputExpr::Round(call)),
            SqlExpr::Aggregate(_) => {
                Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "nested aggregate references inside aggregate input expressions",
                ))
            }
            other => Ok(SqlAggregateInputExpr::Expr(other)),
        }
    }

    pub(super) fn projection_operand_from_sql_expr(expr: &SqlExpr) -> Option<SqlProjectionOperand> {
        match expr {
            SqlExpr::Field(field) => Some(SqlProjectionOperand::Field(field.clone())),
            SqlExpr::Aggregate(aggregate) => {
                Some(SqlProjectionOperand::Aggregate(aggregate.clone()))
            }
            SqlExpr::Literal(literal) => Some(SqlProjectionOperand::Literal(literal.clone())),
            SqlExpr::Binary { op, left, right } => {
                let op = match op {
                    SqlExprBinaryOp::Add => SqlArithmeticProjectionOp::Add,
                    SqlExprBinaryOp::Sub => SqlArithmeticProjectionOp::Sub,
                    SqlExprBinaryOp::Mul => SqlArithmeticProjectionOp::Mul,
                    SqlExprBinaryOp::Div => SqlArithmeticProjectionOp::Div,
                    SqlExprBinaryOp::Or
                    | SqlExprBinaryOp::And
                    | SqlExprBinaryOp::Eq
                    | SqlExprBinaryOp::Ne
                    | SqlExprBinaryOp::Lt
                    | SqlExprBinaryOp::Lte
                    | SqlExprBinaryOp::Gt
                    | SqlExprBinaryOp::Gte => return None,
                };

                Some(SqlProjectionOperand::Arithmetic(Box::new(
                    SqlArithmeticProjectionCall {
                        left: Self::projection_operand_from_sql_expr(left)?,
                        op,
                        right: Self::projection_operand_from_sql_expr(right)?,
                    },
                )))
            }
            SqlExpr::NullTest { .. }
            | SqlExpr::Membership { .. }
            | SqlExpr::TextFunction(_)
            | SqlExpr::FunctionCall { .. }
            | SqlExpr::Round(_)
            | SqlExpr::Unary { .. }
            | SqlExpr::Case { .. } => None,
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
            if self.peek_keyword(Keyword::Filter) {
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "aggregate FILTER clauses",
                ));
            }
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
        if field.eq_ignore_ascii_case("ROUND") {
            let round = if matches!(surface, SqlExprParseSurface::AggregateInput) {
                self.parse_aggregate_input_round_call()?
            } else {
                self.parse_round_projection_call()?
            };
            if self.peek_keyword(Keyword::Over) {
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "window functions / OVER",
                ));
            }

            return Ok(SqlExpr::Round(round));
        }
        if !surface.allows_text_functions() {
            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "functions beyond supported ROUND(...) forms are not supported in this expression position",
            ));
        }

        let Some(function) = SqlTextFunction::from_identifier(field.as_str()) else {
            if self.function_call_is_followed_by_keyword(Keyword::Over) {
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "window functions / OVER",
                ));
            }

            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "SQL function namespace beyond supported aggregate or scalar text projection forms",
            ));
        };

        if matches!(surface, SqlExprParseSurface::Where) {
            return self.parse_where_function_expr(function);
        }

        let call = self.parse_text_function_call(function)?;
        if self.peek_keyword(Keyword::Over) {
            return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                "window functions / OVER",
            ));
        }

        Ok(SqlExpr::TextFunction(call))
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
            let mut lookahead = self.cursor.clone();
            let _ = lookahead.advance();
            if lookahead.peek_identifier_keyword("LIKE") {
                let _ = self.cursor.advance();
                let _ = self.cursor.eat_identifier_keyword("LIKE");

                return self.parse_where_like_expr(left, true, false).map(Some);
            }
            if lookahead.peek_identifier_keyword("ILIKE") {
                let _ = self.cursor.advance();
                let _ = self.cursor.eat_identifier_keyword("ILIKE");

                return self.parse_where_like_expr(left, true, true).map(Some);
            }
            if lookahead.peek_keyword(Keyword::In) {
                let _ = self.cursor.advance();
                let _ = self.cursor.advance();

                return self.parse_where_in_expr(left, true).map(Some);
            }
            if lookahead.peek_keyword(Keyword::Between) {
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
            SqlExpr::Field(field) if casefold => SqlExpr::TextFunction(SqlTextFunctionCall {
                function: SqlTextFunction::Lower,
                field,
                literal: None,
                literal2: None,
                literal3: None,
            }),
            SqlExpr::TextFunction(call)
                if matches!(
                    call.function,
                    SqlTextFunction::Lower | SqlTextFunction::Upper
                ) =>
            {
                Self::canonicalize_where_casefold_text_function(call)
            }
            SqlExpr::Field(field) => SqlExpr::Field(field),
            _ => {
                return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                    "LIKE left-hand expression forms beyond plain or LOWER/UPPER field wrappers",
                ));
            }
        };

        let expr = SqlExpr::FunctionCall {
            function: SqlTextFunction::StartsWith,
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
        function: SqlTextFunction,
    ) -> Result<SqlExpr, crate::db::sql_shared::SqlParseError> {
        match function {
            SqlTextFunction::Trim
            | SqlTextFunction::Ltrim
            | SqlTextFunction::Rtrim
            | SqlTextFunction::Lower
            | SqlTextFunction::Upper
            | SqlTextFunction::Length
            | SqlTextFunction::Left
            | SqlTextFunction::Right
            | SqlTextFunction::Position
            | SqlTextFunction::Replace
            | SqlTextFunction::Substring => {
                let call = self.parse_text_function_call(function)?;

                Ok(SqlExpr::TextFunction(call))
            }
            SqlTextFunction::StartsWith | SqlTextFunction::EndsWith | SqlTextFunction::Contains => {
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

                if !matches!(
                    left,
                    SqlExpr::Field(_)
                        | SqlExpr::TextFunction(SqlTextFunctionCall {
                            function: SqlTextFunction::Lower | SqlTextFunction::Upper,
                            ..
                        })
                ) {
                    return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
                        match function {
                            SqlTextFunction::StartsWith => {
                                "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
                            }
                            SqlTextFunction::EndsWith => {
                                "ENDS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
                            }
                            SqlTextFunction::Contains => {
                                "CONTAINS first argument forms beyond plain or LOWER/UPPER field wrappers"
                            }
                            _ => unreachable!(
                                "bounded WHERE function matcher called with unsupported function"
                            ),
                        },
                    ));
                }

                let left = match left {
                    SqlExpr::TextFunction(call)
                        if matches!(
                            call.function,
                            SqlTextFunction::Lower | SqlTextFunction::Upper
                        ) =>
                    {
                        Self::canonicalize_where_casefold_text_function(call)
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

    fn canonicalize_where_casefold_text_function(call: SqlTextFunctionCall) -> SqlExpr {
        SqlExpr::TextFunction(SqlTextFunctionCall {
            function: SqlTextFunction::Lower,
            field: call.field,
            literal: None,
            literal2: None,
            literal3: None,
        })
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
            (
                SqlExpr::Literal(_),
                SqlExpr::Field(_)
                | SqlExpr::TextFunction(SqlTextFunctionCall {
                    function: SqlTextFunction::Lower | SqlTextFunction::Upper,
                    ..
                }),
            ) => SqlExpr::Binary {
                op: flip_sql_compare_op(op),
                left: Box::new(right),
                right: Box::new(left),
            },
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

const fn arithmetic_projection_op_precedence(op: SqlArithmeticProjectionOp) -> u8 {
    match op {
        SqlArithmeticProjectionOp::Add | SqlArithmeticProjectionOp::Sub => 1,
        SqlArithmeticProjectionOp::Mul | SqlArithmeticProjectionOp::Div => 2,
    }
}
