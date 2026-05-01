//! Module: db::sql::parser::projection
//! Responsibility: reduced SQL projection, aggregate call, and narrow scalar-function parsing.
//! Does not own: statement-level clause ordering, predicate semantics, or execution behavior.
//! Boundary: keeps projection-specific syntax branching out of the statement parser shell.

mod aggregate;
mod arithmetic;
mod functions;
mod postfix;

use crate::db::{
    query::plan::expr::FunctionSurface,
    sql::parser::{
        Parser, SqlCaseArm, SqlExpr, SqlExprBinaryOp, SqlExprUnaryOp, SqlProjection,
        SqlScalarFunction, SqlSelectItem,
    },
    sql_shared::{Keyword, SqlParseError, TokenKind},
};

///
/// SqlExprParseSurface
///
/// Carries the parser-owned expression authority for the current SQL clause.
/// Projection child modules use it to keep aggregate, predicate-postfix, and
/// scalar-function admission tied to the clause that is currently parsing.
///
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

    const fn allows_predicate_postfix(self) -> bool {
        matches!(
            self,
            Self::ProjectionCondition
                | Self::AggregateInputCondition
                | Self::HavingCondition
                | Self::Where
        )
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

    /// Return the planner-owned function surface corresponding to this parser
    /// expression surface.
    #[must_use]
    const fn function_surface(self) -> FunctionSurface {
        match self {
            Self::Projection => FunctionSurface::Projection,
            Self::ProjectionCondition => FunctionSurface::ProjectionCondition,
            Self::AggregateInput => FunctionSurface::AggregateInput,
            Self::AggregateInputCondition => FunctionSurface::AggregateInputCondition,
            Self::HavingCondition => FunctionSurface::HavingCondition,
            Self::Where => FunctionSurface::Where,
        }
    }
}

impl Parser {
    pub(super) fn parse_where_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        self.record_predicate_parse_stage(|parser| {
            parser.parse_sql_expr(SqlExprParseSurface::Where, 0)
        })
    }

    pub(super) fn parse_projection(
        &mut self,
    ) -> Result<(SqlProjection, Vec<Option<String>>), SqlParseError> {
        if self.eat_star() {
            return Ok((SqlProjection::All, Vec::new()));
        }

        let mut items = Vec::new();
        let mut aliases = Vec::new();
        loop {
            let item = if self.projection_item_is_simple_field() {
                self.expect_identifier().map(SqlSelectItem::Field)?
            } else {
                let expr = self.record_expr_parse_stage(|parser| {
                    parser.parse_sql_expr(SqlExprParseSurface::Projection, 0)
                })?;

                Self::select_item_from_sql_expr(expr)?
            };
            items.push(item);
            aliases.push(self.parse_projection_alias_if_present()?);

            if self.eat_comma() {
                continue;
            }

            break;
        }

        if items.is_empty() {
            return Err(SqlParseError::expected(
                "one projection item",
                self.peek_kind(),
            ));
        }

        Ok((SqlProjection::Items(items), aliases))
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

    // Parse one optional projection alias while keeping alias ownership at the
    // parser/session boundary instead of widening planner semantics.
    fn parse_projection_alias_if_present(&mut self) -> Result<Option<String>, SqlParseError> {
        if self.eat_keyword(Keyword::As) {
            return self.expect_identifier().map(Some);
        }

        if matches!(self.peek_kind(), Some(TokenKind::Identifier(_))) {
            return self.expect_identifier().map(Some);
        }

        Ok(None)
    }

    fn select_item_from_sql_expr(expr: SqlExpr) -> Result<SqlSelectItem, SqlParseError> {
        match expr {
            SqlExpr::Field(field) => Ok(SqlSelectItem::Field(field)),
            SqlExpr::FieldPath { .. } => Ok(SqlSelectItem::Expr(expr)),
            SqlExpr::Aggregate(aggregate) => Ok(SqlSelectItem::Aggregate(aggregate)),
            SqlExpr::Literal(_) => Err(SqlParseError::unsupported_feature(
                "standalone literal projection items are not supported",
            )),
            other => Ok(SqlSelectItem::Expr(other)),
        }
    }

    pub(super) fn parse_sql_expr(
        &mut self,
        surface: SqlExprParseSurface,
        min_precedence: u8,
    ) -> Result<SqlExpr, SqlParseError> {
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

            let _ = self.cursor.advance();
            let right = self.parse_sql_expr(surface, precedence.saturating_add(1))?;
            left = SqlExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_sql_expr_prefix(
        &mut self,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, SqlParseError> {
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
                    | TokenKind::BlobLiteral(_)
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
                return Err(SqlParseError::unsupported_feature(
                    "window functions / OVER",
                ));
            }

            return Ok(SqlExpr::Aggregate(aggregate));
        }

        let field = self.expect_identifier()?;
        if !self.peek_lparen() {
            return Ok(
                if matches!(
                    surface,
                    SqlExprParseSurface::Projection | SqlExprParseSurface::AggregateInput
                ) {
                    SqlExpr::from_field_identifier(field)
                } else {
                    SqlExpr::Field(field)
                },
            );
        }

        let Some(function) = SqlScalarFunction::from_identifier(field.as_str()) else {
            if self.function_call_is_followed_by_keyword(Keyword::Over) {
                return Err(SqlParseError::unsupported_feature(
                    "window functions / OVER",
                ));
            }

            return Err(SqlParseError::unsupported_feature(
                "SQL function namespace beyond supported aggregate or scalar function forms",
            ));
        };

        if !function
            .planner_function()
            .supports_surface(surface.function_surface())
        {
            return Err(SqlParseError::unsupported_feature(
                if function.uses_numeric_scale_special_case() {
                    "scale-taking numeric functions are not supported in this expression position"
                } else {
                    "functions beyond supported scalar forms are not supported in this expression position"
                },
            ));
        }

        if matches!(surface, SqlExprParseSurface::Where) {
            return self.parse_where_function_expr(function);
        }

        let call = self.parse_scalar_function_call(function, surface)?;
        if self.peek_keyword(Keyword::Over) {
            return Err(SqlParseError::unsupported_feature(
                "window functions / OVER",
            ));
        }

        Ok(call)
    }

    fn parse_searched_case_expr(
        &mut self,
        surface: SqlExprParseSurface,
    ) -> Result<SqlExpr, SqlParseError> {
        if !self.eat_keyword(Keyword::When) {
            return Err(SqlParseError::unsupported_feature(
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
