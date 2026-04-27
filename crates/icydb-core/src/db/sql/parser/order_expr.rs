//! Module: db::sql::parser::order_expr
//! Responsibility: parser-owned reduced ORDER BY expression parsing.
//! Does not own: planner expression semantics, expression canonicalization, or runtime expression evaluation.
//! Boundary: adapts SQL token streams onto parser-owned `SqlExpr` AST values for lowering to consume.

use crate::{
    db::{
        sql::parser::{
            SqlAggregateCall, SqlAggregateKind, SqlCaseArm, SqlExpr, SqlExprBinaryOp,
            SqlExprUnaryOp, SqlScalarFunction,
        },
        sql_shared::{Keyword, SqlParseError, SqlTokenCursor, TokenKind, tokenize_sql},
    },
    value::Value,
};

pub(in crate::db) type SqlOrderExprAst = SqlExpr;

/// Parse one supported SQL `ORDER BY` expression term into the parser-owned
/// expression tree.
#[must_use]
pub(in crate::db) fn parse_supported_order_expr_ast(term: &str) -> Option<SqlOrderExprAst> {
    let tokens = tokenize_sql(term).ok()?;
    if tokens.is_empty() {
        return None;
    }

    let mut parser = SupportedOrderExprParser::new(SqlTokenCursor::new(tokens));
    let expression = parser.parse_expr().ok()?;

    parser.cursor.is_eof().then_some(expression)
}

/// Parse one grouped post-aggregate SQL `ORDER BY` expression term into the
/// parser-owned expression tree.
#[must_use]
pub(in crate::db) fn parse_grouped_post_aggregate_order_expr_ast(
    term: &str,
) -> Option<SqlOrderExprAst> {
    let tokens = tokenize_sql(term).ok()?;
    if tokens.is_empty() {
        return None;
    }

    let mut parser = SupportedGroupedOrderExprParser::new(SqlTokenCursor::new(tokens));
    let expression = parser.parse_expr().ok()?;

    parser.cursor.is_eof().then_some(expression)
}

///
/// SupportedOrderFunctionShape
///
/// SQL parser argument-shape taxonomy for the reduced ORDER BY function
/// surface.
/// This exists so plain and grouped SQL order parsers share one definition of
/// admitted wrapper forms before producing parser-owned expression AST values.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SupportedOrderFunctionShape {
    UnaryExpr,
    VariadicExprMin2,
    BinaryExpr,
    FieldLiteral,
    LiteralField,
    FieldTwoLiterals,
    FieldOneOrTwoLiterals,
    Round,
}

// Resolve one reduced ORDER BY function name onto the parser-owned function
// taxonomy so both parser seams stay on the same admitted surface.
fn supported_order_function(name: &str) -> Option<SqlScalarFunction> {
    Some(match name.to_ascii_uppercase().as_str() {
        "IS_NULL" => SqlScalarFunction::IsNull,
        "IS_NOT_NULL" => SqlScalarFunction::IsNotNull,
        "IS_MISSING" => SqlScalarFunction::IsMissing,
        "IS_EMPTY" => SqlScalarFunction::IsEmpty,
        "IS_NOT_EMPTY" => SqlScalarFunction::IsNotEmpty,
        "TRIM" => SqlScalarFunction::Trim,
        "LTRIM" => SqlScalarFunction::Ltrim,
        "RTRIM" => SqlScalarFunction::Rtrim,
        "ABS" => SqlScalarFunction::Abs,
        "CEIL" | "CEILING" => SqlScalarFunction::Ceiling,
        "FLOOR" => SqlScalarFunction::Floor,
        "LOWER" => SqlScalarFunction::Lower,
        "UPPER" => SqlScalarFunction::Upper,
        "LENGTH" => SqlScalarFunction::Length,
        "COALESCE" => SqlScalarFunction::Coalesce,
        "NULLIF" => SqlScalarFunction::NullIf,
        "LEFT" => SqlScalarFunction::Left,
        "RIGHT" => SqlScalarFunction::Right,
        "STARTS_WITH" => SqlScalarFunction::StartsWith,
        "ENDS_WITH" => SqlScalarFunction::EndsWith,
        "CONTAINS" => SqlScalarFunction::Contains,
        "POSITION" => SqlScalarFunction::Position,
        "REPLACE" => SqlScalarFunction::Replace,
        "SUBSTRING" => SqlScalarFunction::Substring,
        "ROUND" => SqlScalarFunction::Round,
        _ => return None,
    })
}

// Keep the reduced ORDER BY function family SQL-owned by describing the
// admitted argument shape locally instead of re-encoding it in each parser.
const fn supported_order_function_shape(
    function: SqlScalarFunction,
) -> SupportedOrderFunctionShape {
    match function {
        SqlScalarFunction::IsNull
        | SqlScalarFunction::IsNotNull
        | SqlScalarFunction::IsMissing
        | SqlScalarFunction::IsEmpty
        | SqlScalarFunction::IsNotEmpty
        | SqlScalarFunction::Trim
        | SqlScalarFunction::Ltrim
        | SqlScalarFunction::Rtrim
        | SqlScalarFunction::Abs
        | SqlScalarFunction::Ceiling
        | SqlScalarFunction::Floor
        | SqlScalarFunction::Lower
        | SqlScalarFunction::Upper
        | SqlScalarFunction::Length => SupportedOrderFunctionShape::UnaryExpr,
        SqlScalarFunction::Coalesce => SupportedOrderFunctionShape::VariadicExprMin2,
        SqlScalarFunction::NullIf => SupportedOrderFunctionShape::BinaryExpr,
        SqlScalarFunction::Left
        | SqlScalarFunction::Right
        | SqlScalarFunction::StartsWith
        | SqlScalarFunction::EndsWith
        | SqlScalarFunction::Contains => SupportedOrderFunctionShape::FieldLiteral,
        SqlScalarFunction::Position => SupportedOrderFunctionShape::LiteralField,
        SqlScalarFunction::Replace => SupportedOrderFunctionShape::FieldTwoLiterals,
        SqlScalarFunction::Substring => SupportedOrderFunctionShape::FieldOneOrTwoLiterals,
        SqlScalarFunction::Round => SupportedOrderFunctionShape::Round,
    }
}

///
/// SupportedOrderFunctionParser
///
/// Local parser contract for the reduced ORDER BY function family.
/// This keeps shared call-shape handling in one place while letting each
/// parser own its operand-expression grammar.
///

trait SupportedOrderFunctionParser {
    fn cursor(&mut self) -> &mut SqlTokenCursor;

    fn unsupported_surface(&self) -> &'static str;

    fn parse_expr_arg(&mut self) -> Result<SqlExpr, SqlParseError>;

    fn parse_supported_order_function_expr(
        &mut self,
        name: &str,
    ) -> Result<SqlExpr, SqlParseError> {
        let Some(function) = supported_order_function(name) else {
            return Err(SqlParseError::unsupported_feature(
                self.unsupported_surface(),
            ));
        };

        self.cursor().expect_lparen()?;
        let expr = self.parse_supported_order_function_call(function)?;
        self.cursor().expect_rparen()?;

        Ok(expr)
    }

    fn parse_supported_order_function_call(
        &mut self,
        function: SqlScalarFunction,
    ) -> Result<SqlExpr, SqlParseError> {
        let shape = supported_order_function_shape(function);

        if matches!(shape, SupportedOrderFunctionShape::Round) {
            return self.parse_supported_order_round_expr();
        }

        let args = self.parse_supported_order_function_args(shape)?;

        Ok(SqlExpr::FunctionCall { function, args })
    }

    fn parse_supported_order_function_args(
        &mut self,
        shape: SupportedOrderFunctionShape,
    ) -> Result<Vec<SqlExpr>, SqlParseError> {
        match shape {
            SupportedOrderFunctionShape::UnaryExpr => Ok(vec![self.parse_expr_arg()?]),
            SupportedOrderFunctionShape::VariadicExprMin2 => {
                let mut args = vec![self.parse_expr_arg()?];
                while self.cursor().eat_comma() {
                    args.push(self.parse_expr_arg()?);
                }

                if args.len() < 2 {
                    return Err(SqlParseError::invalid_syntax(
                        "COALESCE requires at least two arguments",
                    ));
                }

                Ok(args)
            }
            SupportedOrderFunctionShape::BinaryExpr => {
                let left = self.parse_expr_arg()?;
                self.expect_function_comma()?;
                let right = self.parse_expr_arg()?;

                Ok(vec![left, right])
            }
            SupportedOrderFunctionShape::FieldLiteral => {
                let field = self.parse_field_arg()?;
                self.expect_function_comma()?;
                let literal = self.parse_literal_arg()?;

                Ok(vec![field, literal])
            }
            SupportedOrderFunctionShape::LiteralField => {
                let literal = self.parse_literal_arg()?;
                self.expect_function_comma()?;
                let field = self.parse_field_arg()?;

                Ok(vec![literal, field])
            }
            SupportedOrderFunctionShape::FieldTwoLiterals => {
                let field = self.parse_field_arg()?;
                self.expect_function_comma()?;
                let from = self.parse_literal_arg()?;
                self.expect_function_comma()?;
                let to = self.parse_literal_arg()?;

                Ok(vec![field, from, to])
            }
            SupportedOrderFunctionShape::FieldOneOrTwoLiterals => {
                let field = self.parse_field_arg()?;
                self.expect_function_comma()?;
                let start = self.parse_literal_arg()?;
                let mut args = vec![field, start];
                if self.cursor().eat_comma() {
                    args.push(self.parse_literal_arg()?);
                }

                Ok(args)
            }
            SupportedOrderFunctionShape::Round => unreachable!("ROUND is handled separately"),
        }
    }

    fn parse_supported_order_round_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let base = self.parse_expr_arg()?;
        self.expect_function_comma()?;
        let scale = self.parse_literal_arg()?;

        Ok(SqlExpr::FunctionCall {
            function: SqlScalarFunction::Round,
            args: vec![base, scale],
        })
    }

    fn parse_field_arg(&mut self) -> Result<SqlExpr, SqlParseError> {
        self.cursor().expect_identifier().map(SqlExpr::Field)
    }

    fn parse_literal_arg(&mut self) -> Result<SqlExpr, SqlParseError> {
        self.cursor().parse_literal().map(SqlExpr::Literal)
    }

    fn expect_function_comma(&mut self) -> Result<(), SqlParseError> {
        if self.cursor().eat_comma() {
            return Ok(());
        }

        Err(SqlParseError::expected(",", self.cursor().peek_kind()))
    }
}

///
/// SupportedOrderExprParser
///
/// SQL parser for one supported scalar ORDER BY expression.
/// This stays intentionally narrower than the full SQL frontend surface and
/// only accepts the reduced family used by SQL order normalization tests.
///

struct SupportedOrderExprParser {
    cursor: SqlTokenCursor,
}

impl SupportedOrderExprParser {
    const fn new(cursor: SqlTokenCursor) -> Self {
        Self { cursor }
    }

    fn parse_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        self.parse_additive_expr()
    }

    fn parse_additive_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            let op = if self.cursor.eat_plus() {
                Some(SqlExprBinaryOp::Add)
            } else if self.cursor.eat_minus() {
                Some(SqlExprBinaryOp::Sub)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };

            left = SqlExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(self.parse_multiplicative_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let mut left = self.parse_unary_expr()?;

        loop {
            let op = if matches!(self.cursor.peek_kind(), Some(TokenKind::Star)) {
                self.cursor.advance();
                Some(SqlExprBinaryOp::Mul)
            } else if self.cursor.eat_slash() {
                Some(SqlExprBinaryOp::Div)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };

            left = SqlExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(self.parse_unary_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        if self.cursor.eat_keyword(Keyword::Not) {
            return Ok(SqlExpr::Unary {
                op: SqlExprUnaryOp::Not,
                expr: Box::new(self.parse_unary_expr()?),
            });
        }

        self.parse_primary_expr()
    }

    fn parse_primary_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        if matches!(self.cursor.peek_kind(), Some(TokenKind::LParen)) {
            self.cursor.expect_lparen()?;
            let expr = self.parse_expr()?;
            self.cursor.expect_rparen()?;

            return Ok(expr);
        }

        if matches!(self.cursor.peek_kind(), Some(TokenKind::Identifier(_))) {
            let head = self.cursor.expect_identifier()?;
            if matches!(self.cursor.peek_kind(), Some(TokenKind::LParen)) {
                return self.parse_function_expr(head.as_str());
            }

            return Ok(SqlExpr::Field(head));
        }

        self.cursor.parse_literal().map(SqlExpr::Literal)
    }

    fn parse_function_expr(&mut self, name: &str) -> Result<SqlExpr, SqlParseError> {
        SupportedOrderFunctionParser::parse_supported_order_function_expr(self, name)
    }
}

impl SupportedOrderFunctionParser for SupportedOrderExprParser {
    fn cursor(&mut self) -> &mut SqlTokenCursor {
        &mut self.cursor
    }

    fn unsupported_surface(&self) -> &'static str {
        "supported ORDER BY expression family"
    }

    fn parse_expr_arg(&mut self) -> Result<SqlExpr, SqlParseError> {
        self.parse_expr()
    }
}

///
/// SupportedGroupedOrderExprParser
///
/// SQL parser for grouped post-aggregate ORDER BY expressions.
/// This admits grouped-key leaves, aggregate leaves, arithmetic, searched CASE,
/// and bounded scalar-function wrappers before planner analysis consumes SqlExpr.
///

struct SupportedGroupedOrderExprParser {
    cursor: SqlTokenCursor,
}

impl SupportedGroupedOrderExprParser {
    const fn new(cursor: SqlTokenCursor) -> Self {
        Self { cursor }
    }

    fn parse_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let mut left = self.parse_and_expr()?;

        while self.cursor.eat_keyword(Keyword::Or) {
            left = SqlExpr::Binary {
                op: SqlExprBinaryOp::Or,
                left: Box::new(left),
                right: Box::new(self.parse_and_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let mut left = self.parse_compare_expr()?;

        while self.cursor.eat_keyword(Keyword::And) {
            left = SqlExpr::Binary {
                op: SqlExprBinaryOp::And,
                left: Box::new(left),
                right: Box::new(self.parse_compare_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_compare_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let left = self.parse_additive_expr()?;
        let Some(op) = self.parse_compare_op() else {
            return Ok(left);
        };

        Ok(SqlExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(self.parse_additive_expr()?),
        })
    }

    fn parse_compare_op(&mut self) -> Option<SqlExprBinaryOp> {
        let op = match self.cursor.peek_kind() {
            Some(TokenKind::Eq) => SqlExprBinaryOp::Eq,
            Some(TokenKind::Ne) => SqlExprBinaryOp::Ne,
            Some(TokenKind::Lt) => SqlExprBinaryOp::Lt,
            Some(TokenKind::Lte) => SqlExprBinaryOp::Lte,
            Some(TokenKind::Gt) => SqlExprBinaryOp::Gt,
            Some(TokenKind::Gte) => SqlExprBinaryOp::Gte,
            _ => return None,
        };

        self.cursor.advance();

        Some(op)
    }

    fn parse_additive_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            let op = if self.cursor.eat_plus() {
                Some(SqlExprBinaryOp::Add)
            } else if self.cursor.eat_minus() {
                Some(SqlExprBinaryOp::Sub)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };

            left = SqlExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(self.parse_multiplicative_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let mut left = self.parse_unary_expr()?;

        loop {
            let op = if matches!(self.cursor.peek_kind(), Some(TokenKind::Star)) {
                self.cursor.advance();
                Some(SqlExprBinaryOp::Mul)
            } else if self.cursor.eat_slash() {
                Some(SqlExprBinaryOp::Div)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };

            left = SqlExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(self.parse_unary_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        if self.cursor.eat_keyword(Keyword::Not) {
            return Ok(SqlExpr::Unary {
                op: SqlExprUnaryOp::Not,
                expr: Box::new(self.parse_unary_expr()?),
            });
        }

        self.parse_primary_expr()
    }

    fn parse_primary_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        if matches!(self.cursor.peek_kind(), Some(TokenKind::LParen)) {
            self.cursor.expect_lparen()?;
            let expr = self.parse_expr()?;
            self.cursor.expect_rparen()?;

            return Ok(expr);
        }
        if self.cursor.eat_keyword(Keyword::Case) {
            return self.parse_case_expr();
        }
        if self.cursor.peek_identifier_keyword("ROUND") {
            let head = self.cursor.expect_identifier()?;

            return self.parse_function_expr(head.as_str());
        }
        if let Some(kind) = self.parse_aggregate_kind() {
            return self.parse_aggregate_expr(kind);
        }
        if matches!(self.cursor.peek_kind(), Some(TokenKind::Identifier(_))) {
            let head = self.cursor.expect_identifier()?;
            if matches!(self.cursor.peek_kind(), Some(TokenKind::LParen)) {
                return self.parse_function_expr(head.as_str());
            }

            return Ok(SqlExpr::Field(head));
        }

        self.cursor.parse_literal().map(SqlExpr::Literal)
    }

    fn parse_case_expr(&mut self) -> Result<SqlExpr, SqlParseError> {
        let mut when_then_arms = Vec::new();

        while self.cursor.eat_keyword(Keyword::When) {
            let condition = self.parse_expr()?;
            if !self.cursor.eat_keyword(Keyword::Then) {
                return Err(SqlParseError::expected("THEN", self.cursor.peek_kind()));
            }
            let result = self.parse_expr()?;
            when_then_arms.push(SqlCaseArm { condition, result });
        }

        if when_then_arms.is_empty() {
            return Err(SqlParseError::unsupported_feature(
                "searched CASE in grouped ORDER BY expressions",
            ));
        }

        let else_expr = if self.cursor.eat_keyword(Keyword::Else) {
            self.parse_expr()?
        } else {
            SqlExpr::Literal(Value::Null)
        };

        if !self.cursor.eat_keyword(Keyword::End) {
            return Err(SqlParseError::expected("END", self.cursor.peek_kind()));
        }

        Ok(SqlExpr::Case {
            arms: when_then_arms,
            else_expr: Some(Box::new(else_expr)),
        })
    }

    // Parse one normalized scalar function call inside the grouped post-
    // aggregate expression seam so filtered aggregate identities can be
    // reconstructed from their rendered labels during grouped SQL lowering.
    fn parse_function_expr(&mut self, name: &str) -> Result<SqlExpr, SqlParseError> {
        SupportedOrderFunctionParser::parse_supported_order_function_expr(self, name)
    }

    fn parse_aggregate_kind(&self) -> Option<SqlAggregateKind> {
        match self.cursor.peek_kind() {
            Some(TokenKind::Keyword(Keyword::Count)) => Some(SqlAggregateKind::Count),
            Some(TokenKind::Keyword(Keyword::Sum)) => Some(SqlAggregateKind::Sum),
            Some(TokenKind::Keyword(Keyword::Avg)) => Some(SqlAggregateKind::Avg),
            Some(TokenKind::Keyword(Keyword::Min)) => Some(SqlAggregateKind::Min),
            Some(TokenKind::Keyword(Keyword::Max)) => Some(SqlAggregateKind::Max),
            _ => None,
        }
    }

    fn parse_aggregate_expr(&mut self, kind: SqlAggregateKind) -> Result<SqlExpr, SqlParseError> {
        // Phase 1: parse the aggregate call shape itself so grouped
        // post-aggregate ORDER BY expressions preserve canonical aggregate
        // identity instead of collapsing back to one string term.
        self.cursor.advance();
        self.cursor.expect_lparen()?;
        let distinct = self.cursor.eat_keyword(Keyword::Distinct);
        let input_expr = if kind == SqlAggregateKind::Count
            && matches!(self.cursor.peek_kind(), Some(TokenKind::Star))
        {
            self.cursor.advance();
            None
        } else {
            Some(self.parse_expr()?)
        };
        self.cursor.expect_rparen()?;

        // Phase 2: parse the optional SQL aggregate FILTER clause on the same
        // semantic expression spine so alias-normalized grouped ORDER BY terms
        // continue to match grouped execution specs with filtered aggregates.
        let filter_expr = self.parse_optional_aggregate_filter_expr()?;

        Ok(SqlExpr::Aggregate(SqlAggregateCall {
            kind,
            input: input_expr.map(Box::new),
            filter_expr: filter_expr.map(Box::new),
            distinct,
        }))
    }

    // Parse one optional SQL aggregate FILTER clause while keeping grouped
    // post-aggregate ORDER BY reconstruction on the shared expression spine.
    // This parser is intentionally narrow: it only admits the shipped
    // `FILTER (WHERE <expr>)` surface and rejects any malformed shell.
    fn parse_optional_aggregate_filter_expr(&mut self) -> Result<Option<SqlExpr>, SqlParseError> {
        if !self.cursor.eat_keyword(Keyword::Filter) {
            return Ok(None);
        }
        self.cursor.expect_lparen()?;
        if !self.cursor.eat_keyword(Keyword::Where) {
            return Err(SqlParseError::expected("WHERE", self.cursor.peek_kind()));
        }
        let filter_expr = self.parse_expr()?;
        self.cursor.expect_rparen()?;

        Ok(Some(filter_expr))
    }
}

impl SupportedOrderFunctionParser for SupportedGroupedOrderExprParser {
    fn cursor(&mut self) -> &mut SqlTokenCursor {
        &mut self.cursor
    }

    fn unsupported_surface(&self) -> &'static str {
        "supported grouped ORDER BY expression family"
    }

    fn parse_expr_arg(&mut self) -> Result<SqlExpr, SqlParseError> {
        self.parse_expr()
    }
}
