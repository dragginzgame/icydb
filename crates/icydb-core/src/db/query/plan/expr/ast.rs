//! Module: query::plan::expr::ast
//! Responsibility: planner expression AST domain types and field/operator identifiers.
//! Does not own: expression type inference policy or runtime expression evaluation.
//! Boundary: defines canonical expression tree structures consumed by planner validation/lowering.

use crate::db::{
    query::builder::aggregate::AggregateExpr,
    sql_shared::{Keyword, SqlParseError, SqlTokenCursor, TokenKind, tokenize_sql},
};
use crate::value::Value;

///
/// FieldId
///
/// Canonical planner-owned field identity token for expression trees.
/// This wrapper carries the declared field name and avoids ad-hoc string use.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct FieldId(String);

impl FieldId {
    /// Build one field-id token from a field name.
    #[must_use]
    pub(crate) fn new(field: impl Into<String>) -> Self {
        Self(field.into())
    }

    /// Borrow the canonical field name.
    #[must_use]
    pub(crate) const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for FieldId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for FieldId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

///
/// Alias
///
/// Canonical planner-owned alias token attached to expression projections.
/// Alias remains presentation metadata and does not affect semantic identity.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Alias(String);

impl Alias {
    /// Build one alias token from owned/borrowed text.
    #[must_use]
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Borrow the alias as text.
    #[must_use]
    pub(crate) const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for Alias {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Alias {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

///
/// UnaryOp
///
/// Canonical unary expression operator taxonomy.
///

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnaryOp {
    Not,
}

///
/// BinaryOp
///
/// Canonical binary expression operator taxonomy.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    #[cfg(test)]
    And,
    #[cfg(test)]
    Eq,
}

///
/// Function
///
/// Canonical bounded function taxonomy admitted by planner-owned projection
/// expressions.
/// This intentionally stays limited to the shipped text-function surface.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Function {
    Trim,
    Ltrim,
    Rtrim,
    Lower,
    Upper,
    Length,
    Left,
    Right,
    StartsWith,
    EndsWith,
    Contains,
    Position,
    Replace,
    Substring,
    Round,
}

impl Function {
    /// Return the stable uppercase SQL label for this bounded function.
    #[must_use]
    pub(crate) const fn sql_label(self) -> &'static str {
        match self {
            Self::Trim => "TRIM",
            Self::Ltrim => "LTRIM",
            Self::Rtrim => "RTRIM",
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
            Self::Length => "LENGTH",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::StartsWith => "STARTS_WITH",
            Self::EndsWith => "ENDS_WITH",
            Self::Contains => "CONTAINS",
            Self::Position => "POSITION",
            Self::Replace => "REPLACE",
            Self::Substring => "SUBSTRING",
            Self::Round => "ROUND",
        }
    }
}

/// Parse one supported canonical internal `ORDER BY` expression term into the
/// canonical expression tree.
#[must_use]
pub(in crate::db) fn parse_supported_order_expr(term: &str) -> Option<Expr> {
    let tokens = tokenize_sql(term).ok()?;
    if tokens.is_empty() {
        return None;
    }

    let mut parser = SupportedOrderExprParser::new(SqlTokenCursor::new(tokens));
    let expression = parser.parse_expr().ok()?;

    parser.cursor.is_eof().then_some(expression)
}

/// Parse one grouped post-aggregate `ORDER BY` expression term into the shared
/// planner expression tree.
///
/// This parser stays intentionally narrow. It admits the grouped post-
/// aggregate expression family needed for `0.88` planning groundwork:
/// grouped-key leaves, aggregate leaves, one binary arithmetic layer, and
/// `ROUND(...)` wrappers over those same admitted inputs.
#[must_use]
pub(in crate::db) fn parse_grouped_post_aggregate_order_expr(term: &str) -> Option<Expr> {
    let tokens = tokenize_sql(term).ok()?;
    if tokens.is_empty() {
        return None;
    }

    let mut parser = SupportedGroupedOrderExprParser::new(SqlTokenCursor::new(tokens));
    let expression = parser.parse_expr().ok()?;

    parser.cursor.is_eof().then_some(expression)
}

/// Return whether one admitted `ORDER BY` expression is only a plain field.
#[must_use]
pub(in crate::db) const fn supported_order_expr_is_plain_field(expr: &Expr) -> bool {
    matches!(expr, Expr::Field(_))
}

/// Parse one supported computed `ORDER BY` term while rejecting plain fields.
#[must_use]
pub(in crate::db) fn parse_supported_computed_order_expr(term: &str) -> Option<Expr> {
    parse_supported_order_expr(term).filter(|expr| !supported_order_expr_is_plain_field(expr))
}

/// Borrow the referenced field when one expression is an admitted `ORDER BY`
/// function term.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn supported_order_expr_field(expr: &Expr) -> Option<&FieldId> {
    match expr {
        Expr::FunctionCall {
            function:
                Function::Trim
                | Function::Ltrim
                | Function::Rtrim
                | Function::Lower
                | Function::Upper
                | Function::Length,
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => Some(field),
            _ => None,
        },
        _ => None,
    }
}

/// Rewrite every field leaf in one admitted canonical `ORDER BY` expression.
#[must_use]
pub(in crate::db) fn rewrite_supported_order_expr_fields<F>(
    expr: &Expr,
    mut rewrite: F,
) -> Option<Expr>
where
    F: FnMut(&str) -> String,
{
    fn rewrite_expr<F>(expr: &Expr, rewrite: &mut F) -> Option<Expr>
    where
        F: FnMut(&str) -> String,
    {
        match expr {
            Expr::Field(field) => Some(Expr::Field(FieldId::new(rewrite(field.as_str())))),
            Expr::Literal(value) => Some(Expr::Literal(value.clone())),
            Expr::FunctionCall { function, args } => Some(Expr::FunctionCall {
                function: *function,
                args: args
                    .iter()
                    .map(|arg| rewrite_expr(arg, rewrite))
                    .collect::<Option<Vec<_>>>()?,
            }),
            Expr::Binary { op, left, right } => Some(Expr::Binary {
                op: *op,
                left: Box::new(rewrite_expr(left.as_ref(), rewrite)?),
                right: Box::new(rewrite_expr(right.as_ref(), rewrite)?),
            }),
            Expr::Aggregate(_) => None,
            #[cfg(test)]
            Expr::Alias { .. } | Expr::Unary { .. } => None,
        }
    }

    rewrite_expr(expr, &mut rewrite)
}

/// Render one admitted canonical `ORDER BY` expression term back into its stable
/// text form.
#[must_use]
pub(in crate::db) fn render_supported_order_expr(expr: &Expr) -> Option<String> {
    render_supported_order_expr_with_parent(expr, None)
}

/// Return whether one admitted `ORDER BY` expression must still satisfy the
/// current index-backed expression-order contract.
#[must_use]
pub(in crate::db) const fn supported_order_expr_requires_index_satisfied_access(
    expr: &Expr,
) -> bool {
    matches!(
        expr,
        Expr::FunctionCall {
            function: Function::Lower | Function::Upper,
            args,
        } if matches!(args.as_slice(), [Expr::Field(_)])
    )
}

fn render_supported_order_function(expr: &Expr) -> Option<String> {
    match expr {
        Expr::FunctionCall {
            function:
                function @ (Function::Trim
                | Function::Ltrim
                | Function::Rtrim
                | Function::Lower
                | Function::Upper
                | Function::Length),
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => Some(format!("{}({})", function.sql_label(), field.as_str())),
            _ => None,
        },
        Expr::FunctionCall {
            function:
                function @ (Function::Left
                | Function::Right
                | Function::StartsWith
                | Function::EndsWith
                | Function::Contains),
            args,
        } => match args.as_slice() {
            [Expr::Field(field), Expr::Literal(literal)] => Some(format!(
                "{}({}, {})",
                function.sql_label(),
                field.as_str(),
                render_supported_order_literal(literal)?
            )),
            _ => None,
        },
        Expr::FunctionCall {
            function: Function::Position,
            args,
        } => match args.as_slice() {
            [Expr::Literal(literal), Expr::Field(field)] => Some(format!(
                "POSITION({}, {})",
                render_supported_order_literal(literal)?,
                field.as_str(),
            )),
            _ => None,
        },
        Expr::FunctionCall {
            function: Function::Replace,
            args,
        } => match args.as_slice() {
            [Expr::Field(field), Expr::Literal(from), Expr::Literal(to)] => Some(format!(
                "REPLACE({}, {}, {})",
                field.as_str(),
                render_supported_order_literal(from)?,
                render_supported_order_literal(to)?,
            )),
            _ => None,
        },
        Expr::FunctionCall {
            function: Function::Substring,
            args,
        } => match args.as_slice() {
            [Expr::Field(field), Expr::Literal(start)] => Some(format!(
                "SUBSTRING({}, {})",
                field.as_str(),
                render_supported_order_literal(start)?,
            )),
            [
                Expr::Field(field),
                Expr::Literal(start),
                Expr::Literal(length),
            ] => Some(format!(
                "SUBSTRING({}, {}, {})",
                field.as_str(),
                render_supported_order_literal(start)?,
                render_supported_order_literal(length)?,
            )),
            _ => None,
        },
        _ => None,
    }
}

fn render_supported_order_expr_with_parent(
    expr: &Expr,
    parent_op: Option<BinaryOp>,
) -> Option<String> {
    match expr {
        Expr::FunctionCall {
            function:
                Function::Trim
                | Function::Ltrim
                | Function::Rtrim
                | Function::Lower
                | Function::Upper
                | Function::Length
                | Function::Left
                | Function::Right
                | Function::StartsWith
                | Function::EndsWith
                | Function::Contains
                | Function::Position
                | Function::Replace
                | Function::Substring,
            ..
        } => render_supported_order_function(expr),
        Expr::Binary { op, left, right }
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
            ) =>
        {
            let left = render_supported_order_expr_with_parent(left.as_ref(), Some(*op))?;
            let right = render_supported_order_expr_with_parent(right.as_ref(), Some(*op))?;
            let rendered = format!("{left} {} {right}", binary_op_sql_label(*op));

            if binary_expr_requires_parentheses(*op, parent_op) {
                Some(format!("({rendered})"))
            } else {
                Some(rendered)
            }
        }
        Expr::FunctionCall {
            function: Function::Round,
            args,
        } => match args.as_slice() {
            [base, Expr::Literal(scale)] => Some(format!(
                "ROUND({}, {})",
                render_supported_order_expr_with_parent(base, None)?,
                render_supported_order_literal(scale)?
            )),
            _ => None,
        },
        Expr::Field(field) => Some(field.as_str().to_string()),
        Expr::Literal(value) => render_supported_order_literal(value),
        Expr::Binary { .. } | Expr::Aggregate(_) => None,
        #[cfg(test)]
        Expr::Alias { .. } | Expr::Unary { .. } => None,
    }
}

const fn binary_expr_requires_parentheses(op: BinaryOp, parent_op: Option<BinaryOp>) -> bool {
    let Some(parent_op) = parent_op else {
        return false;
    };

    binary_op_precedence(op) < binary_op_precedence(parent_op)
}

const fn binary_op_precedence(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Add | BinaryOp::Sub => 1,
        BinaryOp::Mul | BinaryOp::Div => 2,
        #[cfg(test)]
        BinaryOp::And | BinaryOp::Eq => 0,
    }
}

const fn binary_op_sql_label(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        #[cfg(test)]
        BinaryOp::And => "AND",
        #[cfg(test)]
        BinaryOp::Eq => "=",
    }
}

fn render_supported_order_literal(value: &Value) -> Option<String> {
    Some(match value {
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
        _ => return None,
    })
}

// Parse one admitted canonical internal order expression using the reduced-SQL
// lexer so alias normalization and planner identity share one deterministic
// bounded expression surface.
///
/// SupportedOrderExprParser
///
/// Small canonical-order-expression parser over reduced-SQL tokens.
/// This stays intentionally narrower than the SQL frontend surface and only
/// accepts the internal order-expression family produced by alias
/// normalization.
///
struct SupportedOrderExprParser {
    cursor: SqlTokenCursor,
}

impl SupportedOrderExprParser {
    const fn new(cursor: SqlTokenCursor) -> Self {
        Self { cursor }
    }

    fn parse_expr(&mut self) -> Result<Expr, SqlParseError> {
        self.parse_additive_expr()
    }

    fn parse_additive_expr(&mut self) -> Result<Expr, SqlParseError> {
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            let op = if self.cursor.eat_plus() {
                Some(BinaryOp::Add)
            } else if self.cursor.eat_minus() {
                Some(BinaryOp::Sub)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };

            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(self.parse_multiplicative_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr, SqlParseError> {
        let mut left = self.parse_primary_expr()?;

        loop {
            let op = if matches!(self.cursor.peek_kind(), Some(TokenKind::Star)) {
                self.cursor.advance();
                Some(BinaryOp::Mul)
            } else if self.cursor.eat_slash() {
                Some(BinaryOp::Div)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };

            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(self.parse_primary_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, SqlParseError> {
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

            return Ok(Expr::Field(FieldId::new(head)));
        }

        self.cursor.parse_literal().map(Expr::Literal)
    }

    fn parse_function_expr(&mut self, name: &str) -> Result<Expr, SqlParseError> {
        self.cursor.expect_lparen()?;

        let expression = if matches!(
            name.to_ascii_uppercase().as_str(),
            "TRIM"
                | "LTRIM"
                | "RTRIM"
                | "LOWER"
                | "UPPER"
                | "LENGTH"
                | "LEFT"
                | "RIGHT"
                | "STARTS_WITH"
                | "ENDS_WITH"
                | "CONTAINS"
                | "POSITION"
                | "REPLACE"
                | "SUBSTRING"
        ) {
            self.parse_text_function_expr(name)?
        } else if name.eq_ignore_ascii_case("ROUND") {
            self.parse_round_expr()?
        } else {
            return Err(SqlParseError::unsupported_feature(
                "supported ORDER BY expression family",
            ));
        };

        self.cursor.expect_rparen()?;

        Ok(expression)
    }

    fn parse_text_function_expr(&mut self, name: &str) -> Result<Expr, SqlParseError> {
        let function = match name.to_ascii_uppercase().as_str() {
            "TRIM" => Function::Trim,
            "LTRIM" => Function::Ltrim,
            "RTRIM" => Function::Rtrim,
            "LOWER" => Function::Lower,
            "UPPER" => Function::Upper,
            "LENGTH" => Function::Length,
            "LEFT" => Function::Left,
            "RIGHT" => Function::Right,
            "STARTS_WITH" => Function::StartsWith,
            "ENDS_WITH" => Function::EndsWith,
            "CONTAINS" => Function::Contains,
            "POSITION" => Function::Position,
            "REPLACE" => Function::Replace,
            "SUBSTRING" => Function::Substring,
            _ => {
                return Err(SqlParseError::unsupported_feature(
                    "supported ORDER BY expression family",
                ));
            }
        };

        let args = match function {
            Function::Trim
            | Function::Ltrim
            | Function::Rtrim
            | Function::Lower
            | Function::Upper
            | Function::Length => vec![Expr::Field(FieldId::new(self.cursor.expect_identifier()?))],
            Function::Left
            | Function::Right
            | Function::StartsWith
            | Function::EndsWith
            | Function::Contains => {
                let field = self.cursor.expect_identifier()?;
                if !self.cursor.eat_comma() {
                    return Err(SqlParseError::expected(",", self.cursor.peek_kind()));
                }
                let literal = self.cursor.parse_literal()?;

                vec![Expr::Field(FieldId::new(field)), Expr::Literal(literal)]
            }
            Function::Position => {
                let literal = self.cursor.parse_literal()?;
                if !self.cursor.eat_comma() {
                    return Err(SqlParseError::expected(",", self.cursor.peek_kind()));
                }
                let field = self.cursor.expect_identifier()?;

                vec![Expr::Literal(literal), Expr::Field(FieldId::new(field))]
            }
            Function::Replace => {
                let field = self.cursor.expect_identifier()?;
                if !self.cursor.eat_comma() {
                    return Err(SqlParseError::expected(",", self.cursor.peek_kind()));
                }
                let from = self.cursor.parse_literal()?;
                if !self.cursor.eat_comma() {
                    return Err(SqlParseError::expected(",", self.cursor.peek_kind()));
                }
                let to = self.cursor.parse_literal()?;

                vec![
                    Expr::Field(FieldId::new(field)),
                    Expr::Literal(from),
                    Expr::Literal(to),
                ]
            }
            Function::Substring => {
                let field = self.cursor.expect_identifier()?;
                if !self.cursor.eat_comma() {
                    return Err(SqlParseError::expected(",", self.cursor.peek_kind()));
                }
                let start = self.cursor.parse_literal()?;
                let mut args = vec![Expr::Field(FieldId::new(field)), Expr::Literal(start)];
                if self.cursor.eat_comma() {
                    args.push(Expr::Literal(self.cursor.parse_literal()?));
                }
                args
            }
            Function::Round => unreachable!(),
        };

        Ok(Expr::FunctionCall { function, args })
    }

    fn parse_round_expr(&mut self) -> Result<Expr, SqlParseError> {
        let base = self.parse_expr()?;
        if !self.cursor.eat_comma() {
            return Err(SqlParseError::expected(",", self.cursor.peek_kind()));
        }
        let scale = Expr::Literal(self.cursor.parse_literal()?);

        Ok(Expr::FunctionCall {
            function: Function::Round,
            args: vec![base, scale],
        })
    }
}

// Parse one grouped post-aggregate order expression using the same reduced-SQL
// token surface as grouped projection and grouped HAVING parsing, while keeping
// the admitted family intentionally narrower than general SQL expressions.
struct SupportedGroupedOrderExprParser {
    cursor: SqlTokenCursor,
}

impl SupportedGroupedOrderExprParser {
    const fn new(cursor: SqlTokenCursor) -> Self {
        Self { cursor }
    }

    fn parse_expr(&mut self) -> Result<Expr, SqlParseError> {
        self.parse_additive_expr()
    }

    fn parse_additive_expr(&mut self) -> Result<Expr, SqlParseError> {
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            let op = if self.cursor.eat_plus() {
                Some(BinaryOp::Add)
            } else if self.cursor.eat_minus() {
                Some(BinaryOp::Sub)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };

            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(self.parse_multiplicative_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr, SqlParseError> {
        let mut left = self.parse_primary_expr()?;

        loop {
            let op = if matches!(self.cursor.peek_kind(), Some(TokenKind::Star)) {
                self.cursor.advance();
                Some(BinaryOp::Mul)
            } else if self.cursor.eat_slash() {
                Some(BinaryOp::Div)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };

            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(self.parse_primary_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, SqlParseError> {
        if matches!(self.cursor.peek_kind(), Some(TokenKind::LParen)) {
            self.cursor.expect_lparen()?;
            let expr = self.parse_expr()?;
            self.cursor.expect_rparen()?;

            return Ok(expr);
        }
        if self.cursor.peek_identifier_keyword("ROUND") {
            return self.parse_round_expr();
        }
        if let Some(kind) = self.parse_aggregate_kind() {
            return self.parse_aggregate_expr(kind);
        }
        if matches!(self.cursor.peek_kind(), Some(TokenKind::Identifier(_))) {
            return self
                .cursor
                .expect_identifier()
                .map(|field| Expr::Field(field.into()));
        }

        self.cursor.parse_literal().map(Expr::Literal)
    }

    fn parse_round_expr(&mut self) -> Result<Expr, SqlParseError> {
        self.cursor.expect_identifier()?;
        self.cursor.expect_lparen()?;
        let input = self.parse_expr()?;
        if !self.cursor.eat_comma() {
            return Err(SqlParseError::expected(",", self.cursor.peek_kind()));
        }
        let scale = Expr::Literal(self.cursor.parse_literal()?);
        self.cursor.expect_rparen()?;

        Ok(Expr::FunctionCall {
            function: Function::Round,
            args: vec![input, scale],
        })
    }

    fn parse_aggregate_kind(&self) -> Option<crate::db::query::plan::AggregateKind> {
        match self.cursor.peek_kind() {
            Some(TokenKind::Keyword(Keyword::Count)) => {
                Some(crate::db::query::plan::AggregateKind::Count)
            }
            Some(TokenKind::Keyword(Keyword::Sum)) => {
                Some(crate::db::query::plan::AggregateKind::Sum)
            }
            Some(TokenKind::Keyword(Keyword::Avg)) => {
                Some(crate::db::query::plan::AggregateKind::Avg)
            }
            Some(TokenKind::Keyword(Keyword::Min)) => {
                Some(crate::db::query::plan::AggregateKind::Min)
            }
            Some(TokenKind::Keyword(Keyword::Max)) => {
                Some(crate::db::query::plan::AggregateKind::Max)
            }
            _ => None,
        }
    }

    fn parse_aggregate_expr(
        &mut self,
        kind: crate::db::query::plan::AggregateKind,
    ) -> Result<Expr, SqlParseError> {
        self.cursor.advance();
        self.cursor.expect_lparen()?;
        let distinct = self.cursor.eat_keyword(Keyword::Distinct);
        let input_expr = if kind == crate::db::query::plan::AggregateKind::Count
            && matches!(self.cursor.peek_kind(), Some(TokenKind::Star))
        {
            self.cursor.advance();
            None
        } else {
            Some(self.parse_expr()?)
        };
        self.cursor.expect_rparen()?;

        let aggregate = match input_expr {
            Some(input_expr) => AggregateExpr::from_expression_input(kind, input_expr),
            None => AggregateExpr::from_semantic_parts(kind, None, false),
        };

        Ok(Expr::Aggregate(if distinct {
            aggregate.distinct()
        } else {
            aggregate
        }))
    }
}

///
/// Expr
///
/// Canonical planner-owned expression tree for projection semantics.
/// This model is semantic-only and intentionally excludes execution logic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Expr {
    Field(FieldId),
    Literal(Value),
    FunctionCall {
        function: Function,
        args: Vec<Self>,
    },
    #[cfg(test)]
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    Aggregate(AggregateExpr),
    #[cfg(test)]
    Alias {
        expr: Box<Self>,
        name: Alias,
    },
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::query::{
        builder::aggregate::AggregateExpr,
        plan::{AggregateKind, expr::parse_grouped_post_aggregate_order_expr},
    };

    use super::{BinaryOp, Expr, FieldId, Function};

    #[test]
    fn grouped_order_parser_preserves_expression_aggregate_input_shape() {
        let expr = parse_grouped_post_aggregate_order_expr("ROUND(AVG(rank + score), 2)")
            .expect("grouped order expression with aggregate input should parse");

        assert_eq!(
            expr,
            Expr::FunctionCall {
                function: Function::Round,
                args: vec![
                    Expr::Aggregate(AggregateExpr::from_expression_input(
                        AggregateKind::Avg,
                        Expr::Binary {
                            op: BinaryOp::Add,
                            left: Box::new(Expr::Field(FieldId::new("rank"))),
                            right: Box::new(Expr::Field(FieldId::new("score"))),
                        },
                    )),
                    Expr::Literal(crate::value::Value::Int(2)),
                ],
            },
            "aggregate input expressions should stay on the planner expression spine instead of collapsing back to one field-only target",
        );
    }

    #[test]
    fn grouped_order_parser_preserves_parenthesized_expression_aggregate_input_shape() {
        let expr = parse_grouped_post_aggregate_order_expr("ROUND(AVG((rank + score) / 2), 2)")
            .expect("grouped order expression with parenthesized aggregate input should parse");

        assert_eq!(
            expr,
            Expr::FunctionCall {
                function: Function::Round,
                args: vec![
                    Expr::Aggregate(AggregateExpr::from_expression_input(
                        AggregateKind::Avg,
                        Expr::Binary {
                            op: BinaryOp::Div,
                            left: Box::new(Expr::Binary {
                                op: BinaryOp::Add,
                                left: Box::new(Expr::Field(FieldId::new("rank"))),
                                right: Box::new(Expr::Field(FieldId::new("score"))),
                            }),
                            right: Box::new(Expr::Literal(crate::value::Value::Int(2))),
                        },
                    )),
                    Expr::Literal(crate::value::Value::Int(2)),
                ],
            },
            "parenthesized aggregate-input arithmetic should preserve nested grouped order expression structure",
        );
    }
}
