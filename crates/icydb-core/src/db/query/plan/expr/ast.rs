//! Module: query::plan::expr::ast
//! Responsibility: planner expression AST domain types and field/operator identifiers.
//! Does not own: expression type inference policy or runtime expression evaluation.
//! Boundary: defines canonical expression tree structures consumed by planner validation/lowering.

use crate::db::{
    query::builder::aggregate::AggregateExpr,
    sql_shared::{SqlParseError, SqlTokenCursor, TokenKind, tokenize_sql},
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
pub(in crate::db) fn supported_order_expr_field(expr: &Expr) -> Option<&FieldId> {
    match expr {
        Expr::FunctionCall {
            function: Function::Lower | Function::Upper,
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
    match expr {
        Expr::FunctionCall {
            function: Function::Lower | Function::Upper,
            args,
        } if matches!(args.as_slice(), [Expr::Field(_)]) => render_supported_order_function(expr),
        Expr::Binary { op, left, right }
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
            ) && matches!(left.as_ref(), Expr::Field(_))
                && matches!(right.as_ref(), Expr::Field(_) | Expr::Literal(_)) =>
        {
            let left = render_supported_order_expr(left.as_ref())?;
            let right = render_supported_order_expr(right.as_ref())?;

            Some(format!("{left} {} {right}", binary_op_sql_label(*op)))
        }
        Expr::FunctionCall {
            function: Function::Round,
            args,
        } => match args.as_slice() {
            [
                base @ (Expr::Field(_) | Expr::Binary { .. }),
                Expr::Literal(scale),
            ] => Some(format!(
                "ROUND({}, {})",
                render_supported_order_expr(base)?,
                render_supported_order_literal(scale)?
            )),
            _ => None,
        },
        Expr::Field(field) => Some(field.as_str().to_string()),
        Expr::Literal(value) => render_supported_order_literal(value),
        Expr::FunctionCall { .. } | Expr::Aggregate(_) | Expr::Binary { .. } => None,
        #[cfg(test)]
        Expr::Alias { .. } | Expr::Unary { .. } => None,
    }
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
    let function = match expr {
        Expr::FunctionCall {
            function: function @ (Function::Lower | Function::Upper),
            args,
        } if matches!(args.as_slice(), [Expr::Field(_)]) => *function,
        _ => return None,
    };
    let field = supported_order_expr_field(expr)?;

    Some(format!("{}({})", function.sql_label(), field.as_str()))
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
        let head = self.cursor.expect_identifier()?;
        if matches!(self.cursor.peek_kind(), Some(TokenKind::LParen)) {
            return self.parse_function_expr(head.as_str());
        }

        self.parse_field_or_arithmetic_expr(head)
    }

    fn parse_function_expr(&mut self, name: &str) -> Result<Expr, SqlParseError> {
        self.cursor.expect_lparen()?;

        let expression = if name.eq_ignore_ascii_case("LOWER") || name.eq_ignore_ascii_case("UPPER")
        {
            self.parse_casefold_expr(name)?
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

    fn parse_casefold_expr(&mut self, name: &str) -> Result<Expr, SqlParseError> {
        let field = self.cursor.expect_identifier()?;
        let function = if name.eq_ignore_ascii_case("LOWER") {
            Function::Lower
        } else {
            Function::Upper
        };

        Ok(Expr::FunctionCall {
            function,
            args: vec![Expr::Field(FieldId::new(field))],
        })
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

    fn parse_field_or_arithmetic_expr(&mut self, field: String) -> Result<Expr, SqlParseError> {
        let left = Expr::Field(FieldId::new(field));
        let Some(op) = self.parse_binary_op() else {
            return Ok(left);
        };
        let right = if matches!(self.cursor.peek_kind(), Some(TokenKind::Identifier(_))) {
            Expr::Field(FieldId::new(self.cursor.expect_identifier()?))
        } else {
            Expr::Literal(self.cursor.parse_literal()?)
        };

        Ok(Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn parse_binary_op(&mut self) -> Option<BinaryOp> {
        if self.cursor.eat_plus() {
            return Some(BinaryOp::Add);
        }
        if self.cursor.eat_minus() {
            return Some(BinaryOp::Sub);
        }
        if matches!(self.cursor.peek_kind(), Some(TokenKind::Star)) {
            self.cursor.advance();
            return Some(BinaryOp::Mul);
        }
        if self.cursor.eat_slash() {
            return Some(BinaryOp::Div);
        }

        None
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
