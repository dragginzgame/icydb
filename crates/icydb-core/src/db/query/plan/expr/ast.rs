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

#[allow(
    dead_code,
    reason = "0.91 CASE foundation promotes unary conditions before SQL lowering constructs them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnaryOp {
    Not,
}

///
/// BinaryOp
///
/// Canonical binary expression operator taxonomy.
///

#[allow(
    dead_code,
    reason = "0.91 CASE foundation widens the production scalar condition spine before SQL lowering constructs every operator"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BinaryOp {
    Or,
    And,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
}

///
/// Function
///
/// Canonical bounded function taxonomy admitted by planner-owned projection
/// expressions.
/// This intentionally stays limited to the shipped scalar-function surface.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Function {
    IsNull,
    IsNotNull,
    IsMissing,
    IsEmpty,
    IsNotEmpty,
    Trim,
    Ltrim,
    Rtrim,
    Coalesce,
    NullIf,
    Abs,
    Ceil,
    Ceiling,
    Floor,
    Lower,
    Upper,
    Length,
    Left,
    Right,
    StartsWith,
    EndsWith,
    Contains,
    CollectionContains,
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
            Self::IsNull => "IS_NULL",
            Self::IsNotNull => "IS_NOT_NULL",
            Self::IsMissing => "IS_MISSING",
            Self::IsEmpty => "IS_EMPTY",
            Self::IsNotEmpty => "IS_NOT_EMPTY",
            Self::Trim => "TRIM",
            Self::Ltrim => "LTRIM",
            Self::Rtrim => "RTRIM",
            Self::Coalesce => "COALESCE",
            Self::NullIf => "NULLIF",
            Self::Abs => "ABS",
            Self::Ceil => "CEIL",
            Self::Ceiling => "CEILING",
            Self::Floor => "FLOOR",
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
            Self::Length => "LENGTH",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::StartsWith => "STARTS_WITH",
            Self::EndsWith => "ENDS_WITH",
            Self::Contains => "CONTAINS",
            Self::CollectionContains => "COLLECTION_CONTAINS",
            Self::Position => "POSITION",
            Self::Replace => "REPLACE",
            Self::Substring => "SUBSTRING",
            Self::Round => "ROUND",
        }
    }
}

///
/// CaseWhenArm
///
/// Planner-owned searched-CASE branch pairing one boolean condition with the
/// scalar result expression selected when that condition evaluates true.
/// CASE normalization keeps the missing-ELSE rule outside this type by always
/// pairing searched arms with an explicit planner-owned fallback expression.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CaseWhenArm {
    condition: Expr,
    result: Expr,
}

impl CaseWhenArm {
    /// Build one planner-owned searched-CASE arm.
    #[must_use]
    pub(crate) const fn new(condition: Expr, result: Expr) -> Self {
        Self { condition, result }
    }

    /// Borrow the boolean branch condition.
    #[must_use]
    pub(crate) const fn condition(&self) -> &Expr {
        &self.condition
    }

    /// Borrow the scalar branch result expression.
    #[must_use]
    pub(crate) const fn result(&self) -> &Expr {
        &self.result
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
/// bounded scalar-function wrappers over those same admitted inputs.
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
#[cfg(test)]
pub(in crate::db) const fn supported_order_expr_is_plain_field(expr: &Expr) -> bool {
    matches!(expr, Expr::Field(_))
}

/// Parse one supported computed `ORDER BY` term while rejecting plain fields.
#[must_use]
#[cfg(test)]
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
                | Function::Abs
                | Function::Ceil
                | Function::Ceiling
                | Function::Floor
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

/// Render one admitted canonical `ORDER BY` expression term back into its stable
/// text form.
#[must_use]
#[cfg(test)]
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

#[cfg(test)]
fn render_supported_order_function(expr: &Expr) -> Option<String> {
    match expr {
        Expr::FunctionCall {
            function:
                function @ (Function::Trim
                | Function::Ltrim
                | Function::Rtrim
                | Function::Abs
                | Function::Ceil
                | Function::Ceiling
                | Function::Floor
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

#[cfg(test)]
fn render_supported_order_expr_with_parent(
    expr: &Expr,
    parent_op: Option<BinaryOp>,
) -> Option<String> {
    match expr {
        Expr::FunctionCall {
            function:
                Function::IsNull
                | Function::IsNotNull
                | Function::IsMissing
                | Function::IsEmpty
                | Function::IsNotEmpty
                | Function::Trim
                | Function::Ltrim
                | Function::Rtrim
                | Function::Abs
                | Function::Ceil
                | Function::Ceiling
                | Function::Floor
                | Function::Lower
                | Function::Upper
                | Function::Length
                | Function::Left
                | Function::Right
                | Function::StartsWith
                | Function::EndsWith
                | Function::Contains
                | Function::CollectionContains
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
        Expr::FunctionCall {
            function: Function::Coalesce | Function::NullIf,
            ..
        } => None,
        Expr::Field(field) => Some(field.as_str().to_string()),
        Expr::Literal(value) => render_supported_order_literal(value),
        Expr::Binary { .. } | Expr::Aggregate(_) | Expr::Case { .. } => None,
        Expr::Unary { .. } => None,
        #[cfg(test)]
        Expr::Alias { .. } => None,
    }
}

#[cfg(test)]
const fn binary_expr_requires_parentheses(op: BinaryOp, parent_op: Option<BinaryOp>) -> bool {
    let Some(parent_op) = parent_op else {
        return false;
    };

    binary_op_precedence(op) < binary_op_precedence(parent_op)
}

#[cfg(test)]
const fn binary_op_precedence(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Or => 0,
        BinaryOp::And => 1,
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => 2,
        BinaryOp::Add | BinaryOp::Sub => 3,
        BinaryOp::Mul | BinaryOp::Div => 4,
    }
}

#[cfg(test)]
const fn binary_op_sql_label(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Or => "OR",
        BinaryOp::And => "AND",
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Lte => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Gte => ">=",
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
    }
}

#[cfg(test)]
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
        let mut left = self.parse_unary_expr()?;

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
                right: Box::new(self.parse_unary_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr, SqlParseError> {
        if self.cursor.eat_keyword(Keyword::Not) {
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(self.parse_unary_expr()?),
            });
        }

        self.parse_primary_expr()
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
            "ABS" => Function::Abs,
            "CEIL" => Function::Ceil,
            "CEILING" => Function::Ceiling,
            "FLOOR" => Function::Floor,
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
            Function::IsNull
            | Function::IsNotNull
            | Function::IsMissing
            | Function::IsEmpty
            | Function::IsNotEmpty
            | Function::Coalesce
            | Function::NullIf
            | Function::CollectionContains => {
                return Err(SqlParseError::unsupported_feature(
                    "supported ORDER BY expression family",
                ));
            }
            Function::Trim
            | Function::Ltrim
            | Function::Rtrim
            | Function::Abs
            | Function::Ceil
            | Function::Ceiling
            | Function::Floor
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
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, SqlParseError> {
        let mut left = self.parse_and_expr()?;

        while self.cursor.eat_keyword(Keyword::Or) {
            left = Expr::Binary {
                op: BinaryOp::Or,
                left: Box::new(left),
                right: Box::new(self.parse_and_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, SqlParseError> {
        let mut left = self.parse_compare_expr()?;

        while self.cursor.eat_keyword(Keyword::And) {
            left = Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(left),
                right: Box::new(self.parse_compare_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_compare_expr(&mut self) -> Result<Expr, SqlParseError> {
        let left = self.parse_additive_expr()?;
        let Some(op) = self.parse_compare_op() else {
            return Ok(left);
        };

        Ok(Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(self.parse_additive_expr()?),
        })
    }

    fn parse_compare_op(&mut self) -> Option<BinaryOp> {
        let op = match self.cursor.peek_kind() {
            Some(TokenKind::Eq) => BinaryOp::Eq,
            Some(TokenKind::Ne) => BinaryOp::Ne,
            Some(TokenKind::Lt) => BinaryOp::Lt,
            Some(TokenKind::Lte) => BinaryOp::Lte,
            Some(TokenKind::Gt) => BinaryOp::Gt,
            Some(TokenKind::Gte) => BinaryOp::Gte,
            _ => return None,
        };

        self.cursor.advance();

        Some(op)
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
        let mut left = self.parse_unary_expr()?;

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
                right: Box::new(self.parse_unary_expr()?),
            };
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr, SqlParseError> {
        if self.cursor.eat_keyword(Keyword::Not) {
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(self.parse_unary_expr()?),
            });
        }

        self.parse_primary_expr()
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, SqlParseError> {
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
            return self.parse_round_expr();
        }
        if let Some(kind) = self.parse_aggregate_kind() {
            return self.parse_aggregate_expr(kind);
        }
        if matches!(self.cursor.peek_kind(), Some(TokenKind::Identifier(_))) {
            let head = self.cursor.expect_identifier()?;
            if matches!(self.cursor.peek_kind(), Some(TokenKind::LParen)) {
                return self.parse_function_expr(head.as_str());
            }

            return Ok(Expr::Field(head.into()));
        }

        self.cursor.parse_literal().map(Expr::Literal)
    }

    fn parse_case_expr(&mut self) -> Result<Expr, SqlParseError> {
        let mut when_then_arms = Vec::new();

        while self.cursor.eat_keyword(Keyword::When) {
            let condition = self.parse_expr()?;
            if !self.cursor.eat_keyword(Keyword::Then) {
                return Err(SqlParseError::expected("THEN", self.cursor.peek_kind()));
            }
            let result = self.parse_expr()?;
            when_then_arms.push(CaseWhenArm::new(condition, result));
        }

        if when_then_arms.is_empty() {
            return Err(SqlParseError::unsupported_feature(
                "searched CASE in grouped ORDER BY expressions",
            ));
        }

        let else_expr = if self.cursor.eat_keyword(Keyword::Else) {
            self.parse_expr()?
        } else {
            Expr::Literal(Value::Null)
        };

        if !self.cursor.eat_keyword(Keyword::End) {
            return Err(SqlParseError::expected("END", self.cursor.peek_kind()));
        }

        Ok(Expr::Case {
            when_then_arms,
            else_expr: Box::new(else_expr),
        })
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

    // Parse one normalized scalar function call inside the grouped post-
    // aggregate expression seam so filtered aggregate identities can be
    // reconstructed from their rendered labels during grouped Top-K matching.
    fn parse_function_expr(&mut self, name: &str) -> Result<Expr, SqlParseError> {
        self.cursor.expect_lparen()?;

        let expression = if matches!(
            name.to_ascii_uppercase().as_str(),
            "IS_NULL"
                | "IS_NOT_NULL"
                | "IS_MISSING"
                | "IS_EMPTY"
                | "IS_NOT_EMPTY"
                | "TRIM"
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
            self.parse_scalar_function_expr(name)?
        } else if name.eq_ignore_ascii_case("ROUND") {
            self.parse_round_expr()?
        } else {
            return Err(SqlParseError::unsupported_feature(
                "supported grouped ORDER BY expression family",
            ));
        };

        self.cursor.expect_rparen()?;

        Ok(expression)
    }

    // Parse one bounded grouped ORDER BY scalar function on the current
    // direct-field/literal surface without widening grouped parser admission.
    #[expect(
        clippy::too_many_lines,
        reason = "grouped ORDER BY scalar-function admission stays explicit and field-shaped"
    )]
    fn parse_scalar_function_expr(&mut self, name: &str) -> Result<Expr, SqlParseError> {
        let function = match name.to_ascii_uppercase().as_str() {
            "IS_NULL" => Function::IsNull,
            "IS_NOT_NULL" => Function::IsNotNull,
            "IS_MISSING" => Function::IsMissing,
            "IS_EMPTY" => Function::IsEmpty,
            "IS_NOT_EMPTY" => Function::IsNotEmpty,
            "TRIM" => Function::Trim,
            "LTRIM" => Function::Ltrim,
            "RTRIM" => Function::Rtrim,
            "ABS" => Function::Abs,
            "CEIL" => Function::Ceil,
            "CEILING" => Function::Ceiling,
            "FLOOR" => Function::Floor,
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
                    "supported grouped ORDER BY expression family",
                ));
            }
        };

        let args = match function {
            Function::IsNull
            | Function::IsNotNull
            | Function::IsMissing
            | Function::IsEmpty
            | Function::IsNotEmpty
            | Function::Trim
            | Function::Ltrim
            | Function::Rtrim
            | Function::Abs
            | Function::Ceil
            | Function::Ceiling
            | Function::Floor
            | Function::Lower
            | Function::Upper
            | Function::Length => {
                vec![Expr::Field(FieldId::new(self.cursor.expect_identifier()?))]
            }
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
            Function::CollectionContains
            | Function::Round
            | Function::Coalesce
            | Function::NullIf => {
                return Err(SqlParseError::unsupported_feature(
                    "supported grouped ORDER BY expression family",
                ));
            }
        };

        Ok(Expr::FunctionCall { function, args })
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
        // Phase 1: parse the aggregate call shape itself so grouped
        // post-aggregate ORDER BY expressions preserve canonical aggregate
        // identity instead of collapsing back to one string term.
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

        // Phase 2: parse the optional SQL aggregate FILTER clause on the same
        // planner expression seam so alias-normalized grouped ORDER BY terms
        // continue to match grouped execution specs with filtered aggregates.
        let filter_expr = self.parse_optional_aggregate_filter_expr()?;

        let aggregate = match input_expr {
            Some(input_expr) => AggregateExpr::from_expression_input(kind, input_expr),
            None => AggregateExpr::from_semantic_parts(kind, None, false),
        };
        let aggregate = match filter_expr {
            Some(filter_expr) => aggregate.with_filter_expr(filter_expr),
            None => aggregate,
        };

        Ok(Expr::Aggregate(if distinct {
            aggregate.distinct()
        } else {
            aggregate
        }))
    }

    // Parse one optional SQL aggregate FILTER clause while keeping grouped
    // post-aggregate ORDER BY reconstruction on the shared planner expression
    // spine. This parser is intentionally narrow: it only admits the shipped
    // `FILTER (WHERE <expr>)` surface and rejects any malformed shell.
    fn parse_optional_aggregate_filter_expr(&mut self) -> Result<Option<Expr>, SqlParseError> {
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
    #[allow(
        dead_code,
        reason = "0.91 CASE foundation adds a production unary expression node before parser/lowering admission lands"
    )]
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    #[allow(
        dead_code,
        reason = "0.91 searched CASE planner node lands before SQL parser/lowering constructs it"
    )]
    Case {
        when_then_arms: Vec<CaseWhenArm>,
        else_expr: Box<Self>,
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

    use super::{BinaryOp, Expr, FieldId, Function, UnaryOp};

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

    #[test]
    fn grouped_order_parser_preserves_case_aggregate_input_shape() {
        let expr =
            parse_grouped_post_aggregate_order_expr("SUM(CASE WHEN level >= 20 THEN 1 ELSE 0 END)")
                .expect("grouped order expression with searched CASE aggregate input should parse");

        assert_eq!(
            expr,
            Expr::Aggregate(AggregateExpr::from_expression_input(
                AggregateKind::Sum,
                Expr::Case {
                    when_then_arms: vec![crate::db::query::plan::expr::CaseWhenArm::new(
                        Expr::Binary {
                            op: BinaryOp::Gte,
                            left: Box::new(Expr::Field(FieldId::new("level"))),
                            right: Box::new(Expr::Literal(crate::value::Value::Int(20))),
                        },
                        Expr::Literal(crate::value::Value::Int(1)),
                    )],
                    else_expr: Box::new(Expr::Literal(crate::value::Value::Int(0))),
                },
            )),
            "searched CASE aggregate inputs should stay on the grouped post-aggregate order expression spine instead of collapsing to an unknown field label",
        );
    }

    #[test]
    fn grouped_order_parser_preserves_filtered_aggregate_shape() {
        let expr = parse_grouped_post_aggregate_order_expr("COUNT(*) FILTER (WHERE age >= 20)")
            .expect("grouped order expression with filtered aggregate should parse");

        assert_eq!(
            expr,
            Expr::Aggregate(
                AggregateExpr::from_semantic_parts(AggregateKind::Count, None, false)
                    .with_filter_expr(Expr::Binary {
                        op: BinaryOp::Gte,
                        left: Box::new(Expr::Field(FieldId::new("age"))),
                        right: Box::new(Expr::Literal(crate::value::Value::Int(20))),
                    }),
            ),
            "filtered grouped aggregate terms should preserve FILTER semantics instead of collapsing back to a bare aggregate shell",
        );
    }

    #[test]
    fn grouped_order_parser_preserves_filtered_unary_not_aggregate_shape() {
        let expr =
            parse_grouped_post_aggregate_order_expr("SUM(strength) FILTER (WHERE NOT is_npc)")
                .expect("grouped order expression with filtered unary-not aggregate should parse");

        assert_eq!(
            expr,
            Expr::Aggregate(
                AggregateExpr::from_expression_input(
                    AggregateKind::Sum,
                    Expr::Field(FieldId::new("strength")),
                )
                .with_filter_expr(Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(Expr::Field(FieldId::new("is_npc"))),
                }),
            ),
            "filtered grouped aggregate terms should preserve unary NOT filter semantics instead of collapsing back to an unknown field label",
        );
    }

    #[test]
    fn grouped_order_parser_preserves_filtered_null_test_aggregate_shape() {
        let expr = parse_grouped_post_aggregate_order_expr(
            "COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank))",
        )
        .expect("grouped order expression with filtered null-test aggregate should parse");

        assert_eq!(
            expr,
            Expr::Aggregate(
                AggregateExpr::from_semantic_parts(AggregateKind::Count, None, false)
                    .with_filter_expr(Expr::FunctionCall {
                        function: Function::IsNotNull,
                        args: vec![Expr::Field(FieldId::new("guild_rank"))],
                    }),
            ),
            "filtered grouped aggregate terms should preserve null-test FILTER semantics instead of collapsing back to an unknown field label",
        );
    }

    #[test]
    fn grouped_order_parser_preserves_filtered_null_test_boolean_composition_shape() {
        let expr = parse_grouped_post_aggregate_order_expr(
            "COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank) AND level >= 10)",
        )
        .expect(
            "grouped order expression with filtered null-test boolean composition should parse",
        );

        assert_eq!(
            expr,
            Expr::Aggregate(
                AggregateExpr::from_semantic_parts(AggregateKind::Count, None, false)
                    .with_filter_expr(Expr::Binary {
                        op: BinaryOp::And,
                        left: Box::new(Expr::FunctionCall {
                            function: Function::IsNotNull,
                            args: vec![Expr::Field(FieldId::new("guild_rank"))],
                        }),
                        right: Box::new(Expr::Binary {
                            op: BinaryOp::Gte,
                            left: Box::new(Expr::Field(FieldId::new("level"))),
                            right: Box::new(Expr::Literal(crate::value::Value::Int(10))),
                        }),
                    }),
            ),
            "filtered grouped aggregate terms should preserve null-test boolean composition semantics through grouped order parsing",
        );
    }
}
