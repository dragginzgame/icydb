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
#[remain::sorted]
pub(crate) enum Function {
    Abs,
    Ceiling,
    Coalesce,
    CollectionContains,
    Contains,
    EndsWith,
    Floor,
    IsEmpty,
    IsMissing,
    IsNotEmpty,
    IsNotNull,
    IsNull,
    Left,
    Length,
    Lower,
    Ltrim,
    NullIf,
    Position,
    Replace,
    Right,
    Round,
    Rtrim,
    StartsWith,
    Substring,
    Trim,
    Upper,
}

impl Function {
    /// Return the stable uppercase SQL label for this bounded function.
    #[must_use]
    pub(crate) const fn sql_label(self) -> &'static str {
        match self {
            Self::Abs => "ABS",
            Self::Ceiling => "CEILING",
            Self::Coalesce => "COALESCE",
            Self::CollectionContains => "COLLECTION_CONTAINS",
            Self::Contains => "CONTAINS",
            Self::EndsWith => "ENDS_WITH",
            Self::Floor => "FLOOR",
            Self::IsEmpty => "IS_EMPTY",
            Self::IsMissing => "IS_MISSING",
            Self::IsNotEmpty => "IS_NOT_EMPTY",
            Self::IsNotNull => "IS_NOT_NULL",
            Self::IsNull => "IS_NULL",
            Self::Left => "LEFT",
            Self::Length => "LENGTH",
            Self::Lower => "LOWER",
            Self::Ltrim => "LTRIM",
            Self::NullIf => "NULLIF",
            Self::Position => "POSITION",
            Self::Replace => "REPLACE",
            Self::Round => "ROUND",
            Self::Right => "RIGHT",
            Self::Rtrim => "RTRIM",
            Self::StartsWith => "STARTS_WITH",
            Self::Substring => "SUBSTRING",
            Self::Trim => "TRIM",
            Self::Upper => "UPPER",
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
            function,
            args,
        } if function.is_casefold_transform() && matches!(args.as_slice(), [Expr::Field(_)])
    )
}

#[cfg(test)]
fn render_supported_order_function(function: Function, args: &[Expr]) -> Option<String> {
    match supported_order_function_shape(function)? {
        SupportedOrderFunctionShape::UnaryExpr => match args {
            [arg] => Some(format!(
                "{}({})",
                function.sql_label(),
                render_supported_order_expr_with_parent(arg, None)?
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::VariadicExprMin2 => {
            if args.len() < 2 {
                return None;
            }

            let rendered = args
                .iter()
                .map(|arg| render_supported_order_expr_with_parent(arg, None))
                .collect::<Option<Vec<_>>>()?;

            Some(format!("{}({})", function.sql_label(), rendered.join(", ")))
        }
        SupportedOrderFunctionShape::BinaryExpr => match args {
            [left, right] => Some(format!(
                "{}({}, {})",
                function.sql_label(),
                render_supported_order_expr_with_parent(left, None)?,
                render_supported_order_expr_with_parent(right, None)?
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::FieldLiteral => match args {
            [Expr::Field(field), Expr::Literal(literal)] => Some(format!(
                "{}({}, {})",
                function.sql_label(),
                field.as_str(),
                render_supported_order_literal(literal)?
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::LiteralField => match args {
            [Expr::Literal(literal), Expr::Field(field)] => Some(format!(
                "{}({}, {})",
                function.sql_label(),
                render_supported_order_literal(literal)?,
                field.as_str(),
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::FieldTwoLiterals => match args {
            [Expr::Field(field), Expr::Literal(from), Expr::Literal(to)] => Some(format!(
                "{}({}, {}, {})",
                function.sql_label(),
                field.as_str(),
                render_supported_order_literal(from)?,
                render_supported_order_literal(to)?,
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::FieldOneOrTwoLiterals => match args {
            [Expr::Field(field), Expr::Literal(start)] => Some(format!(
                "{}({}, {})",
                function.sql_label(),
                field.as_str(),
                render_supported_order_literal(start)?,
            )),
            [
                Expr::Field(field),
                Expr::Literal(start),
                Expr::Literal(length),
            ] => Some(format!(
                "{}({}, {}, {})",
                function.sql_label(),
                field.as_str(),
                render_supported_order_literal(start)?,
                render_supported_order_literal(length)?,
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::Round => None,
    }
}

#[cfg(test)]
fn render_supported_order_expr_with_parent(
    expr: &Expr,
    parent_op: Option<BinaryOp>,
) -> Option<String> {
    match expr {
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
        Expr::FunctionCall { function, args } => render_supported_order_function(*function, args),
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

///
/// SupportedOrderFunctionShape
///
/// Clause-owned argument-shape taxonomy for the reduced `ORDER BY` function
/// surface.
/// This exists so the plain parser, grouped parser, and test-only renderer
/// share one local definition of admitted wrapper forms.
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

// Resolve one reduced `ORDER BY` function name onto the shared planner
// function taxonomy so both parser seams stay on the same admitted surface.
fn supported_order_function(name: &str) -> Option<Function> {
    Some(match name.to_ascii_uppercase().as_str() {
        "IS_NULL" => Function::IsNull,
        "IS_NOT_NULL" => Function::IsNotNull,
        "IS_MISSING" => Function::IsMissing,
        "IS_EMPTY" => Function::IsEmpty,
        "IS_NOT_EMPTY" => Function::IsNotEmpty,
        "TRIM" => Function::Trim,
        "LTRIM" => Function::Ltrim,
        "RTRIM" => Function::Rtrim,
        "ABS" => Function::Abs,
        "CEIL" | "CEILING" => Function::Ceiling,
        "FLOOR" => Function::Floor,
        "LOWER" => Function::Lower,
        "UPPER" => Function::Upper,
        "LENGTH" => Function::Length,
        "COALESCE" => Function::Coalesce,
        "NULLIF" => Function::NullIf,
        "LEFT" => Function::Left,
        "RIGHT" => Function::Right,
        "STARTS_WITH" => Function::StartsWith,
        "ENDS_WITH" => Function::EndsWith,
        "CONTAINS" => Function::Contains,
        "POSITION" => Function::Position,
        "REPLACE" => Function::Replace,
        "SUBSTRING" => Function::Substring,
        "ROUND" => Function::Round,
        _ => return None,
    })
}

// Keep the reduced `ORDER BY` function family clause-owned by describing the
// admitted argument shape locally instead of re-encoding it in each parser.
const fn supported_order_function_shape(function: Function) -> Option<SupportedOrderFunctionShape> {
    match function {
        Function::IsNull
        | Function::IsNotNull
        | Function::IsMissing
        | Function::IsEmpty
        | Function::IsNotEmpty
        | Function::Trim
        | Function::Ltrim
        | Function::Rtrim
        | Function::Abs
        | Function::Ceiling
        | Function::Floor
        | Function::Lower
        | Function::Upper
        | Function::Length => Some(SupportedOrderFunctionShape::UnaryExpr),
        Function::Coalesce => Some(SupportedOrderFunctionShape::VariadicExprMin2),
        Function::NullIf => Some(SupportedOrderFunctionShape::BinaryExpr),
        Function::Left
        | Function::Right
        | Function::StartsWith
        | Function::EndsWith
        | Function::Contains => Some(SupportedOrderFunctionShape::FieldLiteral),
        Function::Position => Some(SupportedOrderFunctionShape::LiteralField),
        Function::Replace => Some(SupportedOrderFunctionShape::FieldTwoLiterals),
        Function::Substring => Some(SupportedOrderFunctionShape::FieldOneOrTwoLiterals),
        Function::Round => Some(SupportedOrderFunctionShape::Round),
        Function::CollectionContains => None,
    }
}

///
/// SupportedOrderFunctionParser
///
/// Local parser contract for the reduced `ORDER BY` function family.
/// This keeps shared call-shape handling in one place while letting each
/// parser own its operand-expression grammar.
///

trait SupportedOrderFunctionParser {
    fn cursor(&mut self) -> &mut SqlTokenCursor;

    fn unsupported_surface(&self) -> &'static str;

    fn parse_expr_arg(&mut self) -> Result<Expr, SqlParseError>;

    fn parse_supported_order_function_expr(&mut self, name: &str) -> Result<Expr, SqlParseError> {
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
        function: Function,
    ) -> Result<Expr, SqlParseError> {
        let Some(shape) = supported_order_function_shape(function) else {
            return Err(SqlParseError::unsupported_feature(
                self.unsupported_surface(),
            ));
        };

        if matches!(shape, SupportedOrderFunctionShape::Round) {
            return self.parse_supported_order_round_expr();
        }

        let args = self.parse_supported_order_function_args(shape)?;

        Ok(Expr::FunctionCall { function, args })
    }

    fn parse_supported_order_function_args(
        &mut self,
        shape: SupportedOrderFunctionShape,
    ) -> Result<Vec<Expr>, SqlParseError> {
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

    fn parse_supported_order_round_expr(&mut self) -> Result<Expr, SqlParseError> {
        let base = self.parse_expr_arg()?;
        self.expect_function_comma()?;
        let scale = self.parse_literal_arg()?;

        Ok(Expr::FunctionCall {
            function: Function::Round,
            args: vec![base, scale],
        })
    }

    fn parse_field_arg(&mut self) -> Result<Expr, SqlParseError> {
        Ok(Expr::Field(FieldId::new(
            self.cursor().expect_identifier()?,
        )))
    }

    fn parse_literal_arg(&mut self) -> Result<Expr, SqlParseError> {
        self.cursor().parse_literal().map(Expr::Literal)
    }

    fn expect_function_comma(&mut self) -> Result<(), SqlParseError> {
        if self.cursor().eat_comma() {
            return Ok(());
        }

        Err(SqlParseError::expected(",", self.cursor().peek_kind()))
    }
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

    fn parse_expr_arg(&mut self) -> Result<Expr, SqlParseError> {
        self.parse_expr()
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

    // Parse one normalized scalar function call inside the grouped post-
    // aggregate expression seam so filtered aggregate identities can be
    // reconstructed from their rendered labels during grouped Top-K matching.
    fn parse_function_expr(&mut self, name: &str) -> Result<Expr, SqlParseError> {
        SupportedOrderFunctionParser::parse_supported_order_function_expr(self, name)
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

impl SupportedOrderFunctionParser for SupportedGroupedOrderExprParser {
    fn cursor(&mut self) -> &mut SqlTokenCursor {
        &mut self.cursor
    }

    fn unsupported_surface(&self) -> &'static str {
        "supported grouped ORDER BY expression family"
    }

    fn parse_expr_arg(&mut self) -> Result<Expr, SqlParseError> {
        self.parse_expr()
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
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
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

impl Expr {
    /// Return true when this planner expression tree contains any aggregate
    /// leaf.
    #[must_use]
    pub(in crate::db) fn contains_aggregate(&self) -> bool {
        self.any_tree_expr(&mut |expr| matches!(expr, Self::Aggregate(_)))
    }

    /// Return true when this planner expression tree still contains any raw
    /// searched `CASE` node after owner-local canonicalization.
    #[must_use]
    pub(in crate::db) fn contains_case(&self) -> bool {
        self.any_tree_expr(&mut |expr| matches!(expr, Self::Case { .. }))
    }

    /// Return true when any visited planner expression node satisfies the
    /// supplied predicate.
    #[must_use]
    pub(in crate::db) fn any_tree_expr(&self, predicate: &mut impl FnMut(&Self) -> bool) -> bool {
        if predicate(self) {
            return true;
        }

        match self {
            Self::Field(_) | Self::Literal(_) | Self::Aggregate(_) => false,
            Self::FunctionCall { args, .. } => args.iter().any(|arg| arg.any_tree_expr(predicate)),
            Self::Unary { expr, .. } => expr.any_tree_expr(predicate),
            Self::Binary { left, right, .. } => {
                left.any_tree_expr(predicate) || right.any_tree_expr(predicate)
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().any(|arm| {
                    arm.condition().any_tree_expr(predicate)
                        || arm.result().any_tree_expr(predicate)
                }) || else_expr.any_tree_expr(predicate)
            }
            #[cfg(test)]
            Self::Alias { expr, .. } => expr.any_tree_expr(predicate),
        }
    }

    /// Return true when every visited planner expression node satisfies the
    /// supplied predicate.
    #[must_use]
    pub(in crate::db) fn all_tree_expr(&self, predicate: &mut impl FnMut(&Self) -> bool) -> bool {
        if !predicate(self) {
            return false;
        }

        match self {
            Self::Field(_) | Self::Literal(_) | Self::Aggregate(_) => true,
            Self::FunctionCall { args, .. } => args.iter().all(|arg| arg.all_tree_expr(predicate)),
            Self::Unary { expr, .. } => expr.all_tree_expr(predicate),
            Self::Binary { left, right, .. } => {
                left.all_tree_expr(predicate) && right.all_tree_expr(predicate)
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().all(|arm| {
                    arm.condition().all_tree_expr(predicate)
                        && arm.result().all_tree_expr(predicate)
                }) && else_expr.all_tree_expr(predicate)
            }
            #[cfg(test)]
            Self::Alias { expr, .. } => expr.all_tree_expr(predicate),
        }
    }

    /// Visit every planner expression node in this tree through the owner-local
    /// child traversal contract, stopping early on the first error.
    pub(in crate::db) fn try_for_each_tree_expr<E>(
        &self,
        visit: &mut impl FnMut(&Self) -> Result<(), E>,
    ) -> Result<(), E> {
        visit(self)?;

        match self {
            Self::Field(_) | Self::Literal(_) | Self::Aggregate(_) => Ok(()),
            Self::FunctionCall { args, .. } => {
                for arg in args {
                    arg.try_for_each_tree_expr(visit)?;
                }

                Ok(())
            }
            Self::Unary { expr, .. } => expr.try_for_each_tree_expr(visit),
            Self::Binary { left, right, .. } => {
                left.try_for_each_tree_expr(visit)?;
                right.try_for_each_tree_expr(visit)
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    arm.condition().try_for_each_tree_expr(visit)?;
                    arm.result().try_for_each_tree_expr(visit)?;
                }

                else_expr.try_for_each_tree_expr(visit)
            }
            #[cfg(test)]
            Self::Alias { expr, .. } => expr.try_for_each_tree_expr(visit),
        }
    }

    /// Visit every aggregate leaf owned by this planner expression tree through
    /// the canonical traversal contract.
    pub(in crate::db) fn try_for_each_tree_aggregate<E>(
        &self,
        visit: &mut impl FnMut(&AggregateExpr) -> Result<(), E>,
    ) -> Result<(), E> {
        self.try_for_each_tree_expr(&mut |expr| match expr {
            Self::Aggregate(aggregate) => visit(aggregate),
            _ => Ok(()),
        })
    }

    /// Visit every planner expression node through the canonical traversal
    /// contract while tracking compare-family nodes in post-order.
    pub(in crate::db) fn try_for_each_tree_expr_with_compare_index<E>(
        &self,
        next_compare_index: &mut usize,
        visit: &mut impl FnMut(usize, &Self) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Field(_) | Self::Literal(_) | Self::Aggregate(_) => {}
            Self::FunctionCall { args, .. } => {
                for arg in args {
                    arg.try_for_each_tree_expr_with_compare_index(next_compare_index, visit)?;
                }
            }
            Self::Unary { expr, .. } => {
                expr.try_for_each_tree_expr_with_compare_index(next_compare_index, visit)?;
            }
            Self::Binary { left, right, .. } => {
                left.try_for_each_tree_expr_with_compare_index(next_compare_index, visit)?;
                right.try_for_each_tree_expr_with_compare_index(next_compare_index, visit)?;
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    arm.condition()
                        .try_for_each_tree_expr_with_compare_index(next_compare_index, visit)?;
                    arm.result()
                        .try_for_each_tree_expr_with_compare_index(next_compare_index, visit)?;
                }

                else_expr.try_for_each_tree_expr_with_compare_index(next_compare_index, visit)?;
            }
            #[cfg(test)]
            Self::Alias { expr, .. } => {
                expr.try_for_each_tree_expr_with_compare_index(next_compare_index, visit)?;
            }
        }

        let current_index = *next_compare_index;
        visit(current_index, self)?;

        if matches!(
            self,
            Self::Binary {
                op: BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Lte
                    | BinaryOp::Gt
                    | BinaryOp::Gte,
                ..
            }
        ) {
            *next_compare_index = next_compare_index.saturating_add(1);
        }

        Ok(())
    }

    /// Return true when every field leaf referenced by this expression is
    /// present in the supplied allowlist.
    #[must_use]
    pub(in crate::db) fn references_only_fields(&self, allowed: &[&str]) -> bool {
        self.all_tree_expr(&mut |expr| match expr {
            Self::Field(field) => allowed.iter().any(|allowed| *allowed == field.as_str()),
            Self::Aggregate(_) | Self::Literal(_) => true,
            Self::FunctionCall { .. }
            | Self::Unary { .. }
            | Self::Binary { .. }
            | Self::Case { .. } => true,
            #[cfg(test)]
            Self::Alias { .. } => true,
        })
    }
}
