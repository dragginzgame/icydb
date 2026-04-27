//! Module: query::plan::expr::ast
//! Responsibility: planner expression AST domain types and field/operator identifiers.
//! Does not own: expression type inference policy or runtime expression evaluation.
//! Boundary: defines canonical expression tree structures consumed by planner validation/lowering.

use crate::{db::query::builder::aggregate::AggregateExpr, value::Value};

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

impl BinaryOp {
    /// Return the stable planner-owned lowercase label for this binary operator.
    #[must_use]
    pub(crate) const fn canonical_label(self) -> &'static str {
        match self {
            Self::Or => "or",
            Self::And => "and",
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::Lt => "lt",
            Self::Lte => "lte",
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::Add => "add",
            Self::Sub => "sub",
            Self::Mul => "mul",
            Self::Div => "div",
        }
    }

    /// Report whether this operator belongs to the numeric arithmetic family.
    #[must_use]
    pub(crate) const fn is_numeric_arithmetic(self) -> bool {
        matches!(self, Self::Add | Self::Sub | Self::Mul | Self::Div)
    }
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
    Mod,
    NullIf,
    Position,
    Power,
    Replace,
    Right,
    Round,
    Rtrim,
    Sign,
    Sqrt,
    StartsWith,
    Substring,
    Trim,
    Trunc,
    Upper,
}

impl Function {
    /// Return the stable uppercase canonical label for this bounded function.
    #[must_use]
    pub(crate) const fn canonical_label(self) -> &'static str {
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
            Self::Mod => "MOD",
            Self::NullIf => "NULLIF",
            Self::Position => "POSITION",
            Self::Power => "POWER",
            Self::Replace => "REPLACE",
            Self::Round => "ROUND",
            Self::Right => "RIGHT",
            Self::Rtrim => "RTRIM",
            Self::Sign => "SIGN",
            Self::StartsWith => "STARTS_WITH",
            Self::Substring => "SUBSTRING",
            Self::Sqrt => "SQRT",
            Self::Trim => "TRIM",
            Self::Trunc => "TRUNC",
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

/// Return whether one admitted `ORDER BY` expression is only a plain field.
#[must_use]
#[cfg(test)]
pub(in crate::db) const fn supported_order_expr_is_plain_field(expr: &Expr) -> bool {
    matches!(expr, Expr::Field(_))
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
                | Function::Sign
                | Function::Sqrt
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
                function.canonical_label(),
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

            Some(format!(
                "{}({})",
                function.canonical_label(),
                rendered.join(", ")
            ))
        }
        SupportedOrderFunctionShape::BinaryExpr => match args {
            [left, right] => Some(format!(
                "{}({}, {})",
                function.canonical_label(),
                render_supported_order_expr_with_parent(left, None)?,
                render_supported_order_expr_with_parent(right, None)?
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::FieldLiteral => match args {
            [Expr::Field(field), Expr::Literal(literal)] => Some(format!(
                "{}({}, {})",
                function.canonical_label(),
                field.as_str(),
                render_supported_order_literal(literal)?
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::LiteralField => match args {
            [Expr::Literal(literal), Expr::Field(field)] => Some(format!(
                "{}({}, {})",
                function.canonical_label(),
                render_supported_order_literal(literal)?,
                field.as_str(),
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::FieldTwoLiterals => match args {
            [Expr::Field(field), Expr::Literal(from), Expr::Literal(to)] => Some(format!(
                "{}({}, {}, {})",
                function.canonical_label(),
                field.as_str(),
                render_supported_order_literal(from)?,
                render_supported_order_literal(to)?,
            )),
            _ => None,
        },
        SupportedOrderFunctionShape::FieldOneOrTwoLiterals => match args {
            [Expr::Field(field), Expr::Literal(start)] => Some(format!(
                "{}({}, {})",
                function.canonical_label(),
                field.as_str(),
                render_supported_order_literal(start)?,
            )),
            [
                Expr::Field(field),
                Expr::Literal(start),
                Expr::Literal(length),
            ] => Some(format!(
                "{}({}, {}, {})",
                function.canonical_label(),
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
            let rendered = format!("{left} {} {right}", binary_op_symbol(*op));

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
const fn binary_op_symbol(op: BinaryOp) -> &'static str {
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
#[cfg(test)]
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

// Keep the reduced `ORDER BY` function family clause-owned by describing the
// admitted argument shape locally instead of re-encoding it in each parser.
#[cfg(test)]
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
        | Function::Sign
        | Function::Sqrt
        | Function::Lower
        | Function::Upper
        | Function::Length => Some(SupportedOrderFunctionShape::UnaryExpr),
        Function::Coalesce => Some(SupportedOrderFunctionShape::VariadicExprMin2),
        Function::NullIf | Function::Mod | Function::Power => {
            Some(SupportedOrderFunctionShape::BinaryExpr)
        }
        Function::Left
        | Function::Right
        | Function::StartsWith
        | Function::EndsWith
        | Function::Contains => Some(SupportedOrderFunctionShape::FieldLiteral),
        Function::Position => Some(SupportedOrderFunctionShape::LiteralField),
        Function::Replace => Some(SupportedOrderFunctionShape::FieldTwoLiterals),
        Function::Substring => Some(SupportedOrderFunctionShape::FieldOneOrTwoLiterals),
        Function::Round | Function::Trunc => Some(SupportedOrderFunctionShape::Round),
        Function::CollectionContains => None,
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
