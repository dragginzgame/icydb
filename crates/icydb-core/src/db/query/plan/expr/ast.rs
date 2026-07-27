//! Module: query::plan::expr::ast
//! Responsibility: planner expression AST domain types and field/operator identifiers.
//! Does not own: expression type inference policy or runtime expression evaluation.
//! Boundary: defines canonical expression tree structures consumed by planner validation/lowering.

use crate::{db::query::builder::aggregate::AggregateExpr, value::Value};
#[cfg(feature = "sql")]
use std::collections::BTreeSet;

///
/// FieldId
///
/// Canonical planner-owned field identity token for expression trees.
/// This wrapper carries the declared field name and avoids ad-hoc string use.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct FieldId(String);

impl FieldId {
    /// Build one field-id token from a field name.
    #[must_use]
    pub(in crate::db) fn new(field: impl Into<String>) -> Self {
        Self(field.into())
    }

    /// Borrow the canonical field name.
    #[must_use]
    pub(in crate::db) const fn as_str(&self) -> &str {
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
/// PathSpec
///
/// Planner-owned nested path descriptor rooted at one top-level model field.
/// This exists as the shared capability hook for future path optimization:
/// current execution only needs root plus segments, while later planner/index
/// work can hang scalar-leaf or indexability metadata from this boundary.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct PathSpec {
    root: FieldId,
    path: Vec<String>,
}

impl PathSpec {
    /// Build one nested field path from a root field and non-empty path tail.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn new(root: impl Into<FieldId>, segments: Vec<String>) -> Self {
        debug_assert!(
            !segments.is_empty(),
            "field paths must contain at least one nested segment"
        );

        Self {
            root: root.into(),
            path: segments,
        }
    }

    /// Borrow the top-level model field that owns this nested path.
    #[must_use]
    pub(in crate::db) const fn root(&self) -> &FieldId {
        &self.root
    }

    /// Borrow the nested path segments below the root field.
    #[must_use]
    pub(in crate::db) const fn segments(&self) -> &[String] {
        self.path.as_slice()
    }

    /// Return whether this path is expected to resolve to a scalar leaf.
    #[must_use]
    pub(in crate::db) const fn is_scalar_leaf(&self) -> bool {
        !self.path.is_empty()
    }
}

///
/// FieldPath
///
/// Planner-owned nested field path expression rooted at one top-level model
/// field.
/// The expression wrapper preserves AST identity while delegating path
/// capability details to `PathSpec`.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct FieldPath {
    path: PathSpec,
}

impl FieldPath {
    /// Build one nested field path from a root field and non-empty path tail.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn new(root: impl Into<FieldId>, segments: Vec<String>) -> Self {
        Self {
            path: PathSpec::new(root, segments),
        }
    }

    /// Borrow the path capability descriptor.
    #[must_use]
    pub(in crate::db) const fn path_spec(&self) -> &PathSpec {
        &self.path
    }

    /// Borrow the top-level model field that owns this nested path.
    #[must_use]
    pub(in crate::db) const fn root(&self) -> &FieldId {
        self.path.root()
    }

    /// Borrow the nested path segments below the root field.
    #[must_use]
    pub(in crate::db) const fn segments(&self) -> &[String] {
        self.path.segments()
    }
}

///
/// Alias
///
/// Canonical planner-owned alias token attached to expression projections.
/// Alias remains presentation metadata and does not affect semantic identity.
///

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct Alias(String);

impl Alias {
    /// Build one alias token from owned/borrowed text.
    #[must_use]
    pub(in crate::db) fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Borrow the alias as text.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn as_str(&self) -> &str {
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
pub(in crate::db) enum UnaryOp {
    Not,
}

///
/// BinaryOp
///
/// Canonical binary expression operator taxonomy.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum BinaryOp {
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
    /// Report whether this operator belongs to the numeric arithmetic family.
    #[must_use]
    pub(in crate::db) const fn is_numeric_arithmetic(self) -> bool {
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
#[cfg_attr(
    all(not(test), not(feature = "sql")),
    expect(
        dead_code,
        reason = "SQL parsing constructs the full scalar-function taxonomy; no-default fluent queries use only the shared subset"
    )
)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[remain::sorted]
pub(in crate::db) enum Function {
    Abs,
    Cbrt,
    Ceiling,
    Coalesce,
    CollectionContains,
    Contains,
    EndsWith,
    Exp,
    Floor,
    InList,
    IsEmpty,
    IsMissing,
    IsNotEmpty,
    IsNotNull,
    IsNull,
    Left,
    Length,
    Ln,
    Log,
    Log2,
    Log10,
    Lower,
    Ltrim,
    Mod,
    NullIf,
    OctetLength,
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
    pub(in crate::db) const fn canonical_label(self) -> &'static str {
        match self {
            Self::Abs => "ABS",
            Self::Cbrt => "CBRT",
            Self::Ceiling => "CEILING",
            Self::Coalesce => "COALESCE",
            Self::CollectionContains => "COLLECTION_CONTAINS",
            Self::Contains => "CONTAINS",
            Self::EndsWith => "ENDS_WITH",
            Self::Exp => "EXP",
            Self::Floor => "FLOOR",
            Self::InList => "IN_LIST",
            Self::IsEmpty => "IS_EMPTY",
            Self::IsMissing => "IS_MISSING",
            Self::IsNotEmpty => "IS_NOT_EMPTY",
            Self::IsNotNull => "IS_NOT_NULL",
            Self::IsNull => "IS_NULL",
            Self::Left => "LEFT",
            Self::Length => "LENGTH",
            Self::Ln => "LN",
            Self::Log => "LOG",
            Self::Log10 => "LOG10",
            Self::Log2 => "LOG2",
            Self::Lower => "LOWER",
            Self::Ltrim => "LTRIM",
            Self::Mod => "MOD",
            Self::NullIf => "NULLIF",
            Self::OctetLength => "OCTET_LENGTH",
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
pub(in crate::db) struct CaseWhenArm {
    condition: Expr,
    result: Expr,
}

impl CaseWhenArm {
    /// Build one planner-owned searched-CASE arm.
    #[must_use]
    pub(in crate::db) const fn new(condition: Expr, result: Expr) -> Self {
        Self { condition, result }
    }

    /// Borrow the boolean branch condition.
    #[must_use]
    pub(in crate::db) const fn condition(&self) -> &Expr {
        &self.condition
    }

    /// Borrow the scalar branch result expression.
    #[must_use]
    pub(in crate::db) const fn result(&self) -> &Expr {
        &self.result
    }
}

///
/// Expr
///
/// Canonical planner-owned expression tree for projection semantics.
/// This model is semantic-only and intentionally excludes execution logic.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum Expr {
    Field(FieldId),
    #[cfg_attr(
        all(not(test), not(feature = "sql")),
        expect(
            dead_code,
            reason = "nested field-path expressions are constructed by SQL lowering and tests"
        )
    )]
    FieldPath(FieldPath),
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

/// Collect accepted top-level field roots referenced by one scalar expression.
///
/// Returns `false` for expression variants that cannot belong to a resumable
/// scalar scope. Callers must reject that result rather than treating an
/// unknown dependency shape as an empty dependency set.
#[cfg(feature = "sql")]
pub(in crate::db) fn collect_scalar_expr_field_roots(
    expr: &Expr,
    roots: &mut BTreeSet<String>,
) -> bool {
    match expr {
        Expr::Field(field) => {
            roots.insert(field.as_str().to_string());
            true
        }
        Expr::FieldPath(path) => {
            roots.insert(path.root().as_str().to_string());
            true
        }
        Expr::Literal(_) => true,
        Expr::FunctionCall { args, .. } => args
            .iter()
            .all(|argument| collect_scalar_expr_field_roots(argument, roots)),
        Expr::Unary { expr, .. } => collect_scalar_expr_field_roots(expr, roots),
        Expr::Binary { left, right, .. } => {
            collect_scalar_expr_field_roots(left, roots)
                && collect_scalar_expr_field_roots(right, roots)
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                collect_scalar_expr_field_roots(arm.condition(), roots)
                    && collect_scalar_expr_field_roots(arm.result(), roots)
            }) && collect_scalar_expr_field_roots(else_expr, roots)
        }
        Expr::Aggregate(_) => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

impl Expr {
    /// Return true when this planner expression tree still contains any raw
    /// searched `CASE` node after owner-local canonicalization.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn contains_case(&self) -> bool {
        self.any_tree_expr(&mut |expr| matches!(expr, Self::Case { .. }))
    }

    /// Return true when any visited planner expression node satisfies the
    /// supplied predicate.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn any_tree_expr(&self, predicate: &mut impl FnMut(&Self) -> bool) -> bool {
        if predicate(self) {
            return true;
        }

        match self {
            Self::Field(_) | Self::FieldPath(_) | Self::Literal(_) | Self::Aggregate(_) => false,
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
            Self::Field(_) | Self::FieldPath(_) | Self::Literal(_) | Self::Aggregate(_) => true,
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
            Self::Field(_) | Self::FieldPath(_) | Self::Literal(_) | Self::Aggregate(_) => Ok(()),
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

    /// Visit every planner expression node in this tree through the owner-local
    /// child traversal contract.
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn for_each_tree_expr(&self, visit: &mut impl FnMut(&Self)) {
        match self {
            Self::Field(_) | Self::FieldPath(_) | Self::Literal(_) | Self::Aggregate(_) => {
                visit(self);
            }
            Self::FunctionCall { args, .. } => {
                visit(self);
                for arg in args {
                    arg.for_each_tree_expr(visit);
                }
            }
            Self::Unary { expr, .. } => {
                visit(self);
                expr.for_each_tree_expr(visit);
            }
            Self::Binary { left, right, .. } => {
                visit(self);
                left.for_each_tree_expr(visit);
                right.for_each_tree_expr(visit);
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                visit(self);
                for arm in when_then_arms {
                    arm.condition().for_each_tree_expr(visit);
                    arm.result().for_each_tree_expr(visit);
                }
                else_expr.for_each_tree_expr(visit);
            }
            #[cfg(test)]
            Self::Alias { expr, .. } => {
                visit(self);
                expr.for_each_tree_expr(visit);
            }
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
            Self::Field(_) | Self::FieldPath(_) | Self::Literal(_) | Self::Aggregate(_) => {}
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
            Self::FieldPath(_) => false,
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
