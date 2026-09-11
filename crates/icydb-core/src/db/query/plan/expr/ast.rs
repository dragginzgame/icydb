//! Module: query::plan::expr::ast
//! Responsibility: planner expression AST domain types and field/operator identifiers.
//! Does not own: expression type inference policy or runtime expression evaluation.
//! Boundary: defines canonical expression tree structures consumed by planner validation/lowering.

use crate::{
    db::{numeric::NumericArithmeticOp, query::builder::aggregate::AggregateExpr},
    value::Value,
};
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

    /// Render the canonical dotted identity for this field path.
    #[must_use]
    pub(in crate::db) fn dotted_label(&self) -> String {
        let mut label = self.root.as_str().to_string();
        for segment in &self.path {
            label.push('.');
            label.push_str(segment);
        }

        label
    }

    /// Compare the rendered label without allocating it. This compares text,
    /// not structural path identity: dots inside components remain literal.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) fn matches_dotted_label(&self, label: &str) -> bool {
        let Some(mut remaining) = label.strip_prefix(self.root.as_str()) else {
            return false;
        };
        for segment in &self.path {
            let Some(tail) = remaining
                .strip_prefix('.')
                .and_then(|tail| tail.strip_prefix(segment.as_str()))
            else {
                return false;
            };
            remaining = tail;
        }
        remaining.is_empty()
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
    /// Return the shared numeric operation for this arithmetic operator.
    #[must_use]
    pub(in crate::db) const fn numeric_arithmetic_op(self) -> Option<NumericArithmeticOp> {
        match self {
            Self::Add => Some(NumericArithmeticOp::Add),
            Self::Sub => Some(NumericArithmeticOp::Sub),
            Self::Mul => Some(NumericArithmeticOp::Mul),
            Self::Div => Some(NumericArithmeticOp::Div),
            Self::Or
            | Self::And
            | Self::Eq
            | Self::Ne
            | Self::Lt
            | Self::Lte
            | Self::Gt
            | Self::Gte => None,
        }
    }

    /// Report whether this operator belongs to the numeric arithmetic family.
    #[must_use]
    pub(in crate::db) const fn is_numeric_arithmetic(self) -> bool {
        self.numeric_arithmetic_op().is_some()
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
    /// Borrow both children for in-place normalization and owned cleanup.
    pub(in crate::db) const fn children_mut(&mut self) -> [&mut Expr; 2] {
        [&mut self.condition, &mut self.result]
    }

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
    /// Rewrite immediate scalar children in place; aggregate scope stays opaque.
    pub(in crate::db) fn map_scalar_children(&mut self, mut map: impl FnMut(Self) -> Self) {
        match self {
            Self::Unary { expr, .. } => **expr = map(expr.take()),
            Self::Binary { left, right, .. } => {
                **left = map(left.take());
                **right = map(right.take());
            }
            Self::FunctionCall { args, .. } => {
                for arg in args {
                    *arg = map(arg.take());
                }
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    for child in arm.children_mut() {
                        *child = map(child.take());
                    }
                }
                **else_expr = map(else_expr.take());
            }
            Self::Field(_) | Self::FieldPath(_) | Self::Literal(_) | Self::Aggregate(_) => {}
            #[cfg(test)]
            Self::Alias { expr, .. } => **expr = map(expr.take()),
        }
    }

    /// Transfer this expression, leaving a shallow inert leaf for its owner.
    pub(in crate::db) const fn take(&mut self) -> Self {
        std::mem::replace(self, Self::Literal(Value::Null))
    }

    /// Build compact membership from already-lowered operands without expanding
    /// comparisons. Frontends retain ownership of admission and empty-list policy.
    #[must_use]
    pub(in crate::db) fn membership(target: Self, values: Vec<Value>, negated: bool) -> Self {
        let membership = Self::FunctionCall {
            function: Function::InList,
            args: vec![target, Self::Literal(Value::List(values))],
        };

        if negated {
            Self::Unary {
                op: UnaryOp::Not,
                expr: Box::new(membership),
            }
        } else {
            membership
        }
    }

    /// Return true when this planner expression contains a nested field path.
    #[must_use]
    pub(in crate::db) fn contains_field_path(&self) -> bool {
        match self {
            Self::FieldPath(_) => true,
            Self::Field(_) | Self::Literal(_) | Self::Aggregate(_) => false,
            Self::FunctionCall { args, .. } => args.iter().any(Self::contains_field_path),
            Self::Unary { expr, .. } => expr.contains_field_path(),
            Self::Binary { left, right, .. } => {
                left.contains_field_path() || right.contains_field_path()
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().any(|arm| {
                    arm.condition().contains_field_path() || arm.result().contains_field_path()
                }) || else_expr.contains_field_path()
            }
            #[cfg(test)]
            Self::Alias { expr, .. } => expr.contains_field_path(),
        }
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
        !self.all_tree_expr(&mut |node| !predicate(node))
    }

    /// Return true when every visited planner expression node satisfies the
    /// supplied predicate.
    #[must_use]
    pub(in crate::db) fn all_tree_expr(&self, predicate: &mut impl FnMut(&Self) -> bool) -> bool {
        match self.try_all_tree_expr(&mut |node| Ok::<_, std::convert::Infallible>(predicate(node)))
        {
            Ok(result) => result,
            Err(never) => match never {},
        }
    }

    /// Visit borrowed expression nodes in preorder, stopping before later
    /// children on either false or error. Aggregate inputs remain separate.
    pub(in crate::db) fn try_all_tree_expr<E>(
        &self,
        predicate: &mut impl FnMut(&Self) -> Result<bool, E>,
    ) -> Result<bool, E> {
        if !predicate(self)? {
            return Ok(false);
        }

        match self {
            Self::Field(_) | Self::FieldPath(_) | Self::Literal(_) | Self::Aggregate(_) => Ok(true),
            Self::FunctionCall { args, .. } => {
                for arg in args {
                    if !arg.try_all_tree_expr(predicate)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Self::Unary { expr, .. } => expr.try_all_tree_expr(predicate),
            Self::Binary { left, right, .. } => {
                Ok(left.try_all_tree_expr(predicate)? && right.try_all_tree_expr(predicate)?)
            }
            Self::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    if !arm.condition().try_all_tree_expr(predicate)?
                        || !arm.result().try_all_tree_expr(predicate)?
                    {
                        return Ok(false);
                    }
                }
                else_expr.try_all_tree_expr(predicate)
            }
            #[cfg(test)]
            Self::Alias { expr, .. } => expr.try_all_tree_expr(predicate),
        }
    }

    /// Visit every planner expression node in this tree through the owner-local
    /// child traversal contract, stopping early on the first error.
    pub(in crate::db) fn try_for_each_tree_expr<E>(
        &self,
        visit: &mut impl FnMut(&Self) -> Result<(), E>,
    ) -> Result<(), E> {
        self.try_all_tree_expr(&mut |node| {
            visit(node)?;
            Ok(true)
        })
        .map(|_| ())
    }

    /// Visit every planner expression node in this tree through the owner-local
    /// child traversal contract.
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
}

#[cfg(test)]
mod tests {
    use super::PathSpec;

    #[test]
    fn path_spec_renders_one_canonical_dotted_label() {
        let path = PathSpec::new(
            "profile",
            vec!["location".to_string(), "country".to_string()],
        );

        assert_eq!(path.dotted_label(), "profile.location.country");
    }

    #[cfg(feature = "sql")]
    #[test]
    fn path_label_comparison_matches_rendering_without_normalizing_components() {
        for (root, segments) in [
            ("profile", vec!["location", "country"]),
            ("profile.location", vec!["country"]),
            ("profile", vec!["location.country"]),
            ("", vec!["", "name"]),
            ("profile", vec![""]),
            ("é", vec!["名", "🌍"]),
        ] {
            let path = PathSpec::new(root, segments.into_iter().map(str::to_string).collect());
            let rendered = path.dotted_label();
            for label in [
                rendered.clone(),
                format!("{rendered}."),
                format!("x{rendered}"),
                rendered.trim_end_matches('.').to_string(),
                "profile.location.country".to_string(),
                String::new(),
            ] {
                assert_eq!(path.matches_dotted_label(&label), rendered == label);
            }
            // Check every valid UTF-8 truncation, including component interiors.
            for (end, _) in rendered.char_indices() {
                assert!(!path.matches_dotted_label(&rendered[..end]));
            }
        }
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(Alias {
Self(field_0) => [field_0],
});
crate::retained::retained_copy!(BinaryOp);
crate::retained::retained_fields!(CaseWhenArm {
Self{condition,result} => [condition,result],
});
crate::retained::retained_fields!(Expr {
Self::Field(field_0) => [field_0],
Self::FieldPath(field_0) => [field_0],
Self::Literal(field_0) => [field_0],
Self::FunctionCall{function,args} => [function,args],
Self::Unary{op,expr} => [op,expr],
Self::Binary{op,left,right} => [op,left,right],
Self::Case{when_then_arms,else_expr} => [when_then_arms,else_expr],
Self::Aggregate(field_0) => [field_0],
# [cfg (test)]
Self::Alias{expr,name} => [expr,name],
});
crate::retained::retained_fields!(FieldId {
Self(field_0) => [field_0],
});
crate::retained::retained_fields!(FieldPath {
Self{path} => [path],
});
crate::retained::retained_copy!(Function);
crate::retained::retained_fields!(PathSpec {
Self{root,path} => [root,path],
});
crate::retained::retained_copy!(UnaryOp);
