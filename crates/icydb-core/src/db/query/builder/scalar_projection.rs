//! Module: query::builder::scalar_projection
//! Responsibility: shared outward scalar-projection contracts and stable plan
//! label rendering used by bounded projection helpers.
//! Does not own: query planning, generic expression validation, or projection
//! execution policy.
//! Boundary: fluent helper projections share this contract so adapter surfaces
//! can consume one stable projection-helper API.

#[cfg(test)]
mod tests;

use crate::{
    db::{QueryError, query::plan::expr::Expr},
    value::{
        Value,
        decimal::{ValueFormatWriter, write_signed_literal, write_unsigned_literal},
        format::write_value_debug,
    },
};
use std::fmt;

pub(super) mod private {
    pub trait Sealed {}
}

///
/// ScalarProjectionPlan
///
/// Public opaque projection-plan token carried by bounded fluent projection
/// helpers.
/// The expression stays private to the query/executor boundary, while the token
/// lets fluent terminals move projection work below the public terminal layer.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalarProjectionPlan {
    expr: Expr,
}

impl ScalarProjectionPlan {
    pub(in crate::db) const fn new(expr: Expr) -> Self {
        Self { expr }
    }
}

///
/// ValueProjectionExpr
///
/// Shared bounded scalar projection helper contract used by fluent
/// value-projection terminals.
/// Implementors are sealed to the maintained numeric, rounded, and text helper
/// types and do not imply a generic expression-builder surface.
///

pub trait ValueProjectionExpr: private::Sealed {
    /// Borrow the single source field used by this bounded helper.
    fn field(&self) -> &str;

    /// Borrow the canonical planner expression carried by this helper.
    fn projection_plan(&self) -> ScalarProjectionPlan;

    /// Render the stable canonical output label for this projection.
    fn projection_label(&self) -> String;

    /// Apply this projection to one already-loaded source value.
    fn apply_value(&self, value: Value) -> Result<Value, QueryError>;
}

/// Render one canonical bounded scalar projection expression back into a
/// stable plan label.
#[must_use]
pub(in crate::db) fn render_scalar_projection_expr_plan_label(expr: &Expr) -> String {
    let mut rendered = String::new();
    // Every formatter below propagates only sink failures. String's fmt::Write
    // implementation is infallible; fallible sinks use the writer directly.
    write_scalar_projection_expr_plan_label(expr, &mut rendered)
        .expect("writing a planner label into String cannot fail");

    rendered
}

/// Write the maintained planner-label grammar incrementally. Callers may reject
/// a write before retaining its bytes; rendering stops at that first failure.
pub(in crate::db) fn write_scalar_projection_expr_plan_label(
    expr: &Expr,
    output: &mut (impl ValueFormatWriter + ?Sized),
) -> fmt::Result {
    write_scalar_projection_expr_plan_label_with_parent(expr, None, false, output)
}

fn write_scalar_projection_expr_plan_label_with_parent(
    expr: &Expr,
    parent_op: Option<crate::db::query::plan::expr::BinaryOp>,
    is_right_child: bool,
    output: &mut (impl ValueFormatWriter + ?Sized),
) -> fmt::Result {
    match expr {
        Expr::Field(field) => output.write_str(field.as_str()),
        Expr::FieldPath(path) => {
            output.write_str(path.root().as_str())?;
            for segment in path.segments() {
                output.write_char('.')?;
                output.write_str(segment)?;
            }
            Ok(())
        }
        Expr::Literal(value) => write_scalar_projection_literal(value, output),
        Expr::FunctionCall { function, args } => {
            output.write_str(function.canonical_label())?;
            output.write_char('(')?;
            for (index, arg) in args.iter().enumerate() {
                if index != 0 {
                    output.write_str(", ")?;
                }
                write_scalar_projection_expr_plan_label(arg, output)?;
            }
            output.write_char(')')
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            output.write_str("CASE")?;
            for arm in when_then_arms {
                output.write_str(" WHEN ")?;
                write_scalar_projection_expr_plan_label(arm.condition(), output)?;
                output.write_str(" THEN ")?;
                write_scalar_projection_expr_plan_label(arm.result(), output)?;
            }
            output.write_str(" ELSE ")?;
            write_scalar_projection_expr_plan_label(else_expr, output)?;
            output.write_str(" END")
        }
        Expr::Binary { op, left, right } => {
            let parenthesized = binary_expr_requires_parentheses(*op, parent_op, is_right_child);
            if parenthesized {
                output.write_char('(')?;
            }
            write_scalar_projection_expr_plan_label_with_parent(left, Some(*op), false, output)?;
            write!(output, " {} ", binary_op_symbol(*op))?;
            write_scalar_projection_expr_plan_label_with_parent(right, Some(*op), true, output)?;
            if parenthesized {
                output.write_char(')')?;
            }
            Ok(())
        }
        Expr::Aggregate(aggregate) => {
            // Preserve full aggregate identity, including FILTER semantics, so
            // alias-normalized grouped HAVING/ORDER BY terms round-trip back
            // onto the same planner aggregate expression shape.
            output.write_str(aggregate.kind().canonical_label())?;
            output.write_char('(')?;
            if aggregate.is_distinct() {
                output.write_str("DISTINCT ")?;
            }
            if let Some(input_expr) = aggregate.input_expr() {
                write_scalar_projection_expr_plan_label(input_expr, output)?;
            } else {
                output.write_char('*')?;
            }
            output.write_char(')')?;
            if let Some(filter_expr) = aggregate.filter_expr() {
                output.write_str(" FILTER (WHERE ")?;
                write_scalar_projection_expr_plan_label(filter_expr, output)?;
                output.write_char(')')?;
            }
            Ok(())
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => write_scalar_projection_expr_plan_label_with_parent(
            expr.as_ref(),
            parent_op,
            is_right_child,
            output,
        ),
        Expr::Unary { op, expr } => {
            match op {
                crate::db::query::plan::expr::UnaryOp::Not => output.write_str("NOT ")?,
            }
            write_scalar_projection_expr_plan_label(expr, output)
        }
    }
}

const fn binary_expr_requires_parentheses(
    op: crate::db::query::plan::expr::BinaryOp,
    parent_op: Option<crate::db::query::plan::expr::BinaryOp>,
    is_right_child: bool,
) -> bool {
    let Some(parent_op) = parent_op else {
        return false;
    };
    let precedence = binary_op_precedence(op);
    let parent_precedence = binary_op_precedence(parent_op);

    precedence < parent_precedence || (is_right_child && precedence == parent_precedence)
}

const fn binary_op_precedence(op: crate::db::query::plan::expr::BinaryOp) -> u8 {
    match op {
        crate::db::query::plan::expr::BinaryOp::Or => 0,
        crate::db::query::plan::expr::BinaryOp::And => 1,
        crate::db::query::plan::expr::BinaryOp::Eq
        | crate::db::query::plan::expr::BinaryOp::Ne
        | crate::db::query::plan::expr::BinaryOp::Lt
        | crate::db::query::plan::expr::BinaryOp::Lte
        | crate::db::query::plan::expr::BinaryOp::Gt
        | crate::db::query::plan::expr::BinaryOp::Gte => 2,
        crate::db::query::plan::expr::BinaryOp::Add
        | crate::db::query::plan::expr::BinaryOp::Sub => 3,
        crate::db::query::plan::expr::BinaryOp::Mul
        | crate::db::query::plan::expr::BinaryOp::Div => 4,
    }
}

const fn binary_op_symbol(op: crate::db::query::plan::expr::BinaryOp) -> &'static str {
    match op {
        crate::db::query::plan::expr::BinaryOp::Or => "OR",
        crate::db::query::plan::expr::BinaryOp::And => "AND",
        crate::db::query::plan::expr::BinaryOp::Eq => "=",
        crate::db::query::plan::expr::BinaryOp::Ne => "!=",
        crate::db::query::plan::expr::BinaryOp::Lt => "<",
        crate::db::query::plan::expr::BinaryOp::Lte => "<=",
        crate::db::query::plan::expr::BinaryOp::Gt => ">",
        crate::db::query::plan::expr::BinaryOp::Gte => ">=",
        crate::db::query::plan::expr::BinaryOp::Add => "+",
        crate::db::query::plan::expr::BinaryOp::Sub => "-",
        crate::db::query::plan::expr::BinaryOp::Mul => "*",
        crate::db::query::plan::expr::BinaryOp::Div => "/",
    }
}

fn write_scalar_projection_literal(
    value: &Value,
    output: &mut (impl ValueFormatWriter + ?Sized),
) -> fmt::Result {
    match value {
        Value::Null => output.write_str("NULL"),
        Value::Text(text) => {
            output.write_char('\'')?;
            for (index, part) in text.split('\'').enumerate() {
                if index != 0 {
                    output.write_str("''")?;
                }
                output.write_str(part)?;
            }
            output.write_char('\'')
        }
        Value::Int64(value) => write!(output, "{value}"),
        Value::Int128(value) => write!(output, "{value}"),
        Value::IntBig(value) => write_signed_literal(value, output),
        Value::Nat64(value) => write!(output, "{value}"),
        Value::Nat128(value) => write!(output, "{value}"),
        Value::NatBig(value) => write_unsigned_literal(value, output),
        Value::U256(value) => write!(output, "{value}"),
        Value::Decimal(value) => write!(output, "{value}"),
        Value::Float32(value) => write!(output, "{value}"),
        Value::Float64(value) => write!(output, "{value}"),
        Value::Bool(true) => output.write_str("TRUE"),
        Value::Bool(false) => output.write_str("FALSE"),
        other => write_value_debug(other, output),
    }
}
