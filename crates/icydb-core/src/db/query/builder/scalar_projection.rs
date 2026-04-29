//! Module: query::builder::scalar_projection
//! Responsibility: shared outward scalar-projection contracts and stable plan
//! label rendering used by bounded projection helpers.
//! Does not own: query planning, generic expression validation, or projection
//! execution policy.
//! Boundary: fluent helper projections share this contract so adapter surfaces
//! can consume one stable projection-helper API.

use crate::{
    db::{
        QueryError,
        query::plan::expr::{Expr, FieldPath},
    },
    value::Value,
};

///
/// ValueProjectionExpr
///
/// Shared bounded scalar projection helper contract used by fluent
/// value-projection terminals.
/// Implementors stay intentionally narrow and do not imply a generic
/// expression-builder surface.
///

pub trait ValueProjectionExpr {
    /// Borrow the single source field used by this bounded helper.
    fn field(&self) -> &str;

    /// Render the stable canonical output label for this projection.
    fn projection_label(&self) -> String;

    /// Apply this projection to one already-loaded source value.
    fn apply_value(&self, value: Value) -> Result<Value, QueryError>;
}

/// Render one canonical bounded scalar projection expression back into a
/// stable plan label.
#[must_use]
pub(in crate::db) fn render_scalar_projection_expr_plan_label(expr: &Expr) -> String {
    render_scalar_projection_expr_plan_label_with_parent(expr, None, false)
}

fn render_scalar_projection_expr_plan_label_with_parent(
    expr: &Expr,
    parent_op: Option<crate::db::query::plan::expr::BinaryOp>,
    is_right_child: bool,
) -> String {
    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::FieldPath(path) => render_field_path_plan_label(path),
        Expr::Literal(value) => render_scalar_projection_literal(value),
        Expr::FunctionCall { function, args } => {
            let rendered_args = args
                .iter()
                .map(|arg| render_scalar_projection_expr_plan_label_with_parent(arg, None, false))
                .collect::<Vec<_>>()
                .join(", ");

            format!("{}({rendered_args})", function.canonical_label())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => render_case_projection_expr_plan_label(when_then_arms, else_expr.as_ref()),
        Expr::Binary { op, left, right } => {
            let left = render_scalar_projection_expr_plan_label_with_parent(
                left.as_ref(),
                Some(*op),
                false,
            );
            let right = render_scalar_projection_expr_plan_label_with_parent(
                right.as_ref(),
                Some(*op),
                true,
            );
            let rendered = format!("{left} {} {right}", binary_op_symbol(*op));

            if binary_expr_requires_parentheses(*op, parent_op, is_right_child) {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        Expr::Aggregate(aggregate) => {
            // Preserve full aggregate identity, including FILTER semantics, so
            // alias-normalized grouped HAVING/ORDER BY terms round-trip back
            // onto the same planner aggregate expression shape.
            let kind = aggregate.kind().canonical_label();
            let distinct = if aggregate.is_distinct() {
                "DISTINCT "
            } else {
                ""
            };
            let filter = aggregate.filter_expr().map(|filter_expr| {
                format!(
                    " FILTER (WHERE {})",
                    render_scalar_projection_expr_plan_label_with_parent(filter_expr, None, false,)
                )
            });

            if let Some(input_expr) = aggregate.input_expr() {
                let input =
                    render_scalar_projection_expr_plan_label_with_parent(input_expr, None, false);

                return format!("{kind}({distinct}{input}){}", filter.unwrap_or_default());
            }

            format!("{kind}({distinct}*){}", filter.unwrap_or_default())
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => render_scalar_projection_expr_plan_label_with_parent(
            expr.as_ref(),
            parent_op,
            is_right_child,
        ),
        Expr::Unary { op, expr } => {
            let rendered =
                render_scalar_projection_expr_plan_label_with_parent(expr.as_ref(), None, false);
            match op {
                crate::db::query::plan::expr::UnaryOp::Not => format!("NOT {rendered}"),
            }
        }
    }
}

fn render_field_path_plan_label(path: &FieldPath) -> String {
    let mut label = path.root().as_str().to_string();
    for segment in path.segments() {
        label.push('.');
        label.push_str(segment);
    }

    label
}

fn render_case_projection_expr_plan_label(
    when_then_arms: &[crate::db::query::plan::expr::CaseWhenArm],
    else_expr: &Expr,
) -> String {
    let mut rendered = String::from("CASE");

    for arm in when_then_arms {
        rendered.push_str(" WHEN ");
        rendered.push_str(
            render_scalar_projection_expr_plan_label_with_parent(arm.condition(), None, false)
                .as_str(),
        );
        rendered.push_str(" THEN ");
        rendered.push_str(
            render_scalar_projection_expr_plan_label_with_parent(arm.result(), None, false)
                .as_str(),
        );
    }

    rendered.push_str(" ELSE ");
    rendered.push_str(
        render_scalar_projection_expr_plan_label_with_parent(else_expr, None, false).as_str(),
    );
    rendered.push_str(" END");

    rendered
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

fn render_scalar_projection_literal(value: &Value) -> String {
    match value {
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
        other => format!("{other:?}"),
    }
}
