//! Module: query::builder::scalar_projection
//! Responsibility: shared outward scalar-projection contracts and stable SQL
//! label rendering used by bounded projection helpers.
//! Does not own: query planning, generic expression validation, or projection
//! execution policy.
//! Boundary: fluent helper projections share this contract so session and SQL
//! surfaces can consume one stable projection-helper API.

use crate::{
    db::{QueryError, query::plan::expr::Expr},
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

    /// Render the stable SQL-style output label for this projection.
    fn sql_label(&self) -> String;

    /// Apply this projection to one already-loaded source value.
    fn apply_value(&self, value: Value) -> Result<Value, QueryError>;
}

/// Render one canonical bounded scalar projection expression back into a
/// stable SQL-style label.
#[must_use]
pub(in crate::db) fn render_scalar_projection_expr_sql_label(expr: &Expr) -> String {
    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::Literal(value) => render_scalar_projection_literal(value),
        Expr::FunctionCall { function, args } => {
            let rendered_args = args
                .iter()
                .map(render_scalar_projection_expr_sql_label)
                .collect::<Vec<_>>()
                .join(", ");

            format!("{}({rendered_args})", function.sql_label())
        }
        Expr::Binary { op, left, right } => {
            let left = render_scalar_projection_expr_sql_label(left.as_ref());
            let right = render_scalar_projection_expr_sql_label(right.as_ref());

            format!("{left} {} {right}", binary_op_sql_label(*op))
        }
        Expr::Aggregate(aggregate) => {
            let kind = aggregate.kind().sql_label();
            let distinct = if aggregate.is_distinct() {
                "DISTINCT "
            } else {
                ""
            };

            if let Some(field) = aggregate.target_field() {
                return format!("{kind}({distinct}{field})");
            }

            format!("{kind}({distinct}*)")
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => render_scalar_projection_expr_sql_label(expr.as_ref()),
        #[cfg(test)]
        Expr::Unary { .. } => "expr".to_string(),
    }
}

const fn binary_op_sql_label(op: crate::db::query::plan::expr::BinaryOp) -> &'static str {
    match op {
        crate::db::query::plan::expr::BinaryOp::Add => "+",
        #[cfg(test)]
        crate::db::query::plan::expr::BinaryOp::Mul => "*",
        #[cfg(test)]
        crate::db::query::plan::expr::BinaryOp::And => "AND",
        #[cfg(test)]
        crate::db::query::plan::expr::BinaryOp::Eq => "=",
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
