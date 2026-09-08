//! Stack-safe cleanup for partial, rejected and retained SQL expression trees.

use crate::{
    db::sql::parser::{SqlExpr, SqlMembershipValue},
    value::{Value, clear_value},
};

pub(super) fn clear_expr(expr: &mut SqlExpr) {
    let mut pending = Vec::new();
    detach_children(expr, &mut pending);
    while let Some(mut expr) = pending.pop() {
        detach_children(&mut expr, &mut pending);
    }
}

// Leave only shallow children on a processed parent. Its subsequent Drop sees
// no recursive descendants and allocates no new frontier. Leaf-only expressions
// likewise need no temporary allocation on the ordinary path.
fn detach_child(child: &mut SqlExpr, pending: &mut Vec<SqlExpr>) {
    match child {
        SqlExpr::Field(_)
        | SqlExpr::FieldPath { .. }
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. } => {}
        _ => pending.push(std::mem::replace(child, SqlExpr::Literal(Value::Null))),
    }
}

fn detach_value(value: &mut Value) {
    if matches!(value, Value::List(_) | Value::Map(_) | Value::Enum(_)) {
        clear_value(std::mem::replace(value, Value::Null));
    }
}

fn detach_children(expr: &mut SqlExpr, pending: &mut Vec<SqlExpr>) {
    match expr {
        SqlExpr::Field(_) | SqlExpr::FieldPath { .. } | SqlExpr::Param { .. } => {}
        SqlExpr::Literal(value) => detach_value(value),
        SqlExpr::Aggregate(aggregate) => {
            for child in [&mut aggregate.input, &mut aggregate.filter_expr]
                .into_iter()
                .flatten()
            {
                detach_child(child, pending);
            }
        }
        SqlExpr::Membership { expr, values, .. } => {
            detach_child(expr, pending);
            for value in values {
                if let SqlMembershipValue::Literal(value) = value {
                    detach_value(value);
                }
            }
        }
        SqlExpr::NullTest { expr, .. }
        | SqlExpr::Like { expr, .. }
        | SqlExpr::Unary { expr, .. } => detach_child(expr, pending),
        SqlExpr::Binary { left, right, .. } => {
            detach_child(left, pending);
            detach_child(right, pending);
        }
        SqlExpr::FunctionCall { args, .. } => {
            for arg in args {
                detach_child(arg, pending);
            }
        }
        SqlExpr::Case { arms, else_expr } => {
            for arm in arms {
                detach_child(&mut arm.condition, pending);
                detach_child(&mut arm.result, pending);
            }
            if let Some(child) = else_expr {
                detach_child(child, pending);
            }
        }
    }
}
