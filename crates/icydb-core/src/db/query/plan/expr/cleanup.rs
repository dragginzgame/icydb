//! Dispose of planner expression trees without recursive descendant destruction.
//! This ownership boundary also covers partially prepared trees on typed errors.

#[cfg(test)]
mod tests;

use crate::{
    db::query::plan::expr::Expr,
    value::{Value, clear_value},
};

impl Drop for Expr {
    fn drop(&mut self) {
        let mut pending = Vec::new();
        detach_children(self, &mut pending);
        while let Some(mut expr) = pending.pop() {
            detach_children(&mut expr, &mut pending);
        }
    }
}

// Processed nodes retain only shallow children. Their automatic second visit
// during destruction needs no further frontier allocation.
fn detach_child(expr: &mut Expr, pending: &mut Vec<Expr>) {
    match expr {
        Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(_) => {}
        _ => pending.push(expr.take()),
    }
}

fn detach_children(expr: &mut Expr, pending: &mut Vec<Expr>) {
    match expr {
        Expr::Literal(value) => {
            if matches!(value, Value::List(_) | Value::Map(_) | Value::Enum(_)) {
                clear_value(std::mem::replace(value, Value::Null));
            }
        }
        Expr::Unary { expr, .. } => detach_child(expr, pending),
        Expr::Binary { left, right, .. } => {
            detach_child(left, pending);
            detach_child(right, pending);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                detach_child(arg, pending);
            }
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                for child in arm.children_mut() {
                    detach_child(child, pending);
                }
            }
            detach_child(else_expr, pending);
        }
        Expr::Aggregate(aggregate) => {
            for child in aggregate.take_expressions().into_iter().flatten() {
                pending.push(*child);
            }
        }
        Expr::Field(_) | Expr::FieldPath(_) => {}
        #[cfg(test)]
        Expr::Alias { expr, .. } => detach_child(expr, pending),
    }
}
