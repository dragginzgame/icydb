//! Module: predicate::render
//! Responsibility: share SQL comparison spelling for accepted metadata and parser tests.
//! Does not own: SQL statement formatting, query planning, or schema mutation.
//! Boundary: accepted metadata renders directly; tests retain runtime-tree round-trips.

use crate::db::predicate::{CoercionId, CompareOp};

#[cfg(test)]
use crate::{
    db::predicate::{ComparePredicate, Predicate},
    db::sql_shared::render_scalar_sql_value,
    value::Value,
};

#[cfg(test)]
pub(in crate::db) fn render_sql_predicate(predicate: &Predicate) -> Option<String> {
    match predicate {
        Predicate::True => Some("TRUE".to_string()),
        Predicate::False => Some("FALSE".to_string()),
        Predicate::And(children) => render_sql_predicate_children(children, "AND"),
        Predicate::Or(children) => render_sql_predicate_children(children, "OR"),
        Predicate::Not(inner) => Some(format!("NOT ({})", render_sql_predicate(inner)?)),
        Predicate::Compare(compare) => render_compare_predicate(compare),
        Predicate::CompareFields(compare) => render_sql_field_comparison(
            compare.left_field(),
            compare.op(),
            compare.right_field(),
            compare.coercion().id,
        ),
        Predicate::IsNull { field } => Some(format!("{field} IS NULL")),
        Predicate::IsNotNull { field } => Some(format!("{field} IS NOT NULL")),
        Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => None,
    }
}

#[cfg(test)]
fn render_sql_predicate_children(children: &[Predicate], op: &str) -> Option<String> {
    let rendered = children
        .iter()
        .map(render_sql_predicate)
        .collect::<Option<Vec<_>>>()?;

    Some(
        rendered
            .into_iter()
            .map(|child| format!("({child})"))
            .collect::<Vec<_>>()
            .join(format!(" {op} ").as_str()),
    )
}

#[cfg(test)]
fn render_compare_predicate(compare: &ComparePredicate) -> Option<String> {
    let rhs = if matches!(compare.op(), CompareOp::In | CompareOp::NotIn) {
        render_value_list(compare.value())?
    } else {
        render_scalar_sql_value(compare.value())?
    };
    render_sql_comparison(compare.field(), compare.op(), compare.coercion().id, &rhs)
}

// Both accepted metadata and parser round-trips use the same SQL spelling.
pub(in crate::db) fn render_sql_comparison(
    field: &str,
    op: CompareOp,
    coercion: CoercionId,
    rhs: &str,
) -> Option<String> {
    match op {
        CompareOp::In | CompareOp::NotIn => {
            Some(format!("{field} {} ({rhs})", compare_op_sql(op)?))
        }
        CompareOp::StartsWith => Some(format!(
            "STARTS_WITH({}, {rhs})",
            render_field_operand(field, coercion)
        )),
        CompareOp::Contains | CompareOp::EndsWith => None,
        _ => Some(format!(
            "{} {} {rhs}",
            render_field_operand(field, coercion),
            compare_op_sql(op)?
        )),
    }
}

pub(in crate::db) fn render_sql_field_comparison(
    left: &str,
    op: CompareOp,
    right: &str,
    coercion: CoercionId,
) -> Option<String> {
    Some(format!(
        "{} {} {}",
        render_field_operand(left, coercion),
        compare_op_sql(op)?,
        render_field_operand(right, coercion)
    ))
}

fn render_field_operand(field: &str, coercion: CoercionId) -> String {
    match coercion {
        CoercionId::TextCasefold => format!("LOWER({field})"),
        CoercionId::Strict | CoercionId::NumericWiden | CoercionId::CollectionElement => {
            field.to_string()
        }
    }
}

const fn compare_op_sql(op: CompareOp) -> Option<&'static str> {
    match op {
        CompareOp::Eq => Some("="),
        CompareOp::Ne => Some("!="),
        CompareOp::Lt => Some("<"),
        CompareOp::Lte => Some("<="),
        CompareOp::Gt => Some(">"),
        CompareOp::Gte => Some(">="),
        CompareOp::In => Some("IN"),
        CompareOp::NotIn => Some("NOT IN"),
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => None,
    }
}

#[cfg(test)]
fn render_value_list(value: &Value) -> Option<String> {
    let Value::List(items) = value else {
        return None;
    };

    items
        .iter()
        .map(render_scalar_sql_value)
        .collect::<Option<Vec<_>>>()
        .map(|items| items.join(", "))
}
