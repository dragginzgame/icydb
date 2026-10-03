//! Module: predicate::render
//! Responsibility: render reduced predicate SQL after structural AST rewrites.
//! Does not own: SQL statement formatting, query planning, or schema mutation.
//! Boundary: DDL metadata relabeling consumes this to avoid text replacement.

use crate::{
    db::predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
    db::sql_shared::render_scalar_sql_value,
    value::Value,
};

pub(in crate::db) fn render_sql_predicate(predicate: &Predicate) -> Option<String> {
    match predicate {
        Predicate::True => Some("TRUE".to_string()),
        Predicate::False => Some("FALSE".to_string()),
        Predicate::And(children) => render_sql_predicate_children(children, "AND"),
        Predicate::Or(children) => render_sql_predicate_children(children, "OR"),
        Predicate::Not(inner) => Some(format!("NOT ({})", render_sql_predicate(inner)?)),
        Predicate::Compare(compare) => render_compare_predicate(compare),
        Predicate::CompareFields(compare) => Some(format!(
            "{} {} {}",
            render_field_operand(compare.left_field(), compare.coercion().id),
            compare_op_sql(compare.op())?,
            render_field_operand(compare.right_field(), compare.coercion().id)
        )),
        Predicate::IsNull { field } => Some(format!("{field} IS NULL")),
        Predicate::IsNotNull { field } => Some(format!("{field} IS NOT NULL")),
        Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => None,
    }
}

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

fn render_compare_predicate(compare: &ComparePredicate) -> Option<String> {
    match compare.op() {
        CompareOp::In | CompareOp::NotIn => Some(format!(
            "{} {} ({})",
            compare.field(),
            compare_op_sql(compare.op())?,
            render_value_list(compare.value())?
        )),
        CompareOp::StartsWith => Some(format!(
            "STARTS_WITH({}, {})",
            render_field_operand(compare.field(), compare.coercion().id),
            render_scalar_sql_value(compare.value())?
        )),
        CompareOp::Contains | CompareOp::EndsWith => None,
        _ => Some(format!(
            "{} {} {}",
            render_field_operand(compare.field(), compare.coercion().id),
            compare_op_sql(compare.op())?,
            render_scalar_sql_value(compare.value())?
        )),
    }
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
