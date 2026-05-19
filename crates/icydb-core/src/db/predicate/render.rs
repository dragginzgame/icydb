//! Module: predicate::render
//! Responsibility: render reduced predicate SQL after structural AST rewrites.
//! Does not own: SQL statement formatting, query planning, or schema mutation.
//! Boundary: DDL metadata relabeling consumes this to avoid text replacement.

use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, parse_sql_predicate},
        sql::identifier::rewrite_field_identifiers,
    },
    value::Value,
};

/// Parse, structurally relabel, and render one reduced SQL predicate.
///
/// This intentionally round-trips through the predicate AST so DDL metadata
/// updates never rewrite SQL text by substring.
pub(in crate::db) fn relabel_sql_predicate_field_root(
    predicate_sql: &str,
    old_name: &str,
    new_name: &str,
) -> Option<String> {
    let predicate = parse_sql_predicate(predicate_sql).ok()?;
    let predicate = rewrite_field_identifiers(predicate, |field| {
        relabel_field_root(field, old_name, new_name)
    });

    render_sql_predicate(&predicate)
}

fn relabel_field_root(field: String, old_name: &str, new_name: &str) -> String {
    if field == old_name {
        return new_name.to_string();
    }

    let Some(tail) = field
        .strip_prefix(old_name)
        .and_then(|tail| tail.strip_prefix('.'))
    else {
        return field;
    };

    format!("{new_name}.{tail}")
}

fn render_sql_predicate(predicate: &Predicate) -> Option<String> {
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

fn render_scalar_sql_value(value: &Value) -> Option<String> {
    Some(match value {
        Value::Blob(bytes) => render_blob_sql_value(bytes),
        Value::Bool(value) => value.to_string().to_uppercase(),
        Value::Decimal(value) => value.to_string(),
        Value::Float32(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Int128(value) => value.to_string(),
        Value::IntBig(value) => value.to_string(),
        Value::Nat(value) => value.to_string(),
        Value::Nat128(value) => value.to_string(),
        Value::NatBig(value) => value.to_string(),
        Value::Null => "NULL".to_string(),
        Value::Text(text) => format!("'{}'", text.replace('\'', "''")),
        Value::List(_)
        | Value::Account(_)
        | Value::Date(_)
        | Value::Duration(_)
        | Value::Enum(_)
        | Value::Map(_)
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Timestamp(_)
        | Value::Ulid(_)
        | Value::Unit => return None,
    })
}

fn render_blob_sql_value(bytes: &[u8]) -> String {
    let mut rendered = String::with_capacity(bytes.len().saturating_mul(2) + 3);
    rendered.push_str("X'");
    for byte in bytes {
        rendered.push(hex_digit(byte >> 4));
        rendered.push(hex_digit(byte & 0x0f));
    }
    rendered.push('\'');
    rendered
}

const fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'A' + nibble - 10) as char,
        _ => '?',
    }
}

#[cfg(test)]
mod tests {
    use super::relabel_sql_predicate_field_root;

    #[test]
    fn relabel_sql_predicate_field_root_updates_literal_predicate_fields() {
        assert_eq!(
            relabel_sql_predicate_field_root(
                "nickname IS NOT NULL AND active = TRUE",
                "nickname",
                "handle",
            ),
            Some("(handle IS NOT NULL) AND (active = TRUE)".to_string()),
        );
    }

    #[test]
    fn relabel_sql_predicate_field_root_updates_list_and_field_compare_predicates() {
        assert_eq!(
            relabel_sql_predicate_field_root(
                "nickname IN ('Ada', 'Grace') OR nickname > display_name",
                "nickname",
                "handle",
            ),
            Some("(handle IN ('Ada', 'Grace')) OR (handle > display_name)".to_string()),
        );
    }

    #[test]
    fn relabel_sql_predicate_field_root_updates_nested_field_roots() {
        assert_eq!(
            relabel_sql_predicate_field_root("profile.rank >= 7", "profile", "bio"),
            Some("bio.rank >= 7".to_string()),
        );
    }

    #[test]
    fn relabel_sql_predicate_field_root_updates_casefold_prefix_predicates() {
        assert_eq!(
            relabel_sql_predicate_field_root(
                "STARTS_WITH(LOWER(nickname), 'al')",
                "nickname",
                "handle",
            ),
            Some("STARTS_WITH(LOWER(handle), 'al')".to_string()),
        );
    }
}
