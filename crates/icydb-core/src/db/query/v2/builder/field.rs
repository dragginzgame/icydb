use crate::{
    db::query::v2::predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
    traits::FieldValue,
    value::Value,
};

#[must_use]
pub fn eq(field: &'static str, value: impl FieldValue) -> Predicate {
    compare(field, CompareOp::Eq, value.to_value(), CoercionId::Strict)
}

#[must_use]
pub fn eq_ci(field: &'static str, value: impl FieldValue) -> Predicate {
    compare(
        field,
        CompareOp::Eq,
        value.to_value(),
        CoercionId::TextCasefold,
    )
}

#[must_use]
pub fn ne(field: &'static str, value: impl FieldValue) -> Predicate {
    compare(field, CompareOp::Ne, value.to_value(), CoercionId::Strict)
}

#[must_use]
pub fn lt(field: &'static str, value: impl FieldValue) -> Predicate {
    compare(
        field,
        CompareOp::Lt,
        value.to_value(),
        CoercionId::NumericWiden,
    )
}

#[must_use]
pub fn lte(field: &'static str, value: impl FieldValue) -> Predicate {
    compare(
        field,
        CompareOp::Lte,
        value.to_value(),
        CoercionId::NumericWiden,
    )
}

#[must_use]
pub fn gt(field: &'static str, value: impl FieldValue) -> Predicate {
    compare(
        field,
        CompareOp::Gt,
        value.to_value(),
        CoercionId::NumericWiden,
    )
}

#[must_use]
pub fn gte(field: &'static str, value: impl FieldValue) -> Predicate {
    compare(
        field,
        CompareOp::Gte,
        value.to_value(),
        CoercionId::NumericWiden,
    )
}

#[must_use]
pub fn in_list(field: &'static str, values: Vec<Value>) -> Predicate {
    compare(
        field,
        CompareOp::In,
        Value::List(values),
        CoercionId::Strict,
    )
}

#[must_use]
pub fn is_null(field: &'static str) -> Predicate {
    Predicate::IsNull {
        field: field.to_string(),
    }
}

#[must_use]
pub fn is_missing(field: &'static str) -> Predicate {
    Predicate::IsMissing {
        field: field.to_string(),
    }
}

#[must_use]
pub fn is_empty(field: &'static str) -> Predicate {
    Predicate::IsEmpty {
        field: field.to_string(),
    }
}

#[must_use]
pub fn is_not_empty(field: &'static str) -> Predicate {
    Predicate::IsNotEmpty {
        field: field.to_string(),
    }
}

#[must_use]
pub fn map_contains_key(
    field: &'static str,
    key: impl FieldValue,
    coercion: CoercionId,
) -> Predicate {
    Predicate::MapContainsKey {
        field: field.to_string(),
        key: key.to_value(),
        coercion: CoercionSpec::new(coercion),
    }
}

#[must_use]
pub fn map_contains_value(
    field: &'static str,
    value: impl FieldValue,
    coercion: CoercionId,
) -> Predicate {
    Predicate::MapContainsValue {
        field: field.to_string(),
        value: value.to_value(),
        coercion: CoercionSpec::new(coercion),
    }
}

#[must_use]
pub fn map_contains_entry(
    field: &'static str,
    key: impl FieldValue,
    value: impl FieldValue,
    coercion: CoercionId,
) -> Predicate {
    Predicate::MapContainsEntry {
        field: field.to_string(),
        key: key.to_value(),
        value: value.to_value(),
        coercion: CoercionSpec::new(coercion),
    }
}

fn compare(field: &'static str, op: CompareOp, value: Value, coercion: CoercionId) -> Predicate {
    Predicate::Compare(ComparePredicate {
        field: field.to_string(),
        op,
        value,
        coercion: CoercionSpec::new(coercion),
    })
}
