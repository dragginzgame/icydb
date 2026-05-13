use crate::value::Value;

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_bool_from_value(
    value: &Value,
) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_date_from_value(
    value: &Value,
) -> Option<crate::types::Date> {
    match value {
        Value::Date(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_duration_from_value(
    value: &Value,
) -> Option<crate::types::Duration> {
    match value {
        Value::Duration(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_float32_from_value(
    value: &Value,
) -> Option<crate::types::Float32> {
    match value {
        Value::Float32(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_float64_from_value(
    value: &Value,
) -> Option<crate::types::Float64> {
    match value {
        Value::Float64(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_int_from_value(
    value: &Value,
) -> Option<i64> {
    match value {
        Value::Int(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_principal_from_value(
    value: &Value,
) -> Option<crate::types::Principal> {
    match value {
        Value::Principal(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_subaccount_from_value(
    value: &Value,
) -> Option<crate::types::Subaccount> {
    match value {
        Value::Subaccount(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_timestamp_from_value(
    value: &Value,
) -> Option<crate::types::Timestamp> {
    match value {
        Value::Timestamp(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_nat_from_value(
    value: &Value,
) -> Option<u64> {
    match value {
        Value::Nat(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_ulid_from_value(
    value: &Value,
) -> Option<crate::types::Ulid> {
    match value {
        Value::Ulid(value) => Some(*value),
        _ => None,
    }
}

pub(in crate::db::predicate::runtime::compare::scalar) const fn scalar_unit_from_value(
    value: &Value,
) -> Option<()> {
    match value {
        Value::Unit => Some(()),
        _ => None,
    }
}
