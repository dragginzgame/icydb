mod direct;
mod text;

use crate::{
    db::{
        data::{ScalarSlotValueRef, ScalarValueRef},
        predicate::{CoercionSpec, CompareOp},
    },
    value::Value,
};

use crate::db::predicate::runtime::compare::scalar::{
    direct::{
        compare::{eval_blob_scalar_compare, eval_direct_scalar_compare, eval_null_scalar_compare},
        decode::{
            scalar_bool_from_value, scalar_date_from_value, scalar_duration_from_value,
            scalar_float32_from_value, scalar_float64_from_value, scalar_int_from_value,
            scalar_nat_from_value, scalar_principal_from_value, scalar_subaccount_from_value,
            scalar_timestamp_from_value, scalar_ulid_from_value, scalar_unit_from_value,
        },
    },
    text::eval_text_scalar_compare,
};

// Evaluate one compare op directly against one scalar slot value when possible.
pub(in crate::db::predicate::runtime) fn eval_compare_scalar_slot(
    actual: ScalarSlotValueRef<'_>,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match actual {
        ScalarSlotValueRef::Null => eval_null_scalar_compare(op, value, coercion),
        ScalarSlotValueRef::Value(ScalarValueRef::Text(actual)) => {
            eval_text_scalar_compare(actual, op, value, coercion)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Blob(actual)) => {
            eval_blob_scalar_compare(actual, op, value, coercion)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Bool(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_bool_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Date(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_date_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Duration(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_duration_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Float32(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_float32_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Float64(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_float64_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Int(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_int_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Principal(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_principal_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Subaccount(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_subaccount_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Timestamp(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_timestamp_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Nat(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_nat_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Ulid(actual)) => {
            eval_direct_scalar_compare(actual, op, value, coercion, scalar_ulid_from_value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Unit) => {
            eval_direct_scalar_compare((), op, value, coercion, scalar_unit_from_value)
        }
    }
}

pub(in crate::db::predicate::runtime) use crate::db::predicate::runtime::compare::scalar::text::text_contains_scalar;
