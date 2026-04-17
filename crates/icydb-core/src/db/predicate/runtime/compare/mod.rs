mod scalar;
mod value;

pub(in crate::db) use crate::db::predicate::runtime::compare::value::eval_compare_values;
pub(in crate::db::predicate::runtime) use crate::db::predicate::runtime::compare::{
    scalar::{eval_compare_scalar_slot, text_contains_scalar},
    value::is_empty_value,
};
