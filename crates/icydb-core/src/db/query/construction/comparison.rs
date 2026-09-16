//! Shared operand extent for structural and lexicographic comparisons.
//! Owns admission only, not value equality, ordering or numeric conversion.

#[cfg(test)]
mod tests;

use crate::{db::query::construction::ConstructionBudget, error::InternalError, value::Value};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::borrow::Borrow;

impl dyn ConstructionBudget + '_ {
    /// Admit payload work before applying the existing structural equality rule.
    pub(in crate::db) fn values_equal(
        &self,
        left: &Value,
        right: &Value,
    ) -> Result<bool, InternalError> {
        self.admit_value_comparison(left)?;
        Ok(left == right)
    }

    /// Compare borrowed set/list views without copying or reordering operands.
    pub(in crate::db) fn value_slices_equal<T: Borrow<Value>>(
        &self,
        left: &[T],
        right: &[T],
    ) -> Result<bool, InternalError> {
        if left.len() != right.len() {
            return Ok(false);
        }
        for (left, right) in left.iter().zip(right) {
            if !self.values_equal(left.borrow(), right.borrow())? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Admit one side's full tree before borrowed structural/lexicographic comparison.
    /// Matching children visit at most this extent; unequal tags/lengths can stop
    /// sooner. Two node visits cover this admission walk and the later comparison.
    /// Callers supply depth-admitted trees. This neither sorts nor copies values,
    /// and does not cover numeric-conversion scratch (notably float formatting).
    pub(in crate::db) fn admit_value_comparison(&self, value: &Value) -> Result<(), InternalError> {
        self.charge(Resource::NestedValueSteps, 2)?;
        let bytes = match value {
            Value::Text(text) => text.len() as u64,
            Value::Blob(blob) => blob.len() as u64,
            Value::IntBig(integer) => integer.magnitude_bits().div_ceil(64).saturating_mul(8),
            Value::NatBig(integer) => integer.magnitude_bits().div_ceil(64).saturating_mul(8),
            Value::List(values) => {
                for value in values {
                    self.admit_value_comparison(value)?;
                }
                0
            }
            Value::Map(entries) => {
                // Structural equality compares stored entries; it does not sort.
                for (key, value) in entries {
                    self.admit_value_comparison(key)?;
                    self.admit_value_comparison(value)?;
                }
                0
            }
            Value::Enum(value) => {
                if let Some(payload) = value.payload() {
                    self.admit_value_comparison(payload)?;
                }
                0
            }
            Value::Account(_)
            | Value::Bool(_)
            | Value::Date(_)
            | Value::Decimal(_)
            | Value::Duration(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Int64(_)
            | Value::Int128(_)
            | Value::Nat64(_)
            | Value::Nat128(_)
            | Value::Null
            | Value::Principal(_)
            | Value::Subaccount(_)
            | Value::Timestamp(_)
            | Value::U256(_)
            | Value::Ulid(_)
            | Value::Unit => 0,
        };
        self.charge(Resource::PredicateExpressionSteps, bytes)
    }
}
