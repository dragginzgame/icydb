//! Shared operand extent for structural and lexicographic comparisons.
//! Owns admission only, not value equality, ordering or numeric conversion.

#[cfg(test)]
mod tests;

use crate::{
    db::{predicate::CoercionId, resource::construction::ConstructionBudget},
    error::InternalError,
    value::{Value, lower_text_construction_allowance},
};
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
        admit_value_comparison(value, &mut |resource, amount| self.charge(resource, amount))
    }
}

/// Apply the shared comparison extent through the caller's fallible authority.
/// Pure retained normalization supplies its explicit infallible callback.
pub(in crate::db) fn admit_value_comparison<E>(
    value: &Value,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<(), E> {
    comparison_extent(value, charge).map(|_| ())
}

// Counts come from the same admitted walk used by single comparisons. Retaining
// two scalar maxima avoids storing operands or walking payloads again per pair.
#[derive(Default)]
struct ComparisonExtent {
    nodes: u64,
    bytes: u64,
}

impl ComparisonExtent {
    const fn include(&mut self, other: Self) {
        self.nodes = self.nodes.saturating_add(other.nodes);
        self.bytes = self.bytes.saturating_add(other.bytes);
    }
}

/// Admit each operand, then reserve its largest extent for each comparison in
/// the caller's allowance. The comparator stays infallible and allocation-free.
/// This is construction-policy accounting, not an IC instruction bound.
pub(in crate::db) fn admit_value_comparison_repetitions<'a, E>(
    values: impl IntoIterator<Item = &'a Value>,
    comparisons: u64,
    coercion: CoercionId,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<(), E> {
    let mut maximum = ComparisonExtent::default();
    for value in values {
        let mut extent = comparison_extent(value, charge)?;
        if coercion == CoercionId::TextCasefold
            && let Value::Text(text) = value
        {
            // Only direct text operands are transformed. The text owner's
            // cumulative backing bound also bounds its final comparison bytes;
            // actual coercion construction is admitted by the encoding owner.
            extent.bytes = lower_text_construction_allowance(text.len()).0;
        }
        maximum.nodes = maximum.nodes.max(extent.nodes);
        maximum.bytes = maximum.bytes.max(extent.bytes);
    }
    charge(
        Resource::NestedValueSteps,
        maximum.nodes.saturating_mul(comparisons),
    )?;
    charge(
        Resource::PredicateExpressionSteps,
        maximum.bytes.saturating_mul(comparisons),
    )
}

fn comparison_extent<E>(
    value: &Value,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<ComparisonExtent, E> {
    charge(Resource::NestedValueSteps, 2)?;
    let mut extent = ComparisonExtent { nodes: 2, bytes: 0 };
    let bytes = match value {
        Value::Text(text) => text.len() as u64,
        Value::Blob(blob) => blob.len() as u64,
        Value::List(values) => {
            for value in values {
                extent.include(comparison_extent(value, charge)?);
            }
            0
        }
        Value::Map(entries) => {
            // Structural equality compares stored entries; it does not sort.
            for (key, value) in entries {
                extent.include(comparison_extent(key, charge)?);
                extent.include(comparison_extent(value, charge)?);
            }
            0
        }
        Value::Enum(value) => {
            if let Some(payload) = value.payload() {
                extent.include(comparison_extent(payload, charge)?);
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
    charge(Resource::PredicateExpressionSteps, bytes)?;
    extent.bytes = extent.bytes.saturating_add(bytes);
    Ok(extent)
}
