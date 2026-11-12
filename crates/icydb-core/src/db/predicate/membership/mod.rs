//! Module: predicate::membership
//! Responsibility: shared construction of canonical value sets and membership predicates.
//! Does not own: expression lowering or predicate tree normalization.
//! Boundary: callers provide already-admitted compare leaves; this module owns
//! canonical value-set ordering and the same-field/coercion membership assembly rule.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate},
        resource::{
            budget::sort_comparison_allowance,
            construction::{ConstructionBudget, admit_value_comparison_repetitions},
        },
    },
    error::InternalError,
    value::{Value, canonicalize_value_set},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

///
/// MembershipCompareLeaf
///
/// One admitted equality-family compare leaf used to collapse expanded
/// membership forms back into a compact `IN` or `NOT IN` predicate during
/// expression lowering. Borrowed domain checks and final value assembly are
/// shared with predicate normalization without sharing AST traversal.
///

pub(in crate::db) struct MembershipCompareLeaf<'a> {
    field: &'a str,
    value: Value,
    coercion: CoercionId,
}

impl<'a> MembershipCompareLeaf<'a> {
    /// Construct one admitted membership leaf.
    #[must_use]
    pub(in crate::db) const fn new(field: &'a str, value: Value, coercion: CoercionId) -> Self {
        Self {
            field,
            value,
            coercion,
        }
    }
}

/// Collapse admitted same-field compare leaves into one membership predicate.
pub(in crate::db) fn collapse_membership_compare_leaves(
    leaves: Vec<MembershipCompareLeaf<'_>>,
    target_op: CompareOp,
    budget: &dyn ConstructionBudget,
) -> Result<Option<ComparePredicate>, InternalError> {
    if leaves.len() < 2 {
        return Ok(None);
    }
    let Some((field, coercion)) = membership_compare_domain(
        leaves.iter().map(|leaf| Some((leaf.field, leaf.coercion))),
        &mut |resource, amount| budget.charge(resource, amount),
    )?
    else {
        return Ok(None);
    };
    let field = budget.copy_text(field)?;
    let mut values = budget.vec_with_capacity(leaves.len())?;
    values.extend(leaves.into_iter().map(|leaf| leaf.value));

    Ok(Some(membership_compare_from_values(
        field,
        target_op,
        values,
        coercion,
        &mut |resource, amount| budget.charge(resource, amount),
    )?))
}

/// Inspect borrowed leaf domains without constructing operands. An absent leaf
/// means the caller's shape is ineligible; field/coercion agreement is owned here.
pub(in crate::db::predicate) fn membership_compare_domain<'a, E>(
    leaves: impl IntoIterator<Item = Option<(&'a str, CoercionId)>>,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<Option<(&'a str, CoercionId)>, E> {
    let mut leaves = leaves.into_iter();
    let Some(Some(domain)) = leaves.next() else {
        return Ok(None);
    };
    for leaf in leaves {
        let Some(leaf) = leaf else { return Ok(None) };
        // One domain check plus the maximum borrowed field-label extent.
        charge(
            Resource::PredicateExpressionSteps,
            1 + domain.0.len() as u64,
        )?;
        if leaf != domain {
            return Ok(None);
        }
    }
    Ok(Some(domain))
}

/// Assemble already-admitted same-field values using the canonical set owner.
/// Callers establish the minimum leaf count before consuming their input.
pub(in crate::db::predicate) fn membership_compare_from_values<E>(
    field: impl Into<String>,
    target_op: CompareOp,
    values: Vec<Value>,
    coercion: CoercionId,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<ComparePredicate, E> {
    let value = canonical_value_set_list(values, charge)?;

    Ok(ComparePredicate::with_coercion(
        field, target_op, value, coercion,
    ))
}

/// Admit ordering under the caller's authority, then use the existing set owner.
/// Retained normalization explicitly supplies its infallible charge callback.
pub(in crate::db) fn canonical_value_set_list<E>(
    mut values: Vec<Value>,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<Value, E> {
    if values.len() > 1 {
        charge(Resource::SortEntries, values.len() as u64)?;
        // Reserve the shared sort allowance plus both adjacent scans: the
        // canonical-input check and deduplication. Admission is conservative
        // even when already ordered input avoids sorting. The in-place unstable
        // sort has no auxiliary backing. Reserve payload work through the same
        // admitted extent owner before entering the infallible comparator.
        let adjacent = (values.len() as u64).saturating_sub(1).saturating_mul(2);
        let comparisons = sort_comparison_allowance(values.len()).saturating_add(adjacent);
        charge(Resource::SortComparisons, comparisons)?;
        admit_value_comparison_repetitions(&values, comparisons, CoercionId::Strict, charge)?;
    }
    canonicalize_value_set(&mut values);

    Ok(Value::List(values))
}
