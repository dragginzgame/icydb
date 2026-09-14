//! Module: predicate::membership
//! Responsibility: shared construction of canonical membership compare predicates.
//! Does not own: expression lowering or predicate tree normalization.
//! Boundary: callers provide already-admitted compare leaves; this module owns
//! the same-field and same-coercion membership assembly rule.

use crate::{
    db::predicate::{CoercionId, CompareOp, ComparePredicate},
    value::{Value, canonicalize_value_set},
};

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
) -> Option<ComparePredicate> {
    if leaves.len() < 2 {
        return None;
    }
    let (field, coercion) =
        membership_compare_domain(leaves.iter().map(|leaf| Some((leaf.field, leaf.coercion))))?;
    let values = leaves.into_iter().map(|leaf| leaf.value).collect();

    Some(membership_compare_from_values(
        field, target_op, values, coercion,
    ))
}

/// Inspect borrowed leaf domains without constructing operands. An absent leaf
/// means the caller's shape is ineligible; field/coercion agreement is owned here.
pub(in crate::db::predicate) fn membership_compare_domain<'a>(
    leaves: impl IntoIterator<Item = Option<(&'a str, CoercionId)>>,
) -> Option<(&'a str, CoercionId)> {
    let mut leaves = leaves.into_iter();
    let domain = leaves.next()??;
    for leaf in leaves {
        if leaf? != domain {
            return None;
        }
    }
    Some(domain)
}

/// Assemble already-admitted same-field values using the canonical set owner.
/// Callers establish the minimum leaf count before consuming their input.
pub(in crate::db::predicate) fn membership_compare_from_values(
    field: impl Into<String>,
    target_op: CompareOp,
    values: Vec<Value>,
    coercion: CoercionId,
) -> ComparePredicate {
    let value = canonical_membership_value_list(values);

    ComparePredicate::with_coercion(field, target_op, value, coercion)
}

/// Canonicalize an already-admitted membership literal set.
pub(in crate::db) fn canonical_membership_value_list(mut values: Vec<Value>) -> Value {
    canonicalize_value_set(&mut values);

    Value::List(values)
}
