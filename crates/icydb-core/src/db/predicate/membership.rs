//! Module: predicate::membership
//! Responsibility: shared construction of canonical membership compare predicates.
//! Does not own: expression lowering or predicate tree normalization.
//! Boundary: callers provide already-admitted compare leaves; this module owns
//! the same-field and same-coercion membership assembly rule.

use crate::{
    db::{
        access::canonical::canonicalize_value_set,
        predicate::{CoercionId, CompareOp, ComparePredicate},
    },
    value::Value,
};

///
/// MembershipCompareLeaf
///
/// One admitted equality-family compare leaf used to collapse expanded
/// membership forms back into a compact `IN` or `NOT IN` predicate. It exists
/// so expression lowering and predicate normalization can share the same
/// same-field/same-coercion assembly rule without sharing their AST traversal.
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
pub(in crate::db) fn collapse_membership_compare_leaves<'a>(
    leaves: impl IntoIterator<Item = MembershipCompareLeaf<'a>>,
    target_op: CompareOp,
) -> Option<ComparePredicate> {
    let mut field = None;
    let mut coercion = None;
    let mut values = Vec::new();

    for leaf in leaves {
        if let Some(current) = field {
            if current != leaf.field {
                return None;
            }
        } else {
            field = Some(leaf.field);
        }
        if let Some(current) = coercion {
            if current != leaf.coercion {
                return None;
            }
        } else {
            coercion = Some(leaf.coercion);
        }

        values.push(leaf.value);
    }

    if values.len() < 2 {
        return None;
    }

    canonicalize_value_set(&mut values);

    Some(ComparePredicate::with_coercion(
        field?,
        target_op,
        Value::List(values),
        coercion?,
    ))
}
