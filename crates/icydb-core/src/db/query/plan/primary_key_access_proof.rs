//! Module: query::plan::primary_key_access_proof
//! Responsibility: selected primary-key access proof projection.
//! Does not own: access-path selection, admission policy, or executor routing.
//! Boundary: gives planner pipeline helpers one named view of selected
//! primary-key access shapes.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{CompareOp, Predicate},
    },
    value::{Value, canonicalize_value_set},
};

///
/// PrimaryKeyAccessProof
///
/// Planner-local selected primary-key access proof.
///
/// This is intentionally projected from the already-selected `AccessPlan`.
/// It does not decide which access path wins; it only lets later planner
/// pipeline steps ask whether the selected access already proves one
/// normalized primary-key predicate.
///

pub(in crate::db::query::plan) enum PrimaryKeyAccessProof<'a> {
    ByKey(&'a Value),
    ByKeys(&'a [Value]),
}

impl<'a> PrimaryKeyAccessProof<'a> {
    /// Project one selected access path into primary-key proof shapes.
    #[must_use]
    pub(in crate::db::query::plan) fn from_access(access: &'a AccessPlan<Value>) -> Option<Self> {
        if let Some(access_keys) = access.as_by_keys_path()
            && !access_keys.is_empty()
        {
            return Some(Self::ByKeys(access_keys));
        }
        // Physical ranges include both endpoints; their residual predicate
        // must retain any stricter authored comparison.
        access.as_by_key_path().map(Self::ByKey)
    }

    /// Return whether this selected access proves one normalized primary-key
    /// predicate.
    #[must_use]
    pub(in crate::db::query::plan) fn matches_predicate(
        self,
        predicate: &Predicate,
        primary_key_name: &str,
    ) -> bool {
        match self {
            Self::ByKey(access_key) => {
                matches_primary_key_eq_predicate(predicate, primary_key_name, access_key)
            }
            Self::ByKeys(access_keys) => {
                matches_primary_key_in_predicate(predicate, primary_key_name, access_keys)
            }
        }
    }
}

fn matches_primary_key_eq_predicate(
    predicate: &Predicate,
    primary_key_name: &str,
    access_key: &Value,
) -> bool {
    let Predicate::Compare(cmp) = predicate else {
        return false;
    };
    cmp.field == primary_key_name && cmp.op == CompareOp::Eq && cmp.value == *access_key
}

fn matches_primary_key_in_predicate(
    predicate: &Predicate,
    primary_key_name: &str,
    access_keys: &[Value],
) -> bool {
    let Predicate::Compare(cmp) = predicate else {
        return false;
    };
    if cmp.field != primary_key_name || cmp.op != CompareOp::In {
        return false;
    }

    let Value::List(predicate_keys) = &cmp.value else {
        return false;
    };

    let mut canonical_predicate_keys = predicate_keys.clone();
    canonicalize_value_set(&mut canonical_predicate_keys);

    canonical_predicate_keys == access_keys
}
