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
    HalfOpenRange { start: &'a Value, end: &'a Value },
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
        if let Some(access_key) = access.as_by_key_path() {
            return Some(Self::ByKey(access_key));
        }

        access
            .as_primary_key_range_path()
            .map(|(start, end)| Self::HalfOpenRange { start, end })
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
            Self::HalfOpenRange { start, end } => {
                matches_primary_key_half_open_range(predicate, primary_key_name, start, end)
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

fn matches_primary_key_half_open_range(
    predicate: &Predicate,
    primary_key_name: &str,
    start: &Value,
    end: &Value,
) -> bool {
    let Predicate::And(children) = predicate else {
        return false;
    };
    if children.len() != 2 {
        return false;
    }

    let mut lower_matches = false;
    let mut upper_matches = false;
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return false;
        };
        if cmp.field != primary_key_name {
            return false;
        }

        match cmp.op {
            CompareOp::Gte if cmp.value == *start => lower_matches = true,
            CompareOp::Lt if cmp.value == *end => upper_matches = true,
            _ => return false,
        }
    }

    lower_matches && upper_matches
}
