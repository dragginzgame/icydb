//! Module: predicate::resolved
//! Responsibility: canonical executable predicate representation for runtime execution.
//! Does not own: field-name schema mapping itself.
//! Boundary: produced once at predicate compile time and consumed by runtime and index execution.

use crate::{
    db::predicate::{coercion::CoercionSpec, model::CompareOp},
    value::Value,
};

///
/// ExecutableComparePredicate
///
/// One executable comparison node with a pre-resolved field slot.
/// This is the canonical compare payload shared by runtime filtering and
/// index-only predicate compilation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutableComparePredicate {
    pub(in crate::db) field_slot: Option<usize>,
    pub(in crate::db) op: CompareOp,
    pub(in crate::db) value: Value,
    pub(in crate::db) coercion: CoercionSpec,
}

///
/// ExecutablePredicate
///
/// Canonical predicate execution tree emitted by planning-time compilation.
/// Runtime row filtering and index-only compilation both consume this one
/// structural form instead of maintaining parallel execution representations.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutablePredicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ExecutableComparePredicate),
    IsNull {
        field_slot: Option<usize>,
    },
    IsNotNull {
        field_slot: Option<usize>,
    },
    IsMissing {
        field_slot: Option<usize>,
    },
    IsEmpty {
        field_slot: Option<usize>,
    },
    IsNotEmpty {
        field_slot: Option<usize>,
    },
    TextContains {
        field_slot: Option<usize>,
        value: Value,
    },
    TextContainsCi {
        field_slot: Option<usize>,
        value: Value,
    },
}
