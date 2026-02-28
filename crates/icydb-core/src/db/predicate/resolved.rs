use crate::{
    db::predicate::{coercion::CoercionSpec, model::CompareOp},
    value::Value,
};

///
/// ResolvedComparePredicate
///
/// One comparison node with a pre-resolved field slot.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ResolvedComparePredicate {
    pub(in crate::db) field_slot: Option<usize>,
    pub(in crate::db) op: CompareOp,
    pub(in crate::db) value: Value,
    pub(in crate::db) coercion: CoercionSpec,
}

///
/// ResolvedPredicate
///
/// Predicate AST compiled to field slots for execution hot paths.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ResolvedPredicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ResolvedComparePredicate),
    IsNull {
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
