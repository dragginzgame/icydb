use crate::value::Value;

use super::coercion::CoercionSpec;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    NotIn,
    AnyIn,
    AllIn,
    Contains,
    StartsWith,
    EndsWith,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparePredicate {
    pub field: String,
    pub op: CompareOp,
    pub value: Value,
    pub coercion: CoercionSpec,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    True,
    False,
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    Compare(ComparePredicate),
    IsNull {
        field: String,
    },
    IsMissing {
        field: String,
    },
    IsEmpty {
        field: String,
    },
    IsNotEmpty {
        field: String,
    },
    MapContainsKey {
        field: String,
        key: Value,
        coercion: CoercionSpec,
    },
    MapContainsValue {
        field: String,
        value: Value,
        coercion: CoercionSpec,
    },
    MapContainsEntry {
        field: String,
        key: Value,
        value: Value,
        coercion: CoercionSpec,
    },
}
