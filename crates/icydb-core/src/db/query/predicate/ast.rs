use crate::value::Value;

use super::coercion::CoercionSpec;
use std::ops::BitAnd;

///
/// Predicate AST
///
/// Pure, schema-agnostic representation of query predicates.
/// This layer contains no type validation, index logic, or execution
/// semantics. All interpretation occurs in later passes:
///
/// - normalization
/// - validation (schema-aware)
/// - planning
/// - execution
///

///
/// CompareOp
///

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

///
/// ComparePredicate
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparePredicate {
    pub field: String,
    pub op: CompareOp,
    pub value: Value,
    pub coercion: CoercionSpec,
}

///
/// Predicate
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
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
    TextContains {
        field: String,
        value: Value,
    },
    TextContainsCi {
        field: String,
        value: Value,
    },
}

impl BitAnd for Predicate {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self::And(vec![self, rhs])
    }
}

impl BitAnd for &Predicate {
    type Output = Predicate;

    fn bitand(self, rhs: Self) -> Self::Output {
        Predicate::And(vec![self.clone(), rhs.clone()])
    }
}
