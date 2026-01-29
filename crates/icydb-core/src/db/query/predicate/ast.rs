use crate::{db::query::predicate::coercion::CoercionSpec, value::Value};
use std::ops::{BitAnd, BitOr};

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

impl ComparePredicate {
    fn new(field: String, op: CompareOp, value: Value) -> Self {
        Self {
            field,
            op,
            value,
            coercion: CoercionSpec::default(),
        }
    }

    #[must_use]
    pub fn eq(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Eq, value)
    }

    #[must_use]
    pub fn ne(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Ne, value)
    }

    #[must_use]
    pub fn lt(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Lt, value)
    }

    #[must_use]
    pub fn lte(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Lte, value)
    }

    #[must_use]
    pub fn gt(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Gt, value)
    }

    #[must_use]
    pub fn gte(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Gte, value)
    }

    #[must_use]
    pub fn in_(field: String, values: Vec<Value>) -> Self {
        Self::new(field, CompareOp::In, Value::List(values))
    }

    #[must_use]
    pub fn not_in(field: String, values: Vec<Value>) -> Self {
        Self::new(field, CompareOp::NotIn, Value::List(values))
    }
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

impl Predicate {
    #[must_use]
    pub const fn and(preds: Vec<Self>) -> Self {
        Self::And(preds)
    }

    #[must_use]
    pub const fn or(preds: Vec<Self>) -> Self {
        Self::Or(preds)
    }

    #[allow(clippy::should_implement_trait)]
    #[must_use]
    pub fn not(pred: Self) -> Self {
        Self::Not(Box::new(pred))
    }

    #[must_use]
    pub fn eq(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::eq(field, value))
    }

    #[must_use]
    pub fn ne(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::ne(field, value))
    }

    #[must_use]
    pub fn lt(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::lt(field, value))
    }

    #[must_use]
    pub fn lte(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::lte(field, value))
    }

    #[must_use]
    pub fn gt(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::gt(field, value))
    }

    #[must_use]
    pub fn gte(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::gte(field, value))
    }

    #[must_use]
    pub fn in_(field: String, values: Vec<Value>) -> Self {
        Self::Compare(ComparePredicate::in_(field, values))
    }

    #[must_use]
    pub fn not_in(field: String, values: Vec<Value>) -> Self {
        Self::Compare(ComparePredicate::not_in(field, values))
    }
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

impl BitOr for Predicate {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self::Or(vec![self, rhs])
    }
}

impl BitOr for &Predicate {
    type Output = Predicate;

    fn bitor(self, rhs: Self) -> Self::Output {
        Predicate::Or(vec![self.clone(), rhs.clone()])
    }
}
