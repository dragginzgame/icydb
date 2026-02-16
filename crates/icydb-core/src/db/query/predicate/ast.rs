use crate::{
    db::query::predicate::coercion::{CoercionId, CoercionSpec},
    value::Value,
};
use std::ops::{BitAnd, BitOr};
use thiserror::Error as ThisError;

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
#[repr(u8)]
pub enum CompareOp {
    Eq = 0x01,
    Ne = 0x02,
    Lt = 0x03,
    Lte = 0x04,
    Gt = 0x05,
    Gte = 0x06,
    In = 0x07,
    NotIn = 0x08,
    Contains = 0x09,
    StartsWith = 0x0a,
    EndsWith = 0x0b,
}

impl CompareOp {
    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }
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

    /// Construct a comparison predicate with an explicit coercion policy.
    #[must_use]
    pub fn with_coercion(
        field: impl Into<String>,
        op: CompareOp,
        value: Value,
        coercion: CoercionId,
    ) -> Self {
        Self {
            field: field.into(),
            op,
            value,
            coercion: CoercionSpec::new(coercion),
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
    IsNull { field: String },
    IsMissing { field: String },
    IsEmpty { field: String },
    IsNotEmpty { field: String },
    TextContains { field: String, value: Value },
    TextContainsCi { field: String, value: Value },
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

    #[expect(clippy::should_implement_trait)]
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

///
/// UnsupportedQueryFeature
///
/// Policy-level query features that are intentionally not supported.
///
/// Map predicates are disallowed by design: map storage may exist, but query
/// semantics over map structure are intentionally fenced for deterministic and
/// stable query behavior.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum UnsupportedQueryFeature {
    #[error(
        "map field '{field}' is not queryable; map predicates are disabled. model queryable attributes as scalar/indexed fields or list entries"
    )]
    MapPredicate { field: String },
}
