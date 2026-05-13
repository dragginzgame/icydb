//! Module: predicate::model
//! Responsibility: public predicate AST and construction helpers.
//! Does not own: schema validation or runtime slot resolution.
//! Boundary: user/query-facing predicate model.

use crate::{
    db::predicate::coercion::{CoercionId, CoercionSpec},
    value::Value,
};
use std::ops::{BitAnd, BitOr};
use thiserror::Error as ThisError;

#[cfg_attr(doc, doc = "Predicate")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ComparePredicate),
    CompareFields(CompareFieldsPredicate),
    IsNull { field: String },
    IsNotNull { field: String },
    IsMissing { field: String },
    IsEmpty { field: String },
    IsNotEmpty { field: String },
    TextContains { field: String, value: Value },
    TextContainsCi { field: String, value: Value },
}

impl Predicate {
    /// Build an `And` predicate from child predicates.
    #[must_use]
    pub const fn and(preds: Vec<Self>) -> Self {
        Self::And(preds)
    }

    /// Build an `Or` predicate from child predicates.
    #[must_use]
    pub const fn or(preds: Vec<Self>) -> Self {
        Self::Or(preds)
    }

    /// Negate one predicate.
    #[must_use]
    #[expect(clippy::should_implement_trait)]
    pub fn not(pred: Self) -> Self {
        Self::Not(Box::new(pred))
    }

    /// Compare `field == value`.
    #[must_use]
    pub fn eq(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::eq(field, value))
    }

    /// Compare `field != value`.
    #[must_use]
    pub fn ne(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::ne(field, value))
    }

    /// Compare `field < value`.
    #[must_use]
    pub fn lt(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::lt(field, value))
    }

    /// Compare `field <= value`.
    #[must_use]
    pub fn lte(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::lte(field, value))
    }

    /// Compare `field > value`.
    #[must_use]
    pub fn gt(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::gt(field, value))
    }

    /// Compare `field >= value`.
    #[must_use]
    pub fn gte(field: String, value: Value) -> Self {
        Self::Compare(ComparePredicate::gte(field, value))
    }

    /// Compare `left_field == right_field`.
    #[must_use]
    pub fn eq_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::eq(left_field, right_field))
    }

    /// Compare `left_field != right_field`.
    #[must_use]
    pub fn ne_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::ne(left_field, right_field))
    }

    /// Compare `left_field < right_field`.
    #[must_use]
    pub fn lt_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::with_coercion(
            left_field,
            CompareOp::Lt,
            right_field,
            CoercionId::NumericWiden,
        ))
    }

    /// Compare `left_field <= right_field`.
    #[must_use]
    pub fn lte_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::with_coercion(
            left_field,
            CompareOp::Lte,
            right_field,
            CoercionId::NumericWiden,
        ))
    }

    /// Compare `left_field > right_field`.
    #[must_use]
    pub fn gt_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::with_coercion(
            left_field,
            CompareOp::Gt,
            right_field,
            CoercionId::NumericWiden,
        ))
    }

    /// Compare `left_field >= right_field`.
    #[must_use]
    pub fn gte_fields(left_field: String, right_field: String) -> Self {
        Self::CompareFields(CompareFieldsPredicate::with_coercion(
            left_field,
            CompareOp::Gte,
            right_field,
            CoercionId::NumericWiden,
        ))
    }

    /// Compare `field IN values`.
    #[must_use]
    pub fn in_(field: String, values: Vec<Value>) -> Self {
        Self::Compare(ComparePredicate::in_(field, values))
    }

    /// Compare `field NOT IN values`.
    #[must_use]
    pub fn not_in(field: String, values: Vec<Value>) -> Self {
        Self::Compare(ComparePredicate::not_in(field, values))
    }

    /// Compare `field IS NOT NULL`.
    #[must_use]
    pub const fn is_not_null(field: String) -> Self {
        Self::IsNotNull { field }
    }

    /// Compare `field BETWEEN lower AND upper`.
    #[must_use]
    pub fn between(field: String, lower: Value, upper: Value) -> Self {
        Self::And(vec![
            Self::gte(field.clone(), lower),
            Self::lte(field, upper),
        ])
    }

    /// Compare `field NOT BETWEEN lower AND upper`.
    #[must_use]
    pub fn not_between(field: String, lower: Value, upper: Value) -> Self {
        Self::Or(vec![Self::lt(field.clone(), lower), Self::gt(field, upper)])
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

#[cfg_attr(doc, doc = "CompareOp")]
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
    /// Return the stable wire tag for this compare operator.
    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }

    /// Return whether this operator is one symmetric equality-style compare.
    #[must_use]
    pub const fn is_equality_family(self) -> bool {
        matches!(self, Self::Eq | Self::Ne)
    }

    /// Return whether this operator is one ordered range-bound compare.
    #[must_use]
    pub const fn is_ordering_family(self) -> bool {
        matches!(self, Self::Lt | Self::Lte | Self::Gt | Self::Gte)
    }

    /// Return whether this operator is one list-membership compare.
    #[must_use]
    pub const fn is_membership_family(self) -> bool {
        matches!(self, Self::In | Self::NotIn)
    }

    /// Return whether this operator is one containment compare.
    #[must_use]
    pub const fn is_contains_family(self) -> bool {
        matches!(self, Self::Contains)
    }

    /// Return whether this operator is one text-pattern compare.
    #[must_use]
    pub const fn is_text_pattern_family(self) -> bool {
        matches!(self, Self::StartsWith | Self::EndsWith)
    }

    /// Return whether this operator supports direct field-to-field comparison.
    #[must_use]
    pub const fn supports_field_compare(self) -> bool {
        self.is_equality_family() || self.is_ordering_family()
    }

    /// Return whether this operator contributes one lower bound and whether it
    /// is inclusive when present.
    #[must_use]
    pub const fn lower_bound_inclusive(self) -> Option<bool> {
        match self {
            Self::Gt => Some(false),
            Self::Gte => Some(true),
            Self::Eq
            | Self::Ne
            | Self::Lt
            | Self::Lte
            | Self::In
            | Self::NotIn
            | Self::Contains
            | Self::StartsWith
            | Self::EndsWith => None,
        }
    }

    /// Return whether this operator contributes one upper bound and whether it
    /// is inclusive when present.
    #[must_use]
    pub const fn upper_bound_inclusive(self) -> Option<bool> {
        match self {
            Self::Lt => Some(false),
            Self::Lte => Some(true),
            Self::Eq
            | Self::Ne
            | Self::Gt
            | Self::Gte
            | Self::In
            | Self::NotIn
            | Self::Contains
            | Self::StartsWith
            | Self::EndsWith => None,
        }
    }

    /// Return the operator that preserves semantics when the two operands are swapped.
    #[must_use]
    pub const fn flipped(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Ne => Self::Ne,
            Self::Lt => Self::Gt,
            Self::Lte => Self::Gte,
            Self::Gt => Self::Lt,
            Self::Gte => Self::Lte,
            Self::In => Self::In,
            Self::NotIn => Self::NotIn,
            Self::Contains => Self::Contains,
            Self::StartsWith => Self::StartsWith,
            Self::EndsWith => Self::EndsWith,
        }
    }
}

#[cfg_attr(doc, doc = "ComparePredicate")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparePredicate {
    pub(crate) field: String,
    pub(crate) op: CompareOp,
    pub(crate) value: Value,
    pub(crate) coercion: CoercionSpec,
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
    ///
    /// This is the low-level predicate AST constructor used by SQL lowering,
    /// generated index predicates, and tests that need a precise coercion
    /// contract. It does not validate field existence, operator/literal
    /// compatibility, or schema admissibility; those checks belong to predicate
    /// validation and query planning.
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

    /// Build `Eq` comparison.
    #[must_use]
    pub fn eq(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Eq, value)
    }

    /// Build `Ne` comparison.
    #[must_use]
    pub fn ne(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Ne, value)
    }

    /// Build `Lt` comparison.
    #[must_use]
    pub fn lt(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Lt, value)
    }

    /// Build `Lte` comparison.
    #[must_use]
    pub fn lte(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Lte, value)
    }

    /// Build `Gt` comparison.
    #[must_use]
    pub fn gt(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Gt, value)
    }

    /// Build `Gte` comparison.
    #[must_use]
    pub fn gte(field: String, value: Value) -> Self {
        Self::new(field, CompareOp::Gte, value)
    }

    /// Build `In` comparison.
    #[must_use]
    pub fn in_(field: String, values: Vec<Value>) -> Self {
        Self::new(field, CompareOp::In, Value::List(values))
    }

    /// Build `NotIn` comparison.
    #[must_use]
    pub fn not_in(field: String, values: Vec<Value>) -> Self {
        Self::new(field, CompareOp::NotIn, Value::List(values))
    }

    /// Borrow the compared field name.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Return the compare operator.
    #[must_use]
    pub const fn op(&self) -> CompareOp {
        self.op
    }

    /// Borrow the compared literal value.
    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// Borrow the comparison coercion policy.
    #[must_use]
    pub const fn coercion(&self) -> &CoercionSpec {
        &self.coercion
    }
}

///
/// CompareFieldsPredicate
///
/// Canonical predicate-owned field-to-field comparison leaf.
/// This keeps bounded compare expressions on the predicate authority seam
/// instead of routing them through projection-expression ownership.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompareFieldsPredicate {
    pub(crate) left_field: String,
    pub(crate) op: CompareOp,
    pub(crate) right_field: String,
    pub(crate) coercion: CoercionSpec,
}

impl CompareFieldsPredicate {
    fn canonicalize_symmetric_fields(
        op: CompareOp,
        left_field: String,
        right_field: String,
    ) -> (String, String) {
        if op.is_equality_family() && left_field < right_field {
            (right_field, left_field)
        } else {
            (left_field, right_field)
        }
    }

    fn new(left_field: String, op: CompareOp, right_field: String) -> Self {
        let (left_field, right_field) =
            Self::canonicalize_symmetric_fields(op, left_field, right_field);

        Self {
            left_field,
            op,
            right_field,
            coercion: CoercionSpec::default(),
        }
    }

    /// Construct a field-to-field comparison predicate with an explicit
    /// coercion policy.
    ///
    /// This low-level constructor preserves the provided comparison contract
    /// and only canonicalizes symmetric equality-family field order. It does
    /// not validate that the operator is field-comparison-admissible for a
    /// schema; that remains a validation/planning responsibility.
    #[must_use]
    pub fn with_coercion(
        left_field: impl Into<String>,
        op: CompareOp,
        right_field: impl Into<String>,
        coercion: CoercionId,
    ) -> Self {
        let (left_field, right_field) =
            Self::canonicalize_symmetric_fields(op, left_field.into(), right_field.into());

        Self {
            left_field,
            op,
            right_field,
            coercion: CoercionSpec::new(coercion),
        }
    }

    /// Build `Eq` field-to-field comparison.
    #[must_use]
    pub fn eq(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Eq, right_field)
    }

    /// Build `Ne` field-to-field comparison.
    #[must_use]
    pub fn ne(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Ne, right_field)
    }

    /// Build `Lt` field-to-field comparison.
    #[must_use]
    pub fn lt(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Lt, right_field)
    }

    /// Build `Lte` field-to-field comparison.
    #[must_use]
    pub fn lte(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Lte, right_field)
    }

    /// Build `Gt` field-to-field comparison.
    #[must_use]
    pub fn gt(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Gt, right_field)
    }

    /// Build `Gte` field-to-field comparison.
    #[must_use]
    pub fn gte(left_field: String, right_field: String) -> Self {
        Self::new(left_field, CompareOp::Gte, right_field)
    }

    /// Borrow the left compared field name.
    #[must_use]
    pub fn left_field(&self) -> &str {
        &self.left_field
    }

    /// Return the compare operator.
    #[must_use]
    pub const fn op(&self) -> CompareOp {
        self.op
    }

    /// Borrow the right compared field name.
    #[must_use]
    pub fn right_field(&self) -> &str {
        &self.right_field
    }

    /// Borrow the comparison coercion policy.
    #[must_use]
    pub const fn coercion(&self) -> &CoercionSpec {
        &self.coercion
    }
}

#[cfg_attr(
    doc,
    doc = "UnsupportedQueryFeature\n\nPolicy-level query features intentionally rejected by the engine."
)]
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum UnsupportedQueryFeature {
    #[error("map field '{field}' is not queryable; use scalar/indexed fields or list entries")]
    MapPredicate { field: String },
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_predicate_builders_preserve_operator_shape() {
        assert_eq!(
            Predicate::gt("age".to_string(), Value::Nat(7)),
            Predicate::Compare(ComparePredicate::gt("age".to_string(), Value::Nat(7))),
        );
    }
}
