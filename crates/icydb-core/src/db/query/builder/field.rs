use crate::{
    db::query::predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
    traits::FieldValue,
    value::Value,
};

///
/// FieldRef
///
/// Zero-cost wrapper around a static field name used in predicates.
/// Enables method-based predicate builders without allocating.
/// Carries only a `&'static str` and derefs to `str`.
///

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct FieldRef(&'static str);

impl FieldRef {
    /// Create a new field reference.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Return the underlying field name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }

    // ------------------------------------------------------------------
    // Comparison predicates
    // ------------------------------------------------------------------

    /// Strict equality comparison (no coercion).
    #[must_use]
    pub fn eq(self, value: impl FieldValue) -> Predicate {
        Predicate::Compare(ComparePredicate::with_coercion(
            self.0,
            CompareOp::Eq,
            value.to_value(),
            CoercionId::Strict,
        ))
    }

    /// Case-insensitive equality for text fields.
    #[must_use]
    pub fn text_eq_ci(self, value: impl FieldValue) -> Predicate {
        Predicate::Compare(ComparePredicate::with_coercion(
            self.0,
            CompareOp::Eq,
            value.to_value(),
            CoercionId::TextCasefold,
        ))
    }

    /// Strict inequality comparison.
    #[must_use]
    pub fn ne(self, value: impl FieldValue) -> Predicate {
        Predicate::Compare(ComparePredicate::with_coercion(
            self.0,
            CompareOp::Ne,
            value.to_value(),
            CoercionId::Strict,
        ))
    }

    /// Less-than comparison with numeric widening.
    #[must_use]
    pub fn lt(self, value: impl FieldValue) -> Predicate {
        Predicate::Compare(ComparePredicate::with_coercion(
            self.0,
            CompareOp::Lt,
            value.to_value(),
            CoercionId::NumericWiden,
        ))
    }

    /// Less-than-or-equal comparison with numeric widening.
    #[must_use]
    pub fn lte(self, value: impl FieldValue) -> Predicate {
        Predicate::Compare(ComparePredicate::with_coercion(
            self.0,
            CompareOp::Lte,
            value.to_value(),
            CoercionId::NumericWiden,
        ))
    }

    /// Greater-than comparison with numeric widening.
    #[must_use]
    pub fn gt(self, value: impl FieldValue) -> Predicate {
        Predicate::Compare(ComparePredicate::with_coercion(
            self.0,
            CompareOp::Gt,
            value.to_value(),
            CoercionId::NumericWiden,
        ))
    }

    /// Greater-than-or-equal comparison with numeric widening.
    #[must_use]
    pub fn gte(self, value: impl FieldValue) -> Predicate {
        Predicate::Compare(ComparePredicate::with_coercion(
            self.0,
            CompareOp::Gte,
            value.to_value(),
            CoercionId::NumericWiden,
        ))
    }

    /// Membership test against a fixed list (strict).
    #[must_use]
    pub fn in_list<I, V>(self, values: I) -> Predicate
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        Predicate::Compare(ComparePredicate::with_coercion(
            self.0,
            CompareOp::In,
            Value::List(values.into_iter().map(|v| v.to_value()).collect()),
            CoercionId::Strict,
        ))
    }

    // ------------------------------------------------------------------
    // Structural predicates
    // ------------------------------------------------------------------

    /// Field is present and explicitly null.
    #[must_use]
    pub fn is_null(self) -> Predicate {
        Predicate::IsNull {
            field: self.0.to_string(),
        }
    }

    /// Field is not present at all.
    #[must_use]
    pub fn is_missing(self) -> Predicate {
        Predicate::IsMissing {
            field: self.0.to_string(),
        }
    }

    /// Field is present but empty (collection- or string-specific).
    #[must_use]
    pub fn is_empty(self) -> Predicate {
        Predicate::IsEmpty {
            field: self.0.to_string(),
        }
    }

    /// Field is present and non-empty.
    #[must_use]
    pub fn is_not_empty(self) -> Predicate {
        Predicate::IsNotEmpty {
            field: self.0.to_string(),
        }
    }

    /// Case-sensitive substring match for text fields.
    #[must_use]
    pub fn text_contains(self, value: impl FieldValue) -> Predicate {
        Predicate::TextContains {
            field: self.0.to_string(),
            value: value.to_value(),
        }
    }

    /// Case-insensitive substring match for text fields.
    #[must_use]
    pub fn text_contains_ci(self, value: impl FieldValue) -> Predicate {
        Predicate::TextContainsCi {
            field: self.0.to_string(),
            value: value.to_value(),
        }
    }
}

// ----------------------------------------------------------------------
// Boundary traits
// ----------------------------------------------------------------------

impl AsRef<str> for FieldRef {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl std::ops::Deref for FieldRef {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}
