//! Module: query::builder::field
//! Responsibility: zero-allocation field references and field-scoped predicate builders.
//! Does not own: predicate validation or runtime execution.
//! Boundary: ergonomic query-builder surface for field expressions.

use crate::{
    db::predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
    traits::FieldValue,
    value::Value,
};
use derive_more::Deref;

///
/// FieldRef
///
/// Zero-cost wrapper around a static field name used in predicates.
/// Enables method-based predicate builders without allocating.
/// Carries only a `&'static str` and derefs to `str`.
///

#[derive(Clone, Copy, Deref, Eq, Hash, PartialEq)]
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
    // Internal helpers
    // ------------------------------------------------------------------

    /// Internal comparison predicate builder.
    fn cmp(self, op: CompareOp, value: impl FieldValue, coercion: CoercionId) -> Predicate {
        Predicate::Compare(ComparePredicate::with_coercion(
            self.0,
            op,
            value.to_value(),
            coercion,
        ))
    }

    /// Internal field-to-field comparison predicate builder.
    fn cmp_field(self, op: CompareOp, other: impl AsRef<str>, coercion: CoercionId) -> Predicate {
        Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            self.0,
            op,
            other.as_ref(),
            coercion,
        ))
    }

    // ------------------------------------------------------------------
    // Comparison predicates
    // ------------------------------------------------------------------

    /// Strict equality comparison (no coercion).
    #[must_use]
    pub fn eq(self, value: impl FieldValue) -> Predicate {
        self.cmp(CompareOp::Eq, value, CoercionId::Strict)
    }

    /// Case-insensitive equality for text fields.
    #[must_use]
    pub fn text_eq_ci(self, value: impl FieldValue) -> Predicate {
        self.cmp(CompareOp::Eq, value, CoercionId::TextCasefold)
    }

    /// Strict inequality comparison.
    #[must_use]
    pub fn ne(self, value: impl FieldValue) -> Predicate {
        self.cmp(CompareOp::Ne, value, CoercionId::Strict)
    }

    /// Less-than comparison with numeric widening.
    #[must_use]
    pub fn lt(self, value: impl FieldValue) -> Predicate {
        self.cmp(CompareOp::Lt, value, CoercionId::NumericWiden)
    }

    /// Less-than-or-equal comparison with numeric widening.
    #[must_use]
    pub fn lte(self, value: impl FieldValue) -> Predicate {
        self.cmp(CompareOp::Lte, value, CoercionId::NumericWiden)
    }

    /// Greater-than comparison with numeric widening.
    #[must_use]
    pub fn gt(self, value: impl FieldValue) -> Predicate {
        self.cmp(CompareOp::Gt, value, CoercionId::NumericWiden)
    }

    /// Greater-than-or-equal comparison with numeric widening.
    #[must_use]
    pub fn gte(self, value: impl FieldValue) -> Predicate {
        self.cmp(CompareOp::Gte, value, CoercionId::NumericWiden)
    }

    /// Strict equality comparison against another field.
    #[must_use]
    pub fn eq_field(self, other: impl AsRef<str>) -> Predicate {
        self.cmp_field(CompareOp::Eq, other, CoercionId::Strict)
    }

    /// Strict inequality comparison against another field.
    #[must_use]
    pub fn ne_field(self, other: impl AsRef<str>) -> Predicate {
        self.cmp_field(CompareOp::Ne, other, CoercionId::Strict)
    }

    /// Less-than comparison against another numeric or text field.
    #[must_use]
    pub fn lt_field(self, other: impl AsRef<str>) -> Predicate {
        self.cmp_field(CompareOp::Lt, other, CoercionId::NumericWiden)
    }

    /// Less-than-or-equal comparison against another numeric or text field.
    #[must_use]
    pub fn lte_field(self, other: impl AsRef<str>) -> Predicate {
        self.cmp_field(CompareOp::Lte, other, CoercionId::NumericWiden)
    }

    /// Greater-than comparison against another numeric or text field.
    #[must_use]
    pub fn gt_field(self, other: impl AsRef<str>) -> Predicate {
        self.cmp_field(CompareOp::Gt, other, CoercionId::NumericWiden)
    }

    /// Greater-than-or-equal comparison against another numeric or text field.
    #[must_use]
    pub fn gte_field(self, other: impl AsRef<str>) -> Predicate {
        self.cmp_field(CompareOp::Gte, other, CoercionId::NumericWiden)
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

    /// Field is present and not null.
    #[must_use]
    pub fn is_not_null(self) -> Predicate {
        Predicate::IsNotNull {
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

    /// Case-sensitive prefix match for text fields.
    #[must_use]
    pub fn text_starts_with(self, value: impl FieldValue) -> Predicate {
        self.cmp(CompareOp::StartsWith, value, CoercionId::Strict)
    }

    /// Case-insensitive prefix match for text fields.
    #[must_use]
    pub fn text_starts_with_ci(self, value: impl FieldValue) -> Predicate {
        self.cmp(CompareOp::StartsWith, value, CoercionId::TextCasefold)
    }

    /// Inclusive range predicate lowered as `field >= lower AND field <= upper`.
    #[must_use]
    pub fn between(self, lower: impl FieldValue, upper: impl FieldValue) -> Predicate {
        Predicate::and(vec![self.gte(lower), self.lte(upper)])
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_ref_text_starts_with_uses_strict_prefix_compare() {
        let predicate = FieldRef::new("name").text_starts_with("Al");
        let Predicate::Compare(compare) = predicate else {
            panic!("expected compare predicate");
        };

        assert_eq!(compare.field, "name");
        assert_eq!(compare.op, CompareOp::StartsWith);
        assert_eq!(compare.coercion.id, CoercionId::Strict);
        assert_eq!(compare.value, Value::Text("Al".to_string()));
    }

    #[test]
    fn field_ref_text_starts_with_ci_uses_casefold_prefix_compare() {
        let predicate = FieldRef::new("name").text_starts_with_ci("AL");
        let Predicate::Compare(compare) = predicate else {
            panic!("expected compare predicate");
        };

        assert_eq!(compare.field, "name");
        assert_eq!(compare.op, CompareOp::StartsWith);
        assert_eq!(compare.coercion.id, CoercionId::TextCasefold);
        assert_eq!(compare.value, Value::Text("AL".to_string()));
    }

    #[test]
    fn field_ref_gt_field_builds_compare_fields_predicate() {
        let predicate = FieldRef::new("age").gt_field("rank");
        let Predicate::CompareFields(compare) = predicate else {
            panic!("expected field-to-field compare predicate");
        };

        assert_eq!(compare.left_field(), "age");
        assert_eq!(compare.op(), CompareOp::Gt);
        assert_eq!(compare.right_field(), "rank");
        assert_eq!(compare.coercion().id, CoercionId::NumericWiden);
    }
}
