//! Module: query::builder::field
//! Responsibility: zero-allocation field references and field-scoped predicate builders.
//! Does not own: predicate validation or runtime execution.
//! Boundary: ergonomic query-builder surface for field expressions.

use crate::db::query::expr::{FilterExpr, FilterValue};
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

    /// Internal field-to-field comparison expression builder.
    fn cmp_field(
        self,
        other: impl AsRef<str>,
        build: impl FnOnce(String, String) -> FilterExpr,
    ) -> FilterExpr {
        build(self.0.to_string(), other.as_ref().to_string())
    }

    // ------------------------------------------------------------------
    // Comparison predicates
    // ------------------------------------------------------------------

    /// Strict equality comparison (no coercion).
    #[must_use]
    pub fn eq(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::eq(self.0, value)
    }

    /// Case-insensitive equality for text fields.
    #[must_use]
    pub fn text_eq_ci(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::eq_ci(self.0, value)
    }

    /// Strict inequality comparison.
    #[must_use]
    pub fn ne(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::ne(self.0, value)
    }

    /// Less-than comparison with numeric widening.
    #[must_use]
    pub fn lt(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::lt(self.0, value)
    }

    /// Less-than-or-equal comparison with numeric widening.
    #[must_use]
    pub fn lte(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::lte(self.0, value)
    }

    /// Greater-than comparison with numeric widening.
    #[must_use]
    pub fn gt(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::gt(self.0, value)
    }

    /// Greater-than-or-equal comparison with numeric widening.
    #[must_use]
    pub fn gte(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::gte(self.0, value)
    }

    /// Strict equality comparison against another field.
    #[must_use]
    pub fn eq_field(self, other: impl AsRef<str>) -> FilterExpr {
        self.cmp_field(other, FilterExpr::eq_field)
    }

    /// Strict inequality comparison against another field.
    #[must_use]
    pub fn ne_field(self, other: impl AsRef<str>) -> FilterExpr {
        self.cmp_field(other, FilterExpr::ne_field)
    }

    /// Less-than comparison against another numeric or text field.
    #[must_use]
    pub fn lt_field(self, other: impl AsRef<str>) -> FilterExpr {
        self.cmp_field(other, FilterExpr::lt_field)
    }

    /// Less-than-or-equal comparison against another numeric or text field.
    #[must_use]
    pub fn lte_field(self, other: impl AsRef<str>) -> FilterExpr {
        self.cmp_field(other, FilterExpr::lte_field)
    }

    /// Greater-than comparison against another numeric or text field.
    #[must_use]
    pub fn gt_field(self, other: impl AsRef<str>) -> FilterExpr {
        self.cmp_field(other, FilterExpr::gt_field)
    }

    /// Greater-than-or-equal comparison against another numeric or text field.
    #[must_use]
    pub fn gte_field(self, other: impl AsRef<str>) -> FilterExpr {
        self.cmp_field(other, FilterExpr::gte_field)
    }

    /// Membership test against a fixed list (strict).
    #[must_use]
    pub fn in_list<I, V>(self, values: I) -> FilterExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<FilterValue>,
    {
        FilterExpr::in_list(self.0, values)
    }

    // ------------------------------------------------------------------
    // Structural predicates
    // ------------------------------------------------------------------

    /// Field is present and explicitly null.
    #[must_use]
    pub fn is_null(self) -> FilterExpr {
        FilterExpr::is_null(self.0)
    }

    /// Field is present and not null.
    #[must_use]
    pub fn is_not_null(self) -> FilterExpr {
        FilterExpr::is_not_null(self.0)
    }

    /// Field is not present at all.
    #[must_use]
    pub fn is_missing(self) -> FilterExpr {
        FilterExpr::is_missing(self.0)
    }

    /// Field is present but empty (collection- or string-specific).
    #[must_use]
    pub fn is_empty(self) -> FilterExpr {
        FilterExpr::is_empty(self.0)
    }

    /// Field is present and non-empty.
    #[must_use]
    pub fn is_not_empty(self) -> FilterExpr {
        FilterExpr::is_not_empty(self.0)
    }

    /// Case-sensitive substring match for text fields.
    #[must_use]
    pub fn text_contains(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::text_contains(self.0, value)
    }

    /// Case-insensitive substring match for text fields.
    #[must_use]
    pub fn text_contains_ci(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::text_contains_ci(self.0, value)
    }

    /// Case-sensitive prefix match for text fields.
    #[must_use]
    pub fn text_starts_with(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::starts_with(self.0, value)
    }

    /// Case-insensitive prefix match for text fields.
    #[must_use]
    pub fn text_starts_with_ci(self, value: impl Into<FilterValue>) -> FilterExpr {
        FilterExpr::starts_with_ci(self.0, value)
    }

    /// Inclusive range predicate lowered as `field >= lower AND field <= upper`.
    #[must_use]
    pub fn between(
        self,
        lower: impl Into<FilterValue>,
        upper: impl Into<FilterValue>,
    ) -> FilterExpr {
        FilterExpr::and(vec![self.gte(lower), self.lte(upper)])
    }

    /// Inclusive range predicate against two other fields.
    #[must_use]
    pub fn between_fields(self, lower: impl AsRef<str>, upper: impl AsRef<str>) -> FilterExpr {
        FilterExpr::and(vec![self.gte_field(lower), self.lte_field(upper)])
    }

    /// Exclusive-outside range predicate lowered as `field < lower OR field > upper`.
    #[must_use]
    pub fn not_between(
        self,
        lower: impl Into<FilterValue>,
        upper: impl Into<FilterValue>,
    ) -> FilterExpr {
        FilterExpr::or(vec![self.lt(lower), self.gt(upper)])
    }

    /// Exclusive-outside range predicate against two other fields.
    #[must_use]
    pub fn not_between_fields(self, lower: impl AsRef<str>, upper: impl AsRef<str>) -> FilterExpr {
        FilterExpr::or(vec![self.lt_field(lower), self.gt_field(upper)])
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
    use super::{FieldRef, FilterExpr};

    #[test]
    fn field_ref_text_starts_with_uses_strict_prefix_compare() {
        assert_eq!(
            FieldRef::new("name").text_starts_with("Al"),
            FilterExpr::starts_with("name", "Al"),
        );
    }

    #[test]
    fn field_ref_text_starts_with_ci_uses_casefold_prefix_compare() {
        assert_eq!(
            FieldRef::new("name").text_starts_with_ci("AL"),
            FilterExpr::starts_with_ci("name", "AL"),
        );
    }

    #[test]
    fn field_ref_gt_field_builds_field_compare_filter_expr() {
        assert_eq!(
            FieldRef::new("age").gt_field("rank"),
            FilterExpr::gt_field("age", "rank"),
        );
    }

    #[test]
    fn field_ref_not_between_builds_outside_range_filter_expr() {
        assert_eq!(
            FieldRef::new("age").not_between(10_u64, 20_u64),
            FilterExpr::or(vec![
                FilterExpr::lt("age", 10_u64),
                FilterExpr::gt("age", 20_u64),
            ])
        );
    }

    #[test]
    fn field_ref_between_fields_builds_field_bound_range_filter_expr() {
        assert_eq!(
            FieldRef::new("age").between_fields("min_age", "max_age"),
            FilterExpr::and(vec![
                FilterExpr::gte_field("age", "min_age"),
                FilterExpr::lte_field("age", "max_age"),
            ])
        );
    }
}
