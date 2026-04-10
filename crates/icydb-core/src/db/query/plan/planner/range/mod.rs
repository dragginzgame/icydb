//! Module: query::plan::planner::range
//! Responsibility: planner range-constraint extraction and index-range candidate derivation.
//! Does not own: runtime range traversal execution or cursor persistence format.
//! Boundary: computes planner-side range constraints from predicate semantics.

mod bounds;
mod extract;

use crate::{db::predicate::ComparePredicate, value::Value};
use std::ops::Bound;

pub(in crate::db::query::plan::planner) use extract::{
    index_range_from_and, primary_key_range_from_and,
};

///
/// RangeConstraint
///
/// One-field bounded interval used for index-range candidate extraction.
///
#[derive(Clone, Debug, Eq, PartialEq)]
struct RangeConstraint {
    lower: Bound<Value>,
    upper: Bound<Value>,
}

impl Default for RangeConstraint {
    fn default() -> Self {
        Self {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }
}

///
/// IndexFieldConstraint
///
/// Per-index-field constraint classification while extracting range candidates.
///
#[derive(Clone, Debug, Eq, PartialEq)]
enum IndexFieldConstraint {
    None,
    Eq(Value),
    Range(RangeConstraint),
}

///
/// CachedCompare
///
/// Compare predicate plus precomputed planner-side schema compatibility.
///
#[derive(Clone)]
struct CachedCompare<'a> {
    cmp: &'a ComparePredicate,
    literal_compatible: bool,
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            numeric::compare_numeric_or_strict_order,
            query::plan::planner::range::bounds::compare_range_bound_values,
        },
        value::Value,
    };
    use std::cmp::Ordering;

    #[test]
    fn range_bound_numeric_compare_reuses_shared_numeric_authority() {
        let left = Value::Int(10);
        let right = Value::Uint(10);

        assert_eq!(
            compare_range_bound_values(&left, &right),
            compare_numeric_or_strict_order(&left, &right),
            "planner range numeric bounds should delegate to shared numeric comparator",
        );
    }

    #[test]
    fn range_bound_mixed_non_numeric_values_are_incomparable() {
        assert_eq!(
            compare_range_bound_values(&Value::Text("x".to_string()), &Value::Uint(1)),
            None,
            "mixed non-numeric variants should remain incomparable in range planning",
        );
    }

    #[test]
    fn range_bound_same_variant_non_numeric_uses_strict_ordering() {
        assert_eq!(
            compare_range_bound_values(
                &Value::Text("a".to_string()),
                &Value::Text("b".to_string())
            ),
            Some(Ordering::Less),
            "same-variant non-numeric bounds should use strict value ordering",
        );
    }
}
