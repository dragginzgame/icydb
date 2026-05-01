//! Module: query::plan::planner::range
//! Responsibility: planner range-constraint extraction and index-range candidate derivation.
//! Does not own: runtime range traversal execution or cursor persistence format.
//! Boundary: computes planner-side range constraints from predicate semantics.

mod bounds;
mod extract;
#[cfg(test)]
mod tests;

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
