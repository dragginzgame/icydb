//! Pure plan-layer data types; must not embed planning semantics or validation.

use crate::{model::index::IndexModel, value::Value};
use serde::{Deserialize, Serialize};
use std::ops::Bound;

pub(crate) type IndexRangePathRef<'a> = (
    &'a IndexModel,
    &'a [Value],
    &'a Bound<Value>,
    &'a Bound<Value>,
);

///
/// SemanticIndexRangeSpec
///
/// Planner-owned semantic index-range request for one secondary index path.
/// Stores field-slot shape plus semantic bounds only; no encoded/raw key material.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SemanticIndexRangeSpec {
    index: IndexModel,
    field_slots: Vec<usize>,
    prefix_values: Vec<Value>,
    lower: Bound<Value>,
    upper: Bound<Value>,
}

impl SemanticIndexRangeSpec {
    #[must_use]
    pub(crate) fn new(
        index: IndexModel,
        field_slots: Vec<usize>,
        prefix_values: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    ) -> Self {
        debug_assert!(
            !field_slots.is_empty(),
            "semantic index-range field slots must include the range slot",
        );
        debug_assert_eq!(
            field_slots.len(),
            prefix_values.len().saturating_add(1),
            "semantic index-range slots must include one slot per prefix field plus range slot",
        );
        debug_assert!(
            prefix_values.len() < index.fields.len(),
            "semantic index-range prefix must be shorter than index arity",
        );

        Self {
            index,
            field_slots,
            prefix_values,
            lower,
            upper,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn from_prefix_and_bounds(
        index: IndexModel,
        prefix_values: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    ) -> Self {
        let slot_count = prefix_values.len().saturating_add(1);
        let field_slots = (0..slot_count).collect();

        Self::new(index, field_slots, prefix_values, lower, upper)
    }

    #[must_use]
    pub(crate) const fn index(&self) -> &IndexModel {
        &self.index
    }

    #[must_use]
    pub(crate) const fn field_slots(&self) -> &[usize] {
        self.field_slots.as_slice()
    }

    #[must_use]
    pub(crate) const fn prefix_values(&self) -> &[Value] {
        self.prefix_values.as_slice()
    }

    #[must_use]
    pub(crate) const fn lower(&self) -> &Bound<Value> {
        &self.lower
    }

    #[must_use]
    pub(crate) const fn upper(&self) -> &Bound<Value> {
        &self.upper
    }
}

///
/// AccessPlan
/// Composite access structure; may include unions/intersections and is executor-resolvable.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AccessPlan<K> {
    Path(Box<AccessPath<K>>),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

impl<K> AccessPlan<K> {
    /// Construct a plan from one concrete access path.
    #[must_use]
    pub(crate) fn path(path: AccessPath<K>) -> Self {
        Self::Path(Box::new(path))
    }

    /// Construct a plan that forces a full scan.
    #[must_use]
    pub(crate) fn full_scan() -> Self {
        Self::path(AccessPath::FullScan)
    }

    /// Borrow the concrete path when this plan is a single-path node.
    #[must_use]
    pub(crate) fn as_path(&self) -> Option<&AccessPath<K>> {
        match self {
            Self::Path(path) => Some(path.as_ref()),
            Self::Union(_) | Self::Intersection(_) => None,
        }
    }

    /// Return true when this plan is exactly one full-scan path.
    #[must_use]
    pub(crate) const fn is_single_full_scan(&self) -> bool {
        matches!(self, Self::Path(path) if path.is_full_scan())
    }

    /// Borrow index-prefix access details when this is a single IndexPrefix path.
    #[must_use]
    pub(crate) fn as_index_prefix_path(&self) -> Option<(&IndexModel, &[Value])> {
        self.as_path().and_then(AccessPath::as_index_prefix)
    }

    /// Borrow index-range access details when this is a single IndexRange path.
    #[must_use]
    pub(crate) fn as_index_range_path(&self) -> Option<IndexRangePathRef<'_>> {
        self.as_path().and_then(AccessPath::as_index_range)
    }

    /// Walk the tree and return the first encountered IndexRange details.
    #[must_use]
    pub(crate) fn first_index_range_details(&self) -> Option<(&'static str, usize)> {
        match self {
            Self::Path(path) => path.index_range_details(),
            Self::Union(children) | Self::Intersection(children) => {
                children.iter().find_map(Self::first_index_range_details)
            }
        }
    }
}

impl<K> From<AccessPath<K>> for AccessPlan<K> {
    fn from(value: AccessPath<K>) -> Self {
        Self::path(value)
    }
}

///
/// AccessPath
/// Concrete, executor-facing access path selected by the planner.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AccessPath<K> {
    /// Direct lookup by a single primary key.
    ByKey(K),

    /// Batched lookup by multiple primary keys.
    ///
    /// Keys are treated as a set; order is canonicalized and duplicates are ignored.
    /// Empty key lists are a valid no-op and return no rows.
    ByKeys(Vec<K>),

    /// Range scan over primary keys (inclusive).
    KeyRange { start: K, end: K },

    /// Index scan using a prefix of index fields and bound values.
    ///
    /// The planner guarantees:
    /// - `values.len() <= index.fields.len()`
    /// - All values correspond to strict coercions
    IndexPrefix {
        index: IndexModel,
        values: Vec<Value>,
    },

    /// Index scan using an equality prefix plus one bounded range component.
    ///
    /// This variant is dedicated to secondary range traversal and wraps
    /// planner-owned semantic range metadata.
    IndexRange { spec: SemanticIndexRangeSpec },

    /// Full entity scan with no index assistance.
    FullScan,
}

impl<K> AccessPath<K> {
    /// Construct one semantic index-range path from semantic bounds.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn index_range(
        index: IndexModel,
        prefix_values: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    ) -> Self {
        Self::IndexRange {
            spec: SemanticIndexRangeSpec::from_prefix_and_bounds(
                index,
                prefix_values,
                lower,
                upper,
            ),
        }
    }

    /// Return true when this path is a full scan.
    #[must_use]
    pub(crate) const fn is_full_scan(&self) -> bool {
        matches!(self, Self::FullScan)
    }

    /// Borrow index-prefix details when this path is `IndexPrefix`.
    #[must_use]
    pub(crate) fn as_index_prefix(&self) -> Option<(&IndexModel, &[Value])> {
        match self {
            Self::IndexPrefix { index, values } => Some((index, values)),
            _ => None,
        }
    }

    /// Borrow index-range details when this path is `IndexRange`.
    #[must_use]
    pub(crate) const fn as_index_range(&self) -> Option<IndexRangePathRef<'_>> {
        match self {
            Self::IndexRange { spec } => Some((
                spec.index(),
                spec.prefix_values(),
                spec.lower(),
                spec.upper(),
            )),
            _ => None,
        }
    }

    /// Return index-range identity details when this path is `IndexRange`.
    #[must_use]
    pub(crate) const fn index_range_details(&self) -> Option<(&'static str, usize)> {
        match self {
            Self::IndexRange { spec } => Some((spec.index().name, spec.prefix_values().len())),
            _ => None,
        }
    }
}

///
/// Direction
/// Executor-facing traversal direction for ordered execution and continuations.
///

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Direction {
    #[default]
    Asc,
    Desc,
}

///
/// OrderDirection
/// Executor-facing ordering direction (applied after filtering).
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

///
/// OrderSpec
/// Executor-facing ordering specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderSpec {
    pub(crate) fields: Vec<(String, OrderDirection)>,
}

///
/// DeleteLimitSpec
/// Executor-facing delete bound with no offsets.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteLimitSpec {
    pub max_rows: u32,
}

///
/// PageSpec
/// Executor-facing pagination specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PageSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}
