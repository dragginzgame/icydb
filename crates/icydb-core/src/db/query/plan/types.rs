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

    /// Return cursor continuation support for this access-plan node.
    #[must_use]
    pub(crate) const fn cursor_support(&self) -> CursorSupport {
        match self {
            Self::Path(path) => path.cursor_support(),
            Self::Union(_) | Self::Intersection(_) => CursorSupport::None,
        }
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
    #[allow(dead_code)]
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
    /// This variant is dedicated to secondary range traversal and must not be
    /// conflated with primary-key `KeyRange`.
    ///
    /// The planner guarantees:
    /// - `prefix.len() < index.fields.len()`
    /// - Prefix values correspond to the first `prefix.len()` index fields
    /// - `lower` and `upper` bound the next index component
    IndexRange {
        index: IndexModel,
        prefix: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    },

    /// Full entity scan with no index assistance.
    FullScan,
}

///
/// CursorSupport
/// Cursor-continuation capability exposed by an access path/plan.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CursorSupport {
    None,
    IndexRangeAnchor,
}

impl CursorSupport {
    /// Return true when this support surface accepts index-range anchors.
    #[must_use]
    pub(crate) const fn supports_index_range_anchor(self) -> bool {
        matches!(self, Self::IndexRangeAnchor)
    }
}

impl<K> AccessPath<K> {
    /// Return true when this path is a full scan.
    #[must_use]
    pub(crate) const fn is_full_scan(&self) -> bool {
        matches!(self, Self::FullScan)
    }

    /// Return true when this path is eligible for PK streaming (`FullScan` or `KeyRange`).
    #[must_use]
    pub(crate) const fn is_full_scan_or_key_range(&self) -> bool {
        matches!(self, Self::FullScan | Self::KeyRange { .. })
    }

    /// Return true when this path is backed by a secondary index.
    #[must_use]
    pub(crate) const fn is_index_path(&self) -> bool {
        matches!(self, Self::IndexPrefix { .. } | Self::IndexRange { .. })
    }

    /// Return cursor continuation support for this concrete path.
    #[must_use]
    pub(crate) const fn cursor_support(&self) -> CursorSupport {
        match self {
            Self::IndexRange { .. } => CursorSupport::IndexRangeAnchor,
            _ => CursorSupport::None,
        }
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
    pub(crate) fn as_index_range(&self) -> Option<IndexRangePathRef<'_>> {
        match self {
            Self::IndexRange {
                index,
                prefix,
                lower,
                upper,
            } => Some((index, prefix, lower, upper)),
            _ => None,
        }
    }

    /// Return index-range identity details when this path is `IndexRange`.
    #[must_use]
    pub(crate) const fn index_range_details(&self) -> Option<(&'static str, usize)> {
        match self {
            Self::IndexRange { index, prefix, .. } => Some((index.name, prefix.len())),
            _ => None,
        }
    }
}

impl<K: Copy> AccessPath<K> {
    /// Return canonical PK-stream bounds for `FullScan` or `KeyRange` paths.
    #[must_use]
    pub(crate) const fn pk_stream_bounds(&self) -> Option<(Option<K>, Option<K>)> {
        match self {
            Self::FullScan => Some((None, None)),
            Self::KeyRange { start, end } => Some((Some(*start), Some(*end))),
            _ => None,
        }
    }
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

///
/// CursorBoundarySlot
/// Slot value used for deterministic cursor boundaries.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum CursorBoundarySlot {
    Missing,
    Present(Value),
}

///
/// CursorBoundary
/// Ordered boundary tuple for continuation pagination.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CursorBoundary {
    pub(crate) slots: Vec<CursorBoundarySlot>,
}
