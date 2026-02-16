//! Pure plan-layer data types; must not embed planning semantics or validation.

use crate::{model::index::IndexModel, value::Value};
use serde::{Deserialize, Serialize};
use std::ops::Bound;

///
/// AccessPlan
/// Composite access structure; may include unions/intersections and is executor-resolvable.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessPlan<K> {
    Path(Box<AccessPath<K>>),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

impl<K> AccessPlan<K> {
    /// Construct a plan from one concrete access path.
    #[must_use]
    pub fn path(path: AccessPath<K>) -> Self {
        Self::Path(Box::new(path))
    }

    /// Construct a plan that forces a full scan.
    #[must_use]
    pub fn full_scan() -> Self {
        Self::path(AccessPath::FullScan)
    }

    /// Borrow the concrete path when this plan is a single-path node.
    #[must_use]
    pub fn as_path(&self) -> Option<&AccessPath<K>> {
        match self {
            Self::Path(path) => Some(path.as_ref()),
            Self::Union(_) | Self::Intersection(_) => None,
        }
    }

    /// Consume this plan and return the concrete path when present.
    #[must_use]
    pub fn into_path(self) -> Option<AccessPath<K>> {
        match self {
            Self::Path(path) => Some(*path),
            Self::Union(_) | Self::Intersection(_) => None,
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
pub enum AccessPath<K> {
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
pub struct OrderSpec {
    pub fields: Vec<(String, OrderDirection)>,
}

///
/// DeleteLimitSpec
/// Executor-facing delete bound with no offsets.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteLimitSpec {
    pub max_rows: u32,
}

///
/// PageSpec
/// Executor-facing pagination specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}

///
/// CursorBoundarySlot
/// Slot value used for deterministic cursor boundaries.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CursorBoundarySlot {
    Missing,
    Present(Value),
}

///
/// CursorBoundary
/// Ordered boundary tuple for continuation pagination.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CursorBoundary {
    pub(crate) slots: Vec<CursorBoundarySlot>,
}
