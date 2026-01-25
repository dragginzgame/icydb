//! Pure plan-layer data types; must not embed planning semantics or validation.

use crate::{key::Key, model::index::IndexModel, value::Value};

///
/// AccessPlan
/// Composite access structure; may include unions/intersections and is executor-resolvable.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessPlan {
    Path(AccessPath),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

impl AccessPlan {
    /// Construct a plan that forces a full scan.
    #[must_use]
    pub const fn full_scan() -> Self {
        Self::Path(AccessPath::FullScan)
    }
}

///
/// AccessPath
/// Concrete, executor-facing access path selected by the planner.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessPath {
    /// Direct lookup by a single primary key.
    ByKey(Key),

    /// Batched lookup by multiple primary keys.
    ByKeys(Vec<Key>),

    /// Range scan over primary keys (inclusive).
    KeyRange { start: Key, end: Key },

    /// Index scan using a prefix of index fields and bound values.
    ///
    /// The planner guarantees:
    /// - `values.len() <= index.fields.len()`
    /// - All values correspond to strict coercions
    IndexPrefix {
        index: IndexModel,
        values: Vec<Value>,
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
/// ProjectionSpec
/// Executor-facing projection specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionSpec {
    /// Return all fields (default).
    All,
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
