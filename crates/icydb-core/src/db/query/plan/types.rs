//! Pure plan-layer data types; must not embed planning semantics or validation.

use crate::{model::index::IndexModel, value::Value};

///
/// AccessPlan
/// Composite access structure; may include unions/intersections and is executor-resolvable.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessPlan<K> {
    Path(AccessPath<K>),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

impl<K> AccessPlan<K> {
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
