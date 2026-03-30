//! Module: access::dispatch
//! Responsibility: canonical structural dispatch over semantic access contracts.
//! Does not own: planner path selection policy or executor stream traversal behavior.
//! Boundary: all non-construction AccessPath branching routes through this module.

use crate::{
    db::access::{
        AccessPath, AccessPlan, ExecutableAccessPath, ExecutionPathPayload, SemanticIndexRangeSpec,
    },
    model::index::IndexModel,
    value::Value,
};

///
/// AccessPathDispatch
///
/// Borrowed payload projection for one semantic access-path variant.
/// This keeps direct `AccessPath` matching centralized in one boundary.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db) enum AccessPathDispatch<'a, K> {
    ByKey(&'a K),
    ByKeys(&'a [K]),
    KeyRange {
        start: &'a K,
        end: &'a K,
    },
    IndexPrefix {
        index: IndexModel,
        values: &'a [Value],
    },
    IndexMultiLookup {
        index: IndexModel,
        values: &'a [Value],
    },
    IndexRange {
        spec: &'a SemanticIndexRangeSpec,
    },
    FullScan,
}

///
/// AccessPathKind
///
/// Coarse semantic path discriminator for callers that do not require payload.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AccessPathKind {
    ByKey,
    ByKeys,
    KeyRange,
    IndexPrefix,
    IndexMultiLookup,
    IndexRange,
    FullScan,
}

impl<K> AccessPathDispatch<'_, K> {
    #[must_use]
    pub(in crate::db) const fn kind(self) -> AccessPathKind {
        match self {
            Self::ByKey(_) => AccessPathKind::ByKey,
            Self::ByKeys(_) => AccessPathKind::ByKeys,
            Self::KeyRange { .. } => AccessPathKind::KeyRange,
            Self::IndexPrefix { .. } => AccessPathKind::IndexPrefix,
            Self::IndexMultiLookup { .. } => AccessPathKind::IndexMultiLookup,
            Self::IndexRange { .. } => AccessPathKind::IndexRange,
            Self::FullScan => AccessPathKind::FullScan,
        }
    }
}

impl AccessPathKind {
    /// Return whether this path kind can drive direct PK-stream traversal.
    #[must_use]
    pub(in crate::db) const fn supports_pk_stream_access(self) -> bool {
        matches!(self, Self::KeyRange | Self::FullScan)
    }

    /// Return whether this path kind supports reverse traversal mechanics.
    #[must_use]
    pub(in crate::db) const fn supports_reverse_traversal(self) -> bool {
        !matches!(self, Self::ByKeys)
    }

    /// Return whether this path kind can derive count from one pushdown shape.
    #[must_use]
    pub(in crate::db) const fn supports_count_pushdown_shape(self) -> bool {
        matches!(self, Self::KeyRange | Self::FullScan)
    }

    /// Return whether this path kind supports one primary-scan fetch hint.
    #[must_use]
    pub(in crate::db) const fn supports_primary_scan_fetch_hint(self) -> bool {
        matches!(self, Self::ByKey | Self::KeyRange | Self::FullScan)
    }

    /// Return whether this path kind is one direct key-addressed access shape.
    #[must_use]
    pub(in crate::db) const fn is_key_direct_access(self) -> bool {
        matches!(self, Self::ByKey | Self::ByKeys)
    }

    /// Return whether this path kind supports the PK-window bytes fast path.
    #[must_use]
    pub(in crate::db) const fn supports_bytes_terminal_primary_key_window(self) -> bool {
        matches!(self, Self::FullScan | Self::KeyRange)
    }

    /// Return whether this path kind supports ordered-key-stream bytes fast path.
    #[must_use]
    pub(in crate::db) const fn supports_bytes_terminal_ordered_key_stream_window(self) -> bool {
        matches!(
            self,
            Self::ByKey
                | Self::ByKeys
                | Self::IndexPrefix
                | Self::IndexMultiLookup
                | Self::IndexRange
        )
    }

    /// Return whether COUNT can use already-existing PK rows for this path kind.
    #[must_use]
    pub(in crate::db) const fn supports_count_terminal_primary_key_existing_rows(self) -> bool {
        matches!(self, Self::ByKey | Self::ByKeys)
    }

    /// Return whether top-N seek requires one extra lookahead row for this kind.
    #[must_use]
    pub(in crate::db) const fn requires_top_n_seek_lookahead(self) -> bool {
        matches!(self, Self::ByKeys | Self::IndexMultiLookup)
    }
}

/// Dispatch one semantic access path through the canonical borrowed-variant surface.
#[must_use]
pub(in crate::db) const fn dispatch_access_path<K>(
    path: &AccessPath<K>,
) -> AccessPathDispatch<'_, K> {
    match path {
        AccessPath::ByKey(key) => AccessPathDispatch::ByKey(key),
        AccessPath::ByKeys(keys) => AccessPathDispatch::ByKeys(keys.as_slice()),
        AccessPath::KeyRange { start, end } => AccessPathDispatch::KeyRange { start, end },
        AccessPath::IndexPrefix { index, values } => AccessPathDispatch::IndexPrefix {
            index: *index,
            values: values.as_slice(),
        },
        AccessPath::IndexMultiLookup { index, values } => AccessPathDispatch::IndexMultiLookup {
            index: *index,
            values: values.as_slice(),
        },
        AccessPath::IndexRange { spec } => AccessPathDispatch::IndexRange { spec },
        AccessPath::FullScan => AccessPathDispatch::FullScan,
    }
}

///
/// AccessPlanDispatch
///
/// Borrowed structural dispatch for access-plan tree nodes.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db) enum AccessPlanDispatch<'a, K> {
    Path(AccessPathDispatch<'a, K>),
    Union(&'a [AccessPlan<K>]),
    Intersection(&'a [AccessPlan<K>]),
}

/// Dispatch one semantic access plan through the canonical borrowed-node surface.
#[must_use]
pub(in crate::db) fn dispatch_access_plan<K>(plan: &AccessPlan<K>) -> AccessPlanDispatch<'_, K> {
    match plan {
        AccessPlan::Path(path) => AccessPlanDispatch::Path(dispatch_access_path(path.as_ref())),
        AccessPlan::Union(children) => AccessPlanDispatch::Union(children.as_slice()),
        AccessPlan::Intersection(children) => AccessPlanDispatch::Intersection(children.as_slice()),
    }
}

///
/// ExecutableAccessPathDispatch
///
/// Borrowed executable access-path payload projection used by executor runtime
/// dispatch boundaries.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db) enum ExecutableAccessPathDispatch<'a, K> {
    ByKey(&'a K),
    ByKeys(&'a [K]),
    KeyRange { start: &'a K, end: &'a K },
    IndexPrefix,
    IndexMultiLookup { value_count: usize },
    IndexRange,
    FullScan,
}

/// Dispatch one executable access path through the canonical borrowed payload surface.
#[must_use]
pub(in crate::db) const fn dispatch_executable_access_path<'a, K>(
    path: &'a ExecutableAccessPath<'a, K>,
) -> ExecutableAccessPathDispatch<'a, K> {
    match path.payload() {
        ExecutionPathPayload::ByKey(key) => ExecutableAccessPathDispatch::ByKey(*key),
        ExecutionPathPayload::ByKeys(keys) => ExecutableAccessPathDispatch::ByKeys(keys),
        ExecutionPathPayload::KeyRange { start, end } => {
            ExecutableAccessPathDispatch::KeyRange { start, end }
        }
        ExecutionPathPayload::IndexPrefix => ExecutableAccessPathDispatch::IndexPrefix,
        ExecutionPathPayload::IndexMultiLookup { value_count } => {
            ExecutableAccessPathDispatch::IndexMultiLookup {
                value_count: *value_count,
            }
        }
        ExecutionPathPayload::IndexRange { .. } => ExecutableAccessPathDispatch::IndexRange,
        ExecutionPathPayload::FullScan => ExecutableAccessPathDispatch::FullScan,
    }
}
