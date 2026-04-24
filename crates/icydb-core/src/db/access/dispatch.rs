//! Module: access::dispatch
//! Responsibility: canonical structural dispatch over semantic access contracts.
//! Does not own: planner path selection policy or executor stream traversal behavior.
//! Boundary: all non-construction AccessPath branching routes through this module.

use crate::{
    db::access::{AccessPath, SemanticIndexRangeSpec},
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
