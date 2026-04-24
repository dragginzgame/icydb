//! Module: db::access::execution_contract::types
//! Defines the executable access payload shapes shared between access planning
//! and executor routing.

use crate::{db::access::AccessPathKind, model::index::IndexModel, value::Value};
use std::ops::Bound;

///
/// ExecutionPathPayload
///
/// Variant payload needed for mechanical access execution only.
/// This contract intentionally excludes planner semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionPathPayload<'a, K> {
    ByKey(&'a K),
    ByKeys(&'a [K]),
    KeyRange {
        start: &'a K,
        end: &'a K,
    },
    IndexPrefix {
        index: IndexModel,
        prefix_len: usize,
    },
    IndexMultiLookup {
        index: IndexModel,
        value_count: usize,
    },
    IndexRange {
        index: IndexModel,
        prefix_len: usize,
        prefix_values: &'a [Value],
        lower: &'a Bound<Value>,
        upper: &'a Bound<Value>,
    },
    FullScan,
}

impl<'a, K> ExecutionPathPayload<'a, K> {
    /// Return the canonical execution path kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AccessPathKind {
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

    /// Borrow semantic index-range bounds required for cursor envelope validation.
    #[must_use]
    pub(in crate::db) const fn index_range_semantic_bounds(
        &self,
    ) -> Option<(&'a [Value], &'a Bound<Value>, &'a Bound<Value>)> {
        match self {
            Self::IndexRange {
                prefix_values,
                lower,
                upper,
                ..
            } => Some((prefix_values, lower, upper)),
            Self::ByKey(_)
            | Self::ByKeys(_)
            | Self::KeyRange { .. }
            | Self::IndexPrefix { .. }
            | Self::IndexMultiLookup { .. }
            | Self::FullScan => None,
        }
    }

    /// Borrow index-prefix details when this path is index-prefix.
    #[must_use]
    pub(in crate::db) const fn index_prefix_details(&self) -> Option<(IndexModel, usize)> {
        match self {
            Self::IndexPrefix { index, prefix_len } => Some((*index, *prefix_len)),
            Self::IndexMultiLookup { index, .. } => Some((*index, 1)),
            Self::ByKey(_)
            | Self::ByKeys(_)
            | Self::KeyRange { .. }
            | Self::IndexRange { .. }
            | Self::FullScan => None,
        }
    }

    /// Borrow index-range details when this path is index-range.
    #[must_use]
    pub(in crate::db) const fn index_range_details(&self) -> Option<(IndexModel, usize)> {
        match self {
            Self::IndexRange {
                index, prefix_len, ..
            } => Some((*index, *prefix_len)),
            Self::ByKey(_)
            | Self::ByKeys(_)
            | Self::KeyRange { .. }
            | Self::IndexPrefix { .. }
            | Self::IndexMultiLookup { .. }
            | Self::FullScan => None,
        }
    }
}
