//! Module: db::access::execution_contract::types
//! Defines the executable access payload shapes shared between access planning
//! and executor routing.

use crate::{
    db::access::{AccessPath, AccessPathKind, IndexShapeDetails},
    value::Value,
};
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
        index: IndexShapeDetails,
    },
    IndexMultiLookup {
        index: IndexShapeDetails,
        value_count: usize,
    },
    IndexRange {
        index: IndexShapeDetails,
        prefix_values: &'a [Value],
        lower: &'a Bound<Value>,
        upper: &'a Bound<Value>,
    },
    FullScan,
}

impl<'a, K> ExecutionPathPayload<'a, K> {
    /// Project one semantic access path into its execution-facing payload.
    #[must_use]
    pub(in crate::db::access) const fn from_access_path(path: &'a AccessPath<K>) -> Self {
        if let Some(key) = path.as_by_key() {
            return Self::ByKey(key);
        }
        if let Some(keys) = path.as_by_keys() {
            return Self::ByKeys(keys);
        }
        if let Some((start, end)) = path.as_key_range() {
            return Self::KeyRange { start, end };
        }
        if let Some((index, values)) = path.as_index_prefix() {
            return Self::IndexPrefix {
                index: IndexShapeDetails::new(*index, values.len()),
            };
        }
        if let Some((index, values)) = path.as_index_multi_lookup() {
            return Self::IndexMultiLookup {
                index: IndexShapeDetails::new(*index, 1),
                value_count: values.len(),
            };
        }
        if let Some(spec) = path.as_index_range() {
            return Self::IndexRange {
                index: IndexShapeDetails::new(*spec.index(), spec.prefix_values().len()),
                prefix_values: spec.prefix_values(),
                lower: spec.lower(),
                upper: spec.upper(),
            };
        }

        debug_assert!(path.is_full_scan());

        Self::FullScan
    }

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
    pub(in crate::db) const fn index_prefix_details(&self) -> Option<IndexShapeDetails> {
        match self {
            Self::IndexPrefix { index, .. } | Self::IndexMultiLookup { index, .. } => Some(*index),
            Self::ByKey(_)
            | Self::ByKeys(_)
            | Self::KeyRange { .. }
            | Self::IndexRange { .. }
            | Self::FullScan => None,
        }
    }

    /// Borrow index-range details when this path is index-range.
    #[must_use]
    pub(in crate::db) const fn index_range_details(&self) -> Option<IndexShapeDetails> {
        match self {
            Self::IndexRange { index, .. } => Some(*index),
            Self::ByKey(_)
            | Self::ByKeys(_)
            | Self::KeyRange { .. }
            | Self::IndexPrefix { .. }
            | Self::IndexMultiLookup { .. }
            | Self::FullScan => None,
        }
    }
}
