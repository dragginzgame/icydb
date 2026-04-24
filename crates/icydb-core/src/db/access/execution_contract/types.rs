//! Module: db::access::execution_contract::types
//! Defines the executable access payload shapes shared between access planning
//! and executor routing.

use crate::{model::index::IndexModel, value::Value};
use std::ops::Bound;

///
/// ExecutionBounds
///
/// Minimal bound shape required by executor path mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionBounds {
    Unbounded,
    PrimaryKeyRange,
    IndexPrefix {
        index: IndexModel,
        prefix_len: usize,
    },
    IndexRange {
        index: IndexModel,
        prefix_len: usize,
    },
}

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
    IndexPrefix,
    IndexMultiLookup {
        value_count: usize,
    },
    IndexRange {
        prefix_values: &'a [Value],
        lower: &'a Bound<Value>,
        upper: &'a Bound<Value>,
    },
    FullScan,
}
