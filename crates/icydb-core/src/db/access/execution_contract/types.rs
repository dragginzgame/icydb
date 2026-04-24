//! Module: db::access::execution_contract::types
//! Defines the executable access payload shapes shared between access planning
//! and executor routing.

use crate::{model::index::IndexModel, value::Value};
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
