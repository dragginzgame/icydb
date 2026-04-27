//! Module: executor::pipeline::contracts::fast_stream
//! Responsibility: fast-stream route request DTOs shared by scan/runtime callers.
//! Does not own: fast-stream route dispatch or physical stream execution.
//! Boundary: data-only request shapes consumed by `executor::scan::fast_stream_route`.

use crate::{
    db::{
        access::ExecutableAccessPlan,
        direction::Direction,
        executor::{
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            pipeline::contracts::AccessScanContinuationInput,
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    value::Value,
};

///
/// FastStreamRouteKind
///
/// Canonical fast-stream route discriminator used by shared load adapters.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum FastStreamRouteKind {
    PrimaryKey,
    SecondaryIndex,
    IndexRangeLimitPushdown,
}

///
/// FastStreamRouteRequest
///
/// Route-specific stream binding payload consumed by shared fast-stream dispatch.
///

pub(in crate::db::executor) enum FastStreamRouteRequest<'a, 'plan> {
    PrimaryKey {
        plan: &'a AccessPlannedQuery,
        executable_access: &'a ExecutableAccessPlan<'plan, Value>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
    },
    SecondaryIndex {
        plan: &'a AccessPlannedQuery,
        executable_access: &'a ExecutableAccessPlan<'plan, Value>,
        index_prefix_spec: Option<&'a LoweredIndexPrefixSpec>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    },
    IndexRangeLimitPushdown {
        plan: &'a AccessPlannedQuery,
        executable_access: &'a ExecutableAccessPlan<'plan, Value>,
        index_range_spec: Option<&'a LoweredIndexRangeSpec>,
        continuation: AccessScanContinuationInput<'a>,
        effective_fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    },
}
