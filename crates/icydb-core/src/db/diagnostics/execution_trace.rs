//! Module: diagnostics::execution_trace
//! Responsibility: execution trace contracts and mutation boundaries.
//! Does not own: execution routing policy or stream/materialization behavior.
//! Boundary: shared trace surface used by executor and response APIs.

use crate::db::{
    access::{AccessPathKind, AccessPlan, AccessPlanDispatch, dispatch_access_plan},
    direction::Direction,
    query::plan::OrderDirection,
};

///
/// ExecutionAccessPathVariant
///
/// Coarse access path shape used by the load execution trace surface.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionAccessPathVariant {
    ByKey,
    ByKeys,
    KeyRange,
    IndexPrefix,
    IndexMultiLookup,
    IndexRange,
    FullScan,
    Union,
    Intersection,
}

///
/// ExecutionOptimization
///
/// Canonical load optimization selected by execution, if any.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOptimization {
    PrimaryKey,
    PrimaryKeyTopNSeek,
    SecondaryOrderPushdown,
    SecondaryOrderTopNSeek,
    IndexRangeLimitPushdown,
}

///
/// ExecutionOptimizationCounter
///
/// Canonical test-only optimization counter taxonomy.
/// This keeps fast-path hit counters aligned with one shared naming surface.
///

#[cfg(test)]
#[expect(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionOptimizationCounter {
    BytesPrimaryKeyFastPath,
    BytesStreamFastPath,
    CoveringExistsFastPath,
    CoveringCountFastPath,
    PrimaryKeyCountFastPath,
    PrimaryKeyCardinalityCountFastPath,
    CoveringIndexProjectionFastPath,
    CoveringConstantProjectionFastPath,
}

///
/// ExecutionTrace
///
/// Structured, opt-in load execution introspection snapshot.
/// Captures plan-shape and execution decisions without changing semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionTrace {
    pub(crate) access_path_variant: ExecutionAccessPathVariant,
    pub(crate) direction: OrderDirection,
    pub(crate) optimization: Option<ExecutionOptimization>,
    pub(crate) keys_scanned: u64,
    pub(crate) rows_materialized: u64,
    pub(crate) rows_returned: u64,
    pub(crate) execution_time_micros: u64,
    pub(crate) index_only: bool,
    pub(crate) continuation_applied: bool,
    pub(crate) index_predicate_applied: bool,
    pub(crate) index_predicate_keys_rejected: u64,
    pub(crate) distinct_keys_deduped: u64,
}

///
/// ExecutionMetrics
///
/// Compact execution metrics projection derived from one `ExecutionTrace`.
/// This surface is intentionally small and stable for pre-EXPLAIN observability.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionMetrics {
    pub(crate) rows_scanned: u64,
    pub(crate) rows_materialized: u64,
    pub(crate) execution_time_micros: u64,
    pub(crate) index_only: bool,
}

impl ExecutionTrace {
    /// Build one trace payload from canonical access shape and runtime direction.
    #[must_use]
    pub(in crate::db) fn new<K>(
        access: &AccessPlan<K>,
        direction: Direction,
        continuation_applied: bool,
    ) -> Self {
        Self {
            access_path_variant: access_path_variant(access),
            direction: execution_order_direction(direction),
            optimization: None,
            keys_scanned: 0,
            rows_materialized: 0,
            rows_returned: 0,
            execution_time_micros: 0,
            index_only: false,
            continuation_applied,
            index_predicate_applied: false,
            index_predicate_keys_rejected: 0,
            distinct_keys_deduped: 0,
        }
    }

    /// Apply one finalized path outcome to this trace snapshot.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db) fn set_path_outcome(
        &mut self,
        optimization: Option<ExecutionOptimization>,
        keys_scanned: usize,
        rows_materialized: usize,
        rows_returned: usize,
        execution_time_micros: u64,
        index_only: bool,
        index_predicate_applied: bool,
        index_predicate_keys_rejected: u64,
        distinct_keys_deduped: u64,
    ) {
        self.optimization = optimization;
        self.keys_scanned = u64::try_from(keys_scanned).unwrap_or(u64::MAX);
        self.rows_materialized = u64::try_from(rows_materialized).unwrap_or(u64::MAX);
        self.rows_returned = u64::try_from(rows_returned).unwrap_or(u64::MAX);
        self.execution_time_micros = execution_time_micros;
        self.index_only = index_only;
        self.index_predicate_applied = index_predicate_applied;
        self.index_predicate_keys_rejected = index_predicate_keys_rejected;
        self.distinct_keys_deduped = distinct_keys_deduped;
        debug_assert_eq!(
            self.keys_scanned,
            u64::try_from(keys_scanned).unwrap_or(u64::MAX),
            "execution trace keys_scanned must match rows_scanned metrics input",
        );
    }

    /// Return compact execution metrics for pre-EXPLAIN observability surfaces.
    #[must_use]
    pub const fn metrics(&self) -> ExecutionMetrics {
        ExecutionMetrics {
            rows_scanned: self.keys_scanned,
            rows_materialized: self.rows_materialized,
            execution_time_micros: self.execution_time_micros,
            index_only: self.index_only,
        }
    }

    /// Return the coarse executed access-path variant.
    #[must_use]
    pub const fn access_path_variant(&self) -> ExecutionAccessPathVariant {
        self.access_path_variant
    }

    /// Return executed order direction.
    #[must_use]
    pub const fn direction(&self) -> OrderDirection {
        self.direction
    }

    /// Return selected optimization, if any.
    #[must_use]
    pub const fn optimization(&self) -> Option<ExecutionOptimization> {
        self.optimization
    }

    /// Return number of keys scanned.
    #[must_use]
    pub const fn keys_scanned(&self) -> u64 {
        self.keys_scanned
    }

    /// Return number of rows materialized.
    #[must_use]
    pub const fn rows_materialized(&self) -> u64 {
        self.rows_materialized
    }

    /// Return number of rows returned.
    #[must_use]
    pub const fn rows_returned(&self) -> u64 {
        self.rows_returned
    }

    /// Return execution time in microseconds.
    #[must_use]
    pub const fn execution_time_micros(&self) -> u64 {
        self.execution_time_micros
    }

    /// Return whether execution remained index-only.
    #[must_use]
    pub const fn index_only(&self) -> bool {
        self.index_only
    }

    /// Return whether continuation was applied.
    #[must_use]
    pub const fn continuation_applied(&self) -> bool {
        self.continuation_applied
    }

    /// Return whether index predicate pushdown was applied.
    #[must_use]
    pub const fn index_predicate_applied(&self) -> bool {
        self.index_predicate_applied
    }

    /// Return number of keys rejected by index predicate pushdown.
    #[must_use]
    pub const fn index_predicate_keys_rejected(&self) -> u64 {
        self.index_predicate_keys_rejected
    }

    /// Return number of deduplicated keys under DISTINCT processing.
    #[must_use]
    pub const fn distinct_keys_deduped(&self) -> u64 {
        self.distinct_keys_deduped
    }
}

impl ExecutionMetrics {
    /// Return number of rows scanned.
    #[must_use]
    pub const fn rows_scanned(&self) -> u64 {
        self.rows_scanned
    }

    /// Return number of rows materialized.
    #[must_use]
    pub const fn rows_materialized(&self) -> u64 {
        self.rows_materialized
    }

    /// Return execution time in microseconds.
    #[must_use]
    pub const fn execution_time_micros(&self) -> u64 {
        self.execution_time_micros
    }

    /// Return whether execution remained index-only.
    #[must_use]
    pub const fn index_only(&self) -> bool {
        self.index_only
    }
}

fn access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    match dispatch_access_plan(access) {
        AccessPlanDispatch::Path(path) => match path.kind() {
            AccessPathKind::ByKey => ExecutionAccessPathVariant::ByKey,
            AccessPathKind::ByKeys => ExecutionAccessPathVariant::ByKeys,
            AccessPathKind::KeyRange => ExecutionAccessPathVariant::KeyRange,
            AccessPathKind::IndexPrefix => ExecutionAccessPathVariant::IndexPrefix,
            AccessPathKind::IndexMultiLookup => ExecutionAccessPathVariant::IndexMultiLookup,
            AccessPathKind::IndexRange => ExecutionAccessPathVariant::IndexRange,
            AccessPathKind::FullScan => ExecutionAccessPathVariant::FullScan,
        },
        AccessPlanDispatch::Union(_) => ExecutionAccessPathVariant::Union,
        AccessPlanDispatch::Intersection(_) => ExecutionAccessPathVariant::Intersection,
    }
}

const fn execution_order_direction(direction: Direction) -> OrderDirection {
    match direction {
        Direction::Asc => OrderDirection::Asc,
        Direction::Desc => OrderDirection::Desc,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        access::AccessPlan,
        diagnostics::{ExecutionMetrics, ExecutionOptimization, ExecutionTrace},
        direction::Direction,
    };

    #[test]
    fn execution_trace_metrics_projection_exposes_requested_surface() {
        let access = AccessPlan::by_key(11u64);
        let mut trace = ExecutionTrace::new(&access, Direction::Asc, false);
        trace.set_path_outcome(
            Some(ExecutionOptimization::PrimaryKey),
            5,
            3,
            2,
            42,
            true,
            true,
            7,
            9,
        );

        let metrics = trace.metrics();
        assert_eq!(
            metrics,
            ExecutionMetrics {
                rows_scanned: 5,
                rows_materialized: 3,
                execution_time_micros: 42,
                index_only: true,
            },
            "metrics projection must expose rows_scanned/rows_materialized/execution_time/index_only",
        );
        assert_eq!(
            trace.rows_returned(),
            2,
            "trace should preserve returned-row counters independently from materialization counters",
        );
    }
}
