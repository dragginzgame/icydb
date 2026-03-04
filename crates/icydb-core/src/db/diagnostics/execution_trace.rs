//! Module: diagnostics::execution_trace
//! Responsibility: execution trace contracts and mutation boundaries.
//! Does not own: execution routing policy or stream/materialization behavior.
//! Boundary: shared trace surface used by executor and response APIs.

use crate::db::{
    access::{AccessPath, AccessPlan},
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
    SecondaryOrderPushdown,
    IndexRangeLimitPushdown,
}

///
/// ExecutionTrace
///
/// Structured, opt-in load execution introspection snapshot.
/// Captures plan-shape and execution decisions without changing semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionTrace {
    pub access_path_variant: ExecutionAccessPathVariant,
    pub direction: OrderDirection,
    pub optimization: Option<ExecutionOptimization>,
    pub keys_scanned: u64,
    pub rows_returned: u64,
    pub continuation_applied: bool,
    pub index_predicate_applied: bool,
    pub index_predicate_keys_rejected: u64,
    pub distinct_keys_deduped: u64,
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
            rows_returned: 0,
            continuation_applied,
            index_predicate_applied: false,
            index_predicate_keys_rejected: 0,
            distinct_keys_deduped: 0,
        }
    }

    /// Apply one finalized path outcome to this trace snapshot.
    pub(in crate::db) fn set_path_outcome(
        &mut self,
        optimization: Option<ExecutionOptimization>,
        keys_scanned: usize,
        rows_returned: usize,
        index_predicate_applied: bool,
        index_predicate_keys_rejected: u64,
        distinct_keys_deduped: u64,
    ) {
        self.optimization = optimization;
        self.keys_scanned = u64::try_from(keys_scanned).unwrap_or(u64::MAX);
        self.rows_returned = u64::try_from(rows_returned).unwrap_or(u64::MAX);
        self.index_predicate_applied = index_predicate_applied;
        self.index_predicate_keys_rejected = index_predicate_keys_rejected;
        self.distinct_keys_deduped = distinct_keys_deduped;
        debug_assert_eq!(
            self.keys_scanned,
            u64::try_from(keys_scanned).unwrap_or(u64::MAX),
            "execution trace keys_scanned must match rows_scanned metrics input",
        );
    }
}

fn access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    match access {
        AccessPlan::Path(path) => match path.as_ref() {
            AccessPath::ByKey(_) => ExecutionAccessPathVariant::ByKey,
            AccessPath::ByKeys(_) => ExecutionAccessPathVariant::ByKeys,
            AccessPath::KeyRange { .. } => ExecutionAccessPathVariant::KeyRange,
            AccessPath::IndexPrefix { .. } => ExecutionAccessPathVariant::IndexPrefix,
            AccessPath::IndexRange { .. } => ExecutionAccessPathVariant::IndexRange,
            AccessPath::FullScan => ExecutionAccessPathVariant::FullScan,
        },
        AccessPlan::Union(_) => ExecutionAccessPathVariant::Union,
        AccessPlan::Intersection(_) => ExecutionAccessPathVariant::Intersection,
    }
}

const fn execution_order_direction(direction: Direction) -> OrderDirection {
    match direction {
        Direction::Asc => OrderDirection::Asc,
        Direction::Desc => OrderDirection::Desc,
    }
}
