//! Structural logical-plan model.
//!
//! Execution-phase semantics (post-access filtering, ordering, cursor windows,
//! pagination, delete limits) are implemented in `db::executor::query_bridge`.

use crate::db::query::{
    ReadConsistency,
    intent::QueryMode,
    plan::{AccessPlan, DeleteLimitSpec, OrderSpec, PageSpec},
    predicate::Predicate,
};

#[cfg(test)]
use crate::db::query::intent::LoadSpec;
#[cfg(test)]
use crate::db::query::plan::AccessPath;

///
/// LogicalPlan
///
/// Executor-ready query plan produced by the planner.
///
/// A `LogicalPlan` represents the *complete, linearized execution intent*
/// for a query. All schema validation, predicate normalization, coercion
/// checks, and access-path selection have already occurred by the time a
/// `LogicalPlan` is constructed.
///
/// Design notes:
/// - Access may be a single path or a composite (union/intersection) of paths
/// - Predicates are applied *after* data access
/// - Ordering is applied after filtering
/// - Pagination is applied after ordering (load only)
/// - Delete limits are applied after ordering (delete only)
/// - Missing-row policy is explicit and must not depend on access path
///
/// This struct is the explicit contract between the planner and executors.
/// Executors must be able to execute any valid `LogicalPlan` without
/// additional planning or schema access.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalPlan<K> {
    /// Load vs delete intent.
    pub(crate) mode: QueryMode,

    /// Storage access strategy (single path or composite).
    pub(crate) access: AccessPlan<K>,

    /// Optional residual predicate applied after access.
    pub(crate) predicate: Option<Predicate>,

    /// Optional ordering specification.
    pub(crate) order: Option<OrderSpec>,

    /// Optional distinct semantics over ordered rows.
    pub(crate) distinct: bool,

    /// Optional delete bound (delete intents only).
    pub(crate) delete_limit: Option<DeleteLimitSpec>,

    /// Optional pagination specification.
    pub(crate) page: Option<PageSpec>,

    /// Missing-row policy for execution.
    pub(crate) consistency: ReadConsistency,
}

impl<K> LogicalPlan<K> {
    /// Construct a minimal logical plan with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub(crate) fn new(access: AccessPath<K>, consistency: ReadConsistency) -> Self {
        Self {
            mode: QueryMode::Load(LoadSpec::new()),
            access: AccessPlan::path(access),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency,
        }
    }
}
