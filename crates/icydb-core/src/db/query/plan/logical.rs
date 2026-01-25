//! Executor contract for a fully resolved logical plan; must not plan or validate.

use crate::db::query::{
    ReadConsistency,
    plan::{AccessPath, AccessPlan, OrderSpec, PageSpec, ProjectionSpec},
    predicate::Predicate,
};

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
/// - Pagination is applied last
/// - Projection is applied to the final materialized rows
/// - Missing-row policy is explicit and must not depend on access path
///
/// This struct is the explicit contract between the planner and executors.
/// Executors must be able to execute any valid `LogicalPlan` without
/// additional planning or schema access.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalPlan {
    /// Storage access strategy (single path or composite).
    pub(crate) access: AccessPlan,

    /// Optional residual predicate applied after access.
    pub(crate) predicate: Option<Predicate>,

    /// Optional ordering specification.
    pub(crate) order: Option<OrderSpec>,

    /// Optional pagination specification.
    pub(crate) page: Option<PageSpec>,

    /// Projection specification.
    pub(crate) projection: ProjectionSpec,

    /// Missing-row policy for execution.
    pub(crate) consistency: ReadConsistency,
}

impl LogicalPlan {
    /// Construct a minimal logical plan with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[must_use]
    pub const fn new(access: AccessPath, consistency: ReadConsistency) -> Self {
        Self {
            access: AccessPlan::Path(access),
            predicate: None,
            order: None,
            page: None,
            projection: ProjectionSpec::All,
            consistency,
        }
    }
}
