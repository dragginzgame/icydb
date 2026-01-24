//! Executor contract for a fully resolved logical plan; must not plan or validate.

use crate::db::query::v2::{
    plan::{AccessPath, OrderSpec, PageSpec},
    predicate::Predicate,
};

///
/// LogicalPlan
///
/// Executor-ready query plan produced by the v2 planner.
///
/// A `LogicalPlan` represents the *complete, linearized execution intent*
/// for a query. All schema validation, predicate normalization, coercion
/// checks, and access-path selection have already occurred by the time a
/// `LogicalPlan` is constructed.
///
/// Design notes:
/// - Exactly one `AccessPath` is present (no unions or intersections)
/// - Predicates are applied *after* data access
/// - Ordering is applied after filtering
/// - Pagination is applied last
///
/// This struct is the explicit contract between the planner and executors.
/// Executors must be able to execute any valid `LogicalPlan` without
/// additional planning or schema access.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalPlan {
    /// Concrete storage access strategy.
    pub access: AccessPath,

    /// Optional residual predicate applied after access.
    pub predicate: Option<Predicate>,

    /// Optional ordering specification.
    pub order: Option<OrderSpec>,

    /// Optional pagination specification.
    pub page: Option<PageSpec>,
}

impl LogicalPlan {
    /// Construct a minimal logical plan with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[must_use]
    pub const fn new(access: AccessPath) -> Self {
        Self {
            access,
            predicate: None,
            order: None,
            page: None,
        }
    }
}
