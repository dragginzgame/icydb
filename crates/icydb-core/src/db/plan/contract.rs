//! Passive plan contract types shared across query/executor boundaries.

#[cfg(test)]
use crate::db::access::AccessPath;
#[cfg(test)]
use crate::db::intent::LoadSpec;
use crate::db::{
    access::AccessPlan, consistency::ReadConsistency, intent::QueryMode, predicate::Predicate,
};
use std::ops::{Deref, DerefMut};

///
/// OrderDirection
/// Executor-facing ordering direction (applied after filtering).
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

///
/// OrderSpec
/// Executor-facing ordering specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderSpec {
    pub(crate) fields: Vec<(String, OrderDirection)>,
}

///
/// DeleteLimitSpec
/// Executor-facing delete bound with no offsets.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteLimitSpec {
    pub max_rows: u32,
}

///
/// PageSpec
/// Executor-facing pagination specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PageSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}

///
/// LogicalPlan
///
/// Pure logical query intent produced by the planner.
///
/// A `LogicalPlan` represents the access-independent query semantics:
/// predicate/filter, ordering, distinct behavior, pagination/delete windows,
/// and read-consistency mode.
///
/// Design notes:
/// - Predicates are applied *after* data access
/// - Ordering is applied after filtering
/// - Pagination is applied after ordering (load only)
/// - Delete limits are applied after ordering (delete only)
/// - Missing-row policy is explicit and must not depend on access strategy
///
/// This struct is the logical compiler stage output and intentionally excludes
/// access-path details.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalPlan {
    /// Load vs delete intent.
    pub(crate) mode: QueryMode,

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

///
/// AccessPlannedQuery
///
/// Access-planned query produced after access-path selection.
/// Binds one pure `LogicalPlan` to one chosen `AccessPlan`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccessPlannedQuery<K> {
    pub(crate) logical: LogicalPlan,
    pub(crate) access: AccessPlan<K>,
}

impl<K> AccessPlannedQuery<K> {
    /// Construct an access-planned query from logical + access stages.
    #[must_use]
    pub(crate) const fn from_parts(logical: LogicalPlan, access: AccessPlan<K>) -> Self {
        Self { logical, access }
    }

    /// Decompose into logical + access stages.
    #[must_use]
    pub(crate) fn into_parts(self) -> (LogicalPlan, AccessPlan<K>) {
        (self.logical, self.access)
    }

    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub(crate) fn new(access: AccessPath<K>, consistency: ReadConsistency) -> Self {
        Self {
            logical: LogicalPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: None,
                distinct: false,
                delete_limit: None,
                page: None,
                consistency,
            },
            access: AccessPlan::path(access),
        }
    }
}

impl<K> Deref for AccessPlannedQuery<K> {
    type Target = LogicalPlan;

    fn deref(&self) -> &Self::Target {
        &self.logical
    }
}

impl<K> DerefMut for AccessPlannedQuery<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.logical
    }
}
