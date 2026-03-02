//! Module: query::plan::access
//! Responsibility: post-planning logical+access composite contracts and builders.
//! Does not own: pure logical plan model definitions or semantic interpretation.
//! Boundary: glue between logical plan semantics and selected access paths.

use crate::db::{
    access::{AccessPath, AccessPlan},
    direction::Direction,
    executor::{
        ExecutableAccessPath, ExecutableAccessPlan, ExecutionBounds, ExecutionDistinctMode,
        ExecutionMode, ExecutionOrdering, ExecutionPathPayload,
    },
    query::plan::{GroupHavingSpec, GroupPlan, GroupSpec, LogicalPlan},
};
#[cfg(test)]
use crate::db::{
    predicate::MissingRowPolicy,
    query::plan::{LoadSpec, QueryMode, ScalarPlan},
};

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

    /// Convert this plan into grouped logical form with one explicit group spec.
    #[must_use]
    pub(in crate::db) fn into_grouped(self, group: GroupSpec) -> Self {
        self.into_grouped_with_having(group, None)
    }

    /// Convert this plan into grouped logical form with explicit HAVING shape.
    #[must_use]
    pub(in crate::db) fn into_grouped_with_having(
        self,
        group: GroupSpec,
        having: Option<GroupHavingSpec>,
    ) -> Self {
        let Self { logical, access } = self;
        let scalar = match logical {
            LogicalPlan::Scalar(plan) => plan,
            LogicalPlan::Grouped(plan) => plan.scalar,
        };

        Self {
            logical: LogicalPlan::Grouped(GroupPlan {
                scalar,
                group,
                having,
            }),
            access,
        }
    }

    /// Lower the chosen access plan into an executor-owned normalized contract.
    #[must_use]
    pub(in crate::db) fn to_executable(&self) -> ExecutableAccessPlan<'_, K> {
        lower_executable_access_plan(&self.access)
    }

    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub(crate) fn new(access: AccessPath<K>, consistency: MissingRowPolicy) -> Self {
        Self {
            logical: LogicalPlan::Scalar(ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: None,
                distinct: false,
                delete_limit: None,
                page: None,
                consistency,
            }),
            access: AccessPlan::path(access),
        }
    }
}

/// Lower one logical `AccessPlan` into its normalized executable contract.
#[must_use]
pub(in crate::db) fn lower_executable_access_plan<K>(
    access: &AccessPlan<K>,
) -> ExecutableAccessPlan<'_, K> {
    match access {
        AccessPlan::Path(path) => {
            ExecutableAccessPlan::for_path(lower_executable_access_path(path.as_ref()))
        }
        AccessPlan::Union(children) => {
            ExecutableAccessPlan::union(children.iter().map(lower_executable_access_plan).collect())
        }
        AccessPlan::Intersection(children) => ExecutableAccessPlan::intersection(
            children.iter().map(lower_executable_access_plan).collect(),
        ),
    }
}

/// Lower one logical `AccessPath` into its normalized executable contract.
#[must_use]
pub(in crate::db) const fn lower_executable_access_path<K>(
    path: &AccessPath<K>,
) -> ExecutableAccessPath<'_, K> {
    match path {
        AccessPath::ByKey(key) => ExecutableAccessPath::new(
            ExecutionMode::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::Unbounded,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::ByKey(key),
        ),
        AccessPath::ByKeys(keys) => ExecutableAccessPath::new(
            ExecutionMode::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::Unbounded,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::ByKeys(keys.as_slice()),
        ),
        AccessPath::KeyRange { start, end } => ExecutableAccessPath::new(
            ExecutionMode::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::PrimaryKeyRange,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::KeyRange { start, end },
        ),
        AccessPath::IndexPrefix { index, values } => ExecutableAccessPath::new(
            ExecutionMode::OrderedIndexScan,
            ExecutionOrdering::ByIndex(Direction::Asc),
            ExecutionBounds::IndexPrefix {
                index: *index,
                prefix_len: values.len(),
            },
            ExecutionDistinctMode::PreOrdered,
            true,
            ExecutionPathPayload::IndexPrefix,
        ),
        AccessPath::IndexRange { spec } => {
            let index = *spec.index();
            let prefix_len = spec.prefix_values().len();

            ExecutableAccessPath::new(
                ExecutionMode::IndexRange,
                ExecutionOrdering::ByIndex(Direction::Asc),
                ExecutionBounds::IndexRange { index, prefix_len },
                ExecutionDistinctMode::PreOrdered,
                true,
                ExecutionPathPayload::IndexRange {
                    prefix_values: spec.prefix_values(),
                    lower: spec.lower(),
                    upper: spec.upper(),
                },
            )
        }
        AccessPath::FullScan => ExecutableAccessPath::new(
            ExecutionMode::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::Unbounded,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::FullScan,
        ),
    }
}
