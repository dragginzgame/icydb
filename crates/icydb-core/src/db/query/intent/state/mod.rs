//! Module: query::intent::state
//! Responsibility: internal mutable query-intent state transitions across load/delete modes.
//! Does not own: planner semantic validation or executor runtime behavior.
//! Boundary: records intent-shape state consumed by planner-owned validation/build stages.

use crate::db::{
    query::intent::filter::AdmittedFilter,
    query::plan::{
        AccessPlanningInputs, DeleteSpec, GroupSpec, GroupedExecutionConfig, LoadSpec,
        LogicalPlanningInputs, OrderSpec, QueryMode,
        expr::{Expr, ProjectionSelection},
    },
};

///
/// ScalarIntent
///
/// Owned scalar intent state for query-intent planning.
/// Carries scalar query modifiers that are independent of grouped shape.
///

#[derive(Clone, Debug)]
pub(in crate::db::query::intent) struct ScalarIntent {
    pub(in crate::db::query::intent) filter: Option<AdmittedFilter>,
    pub(in crate::db::query::intent) order: Option<OrderSpec>,
    pub(in crate::db::query::intent) distinct: bool,
    pub(in crate::db::query::intent) projection_selection: ProjectionSelection,
}

impl ScalarIntent {
    #[must_use]
    pub(in crate::db::query::intent) const fn new() -> Self {
        Self {
            filter: None,
            order: None,
            distinct: false,
            projection_selection: ProjectionSelection::All,
        }
    }
}

///
/// GroupedIntent
///
/// Owned grouped intent shape.
/// Wraps scalar modifiers with grouped declarations (`GROUP BY` + `HAVING`).
///

#[derive(Clone, Debug)]
pub(in crate::db::query::intent) struct GroupedIntent {
    pub(in crate::db::query::intent) scalar: ScalarIntent,
    pub(in crate::db::query::intent) group: GroupSpec,
    pub(in crate::db::query::intent) having_expr: Option<Expr>,
}

impl GroupedIntent {
    #[must_use]
    pub(in crate::db::query::intent) const fn from_scalar(scalar: ScalarIntent) -> Self {
        Self {
            scalar,
            group: GroupSpec {
                group_fields: crate::db::query::plan::GroupFieldSet::empty(),
                aggregates: Vec::new(),
                execution: GroupedExecutionConfig::unbounded(),
            },
            having_expr: None,
        }
    }
}

///
/// QueryShape
///
/// Owned scalar/grouped shape for load-mode query intent.
///

// Query intent keeps scalar and grouped state inline so mode transitions can move the
// full owned shape without introducing extra heap indirection across the intent builder.
#[derive(Clone, Debug)]
enum QueryShape {
    Scalar(ScalarIntent),
    Grouped(GroupedIntent),
}

///
/// LoadIntentState
///
/// Typed state for load-mode intent.
/// Keeps load pagination spec and load-mode shape together.
///

#[derive(Clone, Debug)]
pub(in crate::db::query::intent) struct LoadIntentState {
    spec: LoadSpec,
    offset_requested: bool,
    shape: QueryShape,
}

impl LoadIntentState {
    #[must_use]
    const fn new() -> Self {
        Self {
            spec: LoadSpec::new(),
            offset_requested: false,
            shape: QueryShape::Scalar(ScalarIntent::new()),
        }
    }
}

///
/// DeleteIntentState
///
/// Typed state for delete-mode intent.
/// Delete mode intentionally carries only scalar shape plus the grouping fact
/// needed for delete-only validation.
///

#[derive(Clone, Debug)]
pub(in crate::db::query::intent) struct DeleteIntentState {
    spec: DeleteSpec,
    scalar: ScalarIntent,
    grouping_requested: bool,
}

impl DeleteIntentState {
    #[must_use]
    const fn new(scalar: ScalarIntent, grouping_requested: bool) -> Self {
        Self {
            spec: DeleteSpec::new(),
            scalar,
            grouping_requested,
        }
    }
}

///
/// QueryIntent
///
/// Owned intent-state contract used by `QueryModel`.
/// Encodes mode-specific state as typed variants.
///

// Query intent keeps load/delete state inline because mode switches reuse the full owned
// state and the builder is not a hot path where boxing would pay for the extra indirection.
#[derive(Clone, Debug)]
pub(in crate::db::query::intent) enum QueryIntent {
    Load(LoadIntentState),
    Delete(DeleteIntentState),
}

impl QueryIntent {
    #[must_use]
    pub(in crate::db::query::intent) const fn new() -> Self {
        Self::Load(LoadIntentState::new())
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn mode(&self) -> QueryMode {
        match self {
            Self::Load(load) => QueryMode::Load(load.spec),
            Self::Delete(delete) => QueryMode::Delete(delete.spec),
        }
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn is_grouped(&self) -> bool {
        match self {
            Self::Load(load) => matches!(load.shape, QueryShape::Grouped(_)),
            Self::Delete(delete) => delete.grouping_requested,
        }
    }

    #[must_use]
    pub(in crate::db::query::intent) fn set_delete_mode(self) -> Self {
        match self {
            Self::Delete(delete) => Self::Delete(delete),
            Self::Load(load) => {
                let (scalar, grouping_requested) = match load.shape {
                    QueryShape::Scalar(scalar) => (scalar, false),
                    QueryShape::Grouped(grouped) => (grouped.scalar, true),
                };

                Self::Delete(DeleteIntentState::new(scalar, grouping_requested))
            }
        }
    }

    /// Re-express one delete target as a load selection without changing its
    /// predicate, ordering, offset, or limit.
    #[must_use]
    pub(in crate::db::query::intent) fn into_load_selection(self) -> Self {
        match self {
            Self::Load(load) => Self::Load(load),
            Self::Delete(delete) => Self::Load(LoadIntentState {
                spec: LoadSpec {
                    limit: delete.spec.limit,
                    offset: delete.spec.offset,
                },
                offset_requested: delete.spec.offset != 0,
                shape: QueryShape::Scalar(delete.scalar),
            }),
        }
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn apply_limit(mut self, limit: u32) -> Self {
        match &mut self {
            Self::Load(load) => {
                load.spec.limit = Some(limit);
            }
            Self::Delete(delete) => {
                delete.spec.limit = Some(limit);
            }
        }

        self
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn apply_offset(mut self, offset: u32) -> Self {
        match &mut self {
            Self::Load(load) => {
                load.offset_requested = true;
                load.spec.offset = offset;
            }
            Self::Delete(delete) => {
                delete.spec.offset = offset;
            }
        }

        self
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn scalar(&self) -> &ScalarIntent {
        match self {
            Self::Load(load) => match &load.shape {
                QueryShape::Scalar(scalar) => scalar,
                QueryShape::Grouped(grouped) => &grouped.scalar,
            },
            Self::Delete(delete) => &delete.scalar,
        }
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn scalar_mut(&mut self) -> &mut ScalarIntent {
        match self {
            Self::Load(load) => match &mut load.shape {
                QueryShape::Scalar(scalar) => scalar,
                QueryShape::Grouped(grouped) => &mut grouped.scalar,
            },
            Self::Delete(delete) => &mut delete.scalar,
        }
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn grouped(&self) -> Option<&GroupedIntent> {
        match self {
            Self::Load(load) => match &load.shape {
                QueryShape::Grouped(grouped) => Some(grouped),
                QueryShape::Scalar(_) => None,
            },
            Self::Delete(_) => None,
        }
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn grouped_mut(&mut self) -> Option<&mut GroupedIntent> {
        match self {
            Self::Load(load) => match &mut load.shape {
                QueryShape::Grouped(grouped) => Some(grouped),
                QueryShape::Scalar(_) => None,
            },
            Self::Delete(_) => None,
        }
    }

    pub(in crate::db::query::intent) fn ensure_grouped_mut(
        &mut self,
    ) -> Option<&mut GroupedIntent> {
        let Self::Load(load) = self else {
            return None;
        };

        if matches!(load.shape, QueryShape::Scalar(_)) {
            // Lift scalar shape into grouped shape while preserving scalar modifiers.
            let scalar =
                match std::mem::replace(&mut load.shape, QueryShape::Scalar(ScalarIntent::new())) {
                    QueryShape::Scalar(scalar) => scalar,
                    QueryShape::Grouped(grouped) => {
                        load.shape = QueryShape::Grouped(grouped);
                        return None;
                    }
                };
            load.shape = QueryShape::Grouped(GroupedIntent::from_scalar(scalar));
        }

        match &mut load.shape {
            QueryShape::Grouped(grouped) => Some(grouped),
            QueryShape::Scalar(_) => None,
        }
    }

    pub(in crate::db::query::intent) const fn mark_delete_grouping_requested(&mut self) {
        if let Self::Delete(delete) = self {
            delete.grouping_requested = true;
        }
    }

    /// Project logical-planning inputs from intent-owned query state.
    #[must_use]
    pub(in crate::db::query::intent) fn planning_logical_inputs(
        &self,
    ) -> LogicalPlanningInputs<'_> {
        let (group, having_expr) = match self.grouped() {
            Some(grouped) => (Some(&grouped.group), grouped.having_expr.as_ref()),
            None => (None, None),
        };

        LogicalPlanningInputs::new(
            self.mode(),
            self.scalar()
                .filter
                .as_ref()
                .map(AdmittedFilter::logical_filter_expr),
            self.scalar()
                .filter
                .as_ref()
                .is_some_and(AdmittedFilter::predicate_subset_covers_expr),
            self.scalar().distinct,
            group,
            having_expr,
        )
    }
}

impl QueryIntent {
    /// Project access-planning inputs from intent-owned scalar state.
    #[must_use]
    pub(in crate::db::query::intent) fn planning_access_inputs(&self) -> AccessPlanningInputs<'_> {
        let scalar = self.scalar();
        AccessPlanningInputs::new(
            scalar
                .filter
                .as_ref()
                .and_then(AdmittedFilter::predicate_subset),
            scalar.order.as_ref(),
        )
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

crate::retained::retained_fields!(ScalarIntent {
    Self { filter, order, distinct: _, projection_selection } => [filter, order, projection_selection],
});
crate::retained::retained_fields!(GroupedIntent {
    Self { scalar, group, having_expr } => [scalar, group, having_expr],
});
crate::retained::retained_fields!(QueryShape {
    Self::Scalar(scalar) => [scalar], Self::Grouped(grouped) => [grouped],
});
crate::retained::retained_fields!(LoadIntentState {
    Self { spec: _, offset_requested: _, shape } => [shape],
});
crate::retained::retained_fields!(DeleteIntentState {
    Self { spec: _, scalar, grouping_requested: _ } => [scalar],
});
crate::retained::retained_fields!(QueryIntent {
    Self::Load(load) => [load], Self::Delete(delete) => [delete],
});
