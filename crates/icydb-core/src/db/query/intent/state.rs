//! Module: query::intent::state
//! Responsibility: internal mutable query-intent state transitions across load/delete modes.
//! Does not own: planner semantic validation or executor runtime behavior.
//! Boundary: records intent-shape state consumed by planner-owned validation/build stages.

use crate::db::{
    predicate::Predicate,
    query::{
        intent::{KeyAccessState, project_key_access_for_planning},
        plan::{
            AccessPlanningInputs, DeleteSpec, GroupSpec, GroupedExecutionConfig, LoadSpec,
            LogicalPlanningInputs, OrderSpec, QueryMode,
            expr::{
                BinaryOp, Expr, ProjectionSelection, derive_normalized_bool_expr_predicate_subset,
                is_normalized_bool_expr, normalize_bool_expr,
            },
            has_explicit_order,
        },
    },
};

///
/// NormalizedFilter
///
/// Canonical scalar filter representation stored by query intent.
/// It owns the normalized expression that preserves full runtime semantics and
/// the optional runtime-predicate subset derived once at append boundaries.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::intent) enum FilterPredicateCoverage {
    Full,
    Partial,
    None,
}

impl FilterPredicateCoverage {
    const fn from_extracted_subset(predicate_subset: Option<&Predicate>) -> Self {
        if predicate_subset.is_some() {
            Self::Full
        } else {
            Self::None
        }
    }

    const fn combine_for_and(existing: Self, appended: Self, combined_subset_exists: bool) -> Self {
        if !combined_subset_exists {
            return Self::None;
        }

        match (existing, appended) {
            (Self::Full, Self::Full) => Self::Full,
            _ => Self::Partial,
        }
    }

    pub(in crate::db::query::intent) const fn covers_user_visible_filter_semantics(self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial | Self::None => false,
        }
    }
}

#[derive(Clone, Debug)]
enum FilterSemanticAuthority {
    ExpressionBacked(Expr),
    #[cfg_attr(not(any(test, feature = "sql")), allow(dead_code))]
    PredicateOnly,
}

#[derive(Clone, Debug)]
pub(in crate::db::query::intent) struct NormalizedFilter {
    semantic_authority: FilterSemanticAuthority,
    predicate_subset: Option<Predicate>,
    predicate_coverage: FilterPredicateCoverage,
}

impl NormalizedFilter {
    /// Build one normalized filter from a planner-owned boolean expression.
    #[must_use]
    pub(in crate::db::query::intent) fn from_normalized_expr(expr: Expr) -> Self {
        debug_assert!(
            is_normalized_bool_expr(&expr),
            "intent-owned filter expressions must be normalized before storage",
        );

        let predicate_subset = derive_normalized_bool_expr_predicate_subset(&expr);
        let predicate_coverage =
            FilterPredicateCoverage::from_extracted_subset(predicate_subset.as_ref());

        Self {
            semantic_authority: FilterSemanticAuthority::ExpressionBacked(expr),
            predicate_subset,
            predicate_coverage,
        }
    }

    /// Build one normalized filter from an expression plus an already-derived
    /// predicate subset, used by SQL lowering after it has canonicalized both
    /// views through the same semantic pass.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::query::intent) fn from_normalized_expr_and_predicate_subset(
        expr: Expr,
        predicate_subset: Predicate,
    ) -> Self {
        debug_assert!(
            is_normalized_bool_expr(&expr),
            "intent-owned filter expressions must be normalized before storage",
        );

        Self {
            semantic_authority: FilterSemanticAuthority::ExpressionBacked(expr),
            predicate_subset: Some(predicate_subset),
            predicate_coverage: FilterPredicateCoverage::Full,
        }
    }

    /// Build one invisible filter from an already-normalized runtime predicate.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::query::intent) const fn from_normalized_predicate(
        predicate: Predicate,
    ) -> Self {
        Self {
            semantic_authority: FilterSemanticAuthority::PredicateOnly,
            predicate_subset: Some(predicate),
            predicate_coverage: FilterPredicateCoverage::Full,
        }
    }

    /// Borrow the normalized semantic expression only when it should remain
    /// visible as a planner-owned expression filter.
    #[must_use]
    pub(in crate::db::query::intent) const fn logical_filter_expr(&self) -> Option<&Expr> {
        match &self.semantic_authority {
            FilterSemanticAuthority::ExpressionBacked(expr) => Some(expr),
            FilterSemanticAuthority::PredicateOnly => None,
        }
    }

    /// Borrow the already-derived predicate subset used by access planning.
    #[must_use]
    pub(in crate::db::query::intent) const fn predicate_subset(&self) -> Option<&Predicate> {
        self.predicate_subset.as_ref()
    }

    /// Return predicate-subset coverage over the user-visible filter semantics.
    #[must_use]
    pub(in crate::db::query::intent) const fn predicate_coverage(&self) -> FilterPredicateCoverage {
        self.predicate_coverage
    }

    /// Return whether the predicate subset fully represents the visible
    /// expression view used by existing logical-planning inputs.
    #[must_use]
    pub(in crate::db::query::intent) const fn predicate_subset_covers_expr(&self) -> bool {
        matches!(
            self.semantic_authority,
            FilterSemanticAuthority::ExpressionBacked(_)
        ) && self
            .predicate_coverage()
            .covers_user_visible_filter_semantics()
    }

    // Append one filter clause by AND-ing the semantic expression and combining
    // only the predicate subsets that were derived at their original boundary.
    pub(in crate::db::query::intent) fn append(&mut self, filter: Self) {
        let existing_coverage = self.predicate_coverage;
        let filter_coverage = filter.predicate_coverage;

        match (&mut self.semantic_authority, filter.semantic_authority) {
            (
                FilterSemanticAuthority::ExpressionBacked(existing),
                FilterSemanticAuthority::ExpressionBacked(appended),
            ) => {
                *existing = normalize_bool_expr(Expr::Binary {
                    op: BinaryOp::And,
                    left: Box::new(existing.clone()),
                    right: Box::new(appended),
                });
            }
            (
                FilterSemanticAuthority::ExpressionBacked(_)
                | FilterSemanticAuthority::PredicateOnly,
                FilterSemanticAuthority::PredicateOnly,
            ) => {}
            (
                FilterSemanticAuthority::PredicateOnly,
                FilterSemanticAuthority::ExpressionBacked(appended),
            ) => {
                self.semantic_authority = FilterSemanticAuthority::ExpressionBacked(appended);
            }
        }

        if let Some(expr) = self.logical_filter_expr() {
            debug_assert!(
                is_normalized_bool_expr(expr),
                "combined intent-owned filter expressions must stay normalized",
            );
        }

        self.predicate_subset =
            combine_predicate_subset(self.predicate_subset.take(), filter.predicate_subset);
        self.predicate_coverage = FilterPredicateCoverage::combine_for_and(
            existing_coverage,
            filter_coverage,
            self.predicate_subset.is_some(),
        );
    }
}

// Combine independently-derived predicate subsets while preserving the old
// append behavior that left final query predicate normalization to planning.
fn combine_predicate_subset(
    existing: Option<Predicate>,
    appended: Option<Predicate>,
) -> Option<Predicate> {
    match (existing, appended) {
        (Some(existing), Some(appended)) => Some(Predicate::And(vec![existing, appended])),
        (Some(existing), None) => Some(existing),
        (None, Some(appended)) => Some(appended),
        (None, None) => None,
    }
}

///
/// ScalarIntent
///
/// Owned scalar intent state for query-intent planning.
/// Carries scalar query modifiers that are independent of grouped shape.
///

#[derive(Clone, Debug)]
pub(in crate::db::query::intent) struct ScalarIntent<K> {
    pub(in crate::db::query::intent) filter: Option<NormalizedFilter>,
    pub(in crate::db::query::intent) key_access: Option<KeyAccessState<K>>,
    pub(in crate::db::query::intent) key_access_conflict: bool,
    pub(in crate::db::query::intent) order: Option<OrderSpec>,
    pub(in crate::db::query::intent) distinct: bool,
    pub(in crate::db::query::intent) projection_selection: ProjectionSelection,
}

impl<K> ScalarIntent<K> {
    #[must_use]
    pub(in crate::db::query::intent) const fn new() -> Self {
        Self {
            filter: None,
            key_access: None,
            key_access_conflict: false,
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
pub(in crate::db::query::intent) struct GroupedIntent<K> {
    pub(in crate::db::query::intent) scalar: ScalarIntent<K>,
    pub(in crate::db::query::intent) group: GroupSpec,
    pub(in crate::db::query::intent) having_expr: Option<Expr>,
}

impl<K> GroupedIntent<K> {
    #[must_use]
    pub(in crate::db::query::intent) const fn from_scalar(scalar: ScalarIntent<K>) -> Self {
        Self {
            scalar,
            group: GroupSpec {
                group_fields: Vec::new(),
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
enum QueryShape<K> {
    Scalar(ScalarIntent<K>),
    Grouped(GroupedIntent<K>),
}

///
/// LoadIntentState
///
/// Typed state for load-mode intent.
/// Keeps load pagination spec and load-mode shape together.
///

#[derive(Clone, Debug)]
pub(in crate::db::query::intent) struct LoadIntentState<K> {
    spec: LoadSpec,
    offset_requested: bool,
    shape: QueryShape<K>,
}

impl<K> LoadIntentState<K> {
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
pub(in crate::db::query::intent) struct DeleteIntentState<K> {
    spec: DeleteSpec,
    scalar: ScalarIntent<K>,
    grouping_requested: bool,
}

impl<K> DeleteIntentState<K> {
    #[must_use]
    const fn new(scalar: ScalarIntent<K>, grouping_requested: bool) -> Self {
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
pub(in crate::db::query::intent) enum QueryIntent<K> {
    Load(LoadIntentState<K>),
    Delete(DeleteIntentState<K>),
}

impl<K> QueryIntent<K> {
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
    pub(in crate::db::query::intent) fn has_explicit_order(&self) -> bool {
        has_explicit_order(self.scalar().order.as_ref())
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
    pub(in crate::db::query::intent) const fn scalar(&self) -> &ScalarIntent<K> {
        match self {
            Self::Load(load) => match &load.shape {
                QueryShape::Scalar(scalar) => scalar,
                QueryShape::Grouped(grouped) => &grouped.scalar,
            },
            Self::Delete(delete) => &delete.scalar,
        }
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn scalar_mut(&mut self) -> &mut ScalarIntent<K> {
        match self {
            Self::Load(load) => match &mut load.shape {
                QueryShape::Scalar(scalar) => scalar,
                QueryShape::Grouped(grouped) => &mut grouped.scalar,
            },
            Self::Delete(delete) => &mut delete.scalar,
        }
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn grouped(&self) -> Option<&GroupedIntent<K>> {
        match self {
            Self::Load(load) => match &load.shape {
                QueryShape::Grouped(grouped) => Some(grouped),
                QueryShape::Scalar(_) => None,
            },
            Self::Delete(_) => None,
        }
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn grouped_mut(
        &mut self,
    ) -> Option<&mut GroupedIntent<K>> {
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
    ) -> Option<&mut GroupedIntent<K>> {
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
    pub(in crate::db::query::intent) fn planning_logical_inputs(&self) -> LogicalPlanningInputs {
        let (group, having_expr) = match self.grouped() {
            Some(grouped) => (Some(grouped.group.clone()), grouped.having_expr.clone()),
            None => (None, None),
        };

        LogicalPlanningInputs::new(
            self.mode(),
            self.scalar()
                .filter
                .as_ref()
                .and_then(NormalizedFilter::logical_filter_expr)
                .cloned(),
            self.scalar()
                .filter
                .as_ref()
                .is_some_and(NormalizedFilter::predicate_subset_covers_expr),
            self.scalar().order.clone(),
            self.scalar().distinct,
            group,
            having_expr,
        )
    }
}

impl<K: crate::db::KeyValueCodec> QueryIntent<K> {
    /// Project access-planning inputs from intent-owned scalar state.
    #[must_use]
    pub(in crate::db::query::intent) fn planning_access_inputs(&self) -> AccessPlanningInputs<'_> {
        let scalar = self.scalar();
        let key_access_projection = scalar
            .key_access
            .as_ref()
            .map(|state| project_key_access_for_planning(&state.access));
        let (key_access_override, key_access_input_resource) =
            key_access_projection.map_or((None, None), |projection| {
                let (access_plan, input_resource) = projection.into_parts();

                (Some(access_plan), input_resource)
            });

        AccessPlanningInputs::new(
            scalar
                .filter
                .as_ref()
                .and_then(NormalizedFilter::predicate_subset),
            scalar.order.as_ref(),
            key_access_override,
            key_access_input_resource,
        )
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
