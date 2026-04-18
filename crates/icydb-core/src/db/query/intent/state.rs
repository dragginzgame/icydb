//! Module: query::intent::state
//! Responsibility: internal mutable query-intent state transitions across load/delete modes.
//! Does not own: planner semantic validation or executor runtime behavior.
//! Boundary: records intent-shape state consumed by planner-owned validation/build stages.

use crate::db::{
    predicate::Predicate,
    query::{
        intent::{KeyAccessState, build_access_plan_from_keys},
        plan::{
            AccessPlanningInputs, DeleteSpec, GroupSpec, GroupedExecutionConfig, LoadSpec,
            LogicalPlanningInputs, OrderSpec, QueryMode,
            expr::{Expr, ProjectionSelection},
            has_explicit_order,
        },
    },
};

///
/// ScalarIntent
///
/// Owned scalar intent state for query-intent planning.
/// Carries scalar query modifiers that are independent of grouped shape.
///

#[derive(Clone, Debug)]
pub(in crate::db::query::intent) struct ScalarIntent<K> {
    pub(in crate::db::query::intent) predicate: Option<Predicate>,
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
            predicate: None,
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
/// DeletePolicyState
///
/// Delete policy flags preserved for stable intent-policy errors.
/// These flags keep delete-only grouping validation separate from load mode.
///

#[derive(Clone, Copy, Debug)]
struct DeletePolicyState {
    grouping_requested: bool,
}

///
/// DeleteIntentState
///
/// Typed state for delete-mode intent.
/// Delete mode intentionally carries only scalar shape plus delete policy flags.
///

#[derive(Clone, Debug)]
pub(in crate::db::query::intent) struct DeleteIntentState<K> {
    spec: DeleteSpec,
    scalar: ScalarIntent<K>,
    policy: DeletePolicyState,
}

impl<K> DeleteIntentState<K> {
    #[must_use]
    const fn new(scalar: ScalarIntent<K>, policy: DeletePolicyState) -> Self {
        Self {
            spec: DeleteSpec::new(),
            scalar,
            policy,
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
            Self::Delete(delete) => delete.policy.grouping_requested,
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
                let policy = DeletePolicyState { grouping_requested };

                Self::Delete(DeleteIntentState::new(scalar, policy))
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

    pub(in crate::db::query::intent) fn ensure_grouped_mut(&mut self) -> &mut GroupedIntent<K> {
        let Self::Load(load) = self else {
            panic!("grouped shape cannot be materialized in delete mode");
        };

        if matches!(load.shape, QueryShape::Scalar(_)) {
            // Lift scalar shape into grouped shape while preserving scalar modifiers.
            let scalar =
                match std::mem::replace(&mut load.shape, QueryShape::Scalar(ScalarIntent::new())) {
                    QueryShape::Scalar(scalar) => scalar,
                    QueryShape::Grouped(_) => unreachable!("shape checked above"),
                };
            load.shape = QueryShape::Grouped(GroupedIntent::from_scalar(scalar));
        }

        match &mut load.shape {
            QueryShape::Grouped(grouped) => grouped,
            QueryShape::Scalar(_) => unreachable!("scalar shape lifted to grouped"),
        }
    }

    pub(in crate::db::query::intent) const fn mark_delete_grouping_requested(&mut self) {
        if let Self::Delete(delete) = self {
            delete.policy.grouping_requested = true;
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
            self.scalar().order.clone(),
            self.scalar().distinct,
            group,
            having_expr,
        )
    }
}

impl<K: crate::traits::FieldValue> QueryIntent<K> {
    /// Project access-planning inputs from intent-owned scalar state.
    #[must_use]
    pub(in crate::db::query::intent) fn planning_access_inputs(&self) -> AccessPlanningInputs<'_> {
        let scalar = self.scalar();
        let key_access_override = scalar
            .key_access
            .as_ref()
            .map(|state| build_access_plan_from_keys(&state.access));

        AccessPlanningInputs::new(
            scalar.predicate.as_ref(),
            scalar.order.as_ref(),
            key_access_override,
        )
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            predicate::CompareOp,
            query::{
                intent::{IntentError, KeyAccessKind},
                plan::{FieldSlot, GroupHavingClause, GroupHavingSymbol, OrderDirection},
            },
        },
        value::Value,
    };

    fn sample_having_clause() -> GroupHavingClause {
        GroupHavingClause {
            symbol: GroupHavingSymbol::AggregateIndex(0),
            op: CompareOp::Eq,
            value: Value::from(1_u64),
        }
    }

    #[test]
    fn query_intent_new_starts_in_load_scalar_mode() {
        let intent = QueryIntent::<u64>::new();

        assert!(matches!(intent.mode(), QueryMode::Load(_)));
        assert!(matches!(
            intent.mode(),
            QueryMode::Load(LoadSpec {
                limit: None,
                offset: 0
            })
        ));
        assert!(
            !intent.is_grouped(),
            "new intent must start in scalar shape without grouped policy flags"
        );
        assert!(matches!(intent.mode(), QueryMode::Load(_)));
    }

    #[test]
    fn delete_mode_tracks_offset_in_mode_spec() {
        let intent = QueryIntent::<u64>::new().set_delete_mode().apply_offset(5);

        assert!(
            matches!(
                intent.mode(),
                QueryMode::Delete(DeleteSpec { offset: 5, .. })
            ),
            "offset requested in delete mode must remain visible on the delete spec"
        );
        assert!(
            matches!(intent.mode(), QueryMode::Delete(_)),
            "delete mode must expose delete-mode query state"
        );
    }

    #[test]
    fn grouped_load_to_delete_preserves_grouping_policy_without_group_shape() {
        let mut intent = QueryIntent::<u64>::new();
        let _ = intent.ensure_grouped_mut();
        assert!(
            intent.grouped().is_some(),
            "load mode grouped intent should expose grouped shape"
        );

        let intent = intent.set_delete_mode();

        assert!(matches!(intent.mode(), QueryMode::Delete(_)));
        assert!(
            intent.is_grouped(),
            "delete mode should preserve grouped-delete policy signal"
        );
        assert!(
            intent.grouped().is_none(),
            "delete mode must not carry grouped shape state"
        );
    }

    #[test]
    fn grouped_scalar_flags_survive_mode_transition() {
        let mut intent = QueryIntent::<u64>::new();
        intent.scalar_mut().key_access_conflict = true;
        let _ = intent.ensure_grouped_mut();

        let intent = intent.set_delete_mode();

        assert!(
            intent.scalar().key_access_conflict,
            "mode transitions must preserve scalar conflict flags"
        );
    }

    #[test]
    fn group_field_slot_deduplicates_by_slot_index() {
        let mut intent = QueryIntent::<u64>::new();
        intent.push_group_field_slot(FieldSlot::from_parts_for_test(4, "rank"));
        intent.push_group_field_slot(FieldSlot::from_parts_for_test(4, "duplicate-rank"));

        let grouped = intent
            .grouped()
            .expect("grouped shape should be materialized after grouped slot push");

        assert_eq!(
            grouped.group.group_fields.len(),
            1,
            "group field slots should be deduplicated by stable model slot index"
        );
    }

    #[test]
    fn having_clause_requires_grouped_shape() {
        let mut intent = QueryIntent::<u64>::new();

        let result = intent.push_having_clause(sample_having_clause());

        assert!(
            matches!(result, Err(IntentError::HavingRequiresGroupBy)),
            "having clauses should reject scalar shape"
        );
    }

    #[test]
    fn delete_grouping_policy_accepts_having_clause_when_group_requested() {
        let mut intent = QueryIntent::<u64>::new();
        intent.push_group_field_slot(FieldSlot::from_parts_for_test(0, "id"));

        let mut intent = intent.set_delete_mode();
        let result = intent.push_having_clause(sample_having_clause());

        assert!(
            result.is_ok(),
            "delete mode should preserve grouped-delete policy signal for having checks"
        );
        assert!(
            intent.grouped().is_none(),
            "delete mode should not materialize grouped shape state"
        );
        assert!(
            intent.is_grouped(),
            "delete mode should keep grouped policy flag after having clause"
        );
    }

    #[test]
    fn append_predicate_ands_multiple_filters() {
        let mut intent = QueryIntent::<u64>::new();
        intent.append_predicate(Predicate::True);
        intent.append_predicate(Predicate::False);

        assert!(
            matches!(
                intent.scalar().predicate,
                Some(Predicate::And(ref clauses)) if clauses.len() == 2
            ),
            "multiple filters should be preserved as a stable AND chain"
        );
    }

    #[test]
    fn push_order_terms_preserve_declared_order_sequence() {
        let mut intent = QueryIntent::<u64>::new();
        intent.push_order_term(crate::db::asc("rank").lower());
        intent.push_order_term(crate::db::desc("created_at").lower());

        let fields = intent
            .scalar()
            .order
            .as_ref()
            .expect("order should exist after order helper calls")
            .fields
            .clone();

        assert_eq!(
            fields,
            vec![
                crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
                crate::db::query::plan::OrderTerm::field("created_at", OrderDirection::Desc),
            ],
            "typed order-term sequence should match user declaration order"
        );
    }

    #[test]
    fn key_access_conflict_flag_only_flips_for_mixed_access_kinds() {
        let mut intent = QueryIntent::<u64>::new();
        intent.set_by_id(10);
        intent.set_by_id(20);

        assert!(
            !intent.scalar().key_access_conflict,
            "reusing the same key access kind should not mark conflicts"
        );
        assert!(
            matches!(
                intent.scalar().key_access.as_ref().map(|state| state.kind),
                Some(KeyAccessKind::Single)
            ),
            "latest same-kind key access should remain single-key access"
        );

        intent.set_only(20);

        assert!(
            intent.scalar().key_access_conflict,
            "mixing key access kinds should mark intent key-access conflict"
        );
        assert!(
            matches!(
                intent.scalar().key_access.as_ref().map(|state| state.kind),
                Some(KeyAccessKind::Only)
            ),
            "latest mixed-kind key access should keep most recent origin kind"
        );
    }
}
