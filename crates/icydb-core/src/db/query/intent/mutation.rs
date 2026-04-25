//! Module: query::intent::mutation
//! Responsibility: query-intent mutation helpers for scalar/grouped/load/delete intent state.
//! Does not own: final planner validation or executor route/runtime semantics.
//! Boundary: applies fluent/query API mutations to internal intent state contracts.

#[cfg(feature = "sql")]
use crate::db::query::plan::expr::ProjectionSelection;
use crate::db::{
    predicate::Predicate,
    query::{
        intent::{
            IntentError, KeyAccess, KeyAccessKind, KeyAccessState,
            state::{GroupedIntent, NormalizedFilter, QueryIntent},
        },
        plan::{
            FieldSlot, GroupAggregateSpec, GroupedExecutionConfig, OrderSpec, OrderTerm,
            expr::{BinaryOp, Expr, canonicalize_grouped_having_bool_expr, normalize_bool_expr},
        },
    },
};

impl<K> QueryIntent<K> {
    /// Append one normalized scalar filter expression to intent state,
    /// implicitly AND-ing multiple scalar filter clauses.
    pub(in crate::db::query::intent) fn append_filter_expr(&mut self, expr: Expr) {
        self.append_normalized_filter(NormalizedFilter::from_normalized_expr(expr));
    }

    /// Append one filter predicate to scalar intent, implicitly AND-ing chains.
    pub(in crate::db::query::intent) fn append_predicate(&mut self, predicate: Predicate) {
        self.append_normalized_filter(NormalizedFilter::from_normalized_predicate(predicate));
    }

    /// Append one normalized scalar filter with both semantic views already
    /// prepared by the caller.
    pub(in crate::db::query::intent) fn append_filter_with_predicate_subset(
        &mut self,
        expr: Expr,
        predicate: Predicate,
    ) {
        self.append_normalized_filter(NormalizedFilter::from_normalized_expr_and_predicate_subset(
            expr, predicate,
        ));
    }

    // Store scalar filters through the single normalized-filter seam so later
    // planning never has to reconcile independently-mutated filter fields.
    fn append_normalized_filter(&mut self, filter: NormalizedFilter) {
        let scalar = self.scalar_mut();
        match scalar.filter.as_mut() {
            Some(existing) => existing.append(filter),
            None => scalar.filter = Some(filter),
        };
    }

    /// Append one already-lowered ORDER BY term to scalar intent.
    pub(in crate::db::query::intent) fn push_order_term(&mut self, term: OrderTerm) {
        let scalar = self.scalar_mut();
        scalar.order = Some(match scalar.order.take() {
            Some(mut spec) => {
                spec.fields.push(term);
                spec
            }
            None => OrderSpec { fields: vec![term] },
        });
    }

    /// Override scalar ORDER BY with one validated order specification.
    pub(in crate::db::query::intent) fn set_order_spec(&mut self, order: OrderSpec) {
        self.scalar_mut().order = Some(order);
    }

    /// Enable DISTINCT semantics in scalar intent state.
    pub(in crate::db::query::intent) const fn set_distinct(&mut self) {
        self.scalar_mut().distinct = true;
    }

    /// Override scalar projection selection with one explicit planner contract.
    #[cfg(feature = "sql")]
    pub(in crate::db::query::intent) fn set_projection_selection(
        &mut self,
        projection_selection: ProjectionSelection,
    ) {
        self.scalar_mut().projection_selection = projection_selection;
    }

    /// Set key access to one single-key lookup.
    pub(in crate::db::query::intent) fn set_by_id(&mut self, id: K) {
        self.set_key_access(KeyAccessKind::Single, KeyAccess::Single(id));
    }

    /// Set key access to one many-key lookup set.
    pub(in crate::db::query::intent) fn set_by_ids<I>(&mut self, ids: I)
    where
        I: IntoIterator<Item = K>,
    {
        self.set_key_access(
            KeyAccessKind::Many,
            KeyAccess::Many(ids.into_iter().collect()),
        );
    }

    /// Set key access to the singleton key path.
    pub(in crate::db::query::intent) fn set_only(&mut self, id: K) {
        self.set_key_access(KeyAccessKind::Only, KeyAccess::Single(id));
    }

    /// Record one grouped key slot while preserving grouped-delete policy semantics.
    pub(in crate::db::query::intent) fn push_group_field_slot(&mut self, field_slot: FieldSlot) {
        let Some(grouped) = self.grouped_mutation_target() else {
            return;
        };

        let group = &mut grouped.group;
        if !group
            .group_fields
            .iter()
            .any(|existing| existing.index() == field_slot.index())
        {
            group.group_fields.push(field_slot);
        }
    }

    /// Record one grouped aggregate terminal while preserving delete policy flags.
    pub(in crate::db::query::intent) fn push_group_aggregate(
        &mut self,
        aggregate: GroupAggregateSpec,
    ) {
        let Some(grouped) = self.grouped_mutation_target() else {
            return;
        };

        grouped.group.aggregates.push(aggregate);
    }

    /// Override grouped hard limits while preserving delete-grouping policy flags.
    pub(in crate::db::query::intent) fn set_grouped_limits(
        &mut self,
        max_groups: u64,
        max_group_bytes: u64,
    ) {
        let Some(grouped) = self.grouped_mutation_target() else {
            return;
        };

        grouped.group.execution =
            GroupedExecutionConfig::with_hard_limits(max_groups, max_group_bytes);
    }

    /// Record one widened grouped HAVING expression when grouped shape is present.
    pub(in crate::db::query::intent) fn push_having_expr(
        &mut self,
        expr: Expr,
    ) -> Result<(), IntentError> {
        self.push_having_expr_with_policy(expr, true)
    }

    /// Record one grouped HAVING expression while preserving the caller-owned
    /// canonical grouped shape instead of re-running searched-CASE semantic
    /// canonicalization during append.
    pub(in crate::db::query::intent) fn push_having_expr_preserving_shape(
        &mut self,
        expr: Expr,
    ) -> Result<(), IntentError> {
        self.push_having_expr_with_policy(expr, false)
    }

    // Keep grouped HAVING append-order normalization on one seam while letting
    // fluent grouped builders and SQL-lowered grouped queries choose whether
    // searched-CASE semantic canonicalization should run at append time.
    fn push_having_expr_with_policy(
        &mut self,
        expr: Expr,
        canonicalize_case_semantics: bool,
    ) -> Result<(), IntentError> {
        if matches!(self, Self::Delete(_)) {
            if self.is_grouped() {
                self.mark_delete_grouping_requested();
                return Ok(());
            }

            return Err(IntentError::having_requires_group_by());
        }

        let Some(grouped) = self.grouped_mut() else {
            return Err(IntentError::having_requires_group_by());
        };

        let combined = match grouped.having_expr.take() {
            Some(existing) => Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(existing),
                right: Box::new(expr),
            },
            None => expr,
        };
        let canonical = if canonicalize_case_semantics {
            canonicalize_grouped_having_bool_expr(combined)
        } else {
            normalize_bool_expr(combined)
        };

        // Grouped HAVING still normalizes on one append seam, and callers can
        // opt into the shipped searched-CASE semantic canonicalization there
        // when the grouped expression shape is allowed to collapse. Grouped
        // boolean trees may still carry aggregate leaves that the scalar-only
        // normalized-shape checker rejects.
        grouped.having_expr = Some(canonical);

        Ok(())
    }

    // Record key-access origin and detect conflicting key-only builder usage.
    fn set_key_access(&mut self, kind: KeyAccessKind, access: KeyAccess<K>) {
        let scalar = self.scalar_mut();
        if let Some(existing) = &scalar.key_access
            && existing.kind != kind
        {
            scalar.key_access_conflict = true;
        }

        scalar.key_access = Some(KeyAccessState { kind, access });
    }

    // Route grouped declaration mutations onto one materialized grouped shape,
    // or preserve delete-mode grouping policy when grouped state is forbidden.
    fn grouped_mutation_target(&mut self) -> Option<&mut GroupedIntent<K>> {
        if matches!(self, Self::Delete(_)) {
            self.mark_delete_grouping_requested();
            return None;
        }

        Some(self.ensure_grouped_mut())
    }
}
