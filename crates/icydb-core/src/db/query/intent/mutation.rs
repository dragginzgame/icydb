//! Module: query::intent::mutation
//! Responsibility: query-intent mutation helpers for scalar/grouped/load/delete intent state.
//! Does not own: final planner validation or executor route/runtime semantics.
//! Boundary: applies fluent/query API mutations to internal intent state contracts.

use crate::db::query::plan::expr::ProjectionSelection;
use crate::db::query::{
    intent::{
        IntentError,
        state::{GroupedIntent, NormalizedFilter, QueryIntent},
    },
    plan::{
        GroupAggregateSpec, GroupFieldSet, GroupedExecutionConfig, OrderSpec,
        expr::{BinaryOp, Expr, normalize_bool_expr},
    },
};
use crate::db::{QueryError, predicate::Predicate, query::preparation::PreparationWork};

impl QueryIntent {
    /// Append one normalized scalar filter expression to intent state,
    /// implicitly AND-ing multiple scalar filter clauses.
    pub(in crate::db::query::intent) fn append_filter_expr(
        &mut self,
        expr: Expr,
        work: &PreparationWork<'_>,
    ) -> Result<(), QueryError> {
        self.append_normalized_filter(NormalizedFilter::from_normalized_expr(expr), work)
    }

    /// Append one already-normalized filter predicate to scalar intent,
    /// implicitly AND-ing chains.
    pub(in crate::db::query::intent) fn append_predicate(&mut self, predicate: Predicate) {
        let scalar = self.scalar_mut();
        match scalar.filter.as_mut() {
            Some(existing) => existing.append_predicate(predicate),
            None => scalar.filter = Some(NormalizedFilter::from_normalized_predicate(predicate)),
        }
    }

    /// Append one normalized scalar filter with both semantic views already
    /// prepared by the caller.
    pub(in crate::db::query::intent) fn append_filter_with_predicate_subset(
        &mut self,
        expr: Expr,
        predicate: Predicate,
        work: &PreparationWork<'_>,
    ) -> Result<(), QueryError> {
        self.append_normalized_filter(
            NormalizedFilter::from_normalized_expr_and_predicate_subset(expr, predicate),
            work,
        )
    }

    // Store scalar filters through the single normalized-filter seam so later
    // planning never has to reconcile independently-mutated filter fields.
    fn append_normalized_filter(
        &mut self,
        filter: NormalizedFilter,
        work: &PreparationWork<'_>,
    ) -> Result<(), QueryError> {
        let scalar = self.scalar_mut();
        match scalar.filter.as_mut() {
            Some(existing) => existing.append(filter, work)?,
            None => scalar.filter = Some(filter),
        }
        Ok(())
    }

    /// Set already-materialized ordering; planner validation remains downstream.
    pub(in crate::db::query::intent) fn set_order_spec(&mut self, order: OrderSpec) {
        self.scalar_mut().order = Some(order);
    }

    /// Enable DISTINCT semantics in scalar intent state.
    pub(in crate::db::query::intent) const fn set_distinct(&mut self) {
        self.scalar_mut().distinct = true;
    }

    /// Override scalar projection selection with one explicit planner contract.
    pub(in crate::db::query::intent) fn set_projection_selection(
        &mut self,
        projection_selection: ProjectionSelection,
    ) {
        self.scalar_mut().projection_selection = projection_selection;
    }

    /// Move resolved keys while preserving grouped-delete policy semantics.
    pub(in crate::db::query::intent) fn set_group_fields(&mut self, fields: GroupFieldSet) {
        let Some(grouped) = self.grouped_mutation_target() else {
            return;
        };

        grouped.group.group_fields = fields;
    }

    /// Move materialized aggregates while preserving delete policy flags.
    pub(in crate::db::query::intent) fn set_group_aggregates(
        &mut self,
        aggregates: Vec<GroupAggregateSpec>,
    ) {
        let Some(grouped) = self.grouped_mutation_target() else {
            return;
        };

        grouped.group.aggregates = aggregates;
    }

    /// Set explicit hard limits for grouped execution.
    pub(in crate::db::query::intent) fn set_grouped_execution_limits(
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

    /// Record one grouped HAVING expression while preserving the caller-owned
    /// canonical grouped shape instead of re-running searched-CASE semantic
    /// canonicalization during append.
    pub(in crate::db::query::intent) fn push_having_expr_preserving_shape(
        &mut self,
        expr: Expr,
        work: &PreparationWork<'_>,
    ) -> Result<(), QueryError> {
        if matches!(self, Self::Delete(_)) {
            if self.is_grouped() {
                self.mark_delete_grouping_requested();
                return Ok(());
            }

            return Err(QueryError::intent(IntentError::having_requires_group_by()));
        }

        let Some(grouped) = self.grouped_mut() else {
            return Err(QueryError::intent(IntentError::having_requires_group_by()));
        };

        let combined = match grouped.having_expr.take() {
            Some(existing) => Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(existing),
                right: Box::new(expr),
            },
            None => expr,
        };
        let canonical = normalize_bool_expr(combined, work)?;

        // The caller owns searched-CASE semantics; append only establishes
        // canonical ordering for the combined grouped expression.
        grouped.having_expr = Some(canonical);

        Ok(())
    }

    // Record key-access origin and detect conflicting key-only builder usage.

    // Route grouped declaration mutations onto one materialized grouped shape,
    // or preserve delete-mode grouping policy when grouped state is forbidden.
    fn grouped_mutation_target(&mut self) -> Option<&mut GroupedIntent> {
        if matches!(self, Self::Delete(_)) {
            self.mark_delete_grouping_requested();
            return None;
        }

        self.ensure_grouped_mut()
    }
}
