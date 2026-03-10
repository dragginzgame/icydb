//! Module: db::executor::load::contracts::grouped::stages
//! Responsibility: module-local ownership and contracts for db::executor::load::contracts::grouped::stages.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        Context,
        executor::{ExecutionOptimization, ExecutionPreparation, load::ResolvedExecutionKeyStream},
    },
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::load::contracts::GroupedCursorPage;

///
/// GroupedStreamStage
///
/// Stream-construction stage payload for grouped execution.
/// Owns recovered context, execution preparation, and resolved grouped key
/// stream for fold-phase consumption.
///

pub(in crate::db::executor::load) struct GroupedStreamStage<'a, E: EntityKind + EntityValue> {
    ctx: Context<'a, E>,
    execution_preparation: ExecutionPreparation,
    resolved: ResolvedExecutionKeyStream,
}

impl<'a, E> GroupedStreamStage<'a, E>
where
    E: EntityKind + EntityValue,
{
    // Build one grouped stream stage from recovered context, execution preparation,
    // and resolved grouped key stream payload.
    pub(in crate::db::executor::load) const fn new(
        ctx: Context<'a, E>,
        execution_preparation: ExecutionPreparation,
        resolved: ResolvedExecutionKeyStream,
    ) -> Self {
        Self {
            ctx,
            execution_preparation,
            resolved,
        }
    }

    // Borrow grouped runtime context, execution preparation, and mutable resolved
    // key stream together so callers can combine immutable/mutable borrows safely.
    pub(in crate::db::executor::load) const fn parts_mut(
        &mut self,
    ) -> (
        &Context<'a, E>,
        &ExecutionPreparation,
        &mut ResolvedExecutionKeyStream,
    ) {
        (&self.ctx, &self.execution_preparation, &mut self.resolved)
    }

    // Derive grouped path `rows_scanned` from resolved stream metadata or runtime fallback.
    pub(in crate::db::executor::load) fn rows_scanned(&self, fallback: usize) -> usize {
        self.resolved.rows_scanned_override().unwrap_or(fallback)
    }

    // Borrow grouped path optimization outcome metadata.
    pub(in crate::db::executor::load) const fn optimization(
        &self,
    ) -> Option<ExecutionOptimization> {
        self.resolved.optimization()
    }

    // Borrow grouped path index-predicate observability metadata.
    pub(in crate::db::executor::load) const fn index_predicate_applied(&self) -> bool {
        self.resolved.index_predicate_applied()
    }

    // Borrow grouped path index-predicate rejection counter.
    pub(in crate::db::executor::load) const fn index_predicate_keys_rejected(&self) -> u64 {
        self.resolved.index_predicate_keys_rejected()
    }

    // Borrow grouped path DISTINCT-key dedupe counter.
    pub(in crate::db::executor::load) fn distinct_keys_deduped(&self) -> u64 {
        self.resolved.distinct_keys_deduped()
    }
}

///
/// GroupedFoldStage
///
/// Fold-phase output payload for grouped execution.
/// Owns grouped page materialization plus observability counters consumed by
/// the final output stage.
///

pub(in crate::db::executor::load) struct GroupedFoldStage {
    page: GroupedCursorPage,
    filtered_rows: usize,
    check_filtered_rows_upper_bound: bool,
    rows_scanned: usize,
    optimization: Option<ExecutionOptimization>,
    index_predicate_applied: bool,
    index_predicate_keys_rejected: u64,
    distinct_keys_deduped: u64,
}

impl GroupedFoldStage {
    // Build one grouped fold-stage payload from grouped page output plus stream
    // observability metadata captured after grouped fold execution.
    pub(in crate::db::executor::load) fn from_grouped_stream<E>(
        page: GroupedCursorPage,
        filtered_rows: usize,
        check_filtered_rows_upper_bound: bool,
        stream: &GroupedStreamStage<'_, E>,
        scanned_rows_fallback: usize,
    ) -> Self
    where
        E: EntityKind + EntityValue,
    {
        Self {
            page,
            filtered_rows,
            check_filtered_rows_upper_bound,
            rows_scanned: stream.rows_scanned(scanned_rows_fallback),
            optimization: stream.optimization(),
            index_predicate_applied: stream.index_predicate_applied(),
            index_predicate_keys_rejected: stream.index_predicate_keys_rejected(),
            distinct_keys_deduped: stream.distinct_keys_deduped(),
        }
    }

    // Return grouped output row count for observability.
    pub(in crate::db::executor::load) const fn rows_returned(&self) -> usize {
        self.page.rows.len()
    }

    // Borrow grouped path optimization outcome metadata.
    pub(in crate::db::executor::load) const fn optimization(
        &self,
    ) -> Option<ExecutionOptimization> {
        self.optimization
    }

    // Borrow grouped path rows-scanned observability metric.
    pub(in crate::db::executor::load) const fn rows_scanned(&self) -> usize {
        self.rows_scanned
    }

    // Borrow grouped path index-predicate observability metadata.
    pub(in crate::db::executor::load) const fn index_predicate_applied(&self) -> bool {
        self.index_predicate_applied
    }

    // Borrow grouped path index-predicate rejection counter.
    pub(in crate::db::executor::load) const fn index_predicate_keys_rejected(&self) -> u64 {
        self.index_predicate_keys_rejected
    }

    // Borrow grouped path DISTINCT-key dedupe counter.
    pub(in crate::db::executor::load) const fn distinct_keys_deduped(&self) -> u64 {
        self.distinct_keys_deduped
    }

    // Return whether grouped finalization should assert filtered-row upper bound.
    pub(in crate::db::executor::load) const fn should_check_filtered_rows_upper_bound(
        &self,
    ) -> bool {
        self.check_filtered_rows_upper_bound
    }

    // Borrow grouped filtered-row count for pagination sanity checks.
    pub(in crate::db::executor::load) const fn filtered_rows(&self) -> usize {
        self.filtered_rows
    }

    // Consume folded stage and return final grouped page payload.
    pub(in crate::db::executor::load) fn into_page(self) -> GroupedCursorPage {
        self.page
    }
}
