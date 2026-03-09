//! Module: executor::load::execute
//! Responsibility: key-stream resolution and fast-path/fallback execution dispatch.
//! Does not own: cursor decoding policy or logical-plan construction.
//! Boundary: execution-attempt internals used by `executor::load`.

mod fast_path;

#[cfg(test)]
use crate::db::executor::route::ensure_load_fast_path_spec_arity;
use crate::{
    db::{
        Context,
        executor::load::{CursorPage, LoadExecutor},
        executor::plan_metrics::set_rows_from_len,
        executor::{
            AccessStreamBindings, ExecutionOptimization, ExecutionPreparation, ExecutionTrace,
            OrderedKeyStream, OrderedKeyStreamBox, traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    obs::sink::Span,
    traits::{EntityKind, EntityValue},
};
use std::{cell::Cell, rc::Rc};

///
/// ExecutionInputs
///
/// Shared immutable execution inputs for one load execution attempt.
/// Keeps fast-path dispatch signatures compact without changing behavior.
///

pub(in crate::db::executor) struct ExecutionInputs<'a, E: EntityKind + EntityValue> {
    ctx: &'a Context<'a, E>,
    plan: &'a AccessPlannedQuery<E::Key>,
    stream_bindings: AccessStreamBindings<'a>,
    execution_preparation: &'a ExecutionPreparation,
}

impl<'a, E> ExecutionInputs<'a, E>
where
    E: EntityKind + EntityValue,
{
    /// Construct one scalar execution-input projection payload.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        ctx: &'a Context<'a, E>,
        plan: &'a AccessPlannedQuery<E::Key>,
        stream_bindings: AccessStreamBindings<'a>,
        execution_preparation: &'a ExecutionPreparation,
    ) -> Self {
        Self {
            ctx,
            plan,
            stream_bindings,
            execution_preparation,
        }
    }
}

///
/// ExecutionInputsProjection
///
/// Compile-time projection boundary for scalar execution-input consumers.
/// Load/kernel helpers consume this projection surface instead of reaching into
/// `ExecutionInputs` fields directly.
///

pub(in crate::db::executor) trait ExecutionInputsProjection<E>
where
    E: EntityKind + EntityValue,
{
    /// Borrow recovered execution context for row/index reads.
    fn ctx(&self) -> &Context<'_, E>;

    /// Borrow logical access plan payload for this execution attempt.
    fn plan(&self) -> &AccessPlannedQuery<E::Key>;

    /// Borrow lowered access stream bindings for this execution attempt.
    fn stream_bindings(&self) -> &AccessStreamBindings<'_>;

    /// Borrow precomputed execution-preparation payloads.
    fn execution_preparation(&self) -> &ExecutionPreparation;

    /// Return row-read missing-row policy for this execution attempt.
    fn consistency(&self) -> MissingRowPolicy;
}

impl<E> ExecutionInputsProjection<E> for ExecutionInputs<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn ctx(&self) -> &Context<'_, E> {
        self.ctx
    }

    fn plan(&self) -> &AccessPlannedQuery<E::Key> {
        self.plan
    }

    fn stream_bindings(&self) -> &AccessStreamBindings<'_> {
        &self.stream_bindings
    }

    fn execution_preparation(&self) -> &ExecutionPreparation {
        self.execution_preparation
    }

    fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.plan)
    }
}

///
/// ResolvedExecutionKeyStream
///
/// Canonical key-stream resolution output for one load execution attempt.
/// Keeps fast-path metadata and fallback stream output on one shared boundary.
///

pub(in crate::db::executor) struct ResolvedExecutionKeyStream {
    key_stream: OrderedKeyStreamBox,
    optimization: Option<ExecutionOptimization>,
    rows_scanned_override: Option<usize>,
    index_predicate_applied: bool,
    index_predicate_keys_rejected: u64,
    distinct_keys_deduped_counter: Option<Rc<Cell<u64>>>,
}

impl ResolvedExecutionKeyStream {
    /// Construct one resolved key-stream payload.
    #[must_use]
    pub(in crate::db::executor) fn new(
        key_stream: OrderedKeyStreamBox,
        optimization: Option<ExecutionOptimization>,
        rows_scanned_override: Option<usize>,
        index_predicate_applied: bool,
        index_predicate_keys_rejected: u64,
        distinct_keys_deduped_counter: Option<Rc<Cell<u64>>>,
    ) -> Self {
        Self {
            key_stream,
            optimization,
            rows_scanned_override,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped_counter,
        }
    }

    /// Decompose resolved key-stream payload into raw parts.
    #[must_use]
    #[expect(clippy::type_complexity)]
    pub(in crate::db::executor) fn into_parts(
        self,
    ) -> (
        OrderedKeyStreamBox,
        Option<ExecutionOptimization>,
        Option<usize>,
        bool,
        u64,
        Option<Rc<Cell<u64>>>,
    ) {
        (
            self.key_stream,
            self.optimization,
            self.rows_scanned_override,
            self.index_predicate_applied,
            self.index_predicate_keys_rejected,
            self.distinct_keys_deduped_counter,
        )
    }

    /// Borrow mutable ordered key stream.
    pub(in crate::db::executor) fn key_stream_mut(&mut self) -> &mut dyn OrderedKeyStream {
        self.key_stream.as_mut()
    }

    /// Return optional rows-scanned override.
    #[must_use]
    pub(in crate::db::executor) const fn rows_scanned_override(&self) -> Option<usize> {
        self.rows_scanned_override
    }

    /// Return resolved optimization label.
    #[must_use]
    pub(in crate::db::executor) const fn optimization(&self) -> Option<ExecutionOptimization> {
        self.optimization
    }

    /// Return whether index predicate was applied during access stream resolution.
    #[must_use]
    pub(in crate::db::executor) const fn index_predicate_applied(&self) -> bool {
        self.index_predicate_applied
    }

    /// Return count of index predicate key rejections during stream resolution.
    #[must_use]
    pub(in crate::db::executor) const fn index_predicate_keys_rejected(&self) -> u64 {
        self.index_predicate_keys_rejected
    }

    /// Return distinct deduplicated key count for this resolved stream.
    #[must_use]
    pub(in crate::db::executor) fn distinct_keys_deduped(&self) -> u64 {
        self.distinct_keys_deduped_counter
            .as_ref()
            .map_or(0, |counter| counter.get())
    }
}

///
/// MaterializedExecutionAttempt
///
/// Canonical materialization attempt output for load execution.
/// Preserves one shared boundary for retry accounting and page output.
///

pub(in crate::db::executor) struct MaterializedExecutionAttempt<E: EntityKind> {
    pub(in crate::db::executor) page: CursorPage<E>,
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) post_access_rows: usize,
    pub(in crate::db::executor) optimization: Option<ExecutionOptimization>,
    pub(in crate::db::executor) index_predicate_applied: bool,
    pub(in crate::db::executor) index_predicate_keys_rejected: u64,
    pub(in crate::db::executor) distinct_keys_deduped: u64,
}

///
/// ExecutionOutcomeMetrics
///
/// Finalization-time observability metrics for one materialized load execution
/// attempt. Keeps path-outcome reporting fields grouped as one boundary payload.
///

pub(in crate::db::executor) struct ExecutionOutcomeMetrics {
    pub(in crate::db::executor) optimization: Option<ExecutionOptimization>,
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) post_access_rows: usize,
    pub(in crate::db::executor) index_predicate_applied: bool,
    pub(in crate::db::executor) index_predicate_keys_rejected: u64,
    pub(in crate::db::executor) distinct_keys_deduped: u64,
}

impl<E: EntityKind> MaterializedExecutionAttempt<E> {
    // Split one materialized execution attempt into response page + observability metrics.
    pub(in crate::db::executor) fn into_page_and_metrics(
        self,
    ) -> (CursorPage<E>, ExecutionOutcomeMetrics) {
        let metrics = ExecutionOutcomeMetrics {
            optimization: self.optimization,
            rows_scanned: self.rows_scanned,
            post_access_rows: self.post_access_rows,
            index_predicate_applied: self.index_predicate_applied,
            index_predicate_keys_rejected: self.index_predicate_keys_rejected,
            distinct_keys_deduped: self.distinct_keys_deduped,
        };

        (self.page, metrics)
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Apply shared path finalization hooks after page materialization.
    /// Finalize one execution attempt by recording path/row observability outputs.
    pub(super) fn finalize_execution(
        page: CursorPage<E>,
        metrics: ExecutionOutcomeMetrics,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
        execution_time_micros: u64,
    ) -> CursorPage<E> {
        Self::finalize_path_outcome(execution_trace, metrics, false, execution_time_micros);
        set_rows_from_len(span, page.items.len());

        page
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::error::ErrorClass;

    #[test]
    fn fast_path_spec_arity_accepts_single_spec_shapes() {
        let result = super::ensure_load_fast_path_spec_arity(true, 1, true, 1);

        assert!(result.is_ok(), "single fast-path specs should be accepted");
    }

    #[test]
    fn fast_path_spec_arity_rejects_multiple_prefix_specs_for_secondary() {
        let err = super::ensure_load_fast_path_spec_arity(true, 2, false, 0)
            .expect_err("secondary fast-path must reject multiple index-prefix specs");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "prefix-spec arity violation must classify as invariant violation"
        );
        assert!(
            err.message
                .contains("secondary fast-path resolution expects at most one index-prefix spec"),
            "prefix-spec arity violation must return a clear invariant message"
        );
    }

    #[test]
    fn fast_path_spec_arity_rejects_multiple_range_specs_for_index_range() {
        let err = super::ensure_load_fast_path_spec_arity(false, 0, true, 2)
            .expect_err("index-range fast-path must reject multiple index-range specs");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "range-spec arity violation must classify as invariant violation"
        );
        assert!(
            err.message
                .contains("index-range fast-path resolution expects at most one index-range spec"),
            "range-spec arity violation must return a clear invariant message"
        );
    }
}
