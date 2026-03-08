//! Module: executor::load::execute
//! Responsibility: key-stream resolution and fast-path/fallback execution dispatch.
//! Does not own: cursor decoding policy or logical-plan construction.
//! Boundary: execution-attempt internals used by `executor::load`.

use crate::{
    db::{
        Context,
        executor::load::{CursorPage, FastPathKeyResult, LoadExecutor},
        executor::plan_metrics::set_rows_from_len,
        executor::{
            AccessExecutionDescriptor, AccessStreamBindings, ExecutionOptimization, ExecutionPlan,
            ExecutionPreparation, ExecutionTrace, OrderedKeyStream, OrderedKeyStreamBox,
            route::{
                FastPathOrder, RoutedKeyStreamRequest, ensure_load_fast_path_spec_arity,
                try_first_verified_fast_path_hit,
            },
            traversal::row_read_consistency_for_plan,
        },
        index::{IndexCompilePolicy, compile_index_program, predicate::IndexPredicateExecution},
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
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

///
/// FastPathDecision
///
/// Canonical fast-path routing decision for one execution attempt.
///

enum FastPathDecision {
    Hit(FastPathKeyResult),
    None,
}

// Strategy selected once from route shape so key-stream resolution does not
// branch inline on fast-path eligibility policy.
enum FastPathResolutionStrategy {
    StreamingFastPathFirst,
    FallbackOnly,
}

impl FastPathResolutionStrategy {
    const fn for_route(route_plan: &ExecutionPlan) -> Self {
        if route_plan.shape().is_streaming() {
            Self::StreamingFastPathFirst
        } else {
            Self::FallbackOnly
        }
    }

    fn resolve_fast_path_decision<E, I>(
        self,
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<FastPathDecision, InternalError>
    where
        E: EntityKind + EntityValue,
        I: ExecutionInputsProjection<E>,
    {
        match self {
            Self::StreamingFastPathFirst => {
                LoadExecutor::<E>::evaluate_fast_path(inputs, route_plan, index_predicate_execution)
            }
            Self::FallbackOnly => Ok(FastPathDecision::None),
        }
    }
}

// Strategy selected once from verified fast-path route so route-specific stream
// execution stays centralized.
enum FastPathRouteHandler {
    PrimaryKey,
    SecondaryPrefix,
    IndexRange { fetch: Option<usize> },
    None,
}

impl FastPathRouteHandler {
    fn resolve(route_plan: &ExecutionPlan, verified_route: FastPathOrder) -> Self {
        match verified_route {
            FastPathOrder::PrimaryKey => Self::PrimaryKey,
            FastPathOrder::SecondaryPrefix => Self::SecondaryPrefix,
            FastPathOrder::IndexRange => Self::IndexRange {
                fetch: route_plan
                    .index_range_limit_spec
                    .as_ref()
                    .map(|spec| spec.fetch),
            },
            FastPathOrder::PrimaryScan | FastPathOrder::Composite => Self::None,
        }
    }

    fn execute<E, I>(
        self,
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError>
    where
        E: EntityKind + EntityValue,
        I: ExecutionInputsProjection<E>,
    {
        match self {
            Self::PrimaryKey => LoadExecutor::<E>::try_execute_pk_order_stream(
                inputs.ctx(),
                inputs.plan(),
                inputs.stream_bindings().direction(),
                route_plan.scan_hints.physical_fetch_hint,
            ),
            Self::SecondaryPrefix => LoadExecutor::<E>::try_execute_secondary_index_order_stream(
                inputs.ctx(),
                inputs.plan(),
                inputs.stream_bindings().index_prefix_specs.first(),
                inputs.stream_bindings().direction(),
                route_plan.scan_hints.physical_fetch_hint,
                index_predicate_execution,
            ),
            Self::IndexRange { fetch: Some(fetch) } => {
                LoadExecutor::<E>::try_execute_index_range_limit_pushdown_stream(
                    inputs.ctx(),
                    inputs.plan(),
                    inputs.stream_bindings().index_range_specs.first(),
                    inputs.stream_bindings().continuation,
                    fetch,
                    index_predicate_execution,
                )
            }
            Self::IndexRange { fetch: None } | Self::None => Ok(None),
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one canonical execution key stream in fast-path precedence order.
    ///
    /// This is the single shared load key-stream resolver boundary.
    pub(in crate::db::executor) fn resolve_execution_key_stream_without_distinct<I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<ResolvedExecutionKeyStream, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
        // Phase 0: compile optional index predicate execution program.
        let index_predicate_program = inputs
            .execution_preparation()
            .compiled_predicate()
            .and_then(|compiled_predicate| {
                let slot_map = inputs.execution_preparation().slot_map()?;

                compile_index_program(
                    compiled_predicate.resolved(),
                    slot_map,
                    predicate_compile_mode,
                )
            });
        let index_predicate_applied = index_predicate_program.is_some();
        let index_predicate_rejected_counter = Cell::new(0u64);
        let index_predicate_execution =
            index_predicate_program
                .as_ref()
                .map(|program| IndexPredicateExecution {
                    program,
                    rejected_keys_counter: Some(&index_predicate_rejected_counter),
                });

        // Phase 1: select fast-path resolution strategy once from route shape.
        let fast_path_strategy = FastPathResolutionStrategy::for_route(route_plan);
        let fast_path_decision = fast_path_strategy.resolve_fast_path_decision::<E, I>(
            inputs,
            route_plan,
            index_predicate_execution,
        )?;

        // Phase 2: materialize from fast-path hit or canonical fallback stream.
        let resolved = Self::resolve_execution_key_stream_from_decision(
            fast_path_decision,
            inputs,
            route_plan,
            index_predicate_execution,
            index_predicate_applied,
            &index_predicate_rejected_counter,
        )?;

        Ok(resolved)
    }

    // Resolve one canonical key stream from fast-path decision output.
    fn resolve_execution_key_stream_from_decision<I>(
        fast_path_decision: FastPathDecision,
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        index_predicate_applied: bool,
        index_predicate_rejected_counter: &Cell<u64>,
    ) -> Result<ResolvedExecutionKeyStream, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
        match fast_path_decision {
            FastPathDecision::Hit(fast) => Ok(ResolvedExecutionKeyStream::new(
                fast.ordered_key_stream,
                Some(Self::decorate_fast_path_optimization_for_route(
                    fast.optimization,
                    route_plan,
                )),
                Some(fast.rows_scanned),
                index_predicate_applied,
                index_predicate_rejected_counter.get(),
                None,
            )),
            FastPathDecision::None => Self::resolve_fallback_execution_key_stream(
                inputs,
                route_plan,
                index_predicate_execution,
                index_predicate_applied,
                index_predicate_rejected_counter,
            ),
        }
    }

    // Resolve canonical fallback access stream when no fast path produced rows.
    fn resolve_fallback_execution_key_stream<I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        index_predicate_applied: bool,
        index_predicate_rejected_counter: &Cell<u64>,
    ) -> Result<ResolvedExecutionKeyStream, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
        let fallback_fetch_hint =
            route_plan.fallback_physical_fetch_hint(inputs.stream_bindings().direction());
        let descriptor = AccessExecutionDescriptor::from_bindings(
            &inputs.plan().access,
            *inputs.stream_bindings(),
            fallback_fetch_hint,
            index_predicate_execution,
        );
        let key_stream = Self::resolve_routed_key_stream(
            inputs.ctx(),
            RoutedKeyStreamRequest::AccessDescriptor(descriptor),
        )?;

        Ok(ResolvedExecutionKeyStream::new(
            key_stream,
            None,
            None,
            index_predicate_applied,
            index_predicate_rejected_counter.get(),
            None,
        ))
    }

    /// Evaluate fast-path routes in canonical precedence and return one decision.
    // Evaluate fast-path routes in canonical precedence and return one decision.
    fn evaluate_fast_path<I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<FastPathDecision, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
        // Guard fast-path spec arity up front so plan/runtime traversal drift
        // cannot silently consume the wrong spec in release builds.
        ensure_load_fast_path_spec_arity(
            route_plan.secondary_fast_path_eligible(),
            inputs.stream_bindings().index_prefix_specs.len(),
            route_plan.index_range_limit_fast_path_enabled(),
            inputs.stream_bindings().index_range_specs.len(),
        )?;

        let fast = try_first_verified_fast_path_hit(
            route_plan.fast_path_order(),
            |route| {
                Ok(route_plan
                    .load_fast_path_route_eligible(route)
                    .then_some(route))
            },
            |verified_route| {
                Self::try_execute_verified_load_fast_path(
                    inputs,
                    route_plan,
                    index_predicate_execution,
                    verified_route,
                )
            },
        )?;

        if let Some(fast) = fast {
            return Ok(FastPathDecision::Hit(fast));
        }

        Ok(FastPathDecision::None)
    }

    // Execute one verified fast-path route and return keys if the route produces them.
    fn try_execute_verified_load_fast_path<I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        verified_route: FastPathOrder,
    ) -> Result<Option<FastPathKeyResult>, InternalError>
    where
        I: ExecutionInputsProjection<E>,
    {
        let handler = FastPathRouteHandler::resolve(route_plan, verified_route);

        handler.execute::<E, I>(inputs, route_plan, index_predicate_execution)
    }

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

    // Project one fast-path optimization label through route-level top-N seek
    // metadata so trace taxonomy keeps top-N assisted fast paths explicit.
    const fn decorate_fast_path_optimization_for_route(
        optimization: ExecutionOptimization,
        route_plan: &ExecutionPlan,
    ) -> ExecutionOptimization {
        if route_plan.top_n_seek_spec().is_none() {
            return optimization;
        }

        match optimization {
            ExecutionOptimization::PrimaryKey => ExecutionOptimization::PrimaryKeyTopNSeek,
            ExecutionOptimization::SecondaryOrderPushdown => {
                ExecutionOptimization::SecondaryOrderTopNSeek
            }
            ExecutionOptimization::PrimaryKeyTopNSeek
            | ExecutionOptimization::SecondaryOrderTopNSeek
            | ExecutionOptimization::IndexRangeLimitPushdown => optimization,
        }
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
