//! Module: executor::kernel
//! Responsibility: unified read-execution kernel orchestration boundaries.
//! Does not own: logical planning or physical access path lowering policies.
//! Boundary: key-stream decoration, materialization, and residual retry behavior.

#[cfg(feature = "sql")]
use crate::db::executor::{pipeline::contracts::KernelRowsExecutionAttempt, terminal::KernelRow};
use crate::{
    db::{
        executor::{
            ExecutionRoutePlan, ScalarContinuationContext,
            pipeline::{
                contracts::{
                    ExecutionInputs, ExecutionOutcomeMetrics, MaterializedExecutionAttempt,
                    StructuralCursorPage,
                },
                runtime::ExecutionAttemptKernel,
            },
            route::{IndexRangeLimitSpec, widened_residual_filter_predicate_pushdown_fetch},
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
};

///
/// ExecutionKernel
///
/// Canonical kernel boundary for read execution unification.
///

pub(in crate::db::executor) struct ExecutionKernel;

impl ExecutionKernel {
    /// Materialize one load execution attempt with optional residual retry.
    pub(in crate::db::executor) fn materialize_with_optional_residual_retry(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionRoutePlan,
        continuation: &ScalarContinuationContext,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        let route_attempt_materializer =
            RouteAttemptMaterializer::new(inputs, continuation, predicate_compile_mode);
        let residual_retry = ResidualRetrySession::new(&route_attempt_materializer);

        residual_retry.materialize(route_plan, |retry_route_plan| {
            route_attempt_materializer.materialize_route_attempt(retry_route_plan)
        })
    }

    /// Materialize one load execution attempt into post-access kernel rows.
    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn materialize_kernel_rows_with_optional_residual_retry(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionRoutePlan,
        continuation: &ScalarContinuationContext,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<KernelRowsExecutionAttempt, InternalError> {
        let route_attempt_materializer =
            RouteAttemptMaterializer::new(inputs, continuation, predicate_compile_mode);
        let residual_retry = ResidualRetrySession::new(&route_attempt_materializer);

        residual_retry.materialize(route_plan, |retry_route_plan| {
            route_attempt_materializer.materialize_route_attempt_kernel_rows(retry_route_plan)
        })
    }
}

///
/// ResidualRetrySession
///
/// ResidualRetrySession owns the bounded residual-retry session for
/// one kernel materialization attempt.
/// It decides when the probe attempt is complete, when bounded fetch should
/// widen, and when the route must fall back to one final unbounded attempt.
///

struct ResidualRetrySession<'a, 'b, 'c> {
    route_attempt_materializer: &'c RouteAttemptMaterializer<'a, 'b>,
}

impl<'a, 'b, 'c> ResidualRetrySession<'a, 'b, 'c> {
    // Build one retry-session controller around the shared route-attempt
    // materializer for a single kernel attempt.
    const fn new(route_attempt_materializer: &'c RouteAttemptMaterializer<'a, 'b>) -> Self {
        Self {
            route_attempt_materializer,
        }
    }

    // Materialize one kernel attempt, widening bounded residual fetch only
    // while the retry policy still prefers the limited route.
    fn materialize<A, MaterializeAttempt>(
        &self,
        route_plan: &ExecutionRoutePlan,
        materialize_attempt: MaterializeAttempt,
    ) -> Result<A, InternalError>
    where
        A: ResidualRetryAttempt,
        MaterializeAttempt: Fn(&ExecutionRoutePlan) -> Result<A, InternalError>,
    {
        if self
            .route_attempt_materializer
            .residual_retry_impossible(route_plan)
        {
            return materialize_attempt(route_plan);
        }

        // Phase 1: materialize the planned route once and decide whether the
        // residual underfill is already satisfied.
        let probe_attempt = materialize_attempt(route_plan)?;
        let initial_retry_decision = self.retry_decision(route_plan, &probe_attempt);
        let mut accumulated_attempt = probe_attempt;
        let Some(mut retry_fetch) = initial_retry_decision.widened_fetch() else {
            return Self::finish(
                route_plan,
                accumulated_attempt,
                initial_retry_decision,
                &materialize_attempt,
            );
        };

        // Phase 2: widen the bounded pushdown fetch while it remains under the
        // residual safety cap so selective predicates can satisfy the window
        // without immediately degrading into a full unbounded retry.
        let mut retry_route_plan = route_plan.clone();
        loop {
            Self::apply_retry_fetch(&mut retry_route_plan, retry_fetch);
            let retry_attempt = materialize_attempt(&retry_route_plan)?;
            let retry_decision = self.retry_decision(&retry_route_plan, &retry_attempt);
            accumulated_attempt = accumulated_attempt.merge_latest(retry_attempt);

            if let Some(next_retry_fetch) = retry_decision.widened_fetch() {
                retry_fetch = next_retry_fetch;
                continue;
            }

            return Self::finish(
                route_plan,
                accumulated_attempt,
                retry_decision,
                &materialize_attempt,
            );
        }
    }

    // Finish one retry session either by returning the accumulated bounded
    // attempt or by appending the terminal unbounded fallback attempt.
    fn finish<A, MaterializeAttempt>(
        route_plan: &ExecutionRoutePlan,
        accumulated_attempt: A,
        retry_decision: ResidualRetryDecision,
        materialize_attempt: &MaterializeAttempt,
    ) -> Result<A, InternalError>
    where
        A: ResidualRetryAttempt,
        MaterializeAttempt: Fn(&ExecutionRoutePlan) -> Result<A, InternalError>,
    {
        if retry_decision.requires_unbounded_fallback() {
            let fallback_route_plan = Self::unbounded_retry_route_plan(route_plan);
            let fallback_attempt = materialize_attempt(&fallback_route_plan)?;

            return Ok(accumulated_attempt.merge_latest(fallback_attempt));
        }

        Ok(accumulated_attempt)
    }

    // Decide whether residual underfill should stop, widen the bounded fetch,
    // or fall back to an unbounded retry.
    fn retry_decision<A: ResidualRetryAttempt>(
        &self,
        route_plan: &ExecutionRoutePlan,
        attempt: &A,
    ) -> ResidualRetryDecision {
        let plan = self.route_attempt_materializer.inputs.plan();
        let logical = plan.scalar_plan();
        let Some(fetch) = Self::bounded_retry_fetch(route_plan) else {
            return ResidualRetryDecision::None;
        };
        if self
            .route_attempt_materializer
            .inputs
            .residual_filter_program()
            .is_none()
        {
            return ResidualRetryDecision::None;
        }
        if fetch == 0 {
            return ResidualRetryDecision::None;
        }
        let Some(limit) = logical.page.as_ref().and_then(|page| page.limit) else {
            return ResidualRetryDecision::None;
        };
        let keep_count = self
            .route_attempt_materializer
            .continuation
            .keep_count_for_limit_window(plan, limit);
        if keep_count == 0 {
            return ResidualRetryDecision::None;
        }
        let (rows_scanned, post_access_rows) = attempt.retry_counts();
        if rows_scanned < fetch {
            return ResidualRetryDecision::None;
        }

        // A bounded retry cannot stop merely because it filled the visible
        // page window. Load pagination also needs one lookahead row to decide
        // whether a continuation cursor must be emitted.
        if post_access_rows > keep_count {
            return ResidualRetryDecision::None;
        }

        widened_residual_filter_predicate_pushdown_fetch(fetch, keep_count, post_access_rows)
            .map_or(ResidualRetryDecision::FallbackUnbounded, |fetch| {
                ResidualRetryDecision::WidenBoundedFetch { fetch }
            })
    }

    // Resolve the current bounded retry fetch across both index-range limit
    // pushdown and ordered top-N routes so residual underfill can widen either
    // bounded access family without inventing a second retry contract.
    const fn bounded_retry_fetch(route_plan: &ExecutionRoutePlan) -> Option<usize> {
        if let Some(limit_spec) = route_plan.index_range_limit_spec {
            return Some(limit_spec.fetch);
        }

        if route_plan.top_n_seek_spec.is_some() {
            if let Some(fetch) = route_plan.scan_hints.physical_fetch_hint {
                return Some(fetch);
            }

            if let Some(top_n_seek_spec) = route_plan.top_n_seek_spec {
                return Some(top_n_seek_spec.fetch());
            }
        }

        None
    }

    // Apply one widened residual-retry fetch to the bounded index-range route
    // contract and the coupled scan-budget hints that consume the same window.
    const fn apply_retry_fetch(route_plan: &mut ExecutionRoutePlan, fetch: usize) {
        if route_plan.index_range_limit_spec.is_some() {
            route_plan.index_range_limit_spec = Some(IndexRangeLimitSpec { fetch });
        }
        if route_plan.top_n_seek_spec.is_some() {
            route_plan.top_n_seek_spec = None;
        }
        if route_plan.scan_hints.load_scan_budget_hint.is_some() {
            route_plan.scan_hints.load_scan_budget_hint = Some(fetch);
        }
        if route_plan.scan_hints.physical_fetch_hint.is_some() {
            route_plan.scan_hints.physical_fetch_hint = Some(fetch);
        }
    }

    // Build the terminal residual-retry fallback plan by clearing the bounded
    // pushdown spec and coupled scan-budget hints before re-materialization.
    fn unbounded_retry_route_plan(route_plan: &ExecutionRoutePlan) -> ExecutionRoutePlan {
        let mut fallback_route_plan = route_plan.clone();
        fallback_route_plan.index_range_limit_spec = None;
        fallback_route_plan.top_n_seek_spec = None;
        fallback_route_plan.scan_hints.load_scan_budget_hint = None;
        fallback_route_plan.scan_hints.physical_fetch_hint = None;

        fallback_route_plan
    }
}

/// Output-specific mechanics consumed by the single residual-retry loop.
///
/// Implementations split the attempt into its payload and shared metrics so
/// retry orchestration can discard earlier outputs without knowing their row
/// representation.
trait ResidualRetryAttempt: Sized {
    type Payload;

    fn metrics(&self) -> &ExecutionOutcomeMetrics;

    fn into_retry_parts(self) -> (Self::Payload, ExecutionOutcomeMetrics);

    fn from_retry_parts(payload: Self::Payload, metrics: ExecutionOutcomeMetrics) -> Self;

    fn retry_counts(&self) -> (usize, usize) {
        (self.metrics().rows_scanned, self.metrics().post_access_rows)
    }

    // Preserve only the latest semantic output while accumulating work and
    // rejection metrics from every attempt in the session.
    fn merge_latest(self, latest: Self) -> Self {
        let (_, accumulated_metrics) = self.into_retry_parts();
        let (latest_payload, latest_metrics) = latest.into_retry_parts();

        Self::from_retry_parts(
            latest_payload,
            accumulated_metrics.merge_residual_retry_attempt(latest_metrics),
        )
    }
}

#[cfg(feature = "sql")]
impl ResidualRetryAttempt for KernelRowsExecutionAttempt {
    type Payload = Vec<KernelRow>;

    fn metrics(&self) -> &ExecutionOutcomeMetrics {
        &self.metrics
    }

    fn into_retry_parts(self) -> (Self::Payload, ExecutionOutcomeMetrics) {
        (self.rows, self.metrics)
    }

    fn from_retry_parts(rows: Self::Payload, metrics: ExecutionOutcomeMetrics) -> Self {
        Self { rows, metrics }
    }
}

impl ResidualRetryAttempt for MaterializedExecutionAttempt {
    type Payload = StructuralCursorPage;

    fn metrics(&self) -> &ExecutionOutcomeMetrics {
        &self.metrics
    }

    fn into_retry_parts(self) -> (Self::Payload, ExecutionOutcomeMetrics) {
        (self.payload, self.metrics)
    }

    fn from_retry_parts(payload: Self::Payload, metrics: ExecutionOutcomeMetrics) -> Self {
        Self { payload, metrics }
    }
}

///
/// RouteAttemptMaterializer
///
/// RouteAttemptMaterializer freezes the shared route-attempt
/// materialization contract for one kernel execution attempt.
/// It keeps `inputs`, `continuation`, and predicate compile mode together so
/// the retry loop can materialize probe, widened, and fallback attempts
/// without re-threading the same boundary data through every call.
///

struct RouteAttemptMaterializer<'a, 'b> {
    inputs: &'a ExecutionInputs<'a>,
    continuation: &'b ScalarContinuationContext,
    predicate_compile_mode: IndexCompilePolicy,
}

impl<'a, 'b> RouteAttemptMaterializer<'a, 'b> {
    // Build one owner-local route-attempt materializer for a single kernel
    // execution attempt.
    const fn new(
        inputs: &'a ExecutionInputs<'a>,
        continuation: &'b ScalarContinuationContext,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Self {
        Self {
            inputs,
            continuation,
            predicate_compile_mode,
        }
    }

    // Materialize one structural attempt for a specific route-plan candidate.
    fn materialize_route_attempt(
        &self,
        route_plan: &ExecutionRoutePlan,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        ExecutionAttemptKernel::new(self.inputs).materialize_route_attempt(
            route_plan,
            self.continuation,
            self.predicate_compile_mode,
        )
    }

    // Materialize one kernel-row attempt for a specific route-plan candidate.
    #[cfg(feature = "sql")]
    fn materialize_route_attempt_kernel_rows(
        &self,
        route_plan: &ExecutionRoutePlan,
    ) -> Result<KernelRowsExecutionAttempt, InternalError> {
        ExecutionAttemptKernel::new(self.inputs).materialize_route_attempt_kernel_rows(
            route_plan,
            self.continuation,
            self.predicate_compile_mode,
        )
    }

    // Decide whether residual retry is impossible before the probe attempt.
    // Post-attempt underfill checks intentionally remain in the retry session,
    // so uncertain cases keep the full retry/fallback controller.
    fn residual_retry_impossible(&self, route_plan: &ExecutionRoutePlan) -> bool {
        let Some(fetch) = ResidualRetrySession::bounded_retry_fetch(route_plan) else {
            return true;
        };
        if fetch == 0 {
            return true;
        }
        if self.inputs.residual_filter_program().is_none() {
            return true;
        }
        let plan = self.inputs.plan();
        let logical = plan.scalar_plan();
        let Some(limit) = logical.page.as_ref().and_then(|page| page.limit) else {
            return true;
        };

        self.continuation.keep_count_for_limit_window(plan, limit) == 0
    }
}

///
/// ResidualRetryDecision
///
/// ResidualRetryDecision captures the next bounded-residual step after one
/// materialized attempt completes.
/// The retry session uses it to stop, widen the bounded fetch, or append one
/// final unbounded fallback attempt.
///

#[derive(Clone, Copy)]
enum ResidualRetryDecision {
    None,
    WidenBoundedFetch { fetch: usize },
    FallbackUnbounded,
}

impl ResidualRetryDecision {
    const fn widened_fetch(self) -> Option<usize> {
        match self {
            Self::WidenBoundedFetch { fetch } => Some(fetch),
            Self::None | Self::FallbackUnbounded => None,
        }
    }

    const fn requires_unbounded_fallback(self) -> bool {
        matches!(self, Self::FallbackUnbounded)
    }
}
