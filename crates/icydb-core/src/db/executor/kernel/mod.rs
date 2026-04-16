//! Module: executor::kernel
//! Responsibility: unified read-execution kernel orchestration boundaries.
//! Does not own: logical planning or physical access path lowering policies.
//! Boundary: key-stream decoration, materialization, and residual retry behavior.

use crate::{
    db::{
        executor::{
            ExecutionPlan, ScalarContinuationContext,
            pipeline::contracts::{ExecutionInputs, MaterializedExecutionAttempt},
            route::{IndexRangeLimitSpec, widened_residual_predicate_pushdown_fetch},
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
        route_plan: &ExecutionPlan,
        continuation: &ScalarContinuationContext,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        let route_attempt_materializer =
            RouteAttemptMaterializer::new(inputs, continuation, predicate_compile_mode);
        let residual_retry = ResidualRetrySession::new(&route_attempt_materializer);

        residual_retry.materialize(route_plan)
    }

    // Merge retry attempts under canonical residual-retry accounting.
    fn merge_retry_attempts(
        mut accumulated_attempt: MaterializedExecutionAttempt,
        latest_attempt: MaterializedExecutionAttempt,
    ) -> MaterializedExecutionAttempt {
        accumulated_attempt.rows_scanned = accumulated_attempt
            .rows_scanned
            .saturating_add(latest_attempt.rows_scanned);
        accumulated_attempt.optimization = latest_attempt.optimization;
        accumulated_attempt.index_predicate_applied =
            accumulated_attempt.index_predicate_applied || latest_attempt.index_predicate_applied;
        accumulated_attempt.index_predicate_keys_rejected = accumulated_attempt
            .index_predicate_keys_rejected
            .saturating_add(latest_attempt.index_predicate_keys_rejected);
        accumulated_attempt.distinct_keys_deduped = accumulated_attempt
            .distinct_keys_deduped
            .saturating_add(latest_attempt.distinct_keys_deduped);
        accumulated_attempt.payload = latest_attempt.payload;
        accumulated_attempt.post_access_rows = latest_attempt.post_access_rows;

        accumulated_attempt
    }

    // Build the terminal residual-retry fallback plan by clearing the bounded
    // pushdown spec and coupled scan-budget hints before re-materialization.
    fn unbounded_residual_retry_route_plan(route_plan: &ExecutionPlan) -> ExecutionPlan {
        let mut fallback_route_plan = route_plan.clone();
        fallback_route_plan.index_range_limit_spec = None;
        fallback_route_plan.scan_hints.load_scan_budget_hint = None;
        fallback_route_plan.scan_hints.physical_fetch_hint = None;

        fallback_route_plan
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
    fn materialize(
        &self,
        route_plan: &ExecutionPlan,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        // Phase 1: materialize the planned route once and decide whether the
        // residual underfill is already satisfied.
        let probe_attempt = self
            .route_attempt_materializer
            .materialize_route_attempt(route_plan)?;
        let initial_retry_decision = self.retry_decision(route_plan, &probe_attempt);
        let mut accumulated_attempt = probe_attempt;
        let Some(mut retry_fetch) = initial_retry_decision.widened_fetch() else {
            return self.finish(route_plan, accumulated_attempt, initial_retry_decision);
        };

        // Phase 2: widen the bounded pushdown fetch while it remains under the
        // residual safety cap so selective predicates can satisfy the window
        // without immediately degrading into a full unbounded retry.
        let mut retry_route_plan = route_plan.clone();
        loop {
            Self::apply_retry_fetch(&mut retry_route_plan, retry_fetch);
            let retry_attempt = self
                .route_attempt_materializer
                .materialize_route_attempt(&retry_route_plan)?;
            let retry_decision = self.retry_decision(&retry_route_plan, &retry_attempt);
            accumulated_attempt =
                ExecutionKernel::merge_retry_attempts(accumulated_attempt, retry_attempt);

            if let Some(next_retry_fetch) = retry_decision.widened_fetch() {
                retry_fetch = next_retry_fetch;
                continue;
            }

            return self.finish(route_plan, accumulated_attempt, retry_decision);
        }
    }

    // Finish one retry session either by returning the accumulated bounded
    // attempt or by appending the terminal unbounded fallback attempt.
    fn finish(
        &self,
        route_plan: &ExecutionPlan,
        accumulated_attempt: MaterializedExecutionAttempt,
        retry_decision: ResidualRetryDecision,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        if retry_decision.requires_unbounded_fallback() {
            let fallback_attempt = self
                .route_attempt_materializer
                .materialize_unbounded_retry_fallback(route_plan)?;

            return Ok(ExecutionKernel::merge_retry_attempts(
                accumulated_attempt,
                fallback_attempt,
            ));
        }

        Ok(accumulated_attempt)
    }

    // Decide whether residual underfill should stop, widen the bounded fetch,
    // or fall back to an unbounded retry.
    fn retry_decision(
        &self,
        route_plan: &ExecutionPlan,
        attempt: &MaterializedExecutionAttempt,
    ) -> ResidualRetryDecision {
        let plan = self.route_attempt_materializer.inputs.plan();
        let logical = plan.scalar_plan();
        let Some(limit_spec) = route_plan.index_range_limit_spec else {
            return ResidualRetryDecision::None;
        };
        if logical.predicate.is_none() {
            return ResidualRetryDecision::None;
        }
        if limit_spec.fetch == 0 {
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
        if attempt.rows_scanned < limit_spec.fetch {
            return ResidualRetryDecision::None;
        }

        // A bounded retry cannot stop merely because it filled the visible
        // page window. Load pagination also needs one lookahead row to decide
        // whether a continuation cursor must be emitted.
        if attempt.post_access_rows > keep_count {
            return ResidualRetryDecision::None;
        }

        widened_residual_predicate_pushdown_fetch(
            limit_spec.fetch,
            keep_count,
            attempt.post_access_rows,
        )
        .map_or(ResidualRetryDecision::FallbackUnbounded, |fetch| {
            ResidualRetryDecision::WidenBoundedFetch { fetch }
        })
    }

    // Apply one widened residual-retry fetch to the bounded index-range route
    // contract and the coupled scan-budget hints that consume the same window.
    const fn apply_retry_fetch(route_plan: &mut ExecutionPlan, fetch: usize) {
        if route_plan.index_range_limit_spec.is_some() {
            route_plan.index_range_limit_spec = Some(IndexRangeLimitSpec { fetch });
        }
        if route_plan.scan_hints.load_scan_budget_hint.is_some() {
            route_plan.scan_hints.load_scan_budget_hint = Some(fetch);
        }
        if route_plan.scan_hints.physical_fetch_hint.is_some() {
            route_plan.scan_hints.physical_fetch_hint = Some(fetch);
        }
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
        route_plan: &ExecutionPlan,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        self.inputs.materialize_route_attempt(
            route_plan,
            self.continuation,
            self.predicate_compile_mode,
        )
    }

    // Materialize the terminal residual-retry fallback route by clearing the
    // bounded pushdown spec and coupled scan-budget hints first.
    fn materialize_unbounded_retry_fallback(
        &self,
        route_plan: &ExecutionPlan,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        self.materialize_route_attempt(&ExecutionKernel::unbounded_residual_retry_route_plan(
            route_plan,
        ))
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
