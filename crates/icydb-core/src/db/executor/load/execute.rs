use crate::{
    db::{
        Context,
        executor::load::{
            CursorPage, ExecutionOptimization, ExecutionTrace, FastPathKeyResult, LoadExecutor,
            aggregate_guard::{
                ensure_prefix_spec_at_most_one_if_enabled, ensure_range_spec_at_most_one_if_enabled,
            },
            route::{ExecutionRoutePlan, FastPathOrder, LOAD_FAST_PATH_ORDER},
        },
        executor::plan::set_rows_from_len,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, DistinctOrderedKeyStream,
            OrderedKeyStreamBox,
        },
        query::plan::{Direction, LogicalPlan},
    },
    error::InternalError,
    obs::sink::Span,
    traits::{EntityKind, EntityValue},
};

///
/// ExecutionInputs
///
/// Shared immutable execution inputs for one load execution attempt.
/// Keeps fast-path dispatch signatures compact without changing behavior.
///

pub(super) struct ExecutionInputs<'a, E: EntityKind + EntityValue> {
    pub(super) ctx: &'a Context<'a, E>,
    pub(super) plan: &'a LogicalPlan<E::Key>,
    pub(super) stream_bindings: AccessStreamBindings<'a>,
}

///
/// ResolvedExecutionKeyStream
///
/// Canonical key-stream resolution output for one load execution attempt.
/// Keeps fast-path metadata and fallback stream output on one shared boundary.
///

pub(super) struct ResolvedExecutionKeyStream {
    pub(super) key_stream: OrderedKeyStreamBox,
    pub(super) optimization: Option<ExecutionOptimization>,
    pub(super) rows_scanned_override: Option<usize>,
}

// Canonical fast-path routing decision for one execution attempt.
enum FastPathDecision {
    Hit(FastPathKeyResult),
    None,
}

const SECONDARY_FAST_PATH_PREFIX_ARITY_MESSAGE: &str =
    "secondary fast-path resolution expects at most one index-prefix spec";
const INDEX_RANGE_FAST_PATH_RANGE_ARITY_MESSAGE: &str =
    "index-range fast-path resolution expects at most one index-range spec";

// Enforce fast-path arity assumptions at runtime so `.first()`-based
// resolution remains safe under future planner/eligibility changes.
fn ensure_fast_path_spec_arity(
    secondary_pushdown_eligible: bool,
    index_prefix_spec_count: usize,
    index_range_limit_pushdown_enabled: bool,
    index_range_spec_count: usize,
) -> Result<(), InternalError> {
    ensure_prefix_spec_at_most_one_if_enabled(
        secondary_pushdown_eligible,
        index_prefix_spec_count,
        SECONDARY_FAST_PATH_PREFIX_ARITY_MESSAGE,
    )?;
    ensure_range_spec_at_most_one_if_enabled(
        index_range_limit_pushdown_enabled,
        index_range_spec_count,
        INDEX_RANGE_FAST_PATH_RANGE_ARITY_MESSAGE,
    )?;

    Ok(())
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Resolve one canonical execution key stream in fast-path precedence order.
    // This is the single shared load key-stream resolver boundary.
    pub(super) fn resolve_execution_key_stream(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionRoutePlan,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        // Phase 1: resolve fast-path stream if any.
        let resolved = match Self::evaluate_fast_path(inputs, route_plan)? {
            FastPathDecision::Hit(fast) => ResolvedExecutionKeyStream {
                key_stream: fast.ordered_key_stream,
                optimization: Some(fast.optimization),
                rows_scanned_override: Some(fast.rows_scanned),
            },
            FastPathDecision::None => {
                // Phase 2: resolve canonical fallback access stream.
                let fallback_fetch_hint = if route_plan.desc_physical_reverse_supported
                    || !matches!(inputs.stream_bindings.direction, Direction::Desc)
                {
                    route_plan.scan_hints.physical_fetch_hint
                } else {
                    None
                };
                let stream_request = AccessPlanStreamRequest {
                    access: &inputs.plan.access,
                    bindings: inputs.stream_bindings,
                    key_comparator: super::key_stream_comparator_from_plan(
                        inputs.plan,
                        inputs.stream_bindings.direction,
                    ),
                    physical_fetch_hint: fallback_fetch_hint,
                };
                let key_stream = inputs
                    .ctx
                    .ordered_key_stream_from_access_plan_with_index_range_anchor(stream_request)?;

                ResolvedExecutionKeyStream {
                    key_stream,
                    optimization: None,
                    rows_scanned_override: None,
                }
            }
        };

        // Phase 3: apply DISTINCT at one shared boundary.
        Ok(Self::apply_distinct_if_requested(resolved, inputs.plan))
    }

    // Evaluate fast-path routes in canonical precedence and return one decision.
    fn evaluate_fast_path(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionRoutePlan,
    ) -> Result<FastPathDecision, InternalError> {
        // Guard fast-path spec arity up front so planner/executor traversal
        // drift cannot silently consume the wrong spec in release builds.
        ensure_fast_path_spec_arity(
            route_plan.secondary_pushdown_applicability.is_eligible(),
            inputs.stream_bindings.index_prefix_specs.len(),
            route_plan.index_range_limit_spec.is_some(),
            inputs.stream_bindings.index_range_specs.len(),
        )?;

        for route in LOAD_FAST_PATH_ORDER {
            match route {
                FastPathOrder::PrimaryKey => {
                    if let Some(fast) = Self::try_execute_pk_order_stream(
                        inputs.ctx,
                        inputs.plan,
                        route_plan.scan_hints.physical_fetch_hint,
                    )? {
                        return Ok(FastPathDecision::Hit(fast));
                    }
                }
                FastPathOrder::SecondaryPrefix => {
                    if let Some(fast) = Self::try_execute_secondary_index_order_stream(
                        inputs.ctx,
                        inputs.plan,
                        inputs.stream_bindings.index_prefix_specs.first(),
                        &route_plan.secondary_pushdown_applicability,
                        route_plan.scan_hints.physical_fetch_hint,
                    )? {
                        return Ok(FastPathDecision::Hit(fast));
                    }
                }
                FastPathOrder::IndexRange => {
                    if let Some(spec) = route_plan.index_range_limit_spec.as_ref()
                        && let Some(fast) = Self::try_execute_index_range_limit_pushdown_stream(
                            inputs.ctx,
                            inputs.plan,
                            inputs.stream_bindings.index_range_specs.first(),
                            inputs.stream_bindings.index_range_anchor,
                            inputs.stream_bindings.direction,
                            spec.fetch,
                        )?
                    {
                        return Ok(FastPathDecision::Hit(fast));
                    }
                }
                FastPathOrder::PrimaryScan | FastPathOrder::Composite => {}
            }
        }

        Ok(FastPathDecision::None)
    }
    // Apply DISTINCT before post-access phases so pagination sees unique keys.
    fn apply_distinct_if_requested(
        mut resolved: ResolvedExecutionKeyStream,
        plan: &LogicalPlan<E::Key>,
    ) -> ResolvedExecutionKeyStream {
        if plan.distinct {
            resolved.key_stream = Box::new(DistinctOrderedKeyStream::new(resolved.key_stream));
        }

        resolved
    }

    // Apply shared path finalization hooks after page materialization.
    pub(super) fn finalize_execution(
        page: CursorPage<E>,
        optimization: Option<ExecutionOptimization>,
        rows_scanned: usize,
        post_access_rows: usize,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
    ) -> CursorPage<E> {
        Self::finalize_path_outcome(
            execution_trace,
            optimization,
            rows_scanned,
            post_access_rows,
        );
        set_rows_from_len(span, page.items.0.len());

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
        let result = super::ensure_fast_path_spec_arity(true, 1, true, 1);

        assert!(result.is_ok(), "single fast-path specs should be accepted");
    }

    #[test]
    fn fast_path_spec_arity_rejects_multiple_prefix_specs_for_secondary() {
        let err = super::ensure_fast_path_spec_arity(true, 2, false, 0)
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
        let err = super::ensure_fast_path_spec_arity(false, 0, true, 2)
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
