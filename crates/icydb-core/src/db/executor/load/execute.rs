use crate::{
    db::{
        Context,
        executor::load::{
            CursorPage, ExecutionOptimization, ExecutionTrace, FastPathKeyResult, LoadExecutor,
        },
        executor::plan_metrics::set_rows_from_len,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, ExecutionPlan, ExecutionPreparation,
            IndexPredicateCompileMode, OrderedKeyStreamBox,
            compile_index_predicate_program_from_slots, range_token_from_lowered_anchor,
            route::{
                ExecutionMode, FastPathOrder, RoutedKeyStreamRequest,
                ensure_load_fast_path_spec_arity, try_first_verified_fast_path_hit,
            },
        },
        index::predicate::IndexPredicateExecution,
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
    pub(in crate::db::executor) ctx: &'a Context<'a, E>,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery<E::Key>,
    pub(in crate::db::executor) stream_bindings: AccessStreamBindings<'a>,
    pub(in crate::db::executor) execution_preparation: &'a ExecutionPreparation,
}

///
/// ResolvedExecutionKeyStream
///
/// Canonical key-stream resolution output for one load execution attempt.
/// Keeps fast-path metadata and fallback stream output on one shared boundary.
///

pub(in crate::db::executor) struct ResolvedExecutionKeyStream {
    pub(in crate::db::executor) key_stream: OrderedKeyStreamBox,
    pub(in crate::db::executor) optimization: Option<ExecutionOptimization>,
    pub(in crate::db::executor) rows_scanned_override: Option<usize>,
    pub(in crate::db::executor) index_predicate_applied: bool,
    pub(in crate::db::executor) index_predicate_keys_rejected: u64,
    pub(in crate::db::executor) distinct_keys_deduped_counter: Option<Rc<Cell<u64>>>,
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

// Canonical fast-path routing decision for one execution attempt.
enum FastPathDecision {
    Hit(FastPathKeyResult),
    None,
}

///
/// VerifiedLoadFastPathRoute
///
/// Capability marker returned only by load fast-path eligibility verification.
/// Branch execution requires this marker so route eligibility checks and
/// branch dispatch stay coupled under one shared gate.
///
struct VerifiedLoadFastPathRoute {
    route: FastPathOrder,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Resolve one canonical execution key stream in fast-path precedence order.
    // This is the single shared load key-stream resolver boundary.
    pub(in crate::db::executor) fn resolve_execution_key_stream_without_distinct(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionPlan,
        predicate_compile_mode: IndexPredicateCompileMode,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        let index_predicate_program =
            inputs
                .execution_preparation
                .compiled_predicate()
                .and_then(|compiled_predicate| {
                    let slot_map = inputs.execution_preparation.slot_map()?;

                    compile_index_predicate_program_from_slots(
                        compiled_predicate,
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

        // Phase 1: evaluate fast paths only when routing selected streaming mode.
        let fast_path_decision = match route_plan.execution_mode {
            ExecutionMode::Streaming => {
                Self::evaluate_fast_path(inputs, route_plan, index_predicate_execution)?
            }
            ExecutionMode::Materialized => FastPathDecision::None,
        };
        let resolved = match fast_path_decision {
            FastPathDecision::Hit(fast) => ResolvedExecutionKeyStream {
                key_stream: fast.ordered_key_stream,
                optimization: Some(fast.optimization),
                rows_scanned_override: Some(fast.rows_scanned),
                index_predicate_applied,
                index_predicate_keys_rejected: index_predicate_rejected_counter.get(),
                distinct_keys_deduped_counter: None,
            },
            FastPathDecision::None => {
                // Phase 2: resolve canonical fallback access stream.
                let fallback_fetch_hint =
                    route_plan.fallback_physical_fetch_hint(inputs.stream_bindings.direction);
                let stream_request = AccessPlanStreamRequest::from_bindings(
                    &inputs.plan.access,
                    inputs.stream_bindings,
                    fallback_fetch_hint,
                    index_predicate_execution,
                );
                let key_stream = Self::resolve_routed_key_stream(
                    inputs.ctx,
                    RoutedKeyStreamRequest::AccessPlan(stream_request),
                )?;

                ResolvedExecutionKeyStream {
                    key_stream,
                    optimization: None,
                    rows_scanned_override: None,
                    index_predicate_applied,
                    index_predicate_keys_rejected: index_predicate_rejected_counter.get(),
                    distinct_keys_deduped_counter: None,
                }
            }
        };

        Ok(resolved)
    }

    // Evaluate fast-path routes in canonical precedence and return one decision.
    fn evaluate_fast_path(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<FastPathDecision, InternalError> {
        // Guard fast-path spec arity up front so plan/runtime traversal drift
        // cannot silently consume the wrong spec in release builds.
        ensure_load_fast_path_spec_arity(
            route_plan.secondary_fast_path_eligible(),
            inputs.stream_bindings.index_prefix_specs.len(),
            route_plan.index_range_limit_fast_path_enabled(),
            inputs.stream_bindings.index_range_specs.len(),
        )?;

        let fast = try_first_verified_fast_path_hit(
            route_plan.fast_path_order(),
            |route| Ok(Self::verify_load_fast_path_eligibility(route_plan, route)),
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

    fn verify_load_fast_path_eligibility(
        route_plan: &ExecutionPlan,
        route: FastPathOrder,
    ) -> Option<VerifiedLoadFastPathRoute> {
        let verified = match route {
            FastPathOrder::PrimaryKey if route_plan.pk_order_fast_path_eligible() => Some(route),
            FastPathOrder::SecondaryPrefix if route_plan.secondary_fast_path_eligible() => {
                Some(route)
            }
            FastPathOrder::IndexRange if route_plan.index_range_limit_fast_path_enabled() => {
                Some(route)
            }
            FastPathOrder::PrimaryScan
            | FastPathOrder::Composite
            | FastPathOrder::PrimaryKey
            | FastPathOrder::SecondaryPrefix
            | FastPathOrder::IndexRange => None,
        };

        verified.map(|route| VerifiedLoadFastPathRoute { route })
    }

    fn try_execute_verified_load_fast_path(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionPlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        verified_route: VerifiedLoadFastPathRoute,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        match verified_route.route {
            FastPathOrder::PrimaryKey => Self::try_execute_pk_order_stream(
                inputs.ctx,
                inputs.plan,
                route_plan.scan_hints.physical_fetch_hint,
            ),
            FastPathOrder::SecondaryPrefix => Self::try_execute_secondary_index_order_stream(
                inputs.ctx,
                inputs.plan,
                inputs.stream_bindings.index_prefix_specs.first(),
                route_plan.scan_hints.physical_fetch_hint,
                index_predicate_execution,
            ),
            FastPathOrder::IndexRange => {
                let index_range_token = inputs
                    .stream_bindings
                    .index_range_anchor
                    .map(range_token_from_lowered_anchor);
                let Some(spec) = route_plan.index_range_limit_spec.as_ref() else {
                    return Ok(None);
                };

                Self::try_execute_index_range_limit_pushdown_stream(
                    inputs.ctx,
                    inputs.plan,
                    inputs.stream_bindings.index_range_specs.first(),
                    index_range_token.as_ref(),
                    inputs.stream_bindings.direction,
                    spec.fetch,
                    index_predicate_execution,
                )
            }
            FastPathOrder::PrimaryScan | FastPathOrder::Composite => Ok(None),
        }
    }

    // Apply shared path finalization hooks after page materialization.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn finalize_execution(
        page: CursorPage<E>,
        optimization: Option<ExecutionOptimization>,
        rows_scanned: usize,
        post_access_rows: usize,
        index_predicate_applied: bool,
        index_predicate_keys_rejected: u64,
        distinct_keys_deduped: u64,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
    ) -> CursorPage<E> {
        Self::finalize_path_outcome(
            execution_trace,
            optimization,
            rows_scanned,
            post_access_rows,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
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
