use crate::{
    db::{
        Context,
        executor::load::{
            CursorPage, ExecutionOptimization, ExecutionTrace, FastPathKeyResult, LoadExecutor,
        },
        executor::plan::set_rows_from_len,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, DistinctOrderedKeyStream,
            OrderedKeyStreamBox,
            route::{
                ExecutionMode, ExecutionRoutePlan, FastPathOrder, RoutedKeyStreamRequest,
                ensure_load_fast_path_spec_arity,
            },
        },
        index::predicate::{IndexPredicateExecution, IndexPredicateProgram},
        query::{
            contracts::cursor::{ContinuationSignature, CursorBoundary},
            plan::AccessPlannedQuery,
            predicate::PredicateFieldSlots,
        },
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

pub(super) struct ExecutionInputs<'a, E: EntityKind + EntityValue> {
    pub(super) ctx: &'a Context<'a, E>,
    pub(super) plan: &'a AccessPlannedQuery<E::Key>,
    pub(super) stream_bindings: AccessStreamBindings<'a>,
    pub(super) predicate_slots: Option<&'a PredicateFieldSlots>,
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
    pub(super) index_predicate_applied: bool,
    pub(super) index_predicate_keys_rejected: u64,
    pub(super) distinct_keys_deduped_counter: Option<Rc<Cell<u64>>>,
}

///
/// MaterializedExecutionAttempt
///
/// Canonical materialization attempt output for load execution.
/// Preserves one shared boundary for retry accounting and page output.
///

pub(super) struct MaterializedExecutionAttempt<E: EntityKind> {
    pub(super) page: CursorPage<E>,
    pub(super) rows_scanned: usize,
    pub(super) post_access_rows: usize,
    pub(super) optimization: Option<ExecutionOptimization>,
    pub(super) index_predicate_applied: bool,
    pub(super) index_predicate_keys_rejected: u64,
    pub(super) distinct_keys_deduped: u64,
}

///
/// IndexPredicateCompileMode
///
/// Predicate compile policy for index-only prefilter programs.
/// `ConservativeSubset` keeps load behavior by compiling safe AND-subsets.
/// `StrictAllOrNone` compiles only when every predicate node is supported.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum IndexPredicateCompileMode {
    ConservativeSubset,
    StrictAllOrNone,
}

// Wrap one ordered key stream with DISTINCT semantics through one shared entrypoint.
pub(in crate::db::executor::load) fn wrap_distinct_ordered_key_stream(
    ordered_key_stream: OrderedKeyStreamBox,
    distinct: bool,
    key_comparator: super::KeyOrderComparator,
    dedup_counter: Option<Rc<Cell<u64>>>,
) -> (OrderedKeyStreamBox, Option<Rc<Cell<u64>>>) {
    if !distinct {
        return (ordered_key_stream, None);
    }

    if let Some(counter) = dedup_counter {
        let wrapped = Box::new(DistinctOrderedKeyStream::new_with_dedup_counter(
            ordered_key_stream,
            key_comparator,
            counter.clone(),
        ));
        return (wrapped, Some(counter));
    }

    (
        Box::new(DistinctOrderedKeyStream::new(
            ordered_key_stream,
            key_comparator,
        )),
        None,
    )
}

// Canonical fast-path routing decision for one execution attempt.
enum FastPathDecision {
    Hit(FastPathKeyResult),
    None,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Run one canonical materialization attempt with optional residual retry.
    // This centralizes index-range limited fallback behavior for all load paths.
    pub(super) fn materialize_with_optional_residual_retry(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionRoutePlan,
        cursor_boundary: Option<&CursorBoundary>,
        continuation_signature: ContinuationSignature,
        predicate_compile_mode: IndexPredicateCompileMode,
    ) -> Result<MaterializedExecutionAttempt<E>, InternalError> {
        let mut resolved =
            Self::resolve_execution_key_stream(inputs, route_plan, predicate_compile_mode)?;
        let (mut page, keys_scanned, mut post_access_rows) =
            Self::materialize_key_stream_into_page(
                inputs.ctx,
                inputs.plan,
                inputs.predicate_slots,
                resolved.key_stream.as_mut(),
                route_plan.scan_hints.load_scan_budget_hint,
                route_plan.streaming_access_shape_safe(),
                cursor_boundary,
                route_plan.direction(),
                continuation_signature,
            )?;
        let mut rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        let mut optimization = resolved.optimization;
        let mut index_predicate_applied = resolved.index_predicate_applied;
        let mut index_predicate_keys_rejected = resolved.index_predicate_keys_rejected;
        let mut distinct_keys_deduped = resolved
            .distinct_keys_deduped_counter
            .as_ref()
            .map_or(0, |counter| counter.get());

        if Self::index_range_limited_residual_retry_required(
            inputs.plan,
            cursor_boundary,
            route_plan,
            rows_scanned,
            post_access_rows,
        ) {
            let mut fallback_route_plan = route_plan.clone();
            fallback_route_plan.index_range_limit_spec = None;
            let mut fallback_resolved = Self::resolve_execution_key_stream(
                inputs,
                &fallback_route_plan,
                predicate_compile_mode,
            )?;
            let (fallback_page, fallback_keys_scanned, fallback_post_access_rows) =
                Self::materialize_key_stream_into_page(
                    inputs.ctx,
                    inputs.plan,
                    inputs.predicate_slots,
                    fallback_resolved.key_stream.as_mut(),
                    fallback_route_plan.scan_hints.load_scan_budget_hint,
                    fallback_route_plan.streaming_access_shape_safe(),
                    cursor_boundary,
                    fallback_route_plan.direction(),
                    continuation_signature,
                )?;
            let fallback_rows_scanned = fallback_resolved
                .rows_scanned_override
                .unwrap_or(fallback_keys_scanned);
            let fallback_distinct_keys_deduped = fallback_resolved
                .distinct_keys_deduped_counter
                .as_ref()
                .map_or(0, |counter| counter.get());

            // Retry accounting keeps observability faithful to actual work.
            rows_scanned = rows_scanned.saturating_add(fallback_rows_scanned);
            optimization = fallback_resolved.optimization;
            index_predicate_applied =
                index_predicate_applied || fallback_resolved.index_predicate_applied;
            index_predicate_keys_rejected = index_predicate_keys_rejected
                .saturating_add(fallback_resolved.index_predicate_keys_rejected);
            distinct_keys_deduped =
                distinct_keys_deduped.saturating_add(fallback_distinct_keys_deduped);
            page = fallback_page;
            post_access_rows = fallback_post_access_rows;
        }

        Ok(MaterializedExecutionAttempt {
            page,
            rows_scanned,
            post_access_rows,
            optimization,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
        })
    }

    // Resolve one canonical execution key stream in fast-path precedence order.
    // This is the single shared load key-stream resolver boundary.
    pub(super) fn resolve_execution_key_stream(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionRoutePlan,
        predicate_compile_mode: IndexPredicateCompileMode,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        let index_predicate_program = Self::compile_index_predicate_program(
            inputs.plan,
            inputs.predicate_slots,
            predicate_compile_mode,
        );
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
                let stream_request = AccessPlanStreamRequest {
                    access: &inputs.plan.access,
                    bindings: inputs.stream_bindings,
                    key_comparator: super::key_stream_comparator_from_plan(
                        inputs.plan,
                        inputs.stream_bindings.direction,
                    ),
                    physical_fetch_hint: fallback_fetch_hint,
                    index_predicate_execution,
                };
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

        // Phase 3: apply DISTINCT at one shared boundary.
        let key_comparator =
            super::key_stream_comparator_from_plan(inputs.plan, inputs.stream_bindings.direction);

        Ok(Self::apply_distinct_if_requested(
            resolved,
            inputs.plan,
            key_comparator,
        ))
    }

    // Evaluate fast-path routes in canonical precedence and return one decision.
    fn evaluate_fast_path(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionRoutePlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<FastPathDecision, InternalError> {
        // Guard fast-path spec arity up front so planner/executor traversal
        // drift cannot silently consume the wrong spec in release builds.
        ensure_load_fast_path_spec_arity(
            route_plan.secondary_fast_path_eligible(),
            inputs.stream_bindings.index_prefix_specs.len(),
            route_plan.index_range_limit_fast_path_enabled(),
            inputs.stream_bindings.index_range_specs.len(),
        )?;

        for route in route_plan.fast_path_order().iter().copied() {
            match route {
                FastPathOrder::PrimaryKey => {
                    if route_plan.pk_order_fast_path_eligible()
                        && let Some(fast) = Self::try_execute_pk_order_stream(
                            inputs.ctx,
                            inputs.plan,
                            route_plan.scan_hints.physical_fetch_hint,
                        )?
                    {
                        return Ok(FastPathDecision::Hit(fast));
                    }
                }
                FastPathOrder::SecondaryPrefix => {
                    if route_plan.secondary_fast_path_eligible()
                        && let Some(fast) = Self::try_execute_secondary_index_order_stream(
                            inputs.ctx,
                            inputs.plan,
                            inputs.stream_bindings.index_prefix_specs.first(),
                            route_plan.scan_hints.physical_fetch_hint,
                            index_predicate_execution,
                        )?
                    {
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
                            index_predicate_execution,
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

    // Compile one optional index-only predicate program for load execution when
    // the active access path is index-backed and at least one safe predicate
    // subset can run on index components alone.
    pub(in crate::db::executor) fn compile_index_predicate_program(
        plan: &AccessPlannedQuery<E::Key>,
        predicate_slots: Option<&PredicateFieldSlots>,
        mode: IndexPredicateCompileMode,
    ) -> Option<IndexPredicateProgram> {
        let predicate_slots = predicate_slots?;
        let index_slots = Self::resolved_index_slots_for_access_path(&plan.access)?;

        Self::compile_index_predicate_program_from_slots(
            predicate_slots,
            index_slots.as_slice(),
            mode,
        )
    }

    // Compile one optional index-only predicate program from pre-resolved slots.
    // This is the single compile-mode switch boundary for subset vs strict policy.
    pub(in crate::db::executor) fn compile_index_predicate_program_from_slots(
        predicate_slots: &PredicateFieldSlots,
        index_slots: &[usize],
        mode: IndexPredicateCompileMode,
    ) -> Option<IndexPredicateProgram> {
        match mode {
            IndexPredicateCompileMode::ConservativeSubset => {
                predicate_slots.compile_index_program(index_slots)
            }
            IndexPredicateCompileMode::StrictAllOrNone => {
                predicate_slots.compile_index_program_strict(index_slots)
            }
        }
    }
    // Apply DISTINCT before post-access phases so pagination sees unique keys.
    fn apply_distinct_if_requested(
        mut resolved: ResolvedExecutionKeyStream,
        plan: &AccessPlannedQuery<E::Key>,
        key_comparator: super::KeyOrderComparator,
    ) -> ResolvedExecutionKeyStream {
        let dedup_counter = plan.distinct.then(|| Rc::new(Cell::new(0u64)));
        let (key_stream, dedup_counter) = wrap_distinct_ordered_key_stream(
            resolved.key_stream,
            plan.distinct,
            key_comparator,
            dedup_counter,
        );
        resolved.key_stream = key_stream;
        resolved.distinct_keys_deduped_counter = dedup_counter;

        resolved
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
