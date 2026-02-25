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
                ExecutionRoutePlan, FastPathOrder, RoutedKeyStreamRequest,
                ensure_load_fast_path_spec_arity,
            },
        },
        index::predicate::{IndexPredicateExecution, IndexPredicateProgram},
        query::{plan::AccessPlannedQuery, predicate::PredicateFieldSlots},
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
/// IndexPredicateCompileMode
///
/// Predicate compile policy for index-only prefilter programs.
/// `ConservativeSubset` keeps load behavior by compiling safe AND-subsets.
/// `StrictAllOrNone` compiles only when every predicate node is supported.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor::load) enum IndexPredicateCompileMode {
    ConservativeSubset,
    StrictAllOrNone,
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

        // Phase 1: resolve fast-path stream if any.
        let resolved =
            match Self::evaluate_fast_path(inputs, route_plan, index_predicate_execution)? {
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
    pub(in crate::db::executor::load) fn compile_index_predicate_program(
        plan: &AccessPlannedQuery<E::Key>,
        predicate_slots: Option<&PredicateFieldSlots>,
        mode: IndexPredicateCompileMode,
    ) -> Option<IndexPredicateProgram> {
        let predicate_slots = predicate_slots?;
        let index_slots = Self::resolved_index_slots_for_access_path(&plan.access)?;

        match mode {
            IndexPredicateCompileMode::ConservativeSubset => {
                predicate_slots.compile_index_program(index_slots.as_slice())
            }
            IndexPredicateCompileMode::StrictAllOrNone => {
                predicate_slots.compile_index_program_strict(index_slots.as_slice())
            }
        }
    }
    // Apply DISTINCT before post-access phases so pagination sees unique keys.
    fn apply_distinct_if_requested(
        mut resolved: ResolvedExecutionKeyStream,
        plan: &AccessPlannedQuery<E::Key>,
        key_comparator: super::KeyOrderComparator,
    ) -> ResolvedExecutionKeyStream {
        if plan.distinct {
            let dedup_counter = Rc::new(Cell::new(0u64));
            resolved.key_stream = Box::new(DistinctOrderedKeyStream::new_with_dedup_counter(
                resolved.key_stream,
                key_comparator,
                dedup_counter.clone(),
            ));
            resolved.distinct_keys_deduped_counter = Some(dedup_counter);
        }

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
