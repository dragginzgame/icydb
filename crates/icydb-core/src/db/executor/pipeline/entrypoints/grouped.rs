//! Module: db::executor::pipeline::entrypoints::grouped
//! Defines grouped pipeline entrypoints from prepared route shapes into grouped
//! runtime execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::measure_local_instruction_delta as measure_grouped_execute_phase;
#[cfg(feature = "diagnostics")]
use crate::db::executor::{GroupedCountFoldMetrics, with_grouped_count_fold_metrics};
use crate::db::registry::StoreHandle;
use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutionTrace, LoadCursorInput,
            PreparedLoadPlan, RetainedSlotLayout,
            aggregate::runtime::{
                GroupedOutputRuntimeObserverBindings, build_grouped_stream_with_runtime,
                execute_group_fold_stage, finalize_grouped_output_with_observer,
            },
            pipeline::contracts::{
                ExecutionRuntimeAdapter, GroupedCursorPage, GroupedRouteStage, LoadExecutor,
            },
            pipeline::entrypoints::{LoadSurfaceMode, LoadTracingMode},
            pipeline::grouped_runtime::resolve_grouped_route_for_plan,
            pipeline::orchestrator::LoadExecutionSurface,
            pipeline::runtime::{
                GroupedFoldStage, GroupedStreamStage, StructuralGroupedRowRuntime,
                compile_grouped_row_slot_layout_from_parts,
            },
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            record_aggregation,
            stream::access::TraversalRuntime,
            with_execution_stats_capture,
        },
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue},
};

///
/// GroupedPathRuntimeCore
///
/// GroupedPathRuntimeCore bundles the structural runtime pieces needed by the
/// grouped execution spine after the typed boundary resolves model/store
/// authority.
/// Shared grouped entrypoint orchestration stays monomorphic by driving this
/// structural bundle instead of `LoadExecutor<E>` directly.
///

struct GroupedPathRuntimeCore {
    traversal_runtime: TraversalRuntime,
    row_store: StoreHandle,
    authority: EntityAuthority,
    output_observer: GroupedOutputRuntimeObserverBindings,
}

///
/// PreparedGroupedRouteRuntime
///
/// PreparedGroupedRouteRuntime is the generic-free grouped execution bundle
/// emitted once the typed boundary has resolved route metadata and structural
/// runtime authority.
/// Grouped runtime execution consumes this bundle directly so grouped lanes no
/// longer depend on `LoadExecutor<E>` after preparation.
///

pub(in crate::db::executor) struct PreparedGroupedRouteRuntime {
    route: GroupedRouteStage,
    runtime: GroupedPathRuntimeCore,
    execution_preparation: ExecutionPreparation,
    grouped_slot_layout: RetainedSlotLayout,
}

///
/// GroupedRouteExecutionResult
///
/// GroupedRouteExecutionResult is the canonical grouped runtime output shell
/// used by both ordinary and diagnostics-attributed grouped entrypoints.
/// The grouped lane keeps one execution spine and lets outer wrappers choose
/// whether they need the optional phase split.
///

struct GroupedRouteExecutionResult {
    page: GroupedCursorPage,
    trace: Option<ExecutionTrace>,
    #[cfg(feature = "diagnostics")]
    phase_attribution: Option<GroupedExecutePhaseAttribution>,
}

///
/// GroupedExecutePhaseAttribution
///
/// GroupedExecutePhaseAttribution records the internal grouped-load execute
/// split after one prepared route has already crossed the session compile
/// boundary.
/// It isolates grouped stream build, grouped fold, and grouped page
/// finalization so perf tooling can see which grouped runtime phase still owns
/// the repeated-query floor.
///

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct GroupedCountAttribution {
    pub(in crate::db) borrowed_hash_computations: u64,
    pub(in crate::db) bucket_candidate_checks: u64,
    pub(in crate::db) existing_group_hits: u64,
    pub(in crate::db) new_group_inserts: u64,
    pub(in crate::db) row_materialization_local_instructions: u64,
    pub(in crate::db) group_lookup_local_instructions: u64,
    pub(in crate::db) existing_group_update_local_instructions: u64,
    pub(in crate::db) new_group_insert_local_instructions: u64,
}

#[cfg(feature = "diagnostics")]
impl GroupedCountAttribution {
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            borrowed_hash_computations: 0,
            bucket_candidate_checks: 0,
            existing_group_hits: 0,
            new_group_inserts: 0,
            row_materialization_local_instructions: 0,
            group_lookup_local_instructions: 0,
            existing_group_update_local_instructions: 0,
            new_group_insert_local_instructions: 0,
        }
    }

    #[must_use]
    const fn from_fold_metrics(metrics: GroupedCountFoldMetrics) -> Self {
        Self {
            borrowed_hash_computations: metrics.borrowed_hash_computations,
            bucket_candidate_checks: metrics.bucket_candidate_checks,
            existing_group_hits: metrics.existing_group_hits,
            new_group_inserts: metrics.new_group_inserts,
            row_materialization_local_instructions: metrics.row_materialization_local_instructions,
            group_lookup_local_instructions: metrics.group_lookup_local_instructions,
            existing_group_update_local_instructions: metrics
                .existing_group_update_local_instructions,
            new_group_insert_local_instructions: metrics.new_group_insert_local_instructions,
        }
    }
}

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct GroupedExecutePhaseAttribution {
    pub(in crate::db) stream_local_instructions: u64,
    pub(in crate::db) fold_local_instructions: u64,
    pub(in crate::db) finalize_local_instructions: u64,
    pub(in crate::db) grouped_count: GroupedCountAttribution,
}

impl GroupedPathRuntimeCore {
    // Build the grouped runtime spine once from one recovered store handle and
    // its resolved structural entity authority.
    const fn from_store(store: StoreHandle, authority: EntityAuthority) -> Self {
        let entity_tag = authority.entity_tag();
        let entity_path = authority.entity_path();

        Self {
            traversal_runtime: TraversalRuntime::new(store, entity_tag),
            row_store: store,
            authority,
            output_observer: GroupedOutputRuntimeObserverBindings::for_path(entity_path),
        }
    }

    /// Build one grouped execution stream for an already resolved route.
    fn build_grouped_stream(
        &self,
        route: &GroupedRouteStage,
        execution_preparation: ExecutionPreparation,
        grouped_slot_layout: RetainedSlotLayout,
    ) -> Result<GroupedStreamStage, InternalError> {
        let runtime = ExecutionRuntimeAdapter::from_stream_runtime_parts(self.traversal_runtime);
        build_grouped_stream_with_runtime(
            self.authority.entity_path(),
            route,
            &runtime,
            execution_preparation,
            StructuralGroupedRowRuntime::new(
                self.row_store,
                self.authority.row_layout(),
                grouped_slot_layout,
            ),
        )
    }

    /// Finalize grouped output payloads and observability after fold completion.
    fn finalize_grouped_output(
        &self,
        route: GroupedRouteStage,
        folded: GroupedFoldStage,
        execution_time_micros: u64,
    ) -> (GroupedCursorPage, Option<ExecutionTrace>) {
        finalize_grouped_output_with_observer(
            &self.output_observer,
            route,
            folded,
            execution_time_micros,
        )
    }
}

impl PreparedGroupedRouteRuntime {
    // Build one prepared grouped runtime bundle from one resolved route and
    // one structural grouped runtime core without duplicating plan prep logic.
    fn new(
        route: GroupedRouteStage,
        runtime: GroupedPathRuntimeCore,
        prepared_execution_preparation: Option<ExecutionPreparation>,
        prepared_grouped_slot_layout: Option<RetainedSlotLayout>,
    ) -> Self {
        let execution_preparation = prepared_execution_preparation.unwrap_or_else(|| {
            ExecutionPreparation::from_runtime_plan(
                route.plan(),
                route.plan().slot_map().map(<[usize]>::to_vec),
            )
        });
        let grouped_slot_layout = prepared_grouped_slot_layout.unwrap_or_else(|| {
            compile_grouped_row_slot_layout_from_parts(
                runtime.authority.row_layout(),
                route.group_fields(),
                route.grouped_aggregate_execution_specs(),
                route.grouped_distinct_execution_strategy(),
                execution_preparation.effective_runtime_filter_program(),
            )
        });

        Self {
            route,
            runtime,
            execution_preparation,
            grouped_slot_layout,
        }
    }
}

// Prepare one grouped runtime bundle from one prepared load plan plus the
// caller-resolved grouped cursor so entrypoints and orchestrator strategy
// share one route/runtime assembly seam.
pub(in crate::db::executor) fn prepare_grouped_route_runtime_for_load_plan<C>(
    db: &crate::db::Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    cursor: GroupedPlannedCursor,
) -> Result<PreparedGroupedRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let authority = plan.authority();
    let prepared_runtime_parts = plan.cloned_grouped_runtime_parts();
    let route = resolve_grouped_route_for_plan(plan, cursor, debug)?;
    let store = db.recovered_store(authority.store_path())?;

    Ok(PreparedGroupedRouteRuntime::new(
        route,
        GroupedPathRuntimeCore::from_store(store, authority),
        prepared_runtime_parts.execution_preparation,
        prepared_runtime_parts.grouped_slot_layout,
    ))
}

// Execute one fully resolved grouped route through the canonical grouped
// runtime spine. The grouped route/stream/page contracts are already structural,
// so this orchestration can stay monomorphic.
fn execute_grouped_route_path(
    runtime: &GroupedPathRuntimeCore,
    mut route: GroupedRouteStage,
    execution_preparation: ExecutionPreparation,
    grouped_slot_layout: RetainedSlotLayout,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let collect_stats = route.execution_trace.is_some();
    let execution_started_at = start_execution_timer();
    let (fold_result, mut execution_stats) = with_execution_stats_capture(collect_stats, || {
        let stream =
            runtime.build_grouped_stream(&route, execution_preparation, grouped_slot_layout)?;
        let (folded, aggregation_micros) =
            crate::db::executor::measure_execution_stats_phase(|| {
                execute_group_fold_stage(&route, stream)
            });
        record_aggregation(aggregation_micros);

        folded
    });
    let folded = fold_result?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);
    if let Some(stats) = execution_stats.as_mut() {
        stats.apply_grouped_outcome(folded.rows_returned());
    }
    if let Some(trace) = route.execution_trace_mut().as_mut() {
        trace.set_execution_stats(
            execution_stats.map(crate::db::executor::ExecutionProfileStats::into_execution_stats),
        );
    }

    Ok(runtime.finalize_grouped_output(route, folded, execution_time_micros))
}

// Execute one grouped prepared runtime bundle through the canonical grouped
// runtime spine while optionally capturing diagnostics phase attribution.
fn execute_prepared_grouped_route_runtime_internal(
    prepared: PreparedGroupedRouteRuntime,
    #[cfg(feature = "diagnostics")] collect_phase_attribution: bool,
) -> Result<GroupedRouteExecutionResult, InternalError> {
    let PreparedGroupedRouteRuntime {
        route,
        runtime,
        execution_preparation,
        grouped_slot_layout,
    } = prepared;

    #[cfg(feature = "diagnostics")]
    if collect_phase_attribution {
        let mut route = route;
        let collect_stats = route.execution_trace.is_some();
        let execution_started_at = start_execution_timer();

        let (attributed_result, mut execution_stats) =
            with_execution_stats_capture(collect_stats, || {
                // Phase 1: build the grouped execution stream from the prepared route.
                let (stream_local_instructions, stream) = measure_grouped_execute_phase(|| {
                    runtime.build_grouped_stream(&route, execution_preparation, grouped_slot_layout)
                });
                let stream = stream?;

                // Phase 2: fold grouped rows over the resolved stream contract.
                let mut grouped_count_fold_metrics = GroupedCountFoldMetrics::default();
                let ((fold_local_instructions, folded), aggregation_micros) =
                    crate::db::executor::measure_execution_stats_phase(|| {
                        measure_grouped_execute_phase(|| {
                            let (folded, metrics) = with_grouped_count_fold_metrics(|| {
                                execute_group_fold_stage(&route, stream)
                            });
                            grouped_count_fold_metrics = metrics;

                            folded
                        })
                    });
                record_aggregation(aggregation_micros);
                let folded = folded?;

                Ok::<_, InternalError>((
                    stream_local_instructions,
                    fold_local_instructions,
                    grouped_count_fold_metrics,
                    folded,
                ))
            });
        let (
            stream_local_instructions,
            fold_local_instructions,
            grouped_count_fold_metrics,
            folded,
        ) = attributed_result?;
        if let Some(stats) = execution_stats.as_mut() {
            stats.apply_grouped_outcome(folded.rows_returned());
        }
        if let Some(trace) = route.execution_trace_mut().as_mut() {
            trace.set_execution_stats(
                execution_stats
                    .map(crate::db::executor::ExecutionProfileStats::into_execution_stats),
            );
        }
        let execution_time_micros = elapsed_execution_micros(execution_started_at);

        // Phase 3: finalize grouped rows, cursor payload, and execution trace.
        let (finalize_local_instructions, finalized) = measure_grouped_execute_phase(|| {
            Ok::<(GroupedCursorPage, Option<ExecutionTrace>), InternalError>(
                runtime.finalize_grouped_output(route, folded, execution_time_micros),
            )
        });
        let (page, trace) = finalized?;

        return Ok(GroupedRouteExecutionResult {
            page,
            trace,
            phase_attribution: Some(GroupedExecutePhaseAttribution {
                stream_local_instructions,
                fold_local_instructions,
                finalize_local_instructions,
                grouped_count: GroupedCountAttribution::from_fold_metrics(
                    grouped_count_fold_metrics,
                ),
            }),
        });
    }

    let (page, trace) =
        execute_grouped_route_path(&runtime, route, execution_preparation, grouped_slot_layout)?;

    Ok(GroupedRouteExecutionResult {
        page,
        trace,
        #[cfg(feature = "diagnostics")]
        phase_attribution: None,
    })
}

// Execute one fully prepared grouped runtime bundle through the canonical
// grouped runtime spine without re-entering typed executor state.
pub(in crate::db::executor) fn execute_prepared_grouped_route_runtime(
    prepared: PreparedGroupedRouteRuntime,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let result = execute_prepared_grouped_route_runtime_internal(
        prepared,
        #[cfg(feature = "diagnostics")]
        false,
    )?;

    Ok((result.page, result.trace))
}

/// Execute one prepared grouped runtime bundle while reporting the internal
/// stream/fold/finalize split for perf-only grouped attribution surfaces.
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) fn execute_prepared_grouped_route_runtime_with_phase_attribution(
    prepared: PreparedGroupedRouteRuntime,
) -> Result<
    (
        GroupedCursorPage,
        Option<ExecutionTrace>,
        GroupedExecutePhaseAttribution,
    ),
    InternalError,
> {
    let result = execute_prepared_grouped_route_runtime_internal(prepared, true)?;
    let phase_attribution = result.phase_attribution.ok_or_else(|| {
        InternalError::query_executor_invariant(
            "grouped attributed runtime must emit grouped phase attribution",
        )
    })?;

    Ok((result.page, result.trace, phase_attribution))
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    fn grouped_path_runtime(
        &self,
        authority: EntityAuthority,
    ) -> Result<GroupedPathRuntimeCore, InternalError> {
        let store = self.db.recovered_store(authority.store_path())?;

        Ok(GroupedPathRuntimeCore::from_store(store, authority))
    }

    // Resolve grouped route metadata and structural runtime authority once at
    // the typed boundary before entering grouped runtime execution.
    pub(in crate::db::executor) fn prepare_grouped_route_runtime(
        &self,
        route: GroupedRouteStage,
        authority: EntityAuthority,
        prepared_execution_preparation: Option<ExecutionPreparation>,
        prepared_grouped_slot_layout: Option<RetainedSlotLayout>,
    ) -> Result<PreparedGroupedRouteRuntime, InternalError> {
        Ok(PreparedGroupedRouteRuntime::new(
            route,
            self.grouped_path_runtime(authority)?,
            prepared_execution_preparation,
            prepared_grouped_slot_layout,
        ))
    }

    // Execute one traced paged grouped load and materialize grouped output.
    pub(in crate::db::executor) fn execute_load_grouped_page_with_trace(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load_surface(
            plan,
            cursor,
            LoadSurfaceMode::grouped_paged(LoadTracingMode::Enabled),
        )?;

        Self::expect_grouped_traced_surface(surface)
    }

    // Execute one traced paged grouped load while reporting the grouped runtime
    // stream/fold/finalize split for perf-only attribution surfaces.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db::executor) fn execute_load_grouped_page_with_trace_with_phase_attribution(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
    ) -> Result<
        (
            GroupedCursorPage,
            Option<ExecutionTrace>,
            GroupedExecutePhaseAttribution,
        ),
        InternalError,
    > {
        if !plan.mode().is_load() {
            return Err(InternalError::load_executor_load_plan_required());
        }

        let resolved_cursor = super::resolve_grouped_perf_cursor(&plan, cursor)?;
        let crate::db::executor::PreparedLoadCursor::Grouped(cursor) = resolved_cursor else {
            return Err(InternalError::query_executor_invariant(
                "grouped traced perf entrypoint must resolve a grouped cursor",
            ));
        };

        let prepared_runtime_parts = plan.cloned_grouped_runtime_parts();
        let authority = plan.authority();
        let route = resolve_grouped_route_for_plan(plan, cursor, self.debug)?;
        let prepared = self.prepare_grouped_route_runtime(
            route,
            authority,
            prepared_runtime_parts.execution_preparation,
            prepared_runtime_parts.grouped_slot_layout,
        )?;

        execute_prepared_grouped_route_runtime_with_phase_attribution(prepared)
    }

    // Project one traced grouped load surface and classify shape mismatches.
    fn expect_grouped_traced_surface(
        surface: LoadExecutionSurface,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        match surface {
            LoadExecutionSurface::GroupedPageWithTrace(page, trace) => Ok((page, trace)),
            LoadExecutionSurface::ScalarPageWithTrace(..) => {
                Err(InternalError::query_executor_invariant(
                    "grouped traced entrypoint must produce grouped traced page surface",
                ))
            }
        }
    }
}
