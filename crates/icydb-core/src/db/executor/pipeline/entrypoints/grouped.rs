//! Module: db::executor::pipeline::entrypoints::grouped
//! Defines grouped pipeline entrypoints from prepared route shapes into grouped
//! runtime execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::measure_local_instruction_delta as measure_grouped_execute_phase;
#[cfg(feature = "diagnostics")]
use crate::db::executor::{
    GroupedCountFoldMetrics, aggregate::GroupedRuntimeStats, with_grouped_count_fold_metrics,
};
use crate::db::registry::StoreHandle;
use crate::{
    db::{
        cursor::ValidatedGroupedCursor,
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutionTrace, LoadCursorInput,
            PreparedGroupedRuntimeResidents, PreparedLoadPlan, RetainedSlotLayout,
            aggregate::runtime::{
                GroupedOutputRuntimeObserverBindings, build_grouped_stream_with_runtime,
                execute_group_fold_stage, finalize_grouped_output_with_observer,
            },
            pipeline::contracts::{
                ExecutionRuntimeAdapter, GroupedCursorPage, GroupedRouteStage, LoadExecutor,
            },
            pipeline::entrypoints::LoadSurfaceMode,
            pipeline::grouped_runtime::resolve_grouped_route_for_plan,
            pipeline::orchestrator::LoadExecutionSurface,
            pipeline::runtime::{
                GroupedFoldStage, GroupedStreamStage, StructuralGroupedRowRuntime,
                compile_grouped_row_slot_layout_from_inputs,
            },
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            record_aggregation,
            stream::access::TraversalRuntime,
            with_execution_stats_capture,
        },
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
    traits::CanisterKind,
};

///
/// GroupedPathRuntimeContext
///
/// GroupedPathRuntimeContext is the owner-local runtime context needed by the
/// grouped execution spine after the typed boundary resolves model/store
/// authority.
/// Shared grouped entrypoint orchestration stays monomorphic by driving this
/// structural context instead of `LoadExecutor<E>` directly.
///

struct GroupedPathRuntimeContext {
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
    runtime: GroupedPathRuntimeContext,
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
/// GroupedExecutionObserver
///
/// GroupedExecutionObserver records optional diagnostics around the canonical
/// grouped stream, fold, and finalize operations. Its non-diagnostics form is
/// zero-sized and executes those operations directly.
///

struct GroupedExecutionObserver {
    #[cfg(feature = "diagnostics")]
    collect_phase_attribution: bool,
    #[cfg(feature = "diagnostics")]
    phase_attribution: GroupedExecutePhaseAttribution,
}

///
/// GroupedCountAttribution
///
/// GroupedCountAttribution records dedicated grouped-count lookup and update
/// work from the canonical grouped fold operation.
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
    #[cfg(any(test, feature = "sql"))]
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

///
/// GroupedRuntimeAttribution
///
/// GroupedRuntimeAttribution carries the resource-owner snapshot captured by
/// successful grouped fold execution through diagnostics boundaries.
/// It is the single internal transport for grouped work and peak live-state
/// facts shared by fluent and SQL attribution.
///

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct GroupedRuntimeAttribution {
    pub(in crate::db) rows_scanned: u64,
    pub(in crate::db) groups_observed: u64,
    pub(in crate::db) groups_finalized: u64,
    pub(in crate::db) peak_live_groups: u64,
    pub(in crate::db) peak_live_aggregate_states: u64,
    pub(in crate::db) peak_live_distinct_values: u64,
    pub(in crate::db) early_scan_stop: bool,
}

#[cfg(feature = "diagnostics")]
impl GroupedRuntimeAttribution {
    /// Build the diagnostics transport directly from executor-owned runtime truth.
    #[must_use]
    fn from_runtime_stats(stats: GroupedRuntimeStats, rows_scanned: usize) -> Self {
        Self {
            rows_scanned: u64::try_from(rows_scanned).unwrap_or(u64::MAX),
            groups_observed: stats.groups_observed(),
            groups_finalized: stats.groups_finalized(),
            peak_live_groups: stats.peak_live_groups(),
            peak_live_aggregate_states: stats.peak_live_aggregate_states(),
            peak_live_distinct_values: stats.peak_live_distinct_values(),
            early_scan_stop: stats.early_scan_stop(),
        }
    }

    /// Build the empty runtime attribution used by non-grouped SQL phases.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            rows_scanned: 0,
            groups_observed: 0,
            groups_finalized: 0,
            peak_live_groups: 0,
            peak_live_aggregate_states: 0,
            peak_live_distinct_values: 0,
            early_scan_stop: false,
        }
    }
}

impl GroupedExecutionObserver {
    #[cfg(feature = "diagnostics")]
    fn new(collect_phase_attribution: bool) -> Self {
        Self {
            collect_phase_attribution,
            phase_attribution: GroupedExecutePhaseAttribution::default(),
        }
    }

    #[cfg(not(feature = "diagnostics"))]
    const fn new() -> Self {
        Self {}
    }

    #[cfg(feature = "diagnostics")]
    fn build_stream(
        &mut self,
        build: impl FnOnce() -> Result<GroupedStreamStage, InternalError>,
    ) -> Result<GroupedStreamStage, InternalError> {
        if self.collect_phase_attribution {
            let (local_instructions, stream) = measure_grouped_execute_phase(build);
            self.phase_attribution.stream_local_instructions = local_instructions;

            return stream;
        }

        build()
    }

    #[cfg(not(feature = "diagnostics"))]
    #[expect(
        clippy::unused_self,
        reason = "keeps one observer call shape while the non-diagnostics observer remains zero-sized"
    )]
    fn build_stream(
        &self,
        build: impl FnOnce() -> Result<GroupedStreamStage, InternalError>,
    ) -> Result<GroupedStreamStage, InternalError> {
        build()
    }

    #[cfg(feature = "diagnostics")]
    fn fold(
        &mut self,
        fold: impl FnOnce() -> Result<GroupedFoldStage, InternalError>,
    ) -> Result<GroupedFoldStage, InternalError> {
        if self.collect_phase_attribution {
            let mut grouped_count_fold_metrics = GroupedCountFoldMetrics::default();
            let ((local_instructions, folded), aggregation_micros) =
                crate::db::executor::measure_execution_stats_phase(|| {
                    measure_grouped_execute_phase(|| {
                        let (folded, metrics) = with_grouped_count_fold_metrics(fold);
                        grouped_count_fold_metrics = metrics;

                        folded
                    })
                });
            record_aggregation(aggregation_micros);
            self.phase_attribution.fold_local_instructions = local_instructions;
            self.phase_attribution.grouped_count =
                GroupedCountAttribution::from_fold_metrics(grouped_count_fold_metrics);

            return folded;
        }

        let (folded, aggregation_micros) = crate::db::executor::measure_execution_stats_phase(fold);
        record_aggregation(aggregation_micros);

        folded
    }

    #[cfg(not(feature = "diagnostics"))]
    #[expect(
        clippy::unused_self,
        reason = "keeps one observer call shape while the non-diagnostics observer remains zero-sized"
    )]
    fn fold(
        &self,
        fold: impl FnOnce() -> Result<GroupedFoldStage, InternalError>,
    ) -> Result<GroupedFoldStage, InternalError> {
        let (folded, aggregation_micros) = crate::db::executor::measure_execution_stats_phase(fold);
        record_aggregation(aggregation_micros);

        folded
    }

    #[cfg(feature = "diagnostics")]
    fn observe_runtime(&mut self, folded: &GroupedFoldStage) {
        if self.collect_phase_attribution {
            self.phase_attribution.runtime = GroupedRuntimeAttribution::from_runtime_stats(
                folded.runtime_stats(),
                folded.rows_scanned(),
            );
        }
    }

    #[cfg(not(feature = "diagnostics"))]
    #[expect(
        clippy::unused_self,
        reason = "keeps one observer call shape while the non-diagnostics observer remains zero-sized"
    )]
    const fn observe_runtime(&self, _folded: &GroupedFoldStage) {}

    #[cfg(feature = "diagnostics")]
    fn finalize(
        &mut self,
        finalize: impl FnOnce() -> (GroupedCursorPage, Option<ExecutionTrace>),
    ) -> (GroupedCursorPage, Option<ExecutionTrace>) {
        if self.collect_phase_attribution {
            let (local_instructions, finalized) = measure_grouped_execute_phase(finalize);
            self.phase_attribution.finalize_local_instructions = local_instructions;

            return finalized;
        }

        finalize()
    }

    #[cfg(not(feature = "diagnostics"))]
    #[expect(
        clippy::unused_self,
        reason = "keeps one observer call shape while the non-diagnostics observer remains zero-sized"
    )]
    fn finalize(
        &self,
        finalize: impl FnOnce() -> (GroupedCursorPage, Option<ExecutionTrace>),
    ) -> (GroupedCursorPage, Option<ExecutionTrace>) {
        finalize()
    }

    #[cfg(feature = "diagnostics")]
    fn into_phase_attribution(self) -> Option<GroupedExecutePhaseAttribution> {
        self.collect_phase_attribution
            .then_some(self.phase_attribution)
    }
}

///
/// GroupedExecutePhaseAttribution
///
/// GroupedExecutePhaseAttribution records the internal grouped-load execute
/// split after one prepared route has already crossed the session compile
/// boundary. It observes the canonical stream, fold, and finalization
/// operations without owning an alternate execution path.
///

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct GroupedExecutePhaseAttribution {
    pub(in crate::db) stream_local_instructions: u64,
    pub(in crate::db) fold_local_instructions: u64,
    pub(in crate::db) finalize_local_instructions: u64,
    pub(in crate::db) runtime: GroupedRuntimeAttribution,
    pub(in crate::db) grouped_count: GroupedCountAttribution,
}

impl GroupedPathRuntimeContext {
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
        let runtime = ExecutionRuntimeAdapter::from_stream_runtime(self.traversal_runtime);
        build_grouped_stream_with_runtime(
            self.authority.entity_path(),
            route,
            &runtime,
            execution_preparation,
            StructuralGroupedRowRuntime::new(
                self.row_store,
                self.authority.row_layout()?,
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
        runtime: GroupedPathRuntimeContext,
        prepared_residents: Option<PreparedGroupedRuntimeResidents>,
    ) -> Result<Self, InternalError> {
        let residents = if let Some(residents) = prepared_residents {
            residents
        } else {
            let execution_preparation = ExecutionPreparation::from_runtime_plan(
                route.plan(),
                route.plan().slot_map().map(<[usize]>::to_vec),
            );
            let grouped_slot_layout = compile_grouped_row_slot_layout_from_inputs(
                runtime.authority.row_layout()?,
                route.group_fields(),
                route.grouped_aggregate_execution_specs(),
                route.grouped_distinct_execution_strategy(),
                execution_preparation.effective_runtime_filter_program(),
            );

            PreparedGroupedRuntimeResidents::new(execution_preparation, grouped_slot_layout)
        };
        let (execution_preparation, grouped_slot_layout) = residents.into_parts();

        Ok(Self {
            route,
            runtime,
            execution_preparation,
            grouped_slot_layout,
        })
    }
}

// Prepare one grouped runtime bundle from one prepared load plan plus the
// caller-resolved grouped cursor so entrypoints and orchestrator strategy
// share one route/runtime assembly seam.
pub(in crate::db::executor) fn prepare_grouped_route_runtime_for_load_plan<C>(
    db: &crate::db::Db<C>,
    debug: bool,
    plan: PreparedLoadPlan,
    cursor: ValidatedGroupedCursor,
) -> Result<PreparedGroupedRouteRuntime, InternalError>
where
    C: CanisterKind,
{
    let authority = plan.authority();
    let prepared_residents = plan.cloned_grouped_runtime_residents()?;
    let route = resolve_grouped_route_for_plan(plan, cursor, debug)?;
    let store = db.recovered_store(authority.store_path())?;

    PreparedGroupedRouteRuntime::new(
        route,
        GroupedPathRuntimeContext::from_store(store, authority),
        prepared_residents,
    )
}

// Execute one fully resolved grouped route through the canonical grouped
// runtime spine. The grouped route/stream/page contracts are already structural,
// so this orchestration can stay monomorphic.
fn execute_grouped_route_path(
    runtime: &GroupedPathRuntimeContext,
    mut route: GroupedRouteStage,
    execution_preparation: ExecutionPreparation,
    grouped_slot_layout: RetainedSlotLayout,
    #[cfg(feature = "diagnostics")] collect_phase_attribution: bool,
) -> Result<GroupedRouteExecutionResult, InternalError> {
    #[cfg(feature = "diagnostics")]
    let mut observer = GroupedExecutionObserver::new(collect_phase_attribution);
    #[cfg(not(feature = "diagnostics"))]
    let observer = GroupedExecutionObserver::new();
    let collect_stats = route.execution_trace.is_some();
    let execution_started_at = start_execution_timer();
    let (fold_result, mut execution_stats) = with_execution_stats_capture(collect_stats, || {
        let stream = observer.build_stream(|| {
            runtime.build_grouped_stream(&route, execution_preparation, grouped_slot_layout)
        })?;

        observer.fold(|| execute_group_fold_stage(&route, stream))
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
    observer.observe_runtime(&folded);
    let (page, trace) =
        observer.finalize(|| runtime.finalize_grouped_output(route, folded, execution_time_micros));

    Ok(GroupedRouteExecutionResult {
        page,
        trace,
        #[cfg(feature = "diagnostics")]
        phase_attribution: observer.into_phase_attribution(),
    })
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

    execute_grouped_route_path(
        &runtime,
        route,
        execution_preparation,
        grouped_slot_layout,
        #[cfg(feature = "diagnostics")]
        collect_phase_attribution,
    )
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
    let phase_attribution = result
        .phase_attribution
        .ok_or_else(InternalError::query_executor_invariant)?;

    Ok((result.page, result.trace, phase_attribution))
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Prepare the canonical grouped load runtime from a cursor already
    /// resolved by the parent entrypoint orchestration boundary.
    pub(super) fn prepare_grouped_route_runtime_from_resolved_cursor(
        &self,
        plan: PreparedLoadPlan,
        cursor: ValidatedGroupedCursor,
    ) -> Result<PreparedGroupedRouteRuntime, InternalError> {
        prepare_grouped_route_runtime_for_load_plan(&self.db, self.debug, plan, cursor)
    }

    fn grouped_path_runtime(
        &self,
        authority: EntityAuthority,
    ) -> Result<GroupedPathRuntimeContext, InternalError> {
        let store = self.db.recovered_store(authority.store_path())?;

        Ok(GroupedPathRuntimeContext::from_store(store, authority))
    }

    // Resolve grouped route metadata and structural runtime authority once at
    // the typed boundary before entering grouped runtime execution.
    pub(in crate::db::executor) fn prepare_grouped_route_runtime(
        &self,
        route: GroupedRouteStage,
        authority: EntityAuthority,
        prepared_residents: Option<PreparedGroupedRuntimeResidents>,
    ) -> Result<PreparedGroupedRouteRuntime, InternalError> {
        PreparedGroupedRouteRuntime::new(
            route,
            self.grouped_path_runtime(authority)?,
            prepared_residents,
        )
    }

    // Execute one traced paged grouped load and materialize grouped output.
    pub(in crate::db::executor) fn execute_load_grouped_page_with_trace(
        &self,
        plan: PreparedLoadPlan,
        cursor: LoadCursorInput,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load_surface(plan, cursor, LoadSurfaceMode::GroupedPage)?;

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
        let prepared = self.prepare_grouped_load_route_runtime(plan, cursor)?;

        execute_prepared_grouped_route_runtime_with_phase_attribution(prepared)
    }

    // Project one traced grouped load surface and classify shape mismatches.
    fn expect_grouped_traced_surface(
        surface: LoadExecutionSurface,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        match surface {
            LoadExecutionSurface::GroupedPageWithTrace(page, trace) => Ok((page, trace)),
            LoadExecutionSurface::ScalarPageWithTrace(..) => {
                Err(InternalError::query_executor_invariant())
            }
        }
    }
}
