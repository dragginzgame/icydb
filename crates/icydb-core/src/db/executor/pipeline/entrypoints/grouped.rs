//! Module: db::executor::pipeline::entrypoints::grouped
//! Defines grouped pipeline entrypoints from prepared route shapes into grouped
//! runtime execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(feature = "perf-attribution")]
use crate::db::executor::{GroupedCountFoldMetrics, with_grouped_count_fold_metrics};
use crate::db::registry::StoreHandle;
use crate::{
    db::{
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutionTrace, LoadCursorInput,
            PreparedLoadPlan, RetainedSlotLayout,
            aggregate::runtime::{
                GroupedOutputRuntimeObserverBindings, build_grouped_stream_with_runtime,
                execute_group_fold_stage, finalize_grouped_output_with_observer,
            },
            pipeline::contracts::{
                ExecutionRuntimeAdapter, GroupedCursorPage, GroupedFoldStage, GroupedRouteStage,
                GroupedStreamStage, LoadExecutor, StructuralGroupedRowRuntime,
                grouped::compile_grouped_row_slot_layout_from_parts,
            },
            pipeline::entrypoints::{LoadSurfaceMode, LoadTracingMode},
            pipeline::grouped_runtime::resolve_grouped_route_for_plan,
            pipeline::orchestrator::LoadExecutionSurface,
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            stream::access::TraversalRuntime,
        },
        query::plan::AccessPlannedQuery,
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
/// GroupedExecutePhaseAttribution
///
/// GroupedExecutePhaseAttribution records the internal grouped-load execute
/// split after one prepared route has already crossed the session compile
/// boundary.
/// It isolates grouped stream build, grouped fold, and grouped page
/// finalization so perf tooling can see which grouped runtime phase still owns
/// the repeated-query floor.
///

#[cfg(feature = "perf-attribution")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct GroupedExecutePhaseAttribution {
    pub(in crate::db) stream_local_instructions: u64,
    pub(in crate::db) fold_local_instructions: u64,
    pub(in crate::db) finalize_local_instructions: u64,
    pub(in crate::db) grouped_count_borrowed_hash_computations: u64,
    pub(in crate::db) grouped_count_bucket_candidate_checks: u64,
    pub(in crate::db) grouped_count_existing_group_hits: u64,
    pub(in crate::db) grouped_count_new_group_inserts: u64,
}

#[cfg(feature = "perf-attribution")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_grouped_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "perf-attribution")]
fn measure_grouped_execute_phase<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_grouped_local_instruction_counter();
    let result = run();
    let delta = read_grouped_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

impl GroupedPathRuntimeCore {
    // Build the grouped runtime spine once from one recovered store handle and
    // its resolved structural entity authority.
    const fn from_store(store: StoreHandle, authority: EntityAuthority) -> Self {
        Self {
            traversal_runtime: TraversalRuntime::new(store, authority.entity_tag()),
            row_store: store,
            authority,
            output_observer: GroupedOutputRuntimeObserverBindings::for_path(
                authority.entity_path(),
            ),
        }
    }

    /// Build one grouped execution stream for an already resolved route.
    fn build_grouped_stream(
        &self,
        route: &GroupedRouteStage,
        execution_preparation: ExecutionPreparation,
        grouped_slot_layout: RetainedSlotLayout,
    ) -> Result<GroupedStreamStage, InternalError> {
        let runtime = ExecutionRuntimeAdapter::from_stream_runtime_parts(
            &route.plan().access,
            self.traversal_runtime,
        );
        build_grouped_stream_with_runtime(
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
                execution_preparation.compiled_predicate(),
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

// Execute one fully resolved grouped route through the canonical grouped
// runtime spine. The grouped route/stream/page contracts are already structural,
// so this orchestration can stay monomorphic.
fn execute_grouped_route_path(
    runtime: &GroupedPathRuntimeCore,
    route: GroupedRouteStage,
    execution_preparation: ExecutionPreparation,
    grouped_slot_layout: RetainedSlotLayout,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let execution_started_at = start_execution_timer();
    let stream =
        runtime.build_grouped_stream(&route, execution_preparation, grouped_slot_layout)?;
    let folded = execute_group_fold_stage(&route, stream)?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);

    Ok(runtime.finalize_grouped_output(route, folded, execution_time_micros))
}

// Execute one fully prepared grouped runtime bundle through the canonical
// grouped runtime spine without re-entering typed executor state.
pub(in crate::db::executor) fn execute_prepared_grouped_route_runtime(
    prepared: PreparedGroupedRouteRuntime,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let PreparedGroupedRouteRuntime {
        route,
        runtime,
        execution_preparation,
        grouped_slot_layout,
    } = prepared;

    execute_grouped_route_path(&runtime, route, execution_preparation, grouped_slot_layout)
}

/// Execute one prepared grouped runtime bundle while reporting the internal
/// stream/fold/finalize split for perf-only grouped attribution surfaces.
#[cfg(feature = "perf-attribution")]
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
    let PreparedGroupedRouteRuntime {
        route,
        runtime,
        execution_preparation,
        grouped_slot_layout,
    } = prepared;
    let execution_started_at = start_execution_timer();

    // Phase 1: build the grouped execution stream from the prepared route.
    let (stream_local_instructions, stream) = measure_grouped_execute_phase(|| {
        runtime.build_grouped_stream(&route, execution_preparation, grouped_slot_layout)
    });
    let stream = stream?;

    // Phase 2: fold grouped rows over the resolved stream contract.
    let mut grouped_count_fold_metrics = GroupedCountFoldMetrics::default();
    let (fold_local_instructions, folded) = measure_grouped_execute_phase(|| {
        let (folded, metrics) =
            with_grouped_count_fold_metrics(|| execute_group_fold_stage(&route, stream));
        grouped_count_fold_metrics = metrics;

        folded
    });
    let folded = folded?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);

    // Phase 3: finalize grouped rows, cursor payload, and execution trace.
    let (finalize_local_instructions, finalized) = measure_grouped_execute_phase(|| {
        Ok::<(GroupedCursorPage, Option<ExecutionTrace>), InternalError>(
            runtime.finalize_grouped_output(route, folded, execution_time_micros),
        )
    });
    let (page, trace) = finalized?;

    Ok((
        page,
        trace,
        GroupedExecutePhaseAttribution {
            stream_local_instructions,
            fold_local_instructions,
            finalize_local_instructions,
            grouped_count_borrowed_hash_computations: grouped_count_fold_metrics
                .borrowed_hash_computations,
            grouped_count_bucket_candidate_checks: grouped_count_fold_metrics
                .bucket_candidate_checks,
            grouped_count_existing_group_hits: grouped_count_fold_metrics.existing_group_hits,
            grouped_count_new_group_inserts: grouped_count_fold_metrics.new_group_inserts,
        },
    ))
}

/// Execute one initial grouped rows path directly from one structural load plan.
///
/// This feature-gated helper keeps the generated query surface on the same grouped
/// runtime spine without reopening a typed `LoadExecutor<E>` boundary.
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_initial_grouped_rows_for_canister<C>(
    db: &crate::db::Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<GroupedCursorPage, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: finalize one generic-free grouped route from the initial
    // continuation state and structural authority.
    let plan = PreparedLoadPlan::from_plan(authority, plan);
    let prepared_execution_preparation = plan.cloned_grouped_execution_preparation();
    let prepared_grouped_slot_layout = plan.cloned_grouped_slot_layout();
    let route = resolve_grouped_route_for_plan(
        plan,
        crate::db::cursor::GroupedPlannedCursor::none(),
        debug,
    )?;
    let store = db.recovered_store(authority.store_path())?;
    let prepared = PreparedGroupedRouteRuntime::new(
        route,
        GroupedPathRuntimeCore::from_store(store, authority),
        prepared_execution_preparation,
        prepared_grouped_slot_layout,
    );

    // Phase 2: execute one grouped page and return the grouped cursor payload
    // directly so the outer surface can format the outward cursor as needed.
    let (page, _) = execute_prepared_grouped_route_runtime(prepared)?;

    Ok(page)
}

/// Execute one initial grouped rows path directly from one structural load plan
/// while reporting the grouped runtime phase split for perf-only SQL surfaces.
#[cfg(all(feature = "sql", feature = "perf-attribution"))]
pub(in crate::db) fn execute_initial_grouped_rows_for_canister_with_phase_attribution<C>(
    db: &crate::db::Db<C>,
    debug: bool,
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> Result<(GroupedCursorPage, GroupedExecutePhaseAttribution), InternalError>
where
    C: CanisterKind,
{
    let plan = PreparedLoadPlan::from_plan(authority, plan);
    let prepared_execution_preparation = plan.cloned_grouped_execution_preparation();
    let prepared_grouped_slot_layout = plan.cloned_grouped_slot_layout();
    let route = resolve_grouped_route_for_plan(
        plan,
        crate::db::cursor::GroupedPlannedCursor::none(),
        debug,
    )?;
    let store = db.recovered_store(authority.store_path())?;
    let prepared = PreparedGroupedRouteRuntime::new(
        route,
        GroupedPathRuntimeCore::from_store(store, authority),
        prepared_execution_preparation,
        prepared_grouped_slot_layout,
    );

    let (page, _, phase_attribution) =
        execute_prepared_grouped_route_runtime_with_phase_attribution(prepared)?;

    Ok((page, phase_attribution))
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    fn grouped_path_runtime(&self) -> Result<GroupedPathRuntimeCore, InternalError> {
        let authority = EntityAuthority::for_type::<E>();
        let store = self.db.recovered_store(authority.store_path())?;

        Ok(GroupedPathRuntimeCore::from_store(store, authority))
    }

    // Resolve grouped route metadata and structural runtime authority once at
    // the typed boundary before entering grouped runtime execution.
    pub(in crate::db::executor) fn prepare_grouped_route_runtime(
        &self,
        route: GroupedRouteStage,
        prepared_execution_preparation: Option<ExecutionPreparation>,
        prepared_grouped_slot_layout: Option<RetainedSlotLayout>,
    ) -> Result<PreparedGroupedRouteRuntime, InternalError> {
        Ok(PreparedGroupedRouteRuntime::new(
            route,
            self.grouped_path_runtime()?,
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
    #[cfg(feature = "perf-attribution")]
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

        let resolved_cursor = crate::db::executor::LoadCursorResolver::resolve_load_cursor_context(
            &plan,
            cursor,
            LoadSurfaceMode::grouped_paged(LoadTracingMode::Enabled),
        )?;
        let crate::db::executor::PreparedLoadCursor::Grouped(cursor) = resolved_cursor else {
            return Err(InternalError::query_executor_invariant(
                "grouped traced perf entrypoint must resolve a grouped cursor",
            ));
        };

        let prepared_execution_preparation = plan.cloned_grouped_execution_preparation();
        let prepared_grouped_slot_layout = plan.cloned_grouped_slot_layout();
        let route = resolve_grouped_route_for_plan(plan, cursor, self.debug)?;
        let prepared = self.prepare_grouped_route_runtime(
            route,
            prepared_execution_preparation,
            prepared_grouped_slot_layout,
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
