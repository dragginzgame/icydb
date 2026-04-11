//! Module: db::executor::pipeline::entrypoints::grouped
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::entrypoints::grouped.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

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
            },
            pipeline::entrypoints::{LoadExecutionMode, LoadTracingMode},
            pipeline::grouped_runtime::resolve_grouped_route_for_plan,
            pipeline::orchestrator::LoadExecutionSurface,
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            stream::access::TraversalRuntime,
            terminal::RowLayout,
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
}

impl GroupedPathRuntimeCore {
    /// Build one grouped execution stream for an already resolved route.
    fn build_grouped_stream(
        &self,
        route: &GroupedRouteStage,
        execution_preparation: ExecutionPreparation,
    ) -> Result<GroupedStreamStage, InternalError> {
        let runtime = ExecutionRuntimeAdapter::from_stream_runtime_parts(
            &route.plan().access,
            self.traversal_runtime,
        );
        let grouped_slot_layout = compile_grouped_row_slot_layout(
            self.authority.row_layout(),
            route,
            &execution_preparation,
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

// Compile the grouped ingest slot layout once from planner-owned route
// metadata so grouped row decode only materializes the slots the runtime can
// actually touch.
fn compile_grouped_row_slot_layout(
    row_layout: RowLayout,
    route: &GroupedRouteStage,
    execution_preparation: &ExecutionPreparation,
) -> RetainedSlotLayout {
    let field_count = row_layout.field_count();
    let mut required_slots = vec![false; field_count];

    // Phase 1: every grouped path needs the group key slots themselves.
    for field in route.group_fields() {
        if let Some(required_slot) = required_slots.get_mut(field.index()) {
            *required_slot = true;
        }
    }

    // Phase 2: residual predicate evaluation still runs on grouped row views.
    if let Some(compiled_predicate) = execution_preparation.compiled_predicate() {
        compiled_predicate.mark_referenced_slots(&mut required_slots);
    }

    // Phase 3: grouped reducer state only needs field-target slots for
    // aggregates whose update contract actually reads row values.
    for aggregate in route.grouped_aggregate_execution_specs() {
        let Some(target_field) = aggregate.target_field() else {
            continue;
        };
        if let Some(required_slot) = required_slots.get_mut(target_field.index()) {
            *required_slot = true;
        }
    }

    // Phase 4: the dedicated grouped DISTINCT path still reads its target
    // field from the shared grouped row view when active.
    if let Some(target_field) = route
        .grouped_distinct_execution_strategy()
        .global_distinct_target_slot()
        && let Some(required_slot) = required_slots.get_mut(target_field.index())
    {
        *required_slot = true;
    }

    RetainedSlotLayout::compile(
        field_count,
        required_slots
            .into_iter()
            .enumerate()
            .filter_map(|(slot, required)| required.then_some(slot))
            .collect(),
    )
}

// Execute one fully resolved grouped route through the canonical grouped
// runtime spine. The grouped route/stream/page contracts are already structural,
// so this orchestration can stay monomorphic.
fn execute_grouped_route_path(
    runtime: &GroupedPathRuntimeCore,
    route: GroupedRouteStage,
    execution_preparation: ExecutionPreparation,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let execution_started_at = start_execution_timer();
    let stream = runtime.build_grouped_stream(&route, execution_preparation)?;
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
    } = prepared;

    execute_grouped_route_path(&runtime, route, execution_preparation)
}

/// Execute one initial grouped rows path directly from one structural load plan.
///
/// This SQL-only helper keeps the generated query surface on the same grouped
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
    let route = resolve_grouped_route_for_plan(
        plan,
        crate::db::cursor::GroupedPlannedCursor::none(),
        debug,
    )?;
    let execution_preparation = ExecutionPreparation::from_runtime_plan(
        route.plan(),
        route.plan().slot_map().map(<[usize]>::to_vec),
    );
    let store = db.recovered_store(authority.store_path())?;
    let prepared = PreparedGroupedRouteRuntime {
        route,
        runtime: GroupedPathRuntimeCore {
            traversal_runtime: TraversalRuntime::new(store, authority.entity_tag()),
            row_store: store,
            authority,
            output_observer: GroupedOutputRuntimeObserverBindings::for_path(
                authority.entity_path(),
            ),
        },
        execution_preparation,
    };

    // Phase 2: execute one grouped page and return the grouped cursor payload
    // directly so SQL surfaces can format the outward cursor as needed.
    let (page, _) = execute_prepared_grouped_route_runtime(prepared)?;

    Ok(page)
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    fn grouped_path_runtime(&self) -> Result<GroupedPathRuntimeCore, InternalError> {
        let authority = EntityAuthority::for_type::<E>();
        let store = self.db.recovered_store(authority.store_path())?;

        Ok(GroupedPathRuntimeCore {
            traversal_runtime: TraversalRuntime::new(store, authority.entity_tag()),
            row_store: store,
            authority,
            output_observer: GroupedOutputRuntimeObserverBindings::for_path(
                authority.entity_path(),
            ),
        })
    }

    // Resolve grouped route metadata and structural runtime authority once at
    // the typed boundary before entering grouped runtime execution.
    pub(in crate::db::executor) fn prepare_grouped_route_runtime(
        &self,
        route: GroupedRouteStage,
    ) -> Result<PreparedGroupedRouteRuntime, InternalError> {
        let execution_preparation = ExecutionPreparation::from_runtime_plan(
            route.plan(),
            route.plan().slot_map().map(<[usize]>::to_vec),
        );

        Ok(PreparedGroupedRouteRuntime {
            route,
            runtime: self.grouped_path_runtime()?,
            execution_preparation,
        })
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
            LoadExecutionMode::grouped_paged(LoadTracingMode::Enabled),
        )?;

        Self::expect_grouped_traced_surface(surface)
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
