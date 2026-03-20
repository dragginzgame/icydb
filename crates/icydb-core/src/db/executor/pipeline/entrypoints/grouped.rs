//! Module: db::executor::pipeline::entrypoints::grouped
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::entrypoints::grouped.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::registry::StoreHandle;
use crate::{
    db::executor::{
        EntityAuthority, ExecutionTrace, LoadCursorInput, PreparedLoadPlan,
        aggregate::runtime::{
            GroupedOutputRuntimeObserverBindings, build_grouped_stream_with_runtime,
            execute_group_fold_stage, finalize_grouped_output_with_observer,
        },
        pipeline::contracts::{
            ExecutionRuntimeAdapter, GroupedCursorPage, GroupedFoldStage, GroupedRouteStage,
            GroupedStreamStage, LoadExecutor, StructuralGroupedRowRuntime,
        },
        pipeline::entrypoints::{LoadExecutionMode, LoadTracingMode},
        pipeline::orchestrator::LoadExecutionSurface,
        pipeline::timing::{elapsed_execution_micros, start_execution_timer},
        stream::access::StructuralTraversalRuntime,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
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
    traversal_runtime: StructuralTraversalRuntime,
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
}

impl GroupedPathRuntimeCore {
    /// Build one grouped execution stream for an already resolved route.
    fn build_grouped_stream<'a>(
        &'a self,
        route: &GroupedRouteStage,
    ) -> Result<GroupedStreamStage<'a>, InternalError> {
        let runtime = ExecutionRuntimeAdapter::from_runtime_parts(
            &route.plan().access,
            self.traversal_runtime,
            self.row_store,
            self.authority.model(),
        );

        build_grouped_stream_with_runtime(
            route,
            &runtime,
            self.authority.model(),
            runtime.slot_map().map(<[usize]>::to_vec),
            Box::new(StructuralGroupedRowRuntime::new(
                self.row_store,
                self.authority.model(),
            )),
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

// Execute one fully resolved grouped route through the canonical grouped
// runtime spine. The grouped route/stream/page contracts are already structural,
// so this orchestration can stay monomorphic.
fn execute_grouped_route_path(
    runtime: &GroupedPathRuntimeCore,
    route: GroupedRouteStage,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let execution_started_at = start_execution_timer();
    let stream = runtime.build_grouped_stream(&route)?;
    let folded = execute_group_fold_stage(&route, stream)?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);

    Ok(runtime.finalize_grouped_output(route, folded, execution_time_micros))
}

// Execute one fully prepared grouped runtime bundle through the canonical
// grouped runtime spine without re-entering typed executor state.
pub(in crate::db::executor) fn execute_prepared_grouped_route_runtime(
    prepared: PreparedGroupedRouteRuntime,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let PreparedGroupedRouteRuntime { route, runtime } = prepared;

    execute_grouped_route_path(&runtime, route)
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    fn grouped_path_runtime(&self) -> Result<GroupedPathRuntimeCore, InternalError> {
        let authority = EntityAuthority::for_type::<E>();
        let store = self.db.recovered_store(authority.store_path())?;

        Ok(GroupedPathRuntimeCore {
            traversal_runtime: StructuralTraversalRuntime::new(store, authority.entity_tag()),
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
        Ok(PreparedGroupedRouteRuntime {
            route,
            runtime: self.grouped_path_runtime()?,
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
                Err(crate::db::error::query_executor_invariant(
                    "grouped traced entrypoint must produce grouped traced page surface",
                ))
            }
        }
    }
}
