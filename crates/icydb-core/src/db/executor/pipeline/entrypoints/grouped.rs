//! Module: db::executor::pipeline::entrypoints::grouped
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::entrypoints::grouped.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::registry::StoreHandle;
use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput,
            aggregate::runtime::{
                GroupedOutputRuntimeObserverBindings, build_grouped_stream_with_runtime,
                execute_group_fold_stage, finalize_grouped_output_with_observer,
            },
            pipeline::contracts::{
                ExecutionRuntimeAdapter, GroupedCursorPage, GroupedFoldStage, GroupedRouteStage,
                GroupedStreamStage, LoadExecutor, StructuralGroupedRowRuntime,
            },
            pipeline::entrypoints::{LoadExecutionMode, LoadTracingMode},
            pipeline::orchestrator::ErasedLoadExecutionSurface,
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            stream::access::StructuralTraversalRuntime,
        },
    },
    error::InternalError,
    model::entity::EntityModel,
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

struct GroupedPathRuntimeCore<'a> {
    traversal_runtime: StructuralTraversalRuntime,
    row_store: StoreHandle,
    model: &'static EntityModel,
    output_observer: GroupedOutputRuntimeObserverBindings,
    marker: std::marker::PhantomData<&'a ()>,
}

impl GroupedPathRuntimeCore<'_> {
    /// Build one grouped execution stream for an already resolved route.
    fn build_grouped_stream<'a>(
        &'a self,
        route: &GroupedRouteStage,
    ) -> Result<GroupedStreamStage<'a>, InternalError> {
        let runtime = ExecutionRuntimeAdapter::from_runtime_parts(
            &route.plan().access,
            self.traversal_runtime,
            self.row_store,
            self.model,
        );

        build_grouped_stream_with_runtime(
            route,
            &runtime,
            self.model,
            runtime.slot_map().map(<[usize]>::to_vec),
            Box::new(StructuralGroupedRowRuntime::new(self.row_store, self.model)),
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
    runtime: &GroupedPathRuntimeCore<'_>,
    route: GroupedRouteStage,
) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
    let execution_started_at = start_execution_timer();
    let stream = runtime.build_grouped_stream(&route)?;
    let folded = execute_group_fold_stage(&route, stream)?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);

    Ok(runtime.finalize_grouped_output(route, folded, execution_time_micros))
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    fn grouped_path_runtime(&self) -> Result<GroupedPathRuntimeCore<'_>, InternalError> {
        let ctx = self.db.recovered_context::<E>()?;
        let store = ctx.structural_store()?;

        Ok(GroupedPathRuntimeCore {
            traversal_runtime: ctx.structural_traversal_runtime()?,
            row_store: store,
            model: E::MODEL,
            output_observer: GroupedOutputRuntimeObserverBindings::new::<E>(),
            marker: std::marker::PhantomData,
        })
    }

    // Execute one already-prepared grouped route stage directly through the
    // canonical grouped runtime spine.
    pub(in crate::db::executor) fn execute_prepared_grouped_route(
        &self,
        route: GroupedRouteStage,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let runtime = self.grouped_path_runtime()?;

        execute_grouped_route_path(&runtime, route)
    }

    // Execute one traced paged grouped load and materialize grouped output.
    pub(in crate::db::executor) fn execute_load_grouped_page_with_trace(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load_erased(
            plan,
            cursor,
            LoadExecutionMode::grouped_paged(LoadTracingMode::Enabled),
        )?;

        Self::expect_grouped_traced_surface(surface)
    }

    // Grouped execution spine:
    // 1) resolve grouped route/metadata
    // 2) build grouped key stream
    // 3) execute grouped fold
    // 4) finalize grouped output + observability
    pub(in crate::db::executor) fn execute_grouped_path(
        &self,
        plan: ExecutablePlan<E>,
        cursor: GroupedPlannedCursor,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let route = Self::resolve_grouped_route(plan, cursor, self.debug)?;
        let runtime = self.grouped_path_runtime()?;

        execute_grouped_route_path(&runtime, route)
    }

    // Project one traced grouped load surface and classify shape mismatches.
    fn expect_grouped_traced_surface(
        surface: ErasedLoadExecutionSurface,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        match surface {
            ErasedLoadExecutionSurface::GroupedPageWithTrace(page, trace) => Ok((page, trace)),
            _ => Err(crate::db::error::query_executor_invariant(
                "grouped traced entrypoint must produce grouped traced page surface",
            )),
        }
    }
}
