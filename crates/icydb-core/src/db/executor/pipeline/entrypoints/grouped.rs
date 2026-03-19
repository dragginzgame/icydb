//! Module: db::executor::pipeline::entrypoints::grouped
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::entrypoints::grouped.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput,
            aggregate::runtime::execute_group_fold_stage,
            pipeline::contracts::{
                GroupedCursorPage, GroupedFoldStage, GroupedRouteStage, GroupedStreamStage,
                LoadExecutor,
            },
            pipeline::entrypoints::{LoadExecutionMode, LoadTracingMode},
            pipeline::orchestrator::ErasedLoadExecutionSurface,
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// GroupedPathRuntime
///
/// GroupedPathRuntime isolates the typed grouped execution leaves behind one
/// object-safe runtime boundary.
/// Shared grouped entrypoint orchestration stays monomorphic by driving this
/// trait instead of inlining entity-typed build/fold/finalize logic.
///

trait GroupedPathRuntime {
    /// Build one grouped execution stream for an already resolved route.
    fn build_grouped_stream<'a>(
        &'a self,
        route: &GroupedRouteStage,
    ) -> Result<GroupedStreamStage<'a>, InternalError>;

    /// Finalize grouped output payloads and observability after fold completion.
    fn finalize_grouped_output(
        &self,
        route: GroupedRouteStage,
        folded: GroupedFoldStage,
        execution_time_micros: u64,
    ) -> (GroupedCursorPage, Option<ExecutionTrace>);
}

// Execute one fully resolved grouped route through the canonical grouped
// runtime spine. The grouped route/stream/page contracts are already structural,
// so this orchestration can stay monomorphic.
fn execute_grouped_route_path(
    runtime: &dyn GroupedPathRuntime,
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
    // Execute one already-prepared grouped route stage directly through the
    // canonical grouped runtime spine.
    pub(in crate::db::executor) fn execute_prepared_grouped_route(
        &self,
        route: GroupedRouteStage,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        execute_grouped_route_path(self, route)
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

        execute_grouped_route_path(self, route)
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

impl<E> GroupedPathRuntime for LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    fn build_grouped_stream<'a>(
        &'a self,
        route: &GroupedRouteStage,
    ) -> Result<GroupedStreamStage<'a>, InternalError> {
        Self::build_grouped_stream(self, route)
    }

    fn finalize_grouped_output(
        &self,
        route: GroupedRouteStage,
        folded: GroupedFoldStage,
        execution_time_micros: u64,
    ) -> (GroupedCursorPage, Option<ExecutionTrace>) {
        Self::finalize_grouped_output(self, route, folded, execution_time_micros)
    }
}
