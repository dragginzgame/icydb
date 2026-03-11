//! Module: db::executor::pipeline::entrypoints::grouped
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::entrypoints::grouped.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan, ExecutionTrace, LoadCursorInput,
            pipeline::contracts::{GroupedCursorPage, LoadExecutor},
            pipeline::entrypoints::{LoadExecutionMode, LoadExecutionSurface, LoadTracingMode},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::time::Instant;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one traced paged grouped load and materialize grouped output.
    pub(in crate::db::executor) fn execute_load_grouped_page_with_trace(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let surface = self.execute_load(
            plan,
            cursor,
            LoadExecutionMode::grouped_paged(LoadTracingMode::Enabled),
        )?;
        match surface {
            LoadExecutionSurface::GroupedPageWithTrace(page, trace) => Ok((page, trace)),
            _ => Err(crate::db::error::query_executor_invariant(
                "grouped traced entrypoint must produce grouped traced page surface",
            )),
        }
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
        let execution_started_at = Instant::now();
        let route = Self::resolve_grouped_route(plan, cursor, self.debug)?;
        let stream = self.build_grouped_stream(&route)?;
        let folded = Self::execute_group_fold(&route, stream)?;
        let execution_time_micros =
            u64::try_from(execution_started_at.elapsed().as_micros()).unwrap_or(u64::MAX);

        Ok(Self::finalize_grouped_output(
            route,
            folded,
            execution_time_micros,
        ))
    }
}
