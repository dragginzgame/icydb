//! Module: executor::load
//! Responsibility: load-path execution orchestration, pagination, and trace contracts.
//! Does not own: logical planning semantics or relation/commit mutation policy.
//! Boundary: consumes executable load plans and delegates post-access semantics to kernel.
#![deny(unreachable_patterns)]

mod contracts;
mod entrypoints;
mod execute;
mod fast_stream;
mod grouped_distinct;
mod grouped_fold;
mod grouped_having;
mod grouped_output;
mod grouped_route;
mod grouped_runtime;
mod index_range_limit;
mod page;
mod pk_stream;
mod projection;
mod secondary_index;
mod terminal;

use crate::error::InternalError;

pub(crate) use self::contracts::{CursorPage, LoadExecutor};
pub(in crate::db::executor) use self::contracts::{
    FastPathKeyResult, key_stream_comparator_from_direction,
};
pub(in crate::db) use self::contracts::{GroupedCursorPage, PageCursor};
pub(in crate::db::executor::load) use self::contracts::{
    GroupedFoldStage, GroupedPlannerPayload, GroupedRoutePayload, GroupedRouteStage,
    GroupedRouteStageProjection, GroupedStreamStage, IndexSpecBundle,
};
#[cfg(test)]
pub(in crate::db::executor) use self::entrypoints::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};
pub(in crate::db::executor::load) use self::execute::ExecutionOutcomeMetrics;
pub(in crate::db::executor) use self::execute::{
    ExecutionInputs, ExecutionInputsProjection, MaterializedExecutionAttempt,
    ResolvedExecutionKeyStream,
};
pub(in crate::db::executor::load) use self::grouped_runtime::{
    GroupedContinuationCapabilities, GroupedContinuationContext, GroupedExecutionContext,
    GroupedPaginationWindow, GroupedRuntimeProjection,
};
pub(in crate::db::executor) use self::page::PageMaterializationRequest;

pub(in crate::db::executor::load) fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
