//! Module: executor::load
//! Responsibility: load-path execution orchestration, pagination, and trace contracts.
//! Does not own: logical planning semantics or relation/commit mutation policy.
//! Boundary: consumes executable load plans and delegates post-access semantics to kernel.

mod context;
mod contracts;
mod entrypoints;
mod execute;
mod fast_stream;
mod fast_stream_route;
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

pub(in crate::db) use contracts::CursorPage;
pub(in crate::db) use contracts::LoadExecutor;
pub(in crate::db::executor) use contracts::{
    FastPathKeyResult, key_stream_comparator_from_direction,
};
pub(in crate::db) use contracts::{GroupedCursorPage, PageCursor};
pub(in crate::db::executor::load) use contracts::{
    GroupedFoldStage, GroupedPlannerPayload, GroupedRoutePayload, GroupedRouteStage,
    GroupedRouteStageProjection, GroupedStreamStage, IndexSpecBundle,
};
#[cfg(test)]
pub(in crate::db::executor) use entrypoints::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};
pub(in crate::db::executor::load) use execute::ExecutionOutcomeMetrics;
pub(in crate::db::executor) use execute::{
    ExecutionInputs, ExecutionInputsProjection, MaterializedExecutionAttempt,
    ResolvedExecutionKeyStream,
};
pub(in crate::db::executor::load) use fast_stream_route::{
    FastStreamRouteKind, FastStreamRouteRequest,
};
pub(in crate::db::executor::load) use grouped_runtime::{
    GroupedContinuationCapabilities, GroupedContinuationContext, GroupedExecutionContext,
    GroupedPaginationWindow, GroupedRuntimeProjection,
};
pub(in crate::db::executor) use page::PageMaterializationRequest;

pub(in crate::db::executor::load) use crate::db::error::executor_invariant as invariant;
