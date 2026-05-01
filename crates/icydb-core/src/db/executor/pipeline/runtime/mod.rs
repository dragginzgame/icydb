//! Module: executor::pipeline::runtime
//! Responsibility: key-stream resolution and fast-path/fallback execution dispatch.
//! Does not own: cursor decoding policy or logical-plan construction.
//! Boundary: execution-attempt internals used by pipeline/load orchestration.

mod adapter;
mod attempt;
mod fast_path;
mod grouped;
mod retained_slots;
#[cfg(test)]
mod tests;

use crate::{
    db::executor::{
        ExecutionTrace, aggregate::runtime::finalize_path_outcome_for_path,
        pipeline::contracts::ExecutionOutcomeMetrics, pipeline::contracts::StructuralCursorPage,
    },
    metrics::sink::{ExecKind, PathSpan},
};

pub(in crate::db::executor) use adapter::{
    ExecutionMaterializationContract, ExecutionRuntimeAdapter,
};
pub(in crate::db::executor) use attempt::ExecutionAttemptKernel;
pub(in crate::db::executor) use grouped::{
    GroupedFoldStage, GroupedStreamStage, RowView, StructuralGroupedRowRuntime,
    compile_grouped_row_slot_layout_from_parts,
};
pub(in crate::db::executor) use retained_slots::{
    compile_retained_slot_layout_for_mode, compile_retained_slot_layout_for_mode_with_extra_slots,
};

/// Finalize one structural scalar page before typed or structural surface projection.
pub(in crate::db::executor) fn finalize_structural_page_for_path(
    entity_path: &'static str,
    page: StructuralCursorPage,
    metrics: ExecutionOutcomeMetrics,
    execution_trace: &mut Option<ExecutionTrace>,
    execution_time_micros: u64,
) -> StructuralCursorPage {
    let rows_emitted = page.row_count();

    finalize_path_outcome_for_path(
        entity_path,
        execution_trace,
        metrics,
        rows_emitted,
        false,
        execution_time_micros,
    );
    let mut span = PathSpan::new(ExecKind::Load, entity_path);
    span.set_rows(u64::try_from(page.row_count()).unwrap_or(u64::MAX));

    page
}
