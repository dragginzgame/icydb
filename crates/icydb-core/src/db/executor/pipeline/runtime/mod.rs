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

pub(in crate::db::executor) use adapter::{
    ExecutionMaterializationContract, ExecutionRuntimeAdapter,
};
pub(in crate::db::executor) use attempt::ExecutionAttemptKernel;
pub(in crate::db::executor) use grouped::{
    GroupedFoldStage, GroupedStreamStage, RowView, StructuralGroupedRowRuntime,
    compile_grouped_row_slot_layout_from_inputs,
};
pub(in crate::db::executor) use retained_slots::compile_retained_slot_layout_for_mode;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use retained_slots::compile_retained_slot_layout_for_mode_with_extra_slots;
