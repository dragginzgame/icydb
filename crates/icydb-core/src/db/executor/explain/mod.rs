//! Module: db::executor::explain
//! Responsibility: assemble executor-owned EXPLAIN descriptor payloads.
//! Does not own: explain rendering formats or logical plan projection.
//! Boundary: centralized execution-plan-to-descriptor mapping used by EXPLAIN surfaces.

mod descriptor;

pub(in crate::db) use descriptor::{
    assemble_aggregate_terminal_execution_descriptor_with_model,
    assemble_load_execution_node_descriptor_with_model,
    assemble_load_execution_node_descriptor_with_model_and_visible_indexes,
    assemble_load_execution_verbose_diagnostics_with_model,
    assemble_load_execution_verbose_diagnostics_with_model_and_visible_indexes,
};
