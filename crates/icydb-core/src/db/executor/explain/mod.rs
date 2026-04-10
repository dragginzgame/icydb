//! Module: db::executor::explain
//! Responsibility: assemble executor-owned EXPLAIN descriptor payloads.
//! Does not own: explain rendering formats or logical plan projection.
//! Boundary: centralized execution-plan-to-descriptor mapping used by EXPLAIN surfaces.

mod descriptor;

pub(in crate::db) use descriptor::{
    assemble_aggregate_terminal_execution_descriptor, assemble_load_execution_node_descriptor,
    assemble_load_execution_node_descriptor_with_visible_indexes,
    assemble_load_execution_verbose_diagnostics,
    assemble_load_execution_verbose_diagnostics_with_visible_indexes,
    assemble_prepared_sql_scalar_aggregate_execution_descriptor,
};
