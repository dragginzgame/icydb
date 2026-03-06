//! Module: db::executor::explain
//! Responsibility: assemble executor-owned EXPLAIN descriptor payloads.
//! Does not own: explain rendering formats or logical plan projection.
//! Boundary: centralized execution-plan-to-descriptor mapping used by EXPLAIN surfaces.

mod descriptor;

pub(in crate::db::executor) use descriptor::{
    assemble_aggregate_terminal_execution_descriptor, assemble_load_execution_node_descriptor,
};
