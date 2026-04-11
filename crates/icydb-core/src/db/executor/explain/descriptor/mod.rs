//! Module: db::executor::explain::descriptor
//! Responsibility: canonical assembly for executor EXPLAIN descriptor payloads.
//! Does not own: route-capability derivation or explain rendering output.
//! Boundary: project immutable execution contracts into stable descriptor fields.

mod aggregate;
mod load;
pub(in crate::db::executor::explain::descriptor) mod shared;

pub(in crate::db) use self::{
    aggregate::{
        assemble_aggregate_terminal_execution_descriptor,
        assemble_scalar_aggregate_execution_descriptor_with_projection,
    },
    load::{assemble_load_execution_node_descriptor, assemble_load_execution_verbose_diagnostics},
};
