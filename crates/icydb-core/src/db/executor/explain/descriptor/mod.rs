//! Module: db::executor::explain::descriptor
//! Responsibility: canonical assembly for executor EXPLAIN descriptor payloads.
//! Does not own: route-capability derivation or explain rendering output.
//! Boundary: project immutable execution contracts into stable descriptor fields.

mod aggregate;
mod load;
pub(in crate::db::executor::explain::descriptor) mod shared;

#[cfg(test)]
pub(in crate::db) use self::load::assemble_load_execution_node_descriptor;
pub(in crate::db::executor) use self::load::assemble_load_execution_verbose_diagnostics_from_route_facts;
pub(in crate::db) use self::{
    aggregate::{
        assemble_aggregate_terminal_execution_descriptor,
        assemble_scalar_aggregate_execution_descriptor_with_projection,
    },
    load::{
        LoadExecutionRouteFacts, assemble_load_execution_node_descriptor_for_authority,
        assemble_load_execution_node_descriptor_from_route_facts,
        freeze_load_execution_route_facts, freeze_load_execution_route_facts_for_authority,
    },
};
