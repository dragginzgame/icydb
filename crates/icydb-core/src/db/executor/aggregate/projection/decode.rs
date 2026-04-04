//! Module: db::executor::aggregate::projection::decode
//! Responsibility: aggregate projection access to shared covering-index decode helpers.
//! Does not own: covering payload format or aggregate projection orchestration.
//! Boundary: aggregate projection imports this local wrapper instead of reaching through executor internals directly.

use crate::{
    db::executor::decode_covering_projection_component as shared_decode_covering_projection_component,
    error::InternalError, value::Value,
};

// Delegate aggregate covering decode through the shared executor-owned
// covering payload helper so aggregate and terminal lanes cannot drift on
// supported index component kinds.
pub(super) fn decode_covering_projection_component(
    component: &[u8],
) -> Result<Option<Value>, InternalError> {
    shared_decode_covering_projection_component(component)
}
