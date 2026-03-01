//! Module: executor::stream
//! Responsibility: ordered key-stream primitives and physical access-stream boundaries.
//! Does not own: planning semantics or row materialization policy.
//! Boundary: shared key-stream infrastructure consumed by executor load routes.

pub(super) mod access;
pub(super) mod key;
