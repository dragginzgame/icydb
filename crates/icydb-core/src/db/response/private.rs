//! Module: db::response::private
//! Responsibility: module-local ownership and contracts for db::response::private.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

///
/// Sealed
///
/// Internal marker used to seal response row-shape marker implementations.
///

pub trait Sealed {}
