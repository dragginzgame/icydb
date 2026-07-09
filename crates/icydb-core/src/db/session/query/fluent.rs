//! Module: db::session::query::fluent
//! Responsibility: fluent terminal adapters at the session/executor boundary.
//! Does not own: cursor handling, grouped execution, explain output, or attribution.
//! Boundary: maps fluent prepared strategies into executor requests and maps executor outputs back into fluent DTOs.

mod materialized;
mod projection;
mod scalar;
