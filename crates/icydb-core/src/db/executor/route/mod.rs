//! Module: db::executor::route
//! Responsibility: derive runtime route decisions from validated executor/query inputs.
//! Does not own: logical query semantics or stream/kernel execution internals.
//! Boundary: produces one immutable execution-route contract consumed by runtime dispatch.

mod capability;
mod contracts;
mod fast_path;
mod guard;
mod hints;
mod mode;
mod planner;

pub(in crate::db::executor::route) use capability::direction_allows_physical_fetch_hint;
pub(in crate::db::executor) use capability::supports_pk_stream_access_path;
pub(in crate::db::executor) use contracts::*;
pub(in crate::db::executor) use fast_path::try_first_verified_fast_path_hit;
pub(super) use guard::*;
