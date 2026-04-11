//! Module: db::executor::planning
//! Responsibility: pre-runtime execution-planning boundaries shared by executor consumers.
//! Does not own: kernel execution, row materialization, or terminal payload emission.
//! Boundary: canonical planning root for continuation, route policy, and executor preparation.

pub(in crate::db) mod continuation;
pub(in crate::db) mod preparation;
pub(in crate::db) mod route;
