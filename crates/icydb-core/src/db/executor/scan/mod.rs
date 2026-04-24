//! Module: db::executor::scan
//! Responsibility: fast-path scan execution helpers and route dispatch for PK/secondary/range streams.
//! Does not own: route-planner eligibility derivation or post-access materialization policy.
//! Boundary: executes pre-validated scan routes and returns ordered key streams.

mod fast_stream;
mod fast_stream_route;
mod index_range_limit;
mod secondary_index;

pub(in crate::db::executor) use fast_stream_route::execute_fast_stream_route;
