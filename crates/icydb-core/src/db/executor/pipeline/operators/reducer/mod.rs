//! Module: executor::pipeline::operators::reducer
//! Responsibility: aggregate reducer runner wiring over shared key-stream utilities.
//! Does not own: access-path resolution or planning-time aggregate eligibility.
//! Boundary: reusable reducer operators for execution-kernel orchestration.

mod aggregate;
mod runner;
