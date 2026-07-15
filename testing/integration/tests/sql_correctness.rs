//! Module: sql_correctness
//! Responsibility: focused repository-consistency checks for current SQL evidence authority.
//! Does not own: production SQL behavior or performance-matrix execution.
//! Boundary: validates manifest coverage and the shared typed harness contract.

mod sql_correctness_support;
mod sql_harness;
