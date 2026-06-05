//! Module: CLI integration-style unit tests.
//! Responsibility: group domain-owned command parsing, rendering, and helper tests.
//! Does not own: production CLI behavior or reusable test fixtures.
//! Boundary: test-only access to crate-private CLI seams.

mod cli;
mod config;
mod icp;
mod observability;
mod shell;
