//! Module: sql_correctness_support
//! Responsibility: owner-local support modules for the SQL correctness integration target.
//! Does not own: shared harness contracts or production SQL semantics.
//! Boundary: connects repository coverage evidence to focused correctness checks.

mod coverage_manifest;
mod typed_core;
