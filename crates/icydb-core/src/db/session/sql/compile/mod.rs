//! Module: db::session::sql::compile
//! Responsibility: SQL compile-stage artifacts and semantic statement
//! compilation.
//! Does not own: SQL parsing, cache lookup, or execution.
//! Boundary: compiles parsed SQL statements into session-owned command artifacts.

mod artifacts;
mod core;

pub(in crate::db::session::sql::compile) use artifacts::SqlQueryShape;
pub(in crate::db::session::sql) use artifacts::{
    SqlCompileArtifacts, SqlCompileAttributionBuilder, SqlCompilePhaseAttribution,
};
