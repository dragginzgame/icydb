//! Module: db::sql
//! Responsibility: SQL frontend parsing contracts for reduced SQL entrypoints.
//! Does not own: schema validation, access planning, or executor behavior.
//! Boundary: parses SQL text into deterministic statement AST used by planner-facing layers.

pub(in crate::db) mod ddl;
pub(crate) mod identifier;
pub(crate) mod lowering;
pub(crate) mod parser;
