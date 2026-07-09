//! Module: db::session::sql::compiled
//! Responsibility: session-owned compiled SQL command artifact facade.
//! Does not own: SQL parsing/lowering or execution dispatch.
//! Boundary: keeps compiled command, cache-entry, and execution-context owners separate.

mod cache;
mod command;
mod context;

pub(in crate::db) use cache::{
    SqlCompiledSchemaFingerprint, SqlGlobalAggregateCountPlanCacheEntry,
};
pub(in crate::db) use command::{CompiledSqlCommand, CompiledSqlInsertCommand};
pub(in crate::db) use context::SqlCompiledCommandExecutionContext;
