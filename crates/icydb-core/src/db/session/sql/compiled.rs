//! Module: db::session::sql::compiled
//! Responsibility: session-owned compiled SQL command artifacts.
//! Does not own: SQL parsing/lowering or execution dispatch.
//! Boundary: carries generic-free compiled SQL state between session compile and execute phases.

use crate::db::{
    commit::CommitSchemaFingerprint,
    executor::EntityAuthority,
    query::intent::StructuralQuery,
    schema::AcceptedSchemaSnapshot,
    sql::{
        lowering::{LoweredSqlCommand, StructuralSqlGlobalAggregateCommand},
        parser::{SqlInsertStatement, SqlReturningProjection, SqlUpdateStatement},
    },
};
use std::sync::Arc;

///
/// CompiledSqlCommand
///
/// CompiledSqlCommand is the generic-free SQL compile artifact stored in the
/// session SQL cache and later dispatched by the SQL execution boundary.
/// It deliberately carries syntax-surface commands, not executor scratch state.
///

#[derive(Clone, Debug)]
pub(in crate::db) enum CompiledSqlCommand {
    Select {
        query: Arc<StructuralQuery>,
    },
    Delete {
        query: Arc<StructuralQuery>,
        returning: Option<SqlReturningProjection>,
    },
    GlobalAggregate {
        command: Box<StructuralSqlGlobalAggregateCommand>,
    },
    Explain(Box<LoweredSqlCommand>),
    Insert(SqlInsertStatement),
    Update(SqlUpdateStatement),
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities {
        verbose: bool,
    },
    ShowStores {
        verbose: bool,
    },
    ShowMemory,
}

///
/// SqlCompiledCommandExecutionContext
///
/// SqlCompiledCommandExecutionContext carries the accepted schema facts loaded
/// while compiling one SQL command through to immediate execution. Query calls
/// cannot rely on heap cache writes persisting, so the cold path must avoid
/// reloading the same accepted schema between compile and plan lookup.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SqlCompiledCommandExecutionContext {
    command: CompiledSqlCommand,
    accepted_schema: AcceptedSchemaSnapshot,
    schema_fingerprint: CommitSchemaFingerprint,
    accepted_authority: Option<EntityAuthority>,
}

impl SqlCompiledCommandExecutionContext {
    #[must_use]
    pub(in crate::db) const fn new(
        command: CompiledSqlCommand,
        accepted_schema: AcceptedSchemaSnapshot,
        schema_fingerprint: CommitSchemaFingerprint,
        accepted_authority: Option<EntityAuthority>,
    ) -> Self {
        Self {
            command,
            accepted_schema,
            schema_fingerprint,
            accepted_authority,
        }
    }

    #[must_use]
    pub(in crate::db) const fn command(&self) -> &CompiledSqlCommand {
        &self.command
    }

    #[must_use]
    pub(in crate::db) fn into_command(self) -> CompiledSqlCommand {
        self.command
    }

    #[must_use]
    pub(in crate::db) const fn accepted_schema(&self) -> &AcceptedSchemaSnapshot {
        &self.accepted_schema
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.schema_fingerprint
    }

    #[must_use]
    pub(in crate::db) const fn accepted_authority(&self) -> Option<&EntityAuthority> {
        self.accepted_authority.as_ref()
    }
}

///
/// SqlProjectionContract
///
/// SqlProjectionContract is the outward SQL projection contract derived from
/// one shared lower prepared plan. SQL execution keeps this wrapper so
/// statement shaping stays owner-local while prepared-plan reuse lives below it.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SqlProjectionContract {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
}

impl SqlProjectionContract {
    #[must_use]
    pub(in crate::db) const fn new(columns: Vec<String>, fixed_scales: Vec<Option<u32>>) -> Self {
        Self {
            columns,
            fixed_scales,
        }
    }

    #[must_use]
    pub(in crate::db) fn into_components(self) -> (Vec<String>, Vec<Option<u32>>) {
        (self.columns, self.fixed_scales)
    }
}
