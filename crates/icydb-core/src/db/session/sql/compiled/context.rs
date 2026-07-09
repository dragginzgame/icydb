//! Accepted-schema execution context for compiled SQL commands.
//! Does not own: compiled command variant definitions or SQL execution dispatch.

use super::{CompiledSqlCommand, SqlCompiledSchemaFingerprint};
use crate::db::{
    executor::EntityAuthority,
    schema::{AcceptedSchemaSnapshot, SchemaVersion},
    session::AcceptedSchemaCatalogContext,
};

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
    catalog: AcceptedSchemaCatalogContext,
    accepted_authority: Option<EntityAuthority>,
}

impl SqlCompiledCommandExecutionContext {
    #[must_use]
    pub(in crate::db) fn new(
        command: CompiledSqlCommand,
        catalog: AcceptedSchemaCatalogContext,
        accepted_authority: Option<EntityAuthority>,
    ) -> Self {
        let context = Self {
            command,
            catalog,
            accepted_authority,
        };
        debug_assert_eq!(
            context.schema_version(),
            context.accepted_schema().persisted_snapshot().version()
        );

        context
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
        self.catalog.snapshot()
    }

    #[must_use]
    pub(in crate::db) const fn accepted_catalog(&self) -> &AcceptedSchemaCatalogContext {
        &self.catalog
    }

    #[must_use]
    pub(in crate::db) const fn schema_version(&self) -> SchemaVersion {
        self.catalog.schema_version()
    }

    #[must_use]
    pub(in crate::db) const fn compiled_schema_fingerprint(&self) -> SqlCompiledSchemaFingerprint {
        SqlCompiledSchemaFingerprint::from_catalog(&self.catalog)
    }

    #[must_use]
    pub(in crate::db) const fn accepted_authority(&self) -> Option<&EntityAuthority> {
        self.accepted_authority.as_ref()
    }
}
