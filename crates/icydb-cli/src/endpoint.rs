//! Fixed deployed IcyDB method names used by CLI calls.

/// One maintained IcyDB method invoked directly against the deployed actor.
#[derive(Clone, Copy)]
pub(crate) struct Endpoint(&'static str);

impl Endpoint {
    pub(crate) const fn method(self) -> &'static str {
        self.0
    }
}

pub(crate) const SQL_QUERY_ENDPOINT: Endpoint = Endpoint("icydb_query");
pub(crate) const SQL_DDL_ENDPOINT: Endpoint = Endpoint("icydb_ddl");
pub(crate) const SQL_UPDATE_ENDPOINT: Endpoint = Endpoint("icydb_update");
pub(crate) const FIXTURES_LOAD_ENDPOINT: Endpoint = Endpoint("icydb_fixtures_load");
pub(crate) const SNAPSHOT_ENDPOINT: Endpoint = Endpoint("icydb_snapshot");
pub(crate) const METRICS_ENDPOINT: Endpoint = Endpoint("icydb_metrics");
pub(crate) const METRICS_EXTENDED_ENDPOINT: Endpoint = Endpoint("icydb_metrics_extended");
pub(crate) const METRICS_RESET_ENDPOINT: Endpoint = Endpoint("icydb_metrics_reset");
pub(crate) const SCHEMA_ENDPOINT: Endpoint = Endpoint("icydb_schema");
pub(crate) const SCHEMA_MIGRATE_ENDPOINT: Endpoint = Endpoint("icydb_schema_migrate");
pub(crate) const SCHEMA_MIGRATION_ENDPOINT: Endpoint = Endpoint("icydb_schema_migration");
