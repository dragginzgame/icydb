use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// Resolved IcyDB config and the path it came from, if a manifest exists.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolvedIcydbConfig {
    config_path: Option<PathBuf>,
    config: GeneratedIcydbConfig,
}

impl ResolvedIcydbConfig {
    /// Return the resolved config path, or `None` when no config file exists.
    #[must_use]
    pub fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref()
    }

    /// Borrow the validated generated config model.
    #[must_use]
    pub const fn config(&self) -> &GeneratedIcydbConfig {
        &self.config
    }

    pub(crate) const fn new(config_path: Option<PathBuf>, config: GeneratedIcydbConfig) -> Self {
        Self {
            config_path,
            config,
        }
    }
}

/// Validated IcyDB project config ready for build-script consumption.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GeneratedIcydbConfig {
    canisters: BTreeMap<String, GeneratedCanisterConfig>,
}

impl GeneratedIcydbConfig {
    /// Borrow validated per-canister config entries.
    #[must_use]
    pub const fn canisters(&self) -> &BTreeMap<String, GeneratedCanisterConfig> {
        &self.canisters
    }

    /// Return whether read-only SQL should be generated for one canister.
    #[must_use]
    pub fn canister_sql_readonly_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(canister_name, GeneratedCanisterConfig::sql_readonly)
    }

    /// Return whether SQL DDL/write endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_sql_ddl_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(canister_name, GeneratedCanisterConfig::sql_ddl)
    }

    /// Return whether SQL fixture lifecycle endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_sql_fixtures_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(canister_name, GeneratedCanisterConfig::sql_fixtures)
    }

    /// Return whether metrics report endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_metrics_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(canister_name, GeneratedCanisterConfig::metrics)
    }

    /// Return whether metrics reset endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_metrics_reset_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(canister_name, GeneratedCanisterConfig::metrics_reset)
    }

    /// Return whether storage snapshot endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_snapshot_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(canister_name, GeneratedCanisterConfig::snapshot)
    }

    /// Return whether schema report endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_schema_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(canister_name, GeneratedCanisterConfig::schema)
    }

    pub(crate) const fn new(canisters: BTreeMap<String, GeneratedCanisterConfig>) -> Self {
        Self { canisters }
    }

    fn canister_enabled(
        &self,
        canister_name: &str,
        is_enabled: impl FnOnce(&GeneratedCanisterConfig) -> bool,
    ) -> bool {
        self.canisters.get(canister_name).is_some_and(is_enabled)
    }
}

/// Validated generated settings for one canister.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GeneratedCanisterConfig {
    sql: GeneratedCanisterSqlConfig,
    metrics: GeneratedCanisterMetricsConfig,
    snapshot: bool,
    schema: bool,
}

impl GeneratedCanisterConfig {
    pub(crate) const fn new(
        sql: GeneratedCanisterSqlConfig,
        metrics: GeneratedCanisterMetricsConfig,
        snapshot: bool,
        schema: bool,
    ) -> Self {
        Self {
            sql,
            metrics,
            snapshot,
            schema,
        }
    }

    /// Return whether generated actor glue should export read-only SQL endpoints.
    #[must_use]
    pub const fn sql_readonly(&self) -> bool {
        self.sql.readonly
    }

    /// Return whether generated actor glue should export SQL DDL/write endpoints.
    #[must_use]
    pub const fn sql_ddl(&self) -> bool {
        self.sql.ddl
    }

    /// Return whether generated actor glue should export SQL fixture lifecycle endpoints.
    #[must_use]
    pub const fn sql_fixtures(&self) -> bool {
        self.sql.fixtures
    }

    /// Return whether generated actor glue should export metrics report endpoints.
    #[must_use]
    pub const fn metrics(&self) -> bool {
        self.metrics.enabled
    }

    /// Return whether generated actor glue should export metrics reset endpoints.
    #[must_use]
    pub const fn metrics_reset(&self) -> bool {
        self.metrics.reset
    }

    /// Return whether generated actor glue should export storage snapshot endpoints.
    #[must_use]
    pub const fn snapshot(&self) -> bool {
        self.snapshot
    }

    /// Return whether generated actor glue should export schema report endpoints.
    #[must_use]
    pub const fn schema(&self) -> bool {
        self.schema
    }
}

/// Validated generated SQL endpoint switches for one canister.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct GeneratedCanisterSqlConfig {
    readonly: bool,
    ddl: bool,
    fixtures: bool,
}

impl GeneratedCanisterSqlConfig {
    pub(crate) const fn new(readonly: bool, ddl: bool, fixtures: bool) -> Self {
        Self {
            readonly,
            ddl,
            fixtures,
        }
    }
}

/// Validated generated metrics endpoint switches for one canister.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct GeneratedCanisterMetricsConfig {
    enabled: bool,
    reset: bool,
}

impl GeneratedCanisterMetricsConfig {
    pub(crate) const fn new(enabled: bool, reset: bool) -> Self {
        Self { enabled, reset }
    }
}
