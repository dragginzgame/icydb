use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub(crate) const DEFAULT_SQL_READONLY_ENABLED: bool = false;
pub(crate) const DEFAULT_SQL_DDL_ENABLED: bool = false;
pub(crate) const DEFAULT_SQL_FIXTURES_ENABLED: bool = false;
pub(crate) const DEFAULT_SQL_INTROSPECTION_LOCAL_ENABLED: bool = true;
pub(crate) const DEFAULT_SQL_INTROSPECTION_IC_ENABLED: bool = false;
pub(crate) const DEFAULT_SQL_UPDATE_POLICY: Option<GeneratedSqlUpdatePolicy> = None;
pub(crate) const DEFAULT_METRICS_ENABLED: bool = false;
pub(crate) const DEFAULT_METRICS_EXTENDED_ENABLED: bool = false;
pub(crate) const DEFAULT_SNAPSHOT_ENABLED: bool = false;
pub(crate) const DEFAULT_SCHEMA_ENABLED: bool = false;

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
    build_target: GeneratedBuildTarget,
}

impl GeneratedIcydbConfig {
    /// Borrow validated per-canister config entries.
    #[must_use]
    pub const fn canisters(&self) -> &BTreeMap<String, GeneratedCanisterConfig> {
        &self.canisters
    }

    /// Return the build target used to resolve target-sensitive generated settings.
    #[must_use]
    pub const fn build_target(&self) -> GeneratedBuildTarget {
        self.build_target
    }

    /// Return whether read-only SQL should be generated for one canister.
    #[must_use]
    pub fn canister_sql_readonly_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(
            canister_name,
            DEFAULT_SQL_READONLY_ENABLED,
            GeneratedCanisterConfig::sql_readonly,
        )
    }

    /// Return whether the SQL DDL endpoint should be generated for one canister.
    #[must_use]
    pub fn canister_sql_ddl_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(
            canister_name,
            DEFAULT_SQL_DDL_ENABLED,
            GeneratedCanisterConfig::sql_ddl,
        )
    }

    /// Return whether SQL fixture lifecycle endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_sql_fixtures_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(
            canister_name,
            DEFAULT_SQL_FIXTURES_ENABLED,
            GeneratedCanisterConfig::sql_fixtures,
        )
    }

    /// Return whether generated read-only SQL should admit operational introspection.
    #[must_use]
    pub fn canister_sql_introspection_enabled(&self, canister_name: &str) -> bool {
        self.canister_sql_introspection_policy(canister_name)
            .enabled_for(self.build_target)
    }

    /// Return the local/IC introspection policy for one canister.
    #[must_use]
    pub fn canister_sql_introspection_policy(
        &self,
        canister_name: &str,
    ) -> GeneratedSqlIntrospectionPolicy {
        self.canisters.get(canister_name).map_or(
            GeneratedSqlIntrospectionPolicy::default(),
            GeneratedCanisterConfig::sql_introspection_policy,
        )
    }

    /// Return the configured generated SQL update endpoint policy, if any.
    #[must_use]
    pub fn canister_sql_update_policy(
        &self,
        canister_name: &str,
    ) -> Option<GeneratedSqlUpdatePolicy> {
        self.canisters.get(canister_name).map_or(
            DEFAULT_SQL_UPDATE_POLICY,
            GeneratedCanisterConfig::sql_update_policy,
        )
    }

    /// Return whether metrics report endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_metrics_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(
            canister_name,
            DEFAULT_METRICS_ENABLED,
            GeneratedCanisterConfig::metrics,
        )
    }

    /// Return whether extended metrics report endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_metrics_extended_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(
            canister_name,
            DEFAULT_METRICS_EXTENDED_ENABLED,
            GeneratedCanisterConfig::metrics_extended,
        )
    }

    /// Return whether storage snapshot endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_snapshot_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(
            canister_name,
            DEFAULT_SNAPSHOT_ENABLED,
            GeneratedCanisterConfig::snapshot,
        )
    }

    /// Return whether schema report endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_schema_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(
            canister_name,
            DEFAULT_SCHEMA_ENABLED,
            GeneratedCanisterConfig::schema,
        )
    }

    pub(crate) const fn new(canisters: BTreeMap<String, GeneratedCanisterConfig>) -> Self {
        Self {
            canisters,
            build_target: GeneratedBuildTarget::Unknown,
        }
    }

    pub(crate) fn with_build_target(mut self, build_target: GeneratedBuildTarget) -> Self {
        self.build_target = build_target;

        self
    }

    fn canister_enabled(
        &self,
        canister_name: &str,
        default: bool,
        is_enabled: impl FnOnce(&GeneratedCanisterConfig) -> bool,
    ) -> bool {
        self.canisters
            .get(canister_name)
            .map_or(default, is_enabled)
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

    /// Return whether generated actor glue should export the SQL DDL endpoint.
    #[must_use]
    pub const fn sql_ddl(&self) -> bool {
        self.sql.ddl
    }

    /// Return whether generated actor glue should export SQL fixture lifecycle endpoints.
    #[must_use]
    pub const fn sql_fixtures(&self) -> bool {
        self.sql.fixtures
    }

    /// Return the local/IC policy for operational SQL introspection.
    #[must_use]
    pub const fn sql_introspection_policy(&self) -> GeneratedSqlIntrospectionPolicy {
        self.sql.introspection_policy
    }

    /// Return the generated SQL update endpoint policy, if explicitly selected.
    #[must_use]
    pub const fn sql_update_policy(&self) -> Option<GeneratedSqlUpdatePolicy> {
        self.sql.update_policy
    }

    /// Return whether generated actor glue should export metrics report endpoints.
    #[must_use]
    pub const fn metrics(&self) -> bool {
        self.metrics.enabled
    }

    /// Return whether generated actor glue should export extended metrics report endpoints.
    #[must_use]
    pub const fn metrics_extended(&self) -> bool {
        self.metrics.extended
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
    introspection_policy: GeneratedSqlIntrospectionPolicy,
    update_policy: Option<GeneratedSqlUpdatePolicy>,
}

impl GeneratedCanisterSqlConfig {
    pub(crate) const fn new(
        readonly: bool,
        ddl: bool,
        fixtures: bool,
        introspection_policy: GeneratedSqlIntrospectionPolicy,
        update_policy: Option<GeneratedSqlUpdatePolicy>,
    ) -> Self {
        Self {
            readonly,
            ddl,
            fixtures,
            introspection_policy,
            update_policy,
        }
    }
}

/// Build target used to resolve target-sensitive generated canister settings.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GeneratedBuildTarget {
    /// Local ICP replica target.
    Local,
    /// IC network target.
    Ic,
    /// Unknown target, resolved with IC/fail-closed defaults.
    #[default]
    Unknown,
}

impl GeneratedBuildTarget {
    /// Parse the build target value passed through the build-script environment.
    #[must_use]
    pub fn from_env_value(value: &str) -> Self {
        match value {
            "local" => Self::Local,
            "ic" => Self::Ic,
            _ => Self::Unknown,
        }
    }

    /// Return the build-script environment value for this target.
    #[must_use]
    pub const fn env_value(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Ic => "ic",
            Self::Unknown => "unknown",
        }
    }
}

/// Local/IC policy for generated read-only SQL operational introspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GeneratedSqlIntrospectionPolicy {
    local: bool,
    ic: bool,
}

impl GeneratedSqlIntrospectionPolicy {
    /// Build one SQL introspection policy from explicit target booleans.
    #[must_use]
    pub const fn new(local: bool, ic: bool) -> Self {
        Self { local, ic }
    }

    /// Return whether local builds should admit SQL introspection.
    #[must_use]
    pub const fn local(self) -> bool {
        self.local
    }

    /// Return whether IC builds should admit SQL introspection.
    #[must_use]
    pub const fn ic(self) -> bool {
        self.ic
    }

    /// Return whether this policy admits SQL introspection for one build target.
    #[must_use]
    pub const fn enabled_for(self, build_target: GeneratedBuildTarget) -> bool {
        match build_target {
            GeneratedBuildTarget::Local => self.local,
            GeneratedBuildTarget::Ic => self.ic,
            GeneratedBuildTarget::Unknown => false,
        }
    }
}

impl Default for GeneratedSqlIntrospectionPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_SQL_INTROSPECTION_LOCAL_ENABLED,
            DEFAULT_SQL_INTROSPECTION_IC_ENABLED,
        )
    }
}

/// Generated SQL update endpoint policy selected by `icydb.toml`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratedSqlUpdatePolicy {
    /// Expose only public-safe primary-key `UPDATE` through `__icydb_update`.
    PublicPrimaryKeyOnly,
    /// Expose only public-safe bounded deterministic `UPDATE` through `__icydb_update`.
    PublicBoundedDeterministic,
}

/// Validated generated metrics endpoint switches for one canister.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct GeneratedCanisterMetricsConfig {
    enabled: bool,
    extended: bool,
}

impl GeneratedCanisterMetricsConfig {
    pub(crate) const fn new(enabled: bool, extended: bool) -> Self {
        Self { enabled, extended }
    }
}
