//! Module: model
//! Responsibility: validated host-side config models consumed by code generation.
//! Does not own: TOML parsing, path discovery, or generated Rust emission.
//! Boundary: stores normalized canister feature switches and build-target policy.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub(crate) const DEFAULT_SQL_READONLY_ENABLED: bool = false;
pub(crate) const DEFAULT_SQL_DDL_ENABLED: bool = false;
pub(crate) const DEFAULT_SQL_FIXTURES_ENABLED: bool = false;
pub(crate) const DEFAULT_SQL_INTEGRITY_ENABLED: bool = false;
pub(crate) const DEFAULT_SQL_INTROSPECTION_LOCAL_ENABLED: bool = true;
pub(crate) const DEFAULT_SQL_INTROSPECTION_IC_ENABLED: bool = false;
pub(crate) const DEFAULT_SQL_UPDATE_POLICY: Option<GeneratedSqlUpdatePolicy> = None;
pub(crate) const DEFAULT_METRICS_LOCAL_MODE: GeneratedMetricsMode = GeneratedMetricsMode::Simple;
pub(crate) const DEFAULT_METRICS_IC_MODE: GeneratedMetricsMode = GeneratedMetricsMode::Simple;
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

    /// Return whether the administrative integrity endpoint should be generated.
    #[must_use]
    pub fn canister_sql_integrity_enabled(&self, canister_name: &str) -> bool {
        self.canister_enabled(
            canister_name,
            DEFAULT_SQL_INTEGRITY_ENABLED,
            GeneratedCanisterConfig::sql_integrity,
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
        self.canisters.get(canister_name).map_or_else(
            GeneratedSqlIntrospectionPolicy::default,
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
        self.canister_metrics_mode(canister_name).enabled()
    }

    /// Return whether metrics report endpoints should be generated for one target.
    #[must_use]
    pub fn canister_metrics_enabled_for_build_target(
        &self,
        canister_name: &str,
        build_target: GeneratedBuildTarget,
    ) -> bool {
        self.canister_metrics_mode_for_build_target(canister_name, build_target)
            .enabled()
    }

    /// Return whether extended metrics report endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_metrics_extended_enabled(&self, canister_name: &str) -> bool {
        self.canister_metrics_extended_enabled_for_build_target(canister_name, self.build_target)
    }

    /// Return whether extended metrics should be generated for one canister and target.
    #[must_use]
    pub fn canister_metrics_extended_enabled_for_build_target(
        &self,
        canister_name: &str,
        build_target: GeneratedBuildTarget,
    ) -> bool {
        self.canister_metrics_mode_for_build_target(canister_name, build_target)
            .extended()
    }

    /// Return the metrics mode selected for one canister and this build target.
    #[must_use]
    pub fn canister_metrics_mode(&self, canister_name: &str) -> GeneratedMetricsMode {
        self.canister_metrics_mode_for_build_target(canister_name, self.build_target)
    }

    /// Return the metrics mode selected for one canister and build target.
    #[must_use]
    pub fn canister_metrics_mode_for_build_target(
        &self,
        canister_name: &str,
        build_target: GeneratedBuildTarget,
    ) -> GeneratedMetricsMode {
        self.canister_metrics_policy(canister_name)
            .mode_for(build_target)
    }

    /// Return the local/IC metrics policy for one canister.
    #[must_use]
    pub fn canister_metrics_policy(&self, canister_name: &str) -> GeneratedMetricsPolicy {
        self.canisters.get(canister_name).map_or_else(
            GeneratedMetricsPolicy::default,
            GeneratedCanisterConfig::metrics_policy,
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

    pub(crate) const fn with_build_target(mut self, build_target: GeneratedBuildTarget) -> Self {
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
        self.sql.surfaces.readonly_enabled()
    }

    /// Return whether generated actor glue should export the SQL DDL endpoint.
    #[must_use]
    pub const fn sql_ddl(&self) -> bool {
        self.sql.surfaces.ddl_enabled()
    }

    /// Return whether generated actor glue should export SQL fixture lifecycle endpoints.
    #[must_use]
    pub const fn sql_fixtures(&self) -> bool {
        self.sql.surfaces.fixtures_enabled()
    }

    /// Return whether generated actor glue should export the integrity endpoint.
    #[must_use]
    pub const fn sql_integrity(&self) -> bool {
        self.sql.surfaces.integrity_enabled()
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
        self.metrics.policy().any_enabled()
    }

    /// Return whether generated actor glue should export extended metrics report endpoints.
    #[must_use]
    pub const fn metrics_extended(&self) -> bool {
        self.metrics.policy().any_extended()
    }

    /// Return the local/IC policy for generated metrics endpoints.
    #[must_use]
    pub const fn metrics_policy(&self) -> GeneratedMetricsPolicy {
        self.metrics.policy()
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
    surfaces: GeneratedCanisterSqlSurfaceFlags,
    introspection_policy: GeneratedSqlIntrospectionPolicy,
    update_policy: Option<GeneratedSqlUpdatePolicy>,
}

impl GeneratedCanisterSqlConfig {
    pub(crate) const fn new(
        introspection_policy: GeneratedSqlIntrospectionPolicy,
        update_policy: Option<GeneratedSqlUpdatePolicy>,
    ) -> Self {
        Self {
            surfaces: GeneratedCanisterSqlSurfaceFlags::empty(),
            introspection_policy,
            update_policy,
        }
    }

    pub(crate) const fn with_readonly_enabled(mut self, enabled: bool) -> Self {
        self.surfaces = self.surfaces.with_readonly_enabled(enabled);
        self
    }

    pub(crate) const fn with_ddl_enabled(mut self, enabled: bool) -> Self {
        self.surfaces = self.surfaces.with_ddl_enabled(enabled);
        self
    }

    pub(crate) const fn with_fixtures_enabled(mut self, enabled: bool) -> Self {
        self.surfaces = self.surfaces.with_fixtures_enabled(enabled);
        self
    }

    pub(crate) const fn with_integrity_enabled(mut self, enabled: bool) -> Self {
        self.surfaces = self.surfaces.with_integrity_enabled(enabled);
        self
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GeneratedCanisterSqlSurfaceFlags(u8);

impl GeneratedCanisterSqlSurfaceFlags {
    const DDL: u8 = 1 << 1;
    const FIXTURES: u8 = 1 << 2;
    const INTEGRITY: u8 = 1 << 3;
    const READONLY: u8 = 1;

    const fn empty() -> Self {
        Self(0)
    }

    const fn readonly_enabled(self) -> bool {
        self.contains(Self::READONLY)
    }

    const fn ddl_enabled(self) -> bool {
        self.contains(Self::DDL)
    }

    const fn fixtures_enabled(self) -> bool {
        self.contains(Self::FIXTURES)
    }

    const fn integrity_enabled(self) -> bool {
        self.contains(Self::INTEGRITY)
    }

    const fn with_readonly_enabled(self, enabled: bool) -> Self {
        self.with_flag(Self::READONLY, enabled)
    }

    const fn with_ddl_enabled(self, enabled: bool) -> Self {
        self.with_flag(Self::DDL, enabled)
    }

    const fn with_fixtures_enabled(self, enabled: bool) -> Self {
        self.with_flag(Self::FIXTURES, enabled)
    }

    const fn with_integrity_enabled(self, enabled: bool) -> Self {
        self.with_flag(Self::INTEGRITY, enabled)
    }

    const fn contains(self, flag: u8) -> bool {
        self.0 & flag == flag
    }

    const fn with_flag(self, flag: u8, enabled: bool) -> Self {
        if enabled {
            Self(self.0 | flag)
        } else {
            Self(self.0 & !flag)
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
    /// Expose only public-safe primary-key `UPDATE` through `icydb_update`.
    PublicPrimaryKeyOnly,
    /// Expose only public-safe bounded deterministic `UPDATE` through `icydb_update`.
    PublicBoundedDeterministic,
}

/// Metrics endpoint mode selected by `icydb.toml`.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratedMetricsMode {
    /// Do not expose generated metrics endpoints.
    Off,
    /// Expose compact metrics endpoints.
    Simple,
    /// Expose compact and extended metrics endpoints.
    Extended,
}

impl GeneratedMetricsMode {
    /// Return whether compact metrics endpoints should be generated.
    #[must_use]
    pub const fn enabled(self) -> bool {
        match self {
            Self::Off => false,
            Self::Simple | Self::Extended => true,
        }
    }

    /// Return whether the extended metrics endpoint should be generated.
    #[must_use]
    pub const fn extended(self) -> bool {
        match self {
            Self::Off | Self::Simple => false,
            Self::Extended => true,
        }
    }
}

/// Local/IC policy for generated metrics endpoints.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GeneratedMetricsPolicy {
    local: GeneratedMetricsMode,
    ic: GeneratedMetricsMode,
}

impl GeneratedMetricsPolicy {
    /// Build one metrics policy from explicit target modes.
    #[must_use]
    pub const fn new(local: GeneratedMetricsMode, ic: GeneratedMetricsMode) -> Self {
        Self { local, ic }
    }

    /// Return the local build metrics mode.
    #[must_use]
    pub const fn local(self) -> GeneratedMetricsMode {
        self.local
    }

    /// Return the IC build metrics mode.
    #[must_use]
    pub const fn ic(self) -> GeneratedMetricsMode {
        self.ic
    }

    /// Return whether any build target exposes compact metrics.
    #[must_use]
    pub const fn any_enabled(self) -> bool {
        self.local.enabled() || self.ic.enabled()
    }

    /// Return whether any build target exposes extended metrics.
    #[must_use]
    pub const fn any_extended(self) -> bool {
        self.local.extended() || self.ic.extended()
    }

    /// Return the metrics mode selected for one build target.
    #[must_use]
    pub const fn mode_for(self, build_target: GeneratedBuildTarget) -> GeneratedMetricsMode {
        match build_target {
            GeneratedBuildTarget::Local => self.local,
            GeneratedBuildTarget::Ic | GeneratedBuildTarget::Unknown => self.ic,
        }
    }
}

impl Default for GeneratedMetricsPolicy {
    fn default() -> Self {
        Self::new(DEFAULT_METRICS_LOCAL_MODE, DEFAULT_METRICS_IC_MODE)
    }
}

/// Validated generated metrics endpoint switches for one canister.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct GeneratedCanisterMetricsConfig {
    policy: GeneratedMetricsPolicy,
}

impl GeneratedCanisterMetricsConfig {
    pub(crate) const fn new(policy: GeneratedMetricsPolicy) -> Self {
        Self { policy }
    }

    const fn policy(self) -> GeneratedMetricsPolicy {
        self.policy
    }
}
