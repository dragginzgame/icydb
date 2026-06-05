//! Module: CLI config endpoint surface gates.
//! Responsibility: map generated endpoint methods to `icydb.toml` surface switches.
//! Does not own: config file discovery, report rendering, or endpoint execution.
//! Boundary: exposes endpoint constants and config-surface diagnostics to CLI owners.

use super::ResolvedConfig;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfigSurface {
    SqlReadonly,
    SqlDdl,
    SqlFixtures,
    Metrics,
    MetricsReset,
    Snapshot,
    Schema,
}

impl ConfigSurface {
    const fn label(self) -> &'static str {
        match self {
            Self::SqlReadonly => "readonly SQL",
            Self::SqlDdl => "SQL DDL",
            Self::SqlFixtures => "SQL fixtures",
            Self::Metrics => "metrics",
            Self::MetricsReset => "metrics reset",
            Self::Snapshot => "snapshot",
            Self::Schema => "schema",
        }
    }

    const fn key(self) -> &'static str {
        match self {
            Self::SqlReadonly => "canisters.<name>.sql.readonly",
            Self::SqlDdl => "canisters.<name>.sql.ddl",
            Self::SqlFixtures => "canisters.<name>.sql.fixtures",
            Self::Metrics => "canisters.<name>.metrics.enabled",
            Self::MetricsReset => "canisters.<name>.metrics.reset",
            Self::Snapshot => "canisters.<name>.snapshot.enabled",
            Self::Schema => "canisters.<name>.schema.enabled",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConfiguredEndpoint {
    method: &'static str,
    surface: ConfigSurface,
}

impl ConfiguredEndpoint {
    pub(crate) const fn method(self) -> &'static str {
        self.method
    }

    pub(super) const fn surface(self) -> ConfigSurface {
        self.surface
    }
}

pub(crate) const SQL_QUERY_ENDPOINT: ConfiguredEndpoint = ConfiguredEndpoint {
    method: "__icydb_query",
    surface: ConfigSurface::SqlReadonly,
};
pub(crate) const SQL_DDL_ENDPOINT: ConfiguredEndpoint = ConfiguredEndpoint {
    method: "__icydb_ddl",
    surface: ConfigSurface::SqlDdl,
};
pub(crate) const FIXTURES_LOAD_ENDPOINT: ConfiguredEndpoint = ConfiguredEndpoint {
    method: "__icydb_fixtures_load",
    surface: ConfigSurface::SqlFixtures,
};
pub(crate) const SNAPSHOT_ENDPOINT: ConfiguredEndpoint = ConfiguredEndpoint {
    method: "__icydb_snapshot",
    surface: ConfigSurface::Snapshot,
};
pub(crate) const METRICS_ENDPOINT: ConfiguredEndpoint = ConfiguredEndpoint {
    method: "__icydb_metrics",
    surface: ConfigSurface::Metrics,
};
pub(crate) const METRICS_RESET_ENDPOINT: ConfiguredEndpoint = ConfiguredEndpoint {
    method: "__icydb_metrics_reset",
    surface: ConfigSurface::MetricsReset,
};
pub(crate) const SCHEMA_ENDPOINT: ConfiguredEndpoint = ConfiguredEndpoint {
    method: "__icydb_schema",
    surface: ConfigSurface::Schema,
};
pub(crate) const SCHEMA_CHECK_ENDPOINT: ConfiguredEndpoint = ConfiguredEndpoint {
    method: "__icydb_schema_check",
    surface: ConfigSurface::Schema,
};

pub(super) fn disabled_config_surface_message(
    resolved: &ResolvedConfig,
    canister: &str,
    surface: ConfigSurface,
) -> String {
    let config_location = resolved.config_path().map_or_else(
        || "not found".to_string(),
        |path| path.display().to_string(),
    );

    format!(
        "IcyDB config does not enable {} for canister '{canister}' (config file: {config_location}). Enable `{}` in icydb.toml, then rebuild and deploy the canister.",
        surface.label(),
        surface.key(),
    )
}

pub(super) fn config_surface_enabled_for_resolved(
    resolved: &ResolvedConfig,
    canister: &str,
    surface: ConfigSurface,
) -> bool {
    let config = resolved.config();
    match surface {
        ConfigSurface::SqlReadonly => config.canister_sql_readonly_enabled(canister),
        ConfigSurface::SqlDdl => config.canister_sql_ddl_enabled(canister),
        ConfigSurface::SqlFixtures => config.canister_sql_fixtures_enabled(canister),
        ConfigSurface::Metrics => config.canister_metrics_enabled(canister),
        ConfigSurface::MetricsReset => config.canister_metrics_reset_enabled(canister),
        ConfigSurface::Snapshot => config.canister_snapshot_enabled(canister),
        ConfigSurface::Schema => config.canister_schema_enabled(canister),
    }
}

pub(super) fn configured_endpoint_enabled_for_resolved(
    resolved: &ResolvedConfig,
    canister: &str,
    endpoint: ConfiguredEndpoint,
) -> bool {
    config_surface_enabled_for_resolved(resolved, canister, endpoint.surface())
}
