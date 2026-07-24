//! Module: parse
//! Responsibility: parse and validate `icydb.toml` into generated config models.
//! Does not own: path discovery, build-script emission, or runtime config state.
//! Boundary: turns TOML source plus known canister names into validated host models.

use crate::{
    ConfigError, GeneratedCanisterConfig, GeneratedIcydbConfig, ResolvedIcydbConfig,
    model::{
        DEFAULT_METRICS_IC_MODE, DEFAULT_METRICS_LOCAL_MODE, DEFAULT_SCHEMA_ENABLED,
        DEFAULT_SNAPSHOT_ENABLED, DEFAULT_SQL_DDL_ENABLED, DEFAULT_SQL_FIXTURES_ENABLED,
        DEFAULT_SQL_INTEGRITY_ENABLED, DEFAULT_SQL_INTROSPECTION_IC_ENABLED,
        DEFAULT_SQL_INTROSPECTION_LOCAL_ENABLED, DEFAULT_SQL_READONLY_ENABLED,
        DEFAULT_SQL_UPDATE_POLICY, GeneratedCanisterMetricsConfig, GeneratedCanisterSqlConfig,
        GeneratedMetricsMode, GeneratedMetricsPolicy, GeneratedSqlIntrospectionPolicy,
    },
    resolve::resolve_config_path,
};
use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs,
    path::Path,
};

use serde::Deserialize;

#[cfg(test)]
pub(crate) fn parse_icydb_toml(
    source: &str,
    known_canisters: &[&str],
) -> Result<GeneratedIcydbConfig, ConfigError> {
    parse_icydb_toml_at(source, None, known_canisters)
}

pub(crate) fn load_icydb_toml(
    path: impl AsRef<Path>,
    known_canisters: &[&str],
) -> Result<GeneratedIcydbConfig, ConfigError> {
    let path = path.as_ref();
    let source = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;

    parse_icydb_toml_at(source.as_str(), Some(path), known_canisters)
}

/// Resolve and validate config from a starting directory without writing
/// generated files. This is intended for host tools such as `icydb config`.
pub fn load_resolved_icydb_toml(
    start_dir: impl AsRef<Path>,
    known_canisters: &[&str],
) -> Result<ResolvedIcydbConfig, ConfigError> {
    let resolved = resolve_config_path(start_dir.as_ref());
    let Some(path) = resolved.into_config_path() else {
        return Ok(ResolvedIcydbConfig::default());
    };
    let config = load_icydb_toml(path.as_path(), known_canisters)?;

    Ok(ResolvedIcydbConfig::new(Some(path), config))
}

fn parse_icydb_toml_at(
    source: &str,
    path: Option<&Path>,
    known_canisters: &[&str],
) -> Result<GeneratedIcydbConfig, ConfigError> {
    let path = path.unwrap_or_else(|| Path::new("<inline>"));
    let raw: RawIcydbProjectConfig =
        toml::from_str(source).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;

    validate_raw_config(raw, path, known_canisters)
}

fn validate_raw_config(
    raw: RawIcydbProjectConfig,
    path: &Path,
    known_canisters: &[&str],
) -> Result<GeneratedIcydbConfig, ConfigError> {
    let known_by_normalized = normalized_known_canisters(known_canisters)?;
    let canisters = validate_canisters(
        raw.canisters.unwrap_or_default(),
        path,
        &known_by_normalized,
    )?;

    Ok(GeneratedIcydbConfig::new(canisters))
}

fn validate_canisters(
    raw_canisters: BTreeMap<String, RawCanisterConfig>,
    path: &Path,
    known_by_normalized: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, GeneratedCanisterConfig>, ConfigError> {
    let mut generated = BTreeMap::new();

    for (raw_name, raw_config) in raw_canisters {
        if raw_name.trim().is_empty() {
            return Err(ConfigError::EmptyCanisterName {
                path: path.to_path_buf(),
            });
        }
        if !is_snake_canister_name(raw_name.as_str()) {
            return Err(ConfigError::InvalidCanisterName {
                path: path.to_path_buf(),
                canister: raw_name,
            });
        }

        let resolved_name = if known_by_normalized.is_empty() {
            raw_name
        } else {
            known_by_normalized
                .get(raw_name.as_str())
                .cloned()
                .ok_or_else(|| ConfigError::UnknownCanister {
                    path: path.to_path_buf(),
                    canister: raw_name.clone(),
                })?
        };
        generated.insert(resolved_name, generated_canister_config(&raw_config));
    }

    Ok(generated)
}

fn generated_canister_config(raw_config: &RawCanisterConfig) -> GeneratedCanisterConfig {
    let sql = raw_config.sql.as_ref();
    let metrics = raw_config.metrics.as_ref();

    GeneratedCanisterConfig::new(
        GeneratedCanisterSqlConfig::new(
            sql_introspection_policy(sql.and_then(|sql| sql.introspection.as_ref())),
            sql.and_then(|sql| sql.update.as_ref()).map_or(
                DEFAULT_SQL_UPDATE_POLICY,
                RawCanisterSqlUpdateConfig::generated_policy,
            ),
        )
        .with_readonly_enabled(
            sql.and_then(|sql| sql.readonly)
                .unwrap_or(DEFAULT_SQL_READONLY_ENABLED),
        )
        .with_ddl_enabled(
            sql.and_then(|sql| sql.ddl)
                .unwrap_or(DEFAULT_SQL_DDL_ENABLED),
        )
        .with_fixtures_enabled(
            sql.and_then(|sql| sql.fixtures)
                .unwrap_or(DEFAULT_SQL_FIXTURES_ENABLED),
        )
        .with_integrity_enabled(
            sql.and_then(|sql| sql.integrity)
                .unwrap_or(DEFAULT_SQL_INTEGRITY_ENABLED),
        ),
        GeneratedCanisterMetricsConfig::new(metrics_policy(metrics)),
        raw_config
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.enabled)
            .unwrap_or(DEFAULT_SNAPSHOT_ENABLED),
        raw_config
            .schema
            .as_ref()
            .and_then(|schema| schema.enabled)
            .unwrap_or(DEFAULT_SCHEMA_ENABLED),
    )
}

fn normalized_known_canisters(
    known_canisters: &[&str],
) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut known_by_normalized = BTreeMap::new();
    for known in known_canisters {
        if !is_snake_canister_name(known) {
            return Err(ConfigError::InvalidKnownCanisterName {
                canister: (*known).to_string(),
            });
        }

        match known_by_normalized.entry((*known).to_string()) {
            Entry::Vacant(slot) => {
                slot.insert((*known).to_string());
            }
            Entry::Occupied(existing) => {
                return Err(ConfigError::AmbiguousKnownCanister {
                    first: existing.get().clone(),
                    second: (*known).to_string(),
                });
            }
        }
    }

    Ok(known_by_normalized)
}

fn is_snake_canister_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_lowercase()
        && chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIcydbProjectConfig {
    canisters: Option<BTreeMap<String, RawCanisterConfig>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCanisterConfig {
    sql: Option<RawCanisterSqlConfig>,
    metrics: Option<RawCanisterMetricsConfig>,
    snapshot: Option<RawCanisterSnapshotConfig>,
    schema: Option<RawCanisterSchemaConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCanisterSqlConfig {
    readonly: Option<bool>,
    ddl: Option<bool>,
    fixtures: Option<bool>,
    integrity: Option<bool>,
    introspection: Option<RawCanisterSqlIntrospectionConfig>,
    update: Option<RawCanisterSqlUpdateConfig>,
}

fn sql_introspection_policy(
    raw: Option<&RawCanisterSqlIntrospectionConfig>,
) -> GeneratedSqlIntrospectionPolicy {
    GeneratedSqlIntrospectionPolicy::new(
        raw.and_then(|raw| raw.local)
            .unwrap_or(DEFAULT_SQL_INTROSPECTION_LOCAL_ENABLED),
        raw.and_then(|raw| raw.ic)
            .unwrap_or(DEFAULT_SQL_INTROSPECTION_IC_ENABLED),
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCanisterSqlIntrospectionConfig {
    local: Option<bool>,
    ic: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawCanisterSqlUpdateConfig {
    Enabled(bool),
    Policy(RawGeneratedSqlUpdatePolicy),
}

impl RawCanisterSqlUpdateConfig {
    const fn generated_policy(&self) -> Option<crate::model::GeneratedSqlUpdatePolicy> {
        match self {
            Self::Enabled(true) => {
                Some(crate::model::GeneratedSqlUpdatePolicy::PublicPrimaryKeyOnly)
            }
            Self::Enabled(false) => None,
            Self::Policy(policy) => Some(policy.generated_policy()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawGeneratedSqlUpdatePolicy {
    PrimaryKey,
    Bounded,
}

impl RawGeneratedSqlUpdatePolicy {
    const fn generated_policy(&self) -> crate::model::GeneratedSqlUpdatePolicy {
        match self {
            Self::PrimaryKey => crate::model::GeneratedSqlUpdatePolicy::PublicPrimaryKeyOnly,
            Self::Bounded => crate::model::GeneratedSqlUpdatePolicy::PublicBoundedDeterministic,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCanisterMetricsConfig {
    local: Option<RawGeneratedMetricsMode>,
    ic: Option<RawGeneratedMetricsMode>,
}

fn metrics_policy(raw: Option<&RawCanisterMetricsConfig>) -> GeneratedMetricsPolicy {
    GeneratedMetricsPolicy::new(
        raw.and_then(|raw| raw.local).map_or(
            DEFAULT_METRICS_LOCAL_MODE,
            RawGeneratedMetricsMode::generated,
        ),
        raw.and_then(|raw| raw.ic)
            .map_or(DEFAULT_METRICS_IC_MODE, RawGeneratedMetricsMode::generated),
    )
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawGeneratedMetricsMode {
    Off,
    Simple,
    Extended,
}

impl RawGeneratedMetricsMode {
    const fn generated(self) -> GeneratedMetricsMode {
        match self {
            Self::Off => GeneratedMetricsMode::Off,
            Self::Simple => GeneratedMetricsMode::Simple,
            Self::Extended => GeneratedMetricsMode::Extended,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCanisterSnapshotConfig {
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCanisterSchemaConfig {
    enabled: Option<bool>,
}
