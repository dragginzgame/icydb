use serde::Deserialize;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs,
    path::Path,
};

use crate::{
    ConfigBuildError, GeneratedCanisterConfig, GeneratedIcydbConfig, ResolvedIcydbConfig,
    model::{GeneratedCanisterMetricsConfig, GeneratedCanisterSqlConfig},
    resolve::resolve_config_path,
};

#[cfg(test)]
pub(crate) fn parse_icydb_toml(
    source: &str,
    known_canisters: &[&str],
) -> Result<GeneratedIcydbConfig, ConfigBuildError> {
    parse_icydb_toml_at(source, None, known_canisters)
}

pub(crate) fn load_icydb_toml(
    path: impl AsRef<Path>,
    known_canisters: &[&str],
) -> Result<GeneratedIcydbConfig, ConfigBuildError> {
    let path = path.as_ref();
    let source = fs::read_to_string(path).map_err(|source| ConfigBuildError::Read {
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
) -> Result<ResolvedIcydbConfig, ConfigBuildError> {
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
) -> Result<GeneratedIcydbConfig, ConfigBuildError> {
    let path = path.unwrap_or_else(|| Path::new("<inline>"));
    let raw: RawIcydbProjectConfig =
        toml::from_str(source).map_err(|source| ConfigBuildError::Parse {
            path: path.to_path_buf(),
            source,
        })?;

    validate_raw_config(raw, path, known_canisters)
}

fn validate_raw_config(
    raw: RawIcydbProjectConfig,
    path: &Path,
    known_canisters: &[&str],
) -> Result<GeneratedIcydbConfig, ConfigBuildError> {
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
) -> Result<BTreeMap<String, GeneratedCanisterConfig>, ConfigBuildError> {
    let mut normalized_seen = BTreeMap::new();
    let mut generated = BTreeMap::new();

    for (raw_name, raw_config) in raw_canisters {
        if raw_name.trim().is_empty() {
            return Err(ConfigBuildError::EmptyCanisterName {
                path: path.to_path_buf(),
            });
        }
        let normalized = normalize_canister_name(raw_name.as_str());
        match normalized_seen.entry(normalized.clone()) {
            Entry::Vacant(slot) => {
                slot.insert(raw_name.clone());
            }
            Entry::Occupied(existing) => {
                return Err(ConfigBuildError::AmbiguousCanisterName {
                    path: path.to_path_buf(),
                    first: existing.get().clone(),
                    second: raw_name,
                });
            }
        }

        let resolved_name = if known_by_normalized.is_empty() {
            raw_name
        } else {
            known_by_normalized
                .get(normalized.as_str())
                .cloned()
                .ok_or_else(|| ConfigBuildError::UnknownCanister {
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
            sql.and_then(|sql| sql.readonly).unwrap_or(false),
            sql.and_then(|sql| sql.ddl).unwrap_or(false),
            sql.and_then(|sql| sql.fixtures).unwrap_or(false),
        ),
        GeneratedCanisterMetricsConfig::new(
            metrics.and_then(|metrics| metrics.enabled).unwrap_or(false),
            metrics.and_then(|metrics| metrics.reset).unwrap_or(false),
        ),
        raw_config
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.enabled)
            .unwrap_or(false),
        raw_config
            .schema
            .as_ref()
            .and_then(|schema| schema.enabled)
            .unwrap_or(false),
    )
}

fn normalized_known_canisters(
    known_canisters: &[&str],
) -> Result<BTreeMap<String, String>, ConfigBuildError> {
    let mut known_by_normalized = BTreeMap::new();
    for known in known_canisters {
        let normalized = normalize_canister_name(known);
        match known_by_normalized.entry(normalized) {
            Entry::Vacant(slot) => {
                slot.insert((*known).to_string());
            }
            Entry::Occupied(existing) => {
                return Err(ConfigBuildError::AmbiguousKnownCanister {
                    first: existing.get().clone(),
                    second: (*known).to_string(),
                });
            }
        }
    }

    Ok(known_by_normalized)
}

fn normalize_canister_name(name: &str) -> String {
    name.chars()
        .map(|ch| match ch {
            '-' => '_',
            other => other.to_ascii_lowercase(),
        })
        .collect()
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
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCanisterMetricsConfig {
    enabled: Option<bool>,
    reset: Option<bool>,
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
