//! Host-only build helper for `icydb.toml` project configuration.
//!
//! This crate owns TOML parsing for build scripts. Runtime crates and generated
//! actor code should consume only the generated Rust constants emitted here.

use serde::Deserialize;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    env, fs, io,
    path::{Path, PathBuf},
};
use thiserror::Error as ThisError;

const CONFIG_FILE_NAME: &str = "icydb.toml";
const CONFIG_PATH_ENV: &str = "ICYDB_CONFIG_PATH";
const GENERATED_CONFIG_FILE_NAME: &str = "icydb_config.rs";

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

    const fn new(config_path: Option<PathBuf>, config: GeneratedIcydbConfig) -> Self {
        Self {
            config_path,
            config,
        }
    }
}

/// Validated IcyDB project config ready for generated-Rust emission.
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
        self.canisters
            .get(canister_name)
            .is_some_and(GeneratedCanisterConfig::sql_readonly)
    }

    /// Return whether SQL DDL/write endpoints should be generated for one canister.
    #[must_use]
    pub fn canister_sql_ddl_enabled(&self, canister_name: &str) -> bool {
        self.canisters
            .get(canister_name)
            .is_some_and(GeneratedCanisterConfig::sql_ddl)
    }
}

/// Validated generated settings for one canister.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GeneratedCanisterConfig {
    sql_readonly: bool,
    sql_ddl: bool,
}

impl GeneratedCanisterConfig {
    /// Return whether generated actor glue should export read-only SQL endpoints.
    #[must_use]
    pub const fn sql_readonly(&self) -> bool {
        self.sql_readonly
    }

    /// Return whether generated actor glue should export SQL DDL/write endpoints.
    #[must_use]
    pub const fn sql_ddl(&self) -> bool {
        self.sql_ddl
    }
}

/// Build-script config loading error with path-aware diagnostics.
#[derive(Debug, ThisError)]
pub enum ConfigBuildError {
    #[error("failed to read IcyDB config at '{}': {source}", path.display())]
    Read { path: PathBuf, source: io::Error },

    #[error("failed to parse IcyDB config at '{}': {source}", path.display())]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },

    #[error("IcyDB config at '{}' contains an empty canister name", path.display())]
    EmptyCanisterName { path: PathBuf },

    #[error(
        "IcyDB config at '{}' has ambiguous canister names '{first}' and '{second}' after normalization"
    , path.display())]
    AmbiguousCanisterName {
        path: PathBuf,
        first: String,
        second: String,
    },

    #[error(
        "IcyDB config at '{}' contains canister '{canister}' but the generated schema has no matching canister"
    , path.display())]
    UnknownCanister { path: PathBuf, canister: String },

    #[error(
        "generated schema canister names '{first}' and '{second}' are ambiguous after normalization"
    )]
    AmbiguousKnownCanister { first: String, second: String },

    #[error("OUT_DIR is not set for IcyDB config generation")]
    MissingOutDir,

    #[error("failed to write generated IcyDB config to '{}': {source}", path.display())]
    WriteGenerated { path: PathBuf, source: io::Error },
}

/// Parse and validate one TOML config string.
pub fn parse_icydb_toml(
    source: &str,
    known_canisters: &[&str],
) -> Result<GeneratedIcydbConfig, ConfigBuildError> {
    parse_icydb_toml_at(source, None, known_canisters)
}

/// Parse and validate one TOML config file.
pub fn load_icydb_toml(
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
    let Some(path) = resolved.config_path else {
        return Ok(ResolvedIcydbConfig::default());
    };
    let config = load_icydb_toml(path.as_path(), known_canisters)?;

    Ok(ResolvedIcydbConfig::new(Some(path), config))
}

/// Render generated Rust constants for one canister target.
#[must_use]
pub fn render_rust_config_for_canister(
    config: &GeneratedIcydbConfig,
    canister_name: &str,
) -> String {
    let sql_readonly_enabled = config.canister_sql_readonly_enabled(canister_name);
    let sql_ddl_enabled = config.canister_sql_ddl_enabled(canister_name);

    format!(
        "\
// @generated by icydb-config-build. Do not edit by hand.
pub const ICYDB_SQL_READONLY_ENABLED: bool = {sql_readonly_enabled};
pub const ICYDB_SQL_DDL_ENABLED: bool = {sql_ddl_enabled};
"
    )
}

/// Write generated Rust constants into `OUT_DIR/icydb_config.rs`.
pub fn write_rust_config_for_canister(
    out_dir: impl AsRef<Path>,
    config: &GeneratedIcydbConfig,
    canister_name: &str,
) -> Result<PathBuf, ConfigBuildError> {
    let output_path = out_dir.as_ref().join(GENERATED_CONFIG_FILE_NAME);
    let source = render_rust_config_for_canister(config, canister_name);
    fs::write(output_path.as_path(), source).map_err(|source| {
        ConfigBuildError::WriteGenerated {
            path: output_path.clone(),
            source,
        }
    })?;

    Ok(output_path)
}

/// Resolve, validate, and write config for a canister build script.
///
/// Resolution order:
/// 1. `ICYDB_CONFIG_PATH`
/// 2. nearest `icydb.toml` found by walking up from the canister crate
/// 3. absent config, treated as defaults
pub fn emit_config_for_canister(
    canister_name: &str,
    _known_canisters: &[&str],
) -> Result<GeneratedIcydbConfig, ConfigBuildError> {
    println!("cargo:rerun-if-env-changed={CONFIG_PATH_ENV}");
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR").map_or_else(
        || env::current_dir().expect("current directory should resolve"),
        PathBuf::from,
    );
    let resolved = resolve_config_path(manifest_dir.as_path());
    let config = if let Some(path) = resolved.config_path.as_ref() {
        println!("cargo:rerun-if-changed={}", path.display());
        load_icydb_toml(path.as_path(), &[])?
    } else {
        for candidate in &resolved.candidate_paths {
            println!("cargo:rerun-if-changed={}", candidate.display());
        }
        GeneratedIcydbConfig::default()
    };
    let out_dir = env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .ok_or(ConfigBuildError::MissingOutDir)?;
    write_rust_config_for_canister(out_dir, &config, canister_name)?;

    Ok(config)
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

    Ok(GeneratedIcydbConfig { canisters })
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
        generated.insert(
            resolved_name,
            GeneratedCanisterConfig {
                sql_readonly: raw_config
                    .sql
                    .as_ref()
                    .and_then(|sql| sql.readonly)
                    .unwrap_or(false),
                sql_ddl: raw_config
                    .sql
                    .as_ref()
                    .and_then(|sql| sql.ddl)
                    .unwrap_or(false),
            },
        );
    }

    Ok(generated)
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

struct ResolvedConfigPath {
    config_path: Option<PathBuf>,
    candidate_paths: Vec<PathBuf>,
}

fn resolve_config_path(manifest_dir: &Path) -> ResolvedConfigPath {
    let candidate_paths = config_search_candidates(manifest_dir);
    if let Some(explicit) = env::var_os(CONFIG_PATH_ENV) {
        return ResolvedConfigPath {
            config_path: Some(PathBuf::from(explicit)),
            candidate_paths,
        };
    }

    let config_path = candidate_paths
        .iter()
        .find(|candidate| candidate.exists())
        .cloned();

    ResolvedConfigPath {
        config_path,
        candidate_paths,
    }
}

fn config_search_candidates(manifest_dir: &Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for ancestor in manifest_dir.ancestors() {
        candidates.push(ancestor.join(CONFIG_FILE_NAME));
        if is_workspace_root(ancestor) {
            break;
        }
    }

    candidates
}

fn is_workspace_root(path: &Path) -> bool {
    let manifest = path.join("Cargo.toml");
    let Ok(source) = fs::read_to_string(manifest) else {
        return false;
    };

    source.contains("[workspace]")
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
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCanisterSqlConfig {
    readonly: Option<bool>,
    ddl: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_config_defaults_all_optional_surfaces_off() {
        let config = parse_icydb_toml("", &[]).expect("empty config should parse");

        assert!(!config.canister_sql_readonly_enabled("demo_rpg"));
        assert!(!config.canister_sql_ddl_enabled("demo_rpg"));
    }

    #[test]
    fn readonly_and_ddl_sql_config_validate() {
        let config = parse_icydb_toml(
            r"
                [canisters.demo_rpg.sql]
                readonly = true
                ddl = true
            ",
            &["demo_rpg"],
        )
        .expect("valid config should parse");

        assert!(config.canister_sql_readonly_enabled("demo_rpg"));
        assert!(config.canister_sql_ddl_enabled("demo_rpg"));
    }

    #[test]
    fn demo_sql_config_fails_parse() {
        let err = parse_icydb_toml(
            r#"
                [demo.sql]
                environment = "demo"
                canister = "demo_rpg"
            "#,
            &[],
        )
        .expect_err("demo SQL target defaults should not parse");

        assert!(matches!(err, ConfigBuildError::Parse { .. }));
    }

    #[test]
    fn legacy_admin_sql_config_fails_parse() {
        let err = parse_icydb_toml(
            r"
                [canisters.demo_rpg.sql]
                admin = true
            ",
            &[],
        )
        .expect_err("legacy admin switch should not parse");

        assert!(matches!(err, ConfigBuildError::Parse { .. }));
    }

    #[test]
    fn unknown_top_level_section_fails_parse() {
        let err = parse_icydb_toml(
            r"
                [unexpected]
                enabled = true
            ",
            &[],
        )
        .expect_err("unknown top-level sections should fail");

        assert!(matches!(err, ConfigBuildError::Parse { .. }));
    }

    #[test]
    fn unknown_canister_field_fails_parse() {
        let err = parse_icydb_toml(
            r"
                [canisters.demo_rpg]
                admin = true
            ",
            &[],
        )
        .expect_err("unknown canister fields should fail");

        assert!(matches!(err, ConfigBuildError::Parse { .. }));
    }

    #[test]
    fn unknown_generated_canister_fails_validation() {
        let err = parse_icydb_toml(
            r"
                [canisters.unknown.sql]
                readonly = true
            ",
            &["demo_rpg"],
        )
        .expect_err("config canister must match generated schema canister");

        assert!(matches!(
            err,
            ConfigBuildError::UnknownCanister { canister, .. } if canister == "unknown"
        ));
    }

    #[test]
    fn ambiguous_canister_names_fail_validation() {
        let err = parse_icydb_toml(
            r"
                [canisters.demo-rpg.sql]
                readonly = true

                [canisters.demo_rpg.sql]
                ddl = true
            ",
            &[],
        )
        .expect_err("normalized duplicate canister names should fail");

        assert!(matches!(
            err,
            ConfigBuildError::AmbiguousCanisterName { .. }
        ));
    }

    #[test]
    fn generated_rust_constants_use_typed_values() {
        let config = parse_icydb_toml(
            r"
                [canisters.demo_rpg.sql]
                readonly = true
                ddl = true
            ",
            &["demo_rpg"],
        )
        .expect("valid config should parse");

        let generated = render_rust_config_for_canister(&config, "demo_rpg");

        assert!(generated.contains("pub const ICYDB_SQL_READONLY_ENABLED: bool = true;"));
        assert!(generated.contains("pub const ICYDB_SQL_DDL_ENABLED: bool = true;"));
        assert!(!generated.contains("[demo.sql]"));
    }

    #[test]
    fn config_resolution_uses_nearest_ancestor_before_workspace_root() {
        let root = env::temp_dir().join(format!("icydb-config-build-test-{}", std::process::id()));
        let workspace = root.join("workspace");
        let canister = workspace.join("canisters").join("demo").join("rpg");
        fs::create_dir_all(canister.as_path()).expect("test directory should be created");
        fs::write(workspace.join("Cargo.toml"), "[workspace]\n")
            .expect("workspace manifest should be written");
        fs::write(
            workspace.join("icydb.toml"),
            "[canisters.workspace.sql]\nreadonly = true\n",
        )
        .expect("workspace config should be written");
        let demo_config = workspace.join("canisters").join("demo").join("icydb.toml");
        fs::write(
            demo_config.as_path(),
            "[canisters.demo_rpg.sql]\nreadonly = true\n",
        )
        .expect("demo config should be written");

        let resolved = resolve_config_path(canister.as_path());

        assert_eq!(resolved.config_path.as_deref(), Some(demo_config.as_path()));
        fs::remove_dir_all(root).expect("test directory should be removed");
    }

    #[test]
    fn load_resolved_config_reports_path_and_validated_config() {
        let root = env::temp_dir().join(format!(
            "icydb-config-build-load-test-{}",
            std::process::id()
        ));
        let canister = root.join("canisters").join("demo").join("rpg");
        fs::create_dir_all(canister.as_path()).expect("test directory should be created");
        let config_path = root.join("canisters").join("demo").join(CONFIG_FILE_NAME);
        fs::write(
            config_path.as_path(),
            r"
                [canisters.demo_rpg.sql]
                readonly = true
                ddl = true
            ",
        )
        .expect("config should be written");

        let resolved = load_resolved_icydb_toml(canister.as_path(), &["demo_rpg"])
            .expect("resolved config should load");

        assert_eq!(resolved.config_path(), Some(config_path.as_path()));
        assert!(resolved.config().canister_sql_readonly_enabled("demo_rpg"));
        assert!(resolved.config().canister_sql_ddl_enabled("demo_rpg"));
        fs::remove_dir_all(root).expect("test directory should be removed");
    }
}
