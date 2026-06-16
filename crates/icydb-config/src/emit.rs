//! Module: emit
//! Responsibility: resolve build-script config and emit Cargo rerun directives.
//! Does not own: TOML validation, schema generation, or runtime feature gates.
//! Boundary: adapts resolved host config into build-script-facing generated settings.

use crate::{
    CONFIG_PATH_ENV, ConfigError, GeneratedBuildTarget, GeneratedIcydbConfig,
    ICYDB_BUILD_TARGET_ENV, parse::load_icydb_toml, resolve::resolve_config_path,
};
use std::{env, path::PathBuf};

const CARGO_MANIFEST_DIR_ENV: &str = "CARGO_MANIFEST_DIR";

/// Resolve and validate config for a canister build script.
///
/// Resolution order:
/// 1. `ICYDB_CONFIG_PATH`
/// 2. nearest `icydb.toml` found by walking up from the canister crate
/// 3. absent config, treated as defaults
pub fn emit_config_for_build_script() -> Result<GeneratedIcydbConfig, ConfigError> {
    println!("cargo:rerun-if-env-changed={CONFIG_PATH_ENV}");
    println!("cargo:rerun-if-env-changed={ICYDB_BUILD_TARGET_ENV}");
    let manifest_dir = manifest_dir()?;
    let resolved = resolve_config_path(manifest_dir.as_path());
    let build_target = build_target_from_env();
    if let Some(path) = resolved.config_path() {
        println!("cargo:rerun-if-changed={}", path.display());
        load_icydb_toml(path, &[]).map(|config| config.with_build_target(build_target))
    } else {
        for candidate in resolved.candidate_paths() {
            println!("cargo:rerun-if-changed={}", candidate.display());
        }
        Ok(GeneratedIcydbConfig::default().with_build_target(build_target))
    }
}

fn build_target_from_env() -> GeneratedBuildTarget {
    env::var(ICYDB_BUILD_TARGET_ENV).ok().as_deref().map_or(
        GeneratedBuildTarget::Unknown,
        GeneratedBuildTarget::from_env_value,
    )
}

fn manifest_dir() -> Result<PathBuf, ConfigError> {
    env::var_os(CARGO_MANIFEST_DIR_ENV)
        .map(PathBuf::from)
        .map_or_else(
            || env::current_dir().map_err(|source| ConfigError::CurrentDir { source }),
            Ok,
        )
}
