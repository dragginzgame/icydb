//! Module: emit
//! Responsibility: resolve build-script config and emit Cargo rerun directives.
//! Does not own: TOML validation, schema generation, or runtime feature gates.
//! Boundary: adapts resolved host config into build-script-facing generated settings.

use crate::{
    CONFIG_PATH_ENV, ConfigError, GeneratedBuildTarget, GeneratedIcydbConfig,
    ICYDB_BUILD_TARGET_ENV, parse::load_icydb_toml, resolve::resolve_config_path,
};
use std::{env, fs, path::PathBuf};

const CARGO_MANIFEST_DIR_ENV: &str = "CARGO_MANIFEST_DIR";
const OUT_DIR_ENV: &str = "OUT_DIR";

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

/// Emit generated actor glue for one canister using the effective config.
///
/// The low-level build options remain owned by `icydb-build`; downstream build
/// scripts should call `icydb::build::build_configured_canister!()` instead of
/// constructing them.
pub fn emit_configured_canister_for_build_script(
    canister_path: &str,
    canister_name: &str,
) -> Result<(), ConfigError> {
    let config = emit_config_for_build_script()?;
    let options = icydb_build::BuildOptions::default()
        .with_sql_readonly_enabled(config.canister_sql_readonly_enabled(canister_name))
        .with_sql_ddl_enabled(config.canister_sql_ddl_enabled(canister_name))
        .with_sql_fixtures_enabled(config.canister_sql_fixtures_enabled(canister_name))
        .with_sql_integrity_enabled(config.canister_sql_integrity_enabled(canister_name))
        .with_sql_introspection_enabled(config.canister_sql_introspection_enabled(canister_name))
        .with_sql_update_policy(match config.canister_sql_update_policy(canister_name) {
            Some(crate::GeneratedSqlUpdatePolicy::PublicPrimaryKeyOnly) => {
                Some(icydb_build::BuildSqlUpdatePolicy::PublicPrimaryKeyOnly)
            }
            Some(crate::GeneratedSqlUpdatePolicy::PublicBoundedDeterministic) => {
                Some(icydb_build::BuildSqlUpdatePolicy::PublicBoundedDeterministic)
            }
            None => None,
        })
        .with_metrics_enabled(config.canister_metrics_enabled(canister_name))
        .with_metrics_extended_enabled(config.canister_metrics_extended_enabled(canister_name))
        .with_snapshot_enabled(config.canister_snapshot_enabled(canister_name))
        .with_schema_enabled(config.canister_schema_enabled(canister_name));

    // Register the build inputs and generated-code cfg knobs expected by the
    // emitted actor glue.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(icydb)");
    println!("cargo:rustc-check-cfg=cfg(feature, values(\"sql\"))");
    println!("cargo:rustc-cfg=icydb");

    let out_dir = env::var(OUT_DIR_ENV).map_err(|source| ConfigError::OutDir { source })?;
    let actor_file = PathBuf::from(out_dir).join("actor.rs");
    let output = icydb_build::generate_with_options(canister_path, options);
    fs::write(actor_file.as_path(), output).map_err(|source| ConfigError::WriteGeneratedActor {
        path: actor_file,
        source,
    })
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
