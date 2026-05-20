//! Host-only build helper for `icydb.toml` project configuration.
//!
//! This crate owns TOML parsing for build scripts. Runtime crates and generated
//! actor code should consume only the generated actor source emitted by
//! `icydb-build`.

mod emit;
mod error;
mod model;
mod parse;
mod resolve;

pub use emit::emit_config_for_build_script;
pub use error::ConfigBuildError;
pub use model::{GeneratedCanisterConfig, GeneratedIcydbConfig, ResolvedIcydbConfig};
pub use parse::load_resolved_icydb_toml;

const CONFIG_FILE_NAME: &str = "icydb.toml";
const CONFIG_PATH_ENV: &str = "ICYDB_CONFIG_PATH";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_icydb_toml;
    use crate::resolve::resolve_config_path;
    use std::{env, fs};

    #[test]
    fn absent_config_defaults_all_optional_surfaces_off() {
        let config = parse_icydb_toml("", &[]).expect("empty config should parse");

        assert!(!config.canister_sql_readonly_enabled("demo_rpg"));
        assert!(!config.canister_sql_ddl_enabled("demo_rpg"));
        assert!(!config.canister_sql_fixtures_enabled("demo_rpg"));
        assert!(!config.canister_metrics_enabled("demo_rpg"));
        assert!(!config.canister_metrics_reset_enabled("demo_rpg"));
        assert!(!config.canister_snapshot_enabled("demo_rpg"));
        assert!(!config.canister_schema_enabled("demo_rpg"));
    }

    #[test]
    fn readonly_ddl_fixtures_metrics_snapshot_and_schema_config_validate() {
        let config = parse_icydb_toml(
            r"
                [canisters.demo_rpg.sql]
                readonly = true
                ddl = true
                fixtures = true

                [canisters.demo_rpg.metrics]
                enabled = true
                reset = true

                [canisters.demo_rpg.snapshot]
                enabled = true

                [canisters.demo_rpg.schema]
                enabled = true
            ",
            &["demo_rpg"],
        )
        .expect("valid config should parse");

        assert!(config.canister_sql_readonly_enabled("demo_rpg"));
        assert!(config.canister_sql_ddl_enabled("demo_rpg"));
        assert!(config.canister_sql_fixtures_enabled("demo_rpg"));
        assert!(config.canister_metrics_enabled("demo_rpg"));
        assert!(config.canister_metrics_reset_enabled("demo_rpg"));
        assert!(config.canister_snapshot_enabled("demo_rpg"));
        assert!(config.canister_schema_enabled("demo_rpg"));
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
                unexpected = true
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
                fixtures = true
            ",
        )
        .expect("config should be written");

        let resolved = load_resolved_icydb_toml(canister.as_path(), &["demo_rpg"])
            .expect("resolved config should load");

        assert_eq!(resolved.config_path(), Some(config_path.as_path()));
        assert!(resolved.config().canister_sql_readonly_enabled("demo_rpg"));
        assert!(resolved.config().canister_sql_ddl_enabled("demo_rpg"));
        assert!(resolved.config().canister_sql_fixtures_enabled("demo_rpg"));
        fs::remove_dir_all(root).expect("test directory should be removed");
    }
}
