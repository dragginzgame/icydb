//! Module: tests
//! Responsibility: config parsing and discovery regression coverage.
//! Does not own: runtime config behavior or build-script process integration.
//! Boundary: exercises crate-local host config contracts with filesystem fixtures.

use crate::{
    ConfigError, GeneratedBuildTarget, GeneratedMetricsMode, GeneratedSqlUpdatePolicy,
    ICYDB_CONFIG_FILE_NAME, load_resolved_icydb_toml,
    parse::parse_icydb_toml,
    resolve::{resolve_config_path, resolve_existing_icydb_toml},
};
use std::{env, fs};

#[test]
fn absent_config_defaults_minimal_metrics_on() {
    let config = parse_icydb_toml("", &[]).expect("empty config should parse");

    assert!(!config.canister_sql_readonly_enabled("demo_rpg"));
    assert!(!config.canister_sql_ddl_enabled("demo_rpg"));
    assert!(!config.canister_sql_fixtures_enabled("demo_rpg"));
    assert!(!config.canister_sql_introspection_enabled("demo_rpg"));
    assert!(config.canister_sql_introspection_policy("demo_rpg").local());
    assert!(!config.canister_sql_introspection_policy("demo_rpg").ic());
    assert_eq!(config.canister_sql_update_policy("demo_rpg"), None);
    assert!(config.canister_metrics_enabled("demo_rpg"));
    assert_eq!(
        config.canister_metrics_mode_for_build_target("demo_rpg", GeneratedBuildTarget::Local),
        GeneratedMetricsMode::Simple,
    );
    assert_eq!(
        config.canister_metrics_mode_for_build_target("demo_rpg", GeneratedBuildTarget::Ic),
        GeneratedMetricsMode::Simple,
    );
    assert!(!config.canister_metrics_extended_enabled("demo_rpg"));
    assert!(!config.canister_snapshot_enabled("demo_rpg"));
    assert!(!config.canister_schema_enabled("demo_rpg"));
}

#[test]
fn explicit_off_disables_metrics_default_surface() {
    let config = parse_icydb_toml(
        r#"
            [canisters.demo_rpg.sql]
            readonly = false

            [canisters.demo_rpg.metrics]
            local = "off"
            ic = "off"

            [canisters.demo_rpg.snapshot]
            enabled = false

            [canisters.demo_rpg.schema]
            enabled = false
        "#,
        &["demo_rpg"],
    )
    .expect("valid config should parse");

    assert!(!config.canister_sql_readonly_enabled("demo_rpg"));
    assert!(!config.canister_sql_ddl_enabled("demo_rpg"));
    assert!(!config.canister_sql_fixtures_enabled("demo_rpg"));
    assert_eq!(config.canister_sql_update_policy("demo_rpg"), None);
    assert!(!config.canister_metrics_enabled("demo_rpg"));
    assert_eq!(
        config.canister_metrics_mode_for_build_target("demo_rpg", GeneratedBuildTarget::Local),
        GeneratedMetricsMode::Off,
    );
    assert_eq!(
        config.canister_metrics_mode_for_build_target("demo_rpg", GeneratedBuildTarget::Ic),
        GeneratedMetricsMode::Off,
    );
    assert!(!config.canister_metrics_extended_enabled("demo_rpg"));
    assert!(!config.canister_snapshot_enabled("demo_rpg"));
    assert!(!config.canister_schema_enabled("demo_rpg"));
}

#[test]
fn partial_metrics_config_uses_default_enabled_surface() {
    let config = parse_icydb_toml(
        r#"
            [canisters.demo_rpg.sql]
            ddl = true

            [canisters.demo_rpg.metrics]
            local = "extended"
        "#,
        &["demo_rpg"],
    )
    .expect("valid config should parse");

    assert!(!config.canister_sql_readonly_enabled("demo_rpg"));
    assert!(config.canister_sql_ddl_enabled("demo_rpg"));
    assert!(!config.canister_sql_fixtures_enabled("demo_rpg"));
    assert_eq!(config.canister_sql_update_policy("demo_rpg"), None);
    assert!(config.canister_metrics_enabled("demo_rpg"));
    assert_eq!(
        config.canister_metrics_mode_for_build_target("demo_rpg", GeneratedBuildTarget::Local),
        GeneratedMetricsMode::Extended,
    );
    assert_eq!(
        config.canister_metrics_mode_for_build_target("demo_rpg", GeneratedBuildTarget::Ic),
        GeneratedMetricsMode::Simple,
    );
    assert!(config.canister_metrics_extended_enabled_for_build_target(
        "demo_rpg",
        GeneratedBuildTarget::Local,
    ));
    assert!(
        !config.canister_metrics_extended_enabled_for_build_target(
            "demo_rpg",
            GeneratedBuildTarget::Ic,
        )
    );
    assert!(!config.canister_snapshot_enabled("demo_rpg"));
    assert!(!config.canister_schema_enabled("demo_rpg"));
}

#[test]
fn readonly_ddl_fixtures_update_metrics_snapshot_and_schema_config_validate() {
    let config = parse_icydb_toml(
        r#"
            [canisters.demo_rpg.sql]
            readonly = true
            ddl = true
            fixtures = true
            update = true

            [canisters.demo_rpg.metrics]
            local = "extended"
            ic = "extended"

            [canisters.demo_rpg.snapshot]
            enabled = true

            [canisters.demo_rpg.schema]
            enabled = true
        "#,
        &["demo_rpg"],
    )
    .expect("valid config should parse");

    assert!(config.canister_sql_readonly_enabled("demo_rpg"));
    assert!(config.canister_sql_ddl_enabled("demo_rpg"));
    assert!(config.canister_sql_fixtures_enabled("demo_rpg"));
    assert!(config.canister_sql_introspection_policy("demo_rpg").local());
    assert!(!config.canister_sql_introspection_policy("demo_rpg").ic());
    assert_eq!(
        config.canister_sql_update_policy("demo_rpg"),
        Some(GeneratedSqlUpdatePolicy::PublicPrimaryKeyOnly),
    );
    assert!(config.canister_metrics_enabled("demo_rpg"));
    assert_eq!(
        config.canister_metrics_mode_for_build_target("demo_rpg", GeneratedBuildTarget::Local),
        GeneratedMetricsMode::Extended,
    );
    assert_eq!(
        config.canister_metrics_mode_for_build_target("demo_rpg", GeneratedBuildTarget::Ic),
        GeneratedMetricsMode::Extended,
    );
    assert!(config.canister_metrics_extended_enabled_for_build_target(
        "demo_rpg",
        GeneratedBuildTarget::Local,
    ));
    assert!(
        config.canister_metrics_extended_enabled_for_build_target(
            "demo_rpg",
            GeneratedBuildTarget::Ic,
        )
    );
    assert!(config.canister_metrics_extended_enabled("demo_rpg"));
    assert!(config.canister_snapshot_enabled("demo_rpg"));
    assert!(config.canister_schema_enabled("demo_rpg"));
}

#[test]
fn sql_introspection_policy_defaults_local_on_ic_off() {
    let config = parse_icydb_toml(
        r"
            [canisters.demo_rpg.sql]
            readonly = true
        ",
        &["demo_rpg"],
    )
    .expect("valid config should parse");
    let policy = config.canister_sql_introspection_policy("demo_rpg");

    assert!(policy.local());
    assert!(!policy.ic());
    assert!(policy.enabled_for(GeneratedBuildTarget::Local));
    assert!(!policy.enabled_for(GeneratedBuildTarget::Ic));
    assert!(!policy.enabled_for(GeneratedBuildTarget::Unknown));
}

#[test]
fn sql_introspection_policy_accepts_independent_target_overrides() {
    let config = parse_icydb_toml(
        r"
            [canisters.demo_rpg.sql]
            readonly = true

            [canisters.demo_rpg.sql.introspection]
            local = false
            ic = true
        ",
        &["demo_rpg"],
    )
    .expect("valid config should parse");
    let policy = config.canister_sql_introspection_policy("demo_rpg");

    assert!(!policy.local());
    assert!(policy.ic());
    assert!(!policy.enabled_for(GeneratedBuildTarget::Local));
    assert!(policy.enabled_for(GeneratedBuildTarget::Ic));
    assert!(!policy.enabled_for(GeneratedBuildTarget::Unknown));
}

#[test]
fn invalid_sql_update_policy_fails_parse() {
    let err = parse_icydb_toml(
        r#"
            [canisters.demo_rpg.sql]
            update = "bulk"
        "#,
        &["demo_rpg"],
    )
    .expect_err("unknown generated SQL update policy must fail");

    std::assert_matches!(err, ConfigError::Parse { .. });
}

#[test]
fn boolean_sql_update_policy_enables_primary_key_default() {
    let config = parse_icydb_toml(
        r"
            [canisters.demo_rpg.sql]
            update = true
        ",
        &["demo_rpg"],
    )
    .expect("boolean SQL update config should parse");

    assert_eq!(
        config.canister_sql_update_policy("demo_rpg"),
        Some(GeneratedSqlUpdatePolicy::PublicPrimaryKeyOnly),
    );
}

#[test]
fn named_primary_key_sql_update_policy_enables_primary_key_policy() {
    let config = parse_icydb_toml(
        r#"
            [canisters.demo_rpg.sql]
            update = "primary_key"
        "#,
        &["demo_rpg"],
    )
    .expect("named primary-key SQL update config should parse");

    assert_eq!(
        config.canister_sql_update_policy("demo_rpg"),
        Some(GeneratedSqlUpdatePolicy::PublicPrimaryKeyOnly),
    );
}

#[test]
fn named_bounded_sql_update_policy_enables_bounded_policy() {
    let config = parse_icydb_toml(
        r#"
            [canisters.demo_rpg.sql]
            update = "bounded"
        "#,
        &["demo_rpg"],
    )
    .expect("named bounded SQL update config should parse");

    assert_eq!(
        config.canister_sql_update_policy("demo_rpg"),
        Some(GeneratedSqlUpdatePolicy::PublicBoundedDeterministic),
    );
}

#[test]
fn explicit_false_disables_sql_update_policy() {
    let config = parse_icydb_toml(
        r"
            [canisters.demo_rpg.sql]
            update = false
        ",
        &["demo_rpg"],
    )
    .expect("false SQL update config should parse");

    assert_eq!(config.canister_sql_update_policy("demo_rpg"), None);
}

#[test]
fn unknown_metrics_mode_fails_parse() {
    let err = parse_icydb_toml(
        r#"
            [canisters.demo_rpg.metrics]
            local = "full"
        "#,
        &["demo_rpg"],
    )
    .expect_err("unknown metrics mode must fail");

    std::assert_matches!(err, ConfigError::Parse { .. });
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

    std::assert_matches!(err, ConfigError::Parse { .. });
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

    std::assert_matches!(err, ConfigError::Parse { .. });
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

    std::assert_matches!(
        err,
        ConfigError::UnknownCanister { canister, .. } if canister == "unknown"
    );
}

#[test]
fn non_snake_config_canister_names_fail_validation() {
    let err = parse_icydb_toml(
        r"
            [canisters.demo-rpg.sql]
            readonly = true
        ",
        &["demo_rpg"],
    )
    .expect_err("config canister names must be lower snake_case");

    std::assert_matches!(
        err,
        ConfigError::InvalidCanisterName { canister, .. } if canister == "demo-rpg"
    );
}

#[test]
fn non_snake_known_canister_names_fail_validation() {
    let err = parse_icydb_toml(
        r"
            [canisters.demo_rpg.sql]
            ddl = true
        ",
        &["DemoRpg"],
    )
    .expect_err("known canister names must be lower snake_case");

    std::assert_matches!(
        err,
        ConfigError::InvalidKnownCanisterName { canister } if canister == "DemoRpg"
    );
}

#[test]
fn config_resolution_ignores_ancestor_directories_named_icydb_toml() {
    let root = env::temp_dir().join(format!("icydb-config-dir-test-{}", std::process::id()));
    let canister = root.join("canisters").join("demo").join("rpg");
    fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    fs::create_dir_all(root.join("canisters").join("icydb.toml"))
        .expect("directory named icydb.toml should be created");
    let root_config = root.join("icydb.toml");
    fs::write(
        root_config.as_path(),
        "[canisters.root.sql]\nreadonly = true\n",
    )
    .expect("root config should be written");

    let resolved = resolve_config_path(canister.as_path());

    assert_eq!(resolved.config_path(), Some(root_config.as_path()));
    assert_eq!(
        resolve_existing_icydb_toml(canister.as_path()),
        Some(root_config)
    );
    fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn config_resolution_uses_nearest_ancestor_config() {
    let root = env::temp_dir().join(format!("icydb-config-test-{}", std::process::id()));
    let workspace = root.join("workspace");
    let canister = workspace.join("canisters").join("demo").join("rpg");
    fs::create_dir_all(canister.as_path()).expect("test directory should be created");
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

    assert_eq!(resolved.config_path(), Some(demo_config.as_path()));
    assert_eq!(
        resolve_existing_icydb_toml(canister.as_path()),
        Some(demo_config)
    );
    fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn config_resolution_walks_past_cargo_workspace_root() {
    let root = env::temp_dir().join(format!(
        "icydb-config-workspace-test-{}",
        std::process::id()
    ));
    let workspace = root.join("workspace");
    let canister = workspace
        .join("member")
        .join("canisters")
        .join("demo")
        .join("rpg");
    fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    fs::write(
        workspace.join("Cargo.toml"),
        "[workspace]\nmembers = [\"member\"]\n",
    )
    .expect("workspace manifest should be written");
    let root_config = root.join("icydb.toml");
    fs::write(
        root_config.as_path(),
        "[canisters.root.sql]\nreadonly = true\n",
    )
    .expect("root config should be written");

    let resolved = resolve_config_path(canister.as_path());

    assert_eq!(resolved.config_path(), Some(root_config.as_path()));
    assert_eq!(
        resolve_existing_icydb_toml(canister.as_path()),
        Some(root_config)
    );
    fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn load_resolved_config_reports_path_and_validated_config() {
    let root = env::temp_dir().join(format!("icydb-config-load-test-{}", std::process::id()));
    let canister = root.join("canisters").join("demo").join("rpg");
    fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    let config_path = root
        .join("canisters")
        .join("demo")
        .join(ICYDB_CONFIG_FILE_NAME);
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
    assert_eq!(
        resolved.config().canister_sql_update_policy("demo_rpg"),
        None
    );
    fs::remove_dir_all(root).expect("test directory should be removed");
}
