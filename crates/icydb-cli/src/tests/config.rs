//! Module: CLI config tests.
//! Responsibility: exercise config command inputs, reports, and endpoint surface gates.
//! Does not own: ICP process command construction or observability decoding.
//! Boundary: test-only assertions over config helpers and generated endpoint constants.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use clap::Parser;

use crate::{
    cli::{CliArgs, CliCommand, ConfigCommand},
    config::{
        ConfigSurface, FIXTURES_LOAD_ENDPOINT, METRICS_ENDPOINT, METRICS_EXTENDED_ENDPOINT,
        METRICS_RESET_ENDPOINT, SCHEMA_CHECK_ENDPOINT, SCHEMA_ENDPOINT, SNAPSHOT_ENDPOINT,
        SQL_DDL_ENDPOINT, SQL_QUERY_ENDPOINT, SQL_UPDATE_ENDPOINT, init_config,
        test_support::{
            config_surface_enabled_for_resolved, config_sync_issues,
            configured_endpoint_enabled_for_resolved, disabled_config_surface_message,
            render_config_report,
        },
    },
};

#[test]
fn config_init_writes_default_config_at_workspace_root() {
    let root = config_init_test_root("workspace");
    let workspace = root.join("workspace");
    let package = workspace.join("member");
    let canister = package.join("canisters").join("demo").join("rpg");
    std::fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    write_workspace_manifest(workspace.as_path());
    write_package_manifest(
        package.as_path(),
        "demo_member",
        "# [workspace]\ndescription = \"mentions [workspace]\"\n",
    );

    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--start-dir",
        canister.to_str().expect("test path should be UTF-8"),
        "--canister",
        "demo_rpg",
        "--ddl",
        "--fixtures",
        "--update",
        "--metrics",
        "--metrics-extended",
        "--snapshot",
        "--schema",
    ])
    .expect("config init args should parse");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    init_config(args).expect("config init should succeed");

    let config = std::fs::read_to_string(workspace.join("icydb.toml"))
        .expect("config file should be written");
    assert!(!package.join("icydb.toml").exists());
    assert_eq!(
        config,
        "[canisters.demo_rpg.sql]\nreadonly = true\nddl = true\nfixtures = true\nupdate = true\n\n[canisters.demo_rpg.metrics]\nenabled = true\nextended = true\n\n[canisters.demo_rpg.snapshot]\nenabled = true\n\n[canisters.demo_rpg.schema]\nenabled = true\n"
    );

    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn config_init_writes_bounded_update_policy() {
    let root = config_init_test_root("bounded");
    let workspace = root.join("workspace");
    let package = workspace.join("member");
    let canister = package.join("canisters").join("demo").join("rpg");
    std::fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    write_workspace_manifest(workspace.as_path());
    write_package_manifest(package.as_path(), "demo_member", "");

    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--start-dir",
        canister.to_str().expect("test path should be UTF-8"),
        "--canister",
        "demo_rpg",
        "--update-policy",
        "bounded",
    ])
    .expect("config init args should parse");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    init_config(args).expect("config init should succeed");

    let config = std::fs::read_to_string(workspace.join("icydb.toml"))
        .expect("config file should be written");
    assert_eq!(
        config,
        "[canisters.demo_rpg.sql]\nreadonly = true\nddl = false\nfixtures = false\nupdate = \"bounded\"\n\n[canisters.demo_rpg.metrics]\nenabled = false\nextended = false\n\n[canisters.demo_rpg.snapshot]\nenabled = false\n\n[canisters.demo_rpg.schema]\nenabled = false\n"
    );

    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn config_init_writes_default_config_at_standalone_package_root() {
    let root = config_init_test_root("standalone");
    let package = root.join("package");
    let canister = package.join("canisters").join("demo").join("rpg");
    std::fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    write_package_manifest(package.as_path(), "demo_standalone", "");

    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--start-dir",
        canister.to_str().expect("test path should be UTF-8"),
        "--canister",
        "demo_rpg",
    ])
    .expect("config init args should parse");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    init_config(args).expect("config init should succeed");

    assert!(package.join("icydb.toml").is_file());
    assert!(!canister.join("icydb.toml").exists());

    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn config_init_preserves_non_cargo_directory_fallback() {
    let root = config_init_test_root("non-cargo");
    let start_dir = root.join("project").join("canisters").join("demo");
    std::fs::create_dir_all(start_dir.as_path()).expect("test directory should be created");

    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--start-dir",
        start_dir.to_str().expect("test path should be UTF-8"),
        "--canister",
        "demo_rpg",
    ])
    .expect("config init args should parse");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    init_config(args).expect("config init should succeed");

    assert!(start_dir.join("icydb.toml").is_file());

    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn config_report_marks_canister_settings_against_icp_environment() {
    let root = std::env::temp_dir().join(format!(
        "icydb-cli-config-report-test-{}",
        std::process::id()
    ));
    let canister = root.join("canisters").join("demo").join("rpg");
    std::fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    let config_path = root.join("canisters").join("demo").join("icydb.toml");
    std::fs::write(
        config_path.as_path(),
        r#"
            [canisters.demo_rpg.sql]
            readonly = true
            ddl = true
            fixtures = true
            update = true

            [canisters.demo_rpg.metrics]
            enabled = true
            extended = true

            [canisters.demo_rpg.snapshot]
            enabled = true

            [canisters.demo_rpg.schema]
            enabled = true

            [canisters.admin_rpg.sql]
            update = "bounded"
        "#,
    )
    .expect("config should be written");
    let resolved = icydb_config_build::load_resolved_icydb_toml(
        canister.as_path(),
        &["demo_rpg", "admin_rpg"],
    )
    .expect("config should resolve");

    let report = render_config_report(
        canister.as_path(),
        Some("demo"),
        &[String::from("demo_rpg"), String::from("admin_rpg")],
        &resolved,
    );

    assert!(report.lines().any(|line| {
        line.contains("canister")
            && line.contains("SQL surfaces")
            && line.contains("metrics")
            && line.contains("snapshot")
            && line.contains("schema")
            && line.contains("ICP environment")
    }));
    assert!(report.lines().any(|line| line.starts_with("  --------")));
    assert!(report.lines().any(|line| {
        line.contains("demo_rpg")
            && line.contains("readonly, ddl, fixtures")
            && line.contains("update:primary_key")
            && line.contains("enabled, extended")
            && line.contains("ok")
    }));
    assert!(report.lines().any(|line| {
        line.contains("admin_rpg") && line.contains("update:bounded") && line.contains("ok")
    }));
    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

fn write_workspace_manifest(workspace: &Path) {
    std::fs::write(
        workspace.join("Cargo.toml"),
        "[workspace]\nmembers = [\"member\"]\nresolver = \"2\"\n",
    )
    .expect("workspace manifest should be written");
}

static CONFIG_INIT_TEST_ROOT_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn config_init_test_root(label: &str) -> PathBuf {
    let counter = CONFIG_INIT_TEST_ROOT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after UNIX_EPOCH")
        .as_nanos();

    std::env::temp_dir().join(format!(
        "icydb-cli-config-init-{label}-{}-{counter}-{nanos}",
        std::process::id()
    ))
}

fn write_package_manifest(package: &Path, name: &str, extra_package_fields: &str) {
    std::fs::create_dir_all(package.join("src")).expect("package src directory should be created");
    std::fs::write(
        package.join("Cargo.toml"),
        format!(
            "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n{extra_package_fields}"
        ),
    )
    .expect("package manifest should be written");
    std::fs::write(package.join("src").join("lib.rs"), "")
        .expect("package lib target should be written");
}

#[test]
fn config_check_reports_mismatched_canister_settings() {
    let root = std::env::temp_dir().join(format!(
        "icydb-cli-config-check-test-{}",
        std::process::id()
    ));
    let canister = root.join("canisters").join("demo").join("rpg");
    std::fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    std::fs::write(
        root.join("canisters").join("demo").join("icydb.toml"),
        r"
            [canisters.missing_rpg.sql]
            readonly = true
        ",
    )
    .expect("config should be written");
    let resolved = icydb_config_build::load_resolved_icydb_toml(canister.as_path(), &[])
        .expect("config should resolve without known canister validation");

    let issues = config_sync_issues(Some("test"), &[String::from("demo_rpg")], &resolved);

    assert!(
        issues
            .iter()
            .any(|issue| issue.contains("canisters.missing_rpg")),
        "missing configured canister should be reported: {issues:?}",
    );
    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn config_surface_helper_tracks_generated_endpoint_switches() {
    let root = std::env::temp_dir().join(format!(
        "icydb-cli-config-surface-test-{}",
        std::process::id()
    ));
    let canister = root.join("canisters").join("demo").join("rpg");
    std::fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    std::fs::write(
        root.join("icydb.toml"),
        r#"
            [canisters.demo_rpg.sql]
            readonly = true
            ddl = false
            fixtures = true
            update = true

            [canisters.demo_rpg.metrics]
            enabled = true
            extended = false

            [canisters.demo_rpg.snapshot]
            enabled = true

            [canisters.demo_rpg.schema]
            enabled = true

            [canisters.admin_rpg.sql]
            update = "bounded"
        "#,
    )
    .expect("config should be written");
    let resolved = icydb_config_build::load_resolved_icydb_toml(canister.as_path(), &[])
        .expect("config should resolve");

    assert!(config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::SqlReadonly,
    ));
    assert!(!config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::SqlDdl,
    ));
    assert!(config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::SqlFixtures,
    ));
    assert!(config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::SqlUpdate,
    ));
    assert!(config_surface_enabled_for_resolved(
        &resolved,
        "admin_rpg",
        ConfigSurface::SqlUpdate,
    ));
    assert!(config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::Metrics,
    ));
    assert!(!config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::MetricsExtended,
    ));
    assert!(config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::Snapshot,
    ));
    assert!(config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::Schema,
    ));
    assert!(!config_surface_enabled_for_resolved(
        &resolved,
        "missing_rpg",
        ConfigSurface::Snapshot,
    ));
    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn configured_endpoint_helper_tracks_endpoint_surface_pairs() {
    let root = std::env::temp_dir().join(format!(
        "icydb-cli-configured-endpoint-test-{}",
        std::process::id()
    ));
    let canister = root.join("canisters").join("demo").join("rpg");
    std::fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    std::fs::write(
        root.join("icydb.toml"),
        r#"
            [canisters.demo_rpg.sql]
            readonly = true
            ddl = false
            fixtures = true
            update = true

            [canisters.demo_rpg.metrics]
            enabled = true
            extended = true

            [canisters.demo_rpg.snapshot]
            enabled = true

            [canisters.demo_rpg.schema]
            enabled = true

            [canisters.admin_rpg.sql]
            update = "bounded"
        "#,
    )
    .expect("config should be written");
    let resolved = icydb_config_build::load_resolved_icydb_toml(canister.as_path(), &[])
        .expect("config should resolve");

    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        SQL_QUERY_ENDPOINT,
    ));
    assert!(!configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        SQL_DDL_ENDPOINT,
    ));
    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        FIXTURES_LOAD_ENDPOINT,
    ));
    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        SQL_UPDATE_ENDPOINT,
    ));
    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "admin_rpg",
        SQL_UPDATE_ENDPOINT,
    ));
    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        METRICS_ENDPOINT,
    ));
    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        METRICS_EXTENDED_ENDPOINT,
    ));
    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        METRICS_RESET_ENDPOINT,
    ));
    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        SNAPSHOT_ENDPOINT,
    ));
    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        SCHEMA_ENDPOINT,
    ));
    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn disabled_config_surface_message_names_surface_key_and_rebuild_step() {
    let root = std::env::temp_dir().join(format!(
        "icydb-cli-config-diagnostic-test-{}",
        std::process::id()
    ));
    let canister = root.join("canisters").join("demo").join("rpg");
    std::fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    let config_path = root.join("icydb.toml");
    std::fs::write(
        config_path.as_path(),
        r"
            [canisters.demo_rpg.sql]
            readonly = true
        ",
    )
    .expect("config should be written");
    let resolved = icydb_config_build::load_resolved_icydb_toml(canister.as_path(), &[])
        .expect("config should resolve");

    let message = disabled_config_surface_message(&resolved, "demo_rpg", ConfigSurface::Metrics);

    assert!(message.contains("metrics"));
    assert!(message.contains("canisters.<name>.metrics.enabled"));
    assert!(message.contains(config_path.to_string_lossy().as_ref()));
    assert!(message.contains("rebuild and deploy"));
    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn configured_endpoint_methods_match_generated_endpoint_names() {
    assert_eq!(SNAPSHOT_ENDPOINT.method(), "__icydb_snapshot");
    assert_eq!(SCHEMA_ENDPOINT.method(), "__icydb_schema");
    assert_eq!(SCHEMA_CHECK_ENDPOINT.method(), "__icydb_schema_check");
    assert_eq!(METRICS_ENDPOINT.method(), "__icydb_metrics");
    assert_eq!(
        METRICS_EXTENDED_ENDPOINT.method(),
        "__icydb_metrics_extended"
    );
    assert_eq!(METRICS_RESET_ENDPOINT.method(), "__icydb_metrics_reset");
    assert_eq!(FIXTURES_LOAD_ENDPOINT.method(), "__icydb_fixtures_load");
    assert_eq!(SQL_QUERY_ENDPOINT.method(), "__icydb_query");
    assert_eq!(SQL_DDL_ENDPOINT.method(), "__icydb_ddl");
    assert_eq!(SQL_UPDATE_ENDPOINT.method(), "__icydb_update");
}
