//! Module: CLI argument parsing tests.
//! Responsibility: exercise clap-derived command surfaces and target defaults.
//! Does not own: command execution or helper rendering.
//! Boundary: test-only assertions over parsed `CliArgs`.

use std::path::Path;

use clap::Parser;

use crate::{
    cli::{
        CanisterCommand, CliArgs, CliCommand, ConfigCommand, DEFAULT_ENVIRONMENT, SchemaCommand,
    },
    shell::test_support::sql_shell_config_inputs,
};

#[test]
fn clap_help_exposes_target_environment_flags() {
    for args in [
        ["icydb", "snapshot", "--help"].as_slice(),
        ["icydb", "metrics", "--help"].as_slice(),
        ["icydb", "schema", "show", "--help"].as_slice(),
        ["icydb", "schema", "check", "--help"].as_slice(),
        ["icydb", "canister", "refresh", "--help"].as_slice(),
    ] {
        let help = clap_help_text(args);

        assert!(
            help.contains("<CANISTER>"),
            "help should expose positional canister target: {help}"
        );
        assert!(
            help.contains("-e, --environment"),
            "help should expose -e shorthand: {help}"
        );
        assert!(
            !help.contains("-c, --canister"),
            "target commands should not expose duplicate -c canister target: {help}"
        );
    }
}

#[test]
fn clap_help_exposes_available_short_flags_on_config_commands() {
    let sql_help = clap_help_text(["icydb", "sql", "--help"].as_slice());
    assert!(sql_help.contains("-c, --canister"));
    assert!(sql_help.contains("including supported DDL"));
    assert!(sql_help.contains("icydb sql -c demo_rpg"));
    assert!(sql_help.contains("CREATE INDEX character_renown_idx ON character (renown)"));
    assert!(sql_help.contains("DROP INDEX character_renown_idx ON character"));

    let init_help = clap_help_text(["icydb", "config", "init", "--help"].as_slice());
    assert!(init_help.contains("-c, --canister"));

    for args in [
        ["icydb", "config", "show", "--help"].as_slice(),
        ["icydb", "config", "check", "--help"].as_slice(),
    ] {
        let help = clap_help_text(args);
        assert!(
            help.contains("-e, --environment"),
            "help should expose -e shorthand: {help}"
        );
    }
}

#[test]
fn cli_args_group_diagnostic_lookup_under_top_level_keyword() {
    let args =
        CliArgs::try_parse_from(["icydb", "diagnostic", "E7"]).expect("diagnostic should parse");
    let CliCommand::Diagnostic(args) = args.into_command() else {
        panic!("expected diagnostic command");
    };

    assert_eq!(args.code(), "E7");
}

fn clap_help_text(args: &[&str]) -> String {
    let err = CliArgs::try_parse_from(args).expect_err("help invocation should exit through clap");

    assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);

    err.to_string()
}

#[test]
fn cli_args_preserve_trailing_sql_convenience_form() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "sql",
        "--canister",
        "test_sql",
        "SELECT",
        "name",
        "FROM",
        "character;",
    ])
    .expect("trailing SQL should parse");
    let CliCommand::Sql(sql_args) = args.into_command() else {
        panic!("expected sql command");
    };
    let (canister, environment, _, sql) = sql_shell_config_inputs(sql_args);

    assert_eq!(canister, "test_sql");
    assert_eq!(environment, DEFAULT_ENVIRONMENT);
    assert_eq!(sql.as_deref(), Some("SELECT name FROM character;"));
}

#[test]
fn cli_args_accept_explicit_sql_option() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "sql",
        "--canister",
        "demo_rpg",
        "--history-file",
        ".cache/custom_history",
        "--sql",
        "SELECT name FROM character;",
    ])
    .expect("--sql should parse");
    let CliCommand::Sql(sql_args) = args.into_command() else {
        panic!("expected sql command");
    };
    let (_, environment, history_file, sql) = sql_shell_config_inputs(sql_args);

    assert_eq!(history_file, Path::new(".cache/custom_history"));
    assert_eq!(environment, DEFAULT_ENVIRONMENT);
    assert_eq!(sql.as_deref(), Some("SELECT name FROM character;"));
}

#[test]
fn cli_args_require_sql_target_canister() {
    let err = CliArgs::try_parse_from(["icydb", "sql", "SELECT * FROM character;"])
        .expect_err("sql command should require explicit canister");

    assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
}

#[test]
fn cli_args_accept_explicit_icp_environment() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "sql",
        "--canister",
        "demo_rpg",
        "--environment",
        "test",
        "SELECT",
        "*",
        "FROM",
        "character;",
    ])
    .expect("sql environment should parse");
    let CliCommand::Sql(sql_args) = args.into_command() else {
        panic!("expected sql command");
    };
    let (_, environment, _, sql) = sql_shell_config_inputs(sql_args);

    assert_eq!(environment, "test");
    assert_eq!(sql.as_deref(), Some("SELECT * FROM character;"));
}

#[test]
fn cli_args_group_snapshot_under_top_level_keyword() {
    let args = CliArgs::try_parse_from(["icydb", "snapshot", "demo_rpg", "--environment", "test"])
        .expect("snapshot command should parse");
    let CliCommand::Snapshot(target) = args.into_command() else {
        panic!("expected snapshot command");
    };

    assert_eq!(target.canister_name(), "demo_rpg");
    assert_eq!(target.environment(), "test");
}

#[test]
fn cli_args_reject_canister_flag_on_target_commands() {
    let err = CliArgs::try_parse_from(["icydb", "snapshot", "--canister", "demo_rpg"])
        .expect_err("snapshot command should reject flagged canister target");

    assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
}

#[test]
fn cli_args_group_metrics_under_top_level_keyword() {
    let args =
        CliArgs::try_parse_from(["icydb", "metrics", "demo_rpg", "--window-start-ms", "123"])
            .expect("metrics command should parse");
    let CliCommand::Metrics(args) = args.into_command() else {
        panic!("expected metrics command");
    };

    assert_eq!(args.target().canister_name(), "demo_rpg");
    assert_eq!(args.target().environment(), DEFAULT_ENVIRONMENT);
    assert_eq!(args.window_start_ms(), Some(123));
    assert!(!args.extended());
    assert!(!args.reset());
}

#[test]
fn cli_args_group_extended_metrics_under_top_level_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "metrics",
        "demo_rpg",
        "--extended",
        "--window-start-ms",
        "123",
    ])
    .expect("extended metrics command should parse");
    let CliCommand::Metrics(args) = args.into_command() else {
        panic!("expected metrics command");
    };

    assert_eq!(args.target().canister_name(), "demo_rpg");
    assert_eq!(args.target().environment(), DEFAULT_ENVIRONMENT);
    assert_eq!(args.window_start_ms(), Some(123));
    assert!(args.extended());
    assert!(!args.reset());
}

#[test]
fn cli_args_reject_extended_metrics_reset_conflict() {
    let err = CliArgs::try_parse_from(["icydb", "metrics", "demo_rpg", "--extended", "--reset"])
        .expect_err("metrics reset and extended report should conflict");

    assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
}

#[test]
fn cli_args_group_metrics_reset_under_top_level_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "metrics",
        "demo_rpg",
        "--environment",
        "test",
        "--reset",
    ])
    .expect("metrics reset command should parse");
    let CliCommand::Metrics(args) = args.into_command() else {
        panic!("expected metrics command");
    };

    assert_eq!(args.target().canister_name(), "demo_rpg");
    assert_eq!(args.target().environment(), "test");
    assert_eq!(args.window_start_ms(), None);
    assert!(!args.extended());
    assert!(args.reset());
}

#[test]
fn cli_args_group_schema_under_top_level_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "schema",
        "show",
        "demo_rpg",
        "--environment",
        "test",
    ])
    .expect("schema show command should parse");
    let CliCommand::Schema(SchemaCommand::Show(target)) = args.into_command() else {
        panic!("expected schema command");
    };

    assert_eq!(target.canister_name(), "demo_rpg");
    assert_eq!(target.environment(), "test");
}

#[test]
fn cli_args_group_schema_check_under_schema_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "schema",
        "check",
        "demo_rpg",
        "--environment",
        "test",
    ])
    .expect("schema check command should parse");
    let CliCommand::Schema(SchemaCommand::Check(target)) = args.into_command() else {
        panic!("expected schema check command");
    };

    assert_eq!(target.canister_name(), "demo_rpg");
    assert_eq!(target.environment(), "test");
}

#[test]
fn cli_args_group_config_show_under_config_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "show",
        "--environment",
        "demo",
        "--start-dir",
        "canisters/demo/rpg",
    ])
    .expect("config show should parse");
    let CliCommand::Config(ConfigCommand::Show(args)) = args.into_command() else {
        panic!("expected config show command");
    };

    assert_eq!(args.environment(), Some("demo"));
    assert_eq!(args.start_dir(), Some(Path::new("canisters/demo/rpg")));
}

#[test]
fn cli_args_group_config_check_under_config_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "check",
        "--environment",
        "demo",
        "--start-dir",
        "canisters/demo/rpg",
    ])
    .expect("config check should parse");
    let CliCommand::Config(ConfigCommand::Check(args)) = args.into_command() else {
        panic!("expected config check command");
    };

    assert_eq!(args.environment(), Some("demo"));
    assert_eq!(args.start_dir(), Some(Path::new("canisters/demo/rpg")));
}

#[test]
fn cli_args_group_config_init_under_config_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--canister",
        "demo_rpg",
        "--ddl",
        "--fixtures",
        "--update",
        "--metrics",
        "--metrics-extended",
        "--snapshot",
        "--schema",
        "--start-dir",
        "canisters/demo/rpg",
    ])
    .expect("config init should parse");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    assert_eq!(args.canister_name(), "demo_rpg");
    assert!(args.readonly());
    assert!(args.ddl());
    assert!(args.fixtures());
    assert_eq!(args.update_config_value(), "true");
    assert!(args.metrics());
    assert!(args.metrics_extended_local());
    assert!(!args.metrics_extended_ic());
    assert!(args.snapshot());
    assert!(args.schema());
    assert_eq!(args.start_dir(), Some(Path::new("canisters/demo/rpg")));
}

#[test]
fn cli_args_config_init_update_policy_enables_bounded_update() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--canister",
        "demo_rpg",
        "--update-policy",
        "bounded",
    ])
    .expect("config init should parse bounded update policy");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    assert_eq!(args.update_config_value(), "\"bounded\"");
}

#[test]
fn cli_args_config_init_update_policy_accepts_primary_key_alias() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--canister",
        "demo_rpg",
        "--update-policy",
        "primary_key",
    ])
    .expect("config init should parse primary-key update policy alias");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    assert_eq!(args.update_config_value(), "\"primary_key\"");
}

#[test]
fn cli_args_config_init_no_readonly_overrides_all() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--canister",
        "demo_rpg",
        "--all",
        "--no-readonly",
    ])
    .expect("config init should parse all without readonly");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    assert!(!args.readonly());
    assert!(args.ddl());
    assert!(args.fixtures());
    assert_eq!(args.update_config_value(), "true");
    assert!(args.metrics());
    assert!(args.metrics_extended_local());
    assert!(!args.metrics_extended_ic());
    assert!(args.snapshot());
    assert!(args.schema());
}

#[test]
fn cli_args_config_init_metrics_extended_implies_metrics_surface() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--canister",
        "demo_rpg",
        "--metrics-extended",
    ])
    .expect("extended metrics config init should parse");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    assert!(args.metrics());
    assert!(args.metrics_extended_local());
    assert!(!args.metrics_extended_ic());
}

#[test]
fn cli_args_config_init_metrics_extended_ic_is_separate_target() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "config",
        "init",
        "--canister",
        "demo_rpg",
        "--metrics-extended-ic",
    ])
    .expect("IC extended metrics config init should parse");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.into_command() else {
        panic!("expected config init command");
    };

    assert!(args.metrics());
    assert!(!args.metrics_extended_local());
    assert!(args.metrics_extended_ic());
}

#[test]
fn cli_args_group_canister_list_under_canister_keyword() {
    let args = CliArgs::try_parse_from(["icydb", "canister", "list", "--environment", "test"])
        .expect("canister list should parse");
    let CliCommand::Canister(CanisterCommand::List(target)) = args.into_command() else {
        panic!("expected canister list command");
    };

    assert_eq!(target.environment(), "test");
}

#[test]
fn cli_args_group_canister_status_under_canister_keyword() {
    let args = CliArgs::try_parse_from(["icydb", "canister", "status", "demo"])
        .expect("canister status should parse");
    let CliCommand::Canister(CanisterCommand::Status(target)) = args.into_command() else {
        panic!("expected canister status command");
    };

    assert_eq!(target.canister_name(), "demo");
}

#[test]
fn cli_args_group_canister_refresh_under_canister_keyword() {
    let args = CliArgs::try_parse_from(["icydb", "canister", "refresh", "demo", "-e", "test"])
        .expect("canister refresh should parse");
    let CliCommand::Canister(CanisterCommand::Refresh(target)) = args.into_command() else {
        panic!("expected canister refresh command");
    };

    assert_eq!(target.canister_name(), "demo");
    assert_eq!(target.environment(), "test");
}

#[test]
fn cli_args_group_canister_deploy_under_canister_keyword() {
    let args = CliArgs::try_parse_from(["icydb", "canister", "deploy", "demo", "-e", "test"])
        .expect("canister deploy should parse");
    let CliCommand::Canister(CanisterCommand::Deploy(target)) = args.into_command() else {
        panic!("expected canister deploy command");
    };

    assert_eq!(target.canister_name(), "demo");
    assert_eq!(target.environment(), "test");
}

#[test]
fn cli_args_group_canister_upgrade_under_canister_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "canister",
        "upgrade",
        "demo",
        "-e",
        "test",
        "--wasm",
        ".icp/local/canisters/demo/demo.wasm",
    ])
    .expect("canister upgrade should parse");
    let CliCommand::Canister(CanisterCommand::Upgrade(args)) = args.into_command() else {
        panic!("expected canister upgrade command");
    };

    assert_eq!(args.target().canister_name(), "demo");
    assert_eq!(args.target().environment(), "test");
    assert_eq!(
        args.wasm(),
        Some(Path::new(".icp/local/canisters/demo/demo.wasm")),
    );
}
