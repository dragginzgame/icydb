use std::path::{Path, PathBuf};

use candid::{Decode, Encode};
use clap::Parser;
use icydb::db::sql::{SqlGroupedRowsOutput, SqlQueryResult, SqlQueryRowsOutput};
use serde_json::json;

use crate::{
    cli::{
        CanisterCommand, CliArgs, CliCommand, ConfigCommand, ConfigInitArgs, DEFAULT_ENVIRONMENT,
    },
    config::{
        ConfigSurface, FIXTURES_LOAD_ENDPOINT, METRICS_ENDPOINT, METRICS_RESET_ENDPOINT,
        SNAPSHOT_ENDPOINT, SQL_DDL_ENDPOINT, SQL_QUERY_ENDPOINT,
        config_surface_enabled_for_resolved, config_sync_issues,
        configured_endpoint_enabled_for_resolved, disabled_config_surface_message, init_config,
        render_config_report,
    },
    icp::fixtures_load_command,
    observability::{metrics_candid_arg, render_metrics_report, render_snapshot_report},
    shell::{
        ShellConfig, ShellPerfAttribution, SqlShellCallKind, drain_complete_shell_statements,
        finalize_successful_command_output, hex_response_bytes, icp_query_command,
        icp_update_command, is_shell_help_command, normalize_grouped_next_cursor_json,
        normalize_shell_statement_line, parse_perf_result, render_grouped_shell_text,
        render_perf_suffix, render_projection_shell_text, shell_help_text,
        sql_error_with_recovery_hint, sql_shell_call_kind,
    },
};

#[test]
fn parse_perf_result_accepts_candid_option_none_for_grouped_next_cursor() {
    let value = json!({
        "result": {
            "Grouped": {
                "entity": "Character",
                "columns": ["class_name", "COUNT(*)"],
                "rows": [["Bard", "5"]],
                "row_count": 1,
                "next_cursor": []
            }
        },
        "instructions": "1",
        "planner_instructions": "1",
        "store_instructions": "1",
        "executor_instructions": "1",
        "decode_instructions": "1",
        "compiler_instructions": "1"
    });

    let (result, _) = parse_perf_result(&value).expect("grouped perf result should decode");
    let grouped = match result {
        icydb::db::sql::SqlQueryResult::Grouped(grouped) => grouped,
        other => panic!("expected grouped result, got {other:?}"),
    };

    assert_eq!(grouped.next_cursor, None);
}

#[test]
fn normalize_grouped_next_cursor_json_converts_candid_some_to_plain_string() {
    let mut value = json!({
        "Grouped": {
            "entity": "Character",
            "columns": ["class_name", "COUNT(*)"],
            "rows": [["Bard", "5"]],
            "row_count": 1,
            "next_cursor": ["cursor-token"]
        }
    });

    normalize_grouped_next_cursor_json(&mut value);

    assert_eq!(
        value["Grouped"]["next_cursor"],
        json!("cursor-token"),
        "grouped next_cursor should normalize from candid option encoding",
    );
}

#[test]
fn render_perf_suffix_skips_zero_instruction_segments() {
    let suffix = render_perf_suffix(Some(&ShellPerfAttribution {
        total: 2_400,
        planner: 0,
        store: 0,
        executor: 1_900,
        pure_covering_decode: 0,
        pure_covering_row_assembly: 0,
        decode: 0,
        compiler: 500,
    }))
    .expect("non-zero perf attribution should render a footer");

    assert_eq!(suffix, "2.4Ki [cceeeeeeee]");
}

#[test]
fn render_perf_suffix_omits_empty_attribution() {
    assert!(
        render_perf_suffix(Some(&ShellPerfAttribution {
            total: 0,
            planner: 0,
            store: 0,
            executor: 0,
            pure_covering_decode: 0,
            pure_covering_row_assembly: 0,
            decode: 0,
            compiler: 0,
        }))
        .is_none(),
        "all-zero perf attribution should not render a footer",
    );
}

#[test]
fn render_perf_suffix_scales_bar_width_by_instruction_magnitude() {
    let suffix = render_perf_suffix(Some(&ShellPerfAttribution {
        total: 120_000_000,
        planner: 20_000_000,
        store: 20_000_000,
        executor: 40_000_000,
        pure_covering_decode: 0,
        pure_covering_row_assembly: 0,
        decode: 10_000_000,
        compiler: 10_000_000,
    }))
    .expect("large perf attribution should render a footer");

    assert_eq!(suffix, "120.0Mi [ccppppsssseeeeeeeeedd????]");
}

#[test]
fn render_perf_suffix_omits_unknown_bucket_when_top_level_attribution_is_exhaustive() {
    let suffix = render_perf_suffix(Some(&ShellPerfAttribution {
        total: 10_000_000,
        planner: 2_000_000,
        store: 2_000_000,
        executor: 3_000_000,
        pure_covering_decode: 0,
        pure_covering_row_assembly: 0,
        decode: 2_000_000,
        compiler: 1_000_000,
    }))
    .expect("complete perf attribution should render a footer");

    assert_eq!(suffix, "10.0Mi [ccppppsssseeeeeedddd]");
}

#[test]
fn render_perf_suffix_surfaces_unattributed_remainder_as_unknown_bucket() {
    let suffix = render_perf_suffix(Some(&ShellPerfAttribution {
        total: 10_000_000,
        planner: 1_000_000,
        store: 1_000_000,
        executor: 4_000_000,
        pure_covering_decode: 0,
        pure_covering_row_assembly: 0,
        decode: 1_000_000,
        compiler: 1_000_000,
    }))
    .expect("residual perf attribution should render a footer");

    assert_eq!(suffix, "10.0Mi [ccppsseeeeeeeedd????]");
}

#[test]
fn successful_command_output_keeps_one_blank_separator_line() {
    assert_eq!(
        finalize_successful_command_output("surface=explain"),
        "surface=explain\n\n",
    );
}

#[test]
fn help_command_matches_supported_spellings() {
    for input in ["?", "help", "\\?", "\\help", "help;", " ? "] {
        assert!(
            is_shell_help_command(input),
            "input should be treated as shell help: {input:?}",
        );
    }
}

#[test]
fn clap_help_exposes_short_canister_and_environment_flags() {
    for args in [
        ["icydb", "sql", "--help"].as_slice(),
        ["icydb", "snapshot", "--help"].as_slice(),
        ["icydb", "metrics", "--help"].as_slice(),
        ["icydb", "canister", "refresh", "--help"].as_slice(),
    ] {
        let help = clap_help_text(args);

        assert!(
            help.contains("-c, --canister"),
            "help should expose -c shorthand: {help}"
        );
        assert!(
            help.contains("-e, --environment"),
            "help should expose -e shorthand: {help}"
        );
    }
}

#[test]
fn clap_help_exposes_available_short_flags_on_config_commands() {
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

fn clap_help_text(args: &[&str]) -> String {
    let err = CliArgs::try_parse_from(args).expect_err("help invocation should exit through clap");

    assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);

    err.to_string()
}

#[test]
fn normalize_shell_statement_line_trims_surrounding_whitespace() {
    assert_eq!(
        normalize_shell_statement_line("   SELECT * FROM character   "),
        "SELECT * FROM character",
    );
}

#[test]
fn normalize_shell_statement_line_collapses_repeated_trailing_semicolons() {
    assert_eq!(normalize_shell_statement_line("  query();;   "), "query();",);
}

#[test]
fn normalize_shell_statement_line_preserves_semicolon_only_terminator_lines() {
    assert_eq!(normalize_shell_statement_line("  ;; "), ";");
}

#[test]
fn drain_complete_shell_statements_splits_multiple_pasted_queries() {
    let mut statement = String::from("SELECT 1;\nSELECT 2;");
    let drained = drain_complete_shell_statements(&mut statement);

    assert_eq!(
        drained.into_iter().collect::<Vec<_>>(),
        vec!["SELECT 1;".to_string(), "SELECT 2;".to_string()],
    );
    assert!(statement.is_empty());
}

#[test]
fn drain_complete_shell_statements_preserves_semicolons_inside_strings() {
    let mut statement = String::from("SELECT ';' AS marker;\nSELECT 2;");
    let drained = drain_complete_shell_statements(&mut statement);

    assert_eq!(
        drained.into_iter().collect::<Vec<_>>(),
        vec!["SELECT ';' AS marker;".to_string(), "SELECT 2;".to_string()],
    );
    assert!(statement.is_empty());
}

#[test]
fn drain_complete_shell_statements_keeps_incomplete_remainder() {
    let mut statement = String::from("SELECT 1;\nSELECT");
    let drained = drain_complete_shell_statements(&mut statement);

    assert_eq!(
        drained.into_iter().collect::<Vec<_>>(),
        vec!["SELECT 1;".to_string()]
    );
    assert_eq!(statement, "SELECT");
}

#[test]
fn shell_help_text_mentions_current_perf_legend() {
    let help = shell_help_text();

    assert!(help.contains("? / help         show this help"));
    assert!(help.contains("\\q / quit / exit quit the interactive shell"));
    assert!(!help.contains("icydb-cli help"));
    assert!(help.contains("c = compile"));
    assert!(help.contains("p = planner"));
    assert!(help.contains("s = store"));
    assert!(help.contains("e = executor"));
    assert!(help.contains("d = decode"));
    assert!(help.contains("{pc=.../...}"));
    assert!(help.contains("{er=...}"));
    assert!(help.contains("{r=...}"));
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
    let CliCommand::Sql(sql_args) = args.command else {
        panic!("expected sql command");
    };
    let config = ShellConfig::from_sql_args(sql_args);

    assert_eq!(config.canister, "test_sql");
    assert_eq!(config.environment, DEFAULT_ENVIRONMENT);
    assert_eq!(config.sql.as_deref(), Some("SELECT name FROM character;"));
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
    let CliCommand::Sql(sql_args) = args.command else {
        panic!("expected sql command");
    };
    let config = ShellConfig::from_sql_args(sql_args);

    assert_eq!(config.history_file, PathBuf::from(".cache/custom_history"));
    assert_eq!(config.environment, DEFAULT_ENVIRONMENT);
    assert_eq!(config.sql.as_deref(), Some("SELECT name FROM character;"));
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
    let CliCommand::Sql(sql_args) = args.command else {
        panic!("expected sql command");
    };
    let config = ShellConfig::from_sql_args(sql_args);

    assert_eq!(config.environment, "test");
    assert_eq!(config.sql.as_deref(), Some("SELECT * FROM character;"));
}

#[test]
fn cli_args_group_snapshot_under_top_level_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "snapshot",
        "--canister",
        "demo_rpg",
        "--environment",
        "test",
    ])
    .expect("snapshot command should parse");
    let CliCommand::Snapshot(target) = args.command else {
        panic!("expected snapshot command");
    };

    assert_eq!(target.canister_name(), "demo_rpg");
    assert_eq!(target.environment(), "test");
}

#[test]
fn cli_args_group_metrics_under_top_level_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "metrics",
        "--canister",
        "demo_rpg",
        "--window-start-ms",
        "123",
    ])
    .expect("metrics command should parse");
    let CliCommand::Metrics(args) = args.command else {
        panic!("expected metrics command");
    };

    assert_eq!(args.target().canister_name(), "demo_rpg");
    assert_eq!(args.target().environment(), DEFAULT_ENVIRONMENT);
    assert_eq!(args.window_start_ms(), Some(123));
    assert!(!args.reset());
}

#[test]
fn cli_args_group_metrics_reset_under_top_level_keyword() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "metrics",
        "--canister",
        "demo_rpg",
        "--environment",
        "test",
        "--reset",
    ])
    .expect("metrics reset command should parse");
    let CliCommand::Metrics(args) = args.command else {
        panic!("expected metrics command");
    };

    assert_eq!(args.target().canister_name(), "demo_rpg");
    assert_eq!(args.target().environment(), "test");
    assert_eq!(args.window_start_ms(), None);
    assert!(args.reset());
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
    let CliCommand::Config(ConfigCommand::Show(args)) = args.command else {
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
    let CliCommand::Config(ConfigCommand::Check(args)) = args.command else {
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
        "--metrics",
        "--metrics-reset",
        "--snapshot",
        "--start-dir",
        "canisters/demo/rpg",
    ])
    .expect("config init should parse");
    let CliCommand::Config(ConfigCommand::Init(args)) = args.command else {
        panic!("expected config init command");
    };

    assert_eq!(args.canister_name(), "demo_rpg");
    assert!(args.readonly());
    assert!(args.ddl());
    assert!(args.fixtures());
    assert!(args.metrics());
    assert!(args.metrics_reset());
    assert!(args.snapshot());
    assert_eq!(args.start_dir(), Some(Path::new("canisters/demo/rpg")));
}

#[test]
fn cli_args_group_canister_list_under_canister_keyword() {
    let args = CliArgs::try_parse_from(["icydb", "canister", "list", "--environment", "test"])
        .expect("canister list should parse");
    let CliCommand::Canister(CanisterCommand::List(target)) = args.command else {
        panic!("expected canister list command");
    };

    assert_eq!(target.environment(), "test");
}

#[test]
fn config_init_writes_default_config_at_workspace_root() {
    let root =
        std::env::temp_dir().join(format!("icydb-cli-config-init-test-{}", std::process::id()));
    let workspace = root.join("workspace");
    let canister = workspace.join("canisters").join("demo").join("rpg");
    std::fs::create_dir_all(canister.as_path()).expect("test directory should be created");
    std::fs::write(workspace.join("Cargo.toml"), "[workspace]\n")
        .expect("workspace manifest should be written");

    init_config(ConfigInitArgs {
        start_dir: Some(canister),
        canister: "demo_rpg".to_string(),
        ddl: true,
        fixtures: true,
        metrics: true,
        metrics_reset: true,
        snapshot: true,
        all: false,
        no_readonly: false,
        force: false,
    })
    .expect("config init should succeed");

    let config = std::fs::read_to_string(workspace.join("icydb.toml"))
        .expect("config file should be written");
    assert_eq!(
        config,
        "[canisters.demo_rpg.sql]\nreadonly = true\nddl = true\nfixtures = true\n\n[canisters.demo_rpg.metrics]\nenabled = true\nreset = true\n\n[canisters.demo_rpg.snapshot]\nenabled = true\n"
    );

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
        ",
    )
    .expect("config should be written");
    let resolved = icydb_config_build::load_resolved_icydb_toml(canister.as_path(), &["demo_rpg"])
        .expect("config should resolve");

    let report = render_config_report(
        canister.as_path(),
        Some("demo"),
        &[String::from("demo_rpg")],
        &resolved,
    );

    assert!(report.contains("demo_rpg  readonly, ddl, fixtures  enabled, reset  enabled   ok"));
    std::fs::remove_dir_all(root).expect("test directory should be removed");
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
        r"
            [canisters.demo_rpg.sql]
            readonly = true
            ddl = false
            fixtures = true

            [canisters.demo_rpg.metrics]
            enabled = true
            reset = false

            [canisters.demo_rpg.snapshot]
            enabled = true
        ",
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
        ConfigSurface::Metrics,
    ));
    assert!(!config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::MetricsReset,
    ));
    assert!(config_surface_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        ConfigSurface::Snapshot,
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
        r"
            [canisters.demo_rpg.sql]
            readonly = true
            ddl = false
            fixtures = true

            [canisters.demo_rpg.metrics]
            enabled = true
            reset = false

            [canisters.demo_rpg.snapshot]
            enabled = true
        ",
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
        METRICS_ENDPOINT,
    ));
    assert!(!configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        METRICS_RESET_ENDPOINT,
    ));
    assert!(configured_endpoint_enabled_for_resolved(
        &resolved,
        "demo_rpg",
        SNAPSHOT_ENDPOINT,
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

    let message =
        disabled_config_surface_message(&resolved, "demo_rpg", ConfigSurface::MetricsReset);

    assert!(message.contains("metrics reset"));
    assert!(message.contains("canisters.<name>.metrics.reset"));
    assert!(message.contains(config_path.to_string_lossy().as_ref()));
    assert!(message.contains("rebuild and deploy"));
    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[test]
fn cli_args_group_canister_status_under_canister_keyword() {
    let args = CliArgs::try_parse_from(["icydb", "canister", "status", "--canister", "demo"])
        .expect("canister status should parse");
    let CliCommand::Canister(CanisterCommand::Status(target)) = args.command else {
        panic!("expected canister status command");
    };

    assert_eq!(target.canister_name(), "demo");
}

#[test]
fn cli_args_group_canister_refresh_under_canister_keyword() {
    let args =
        CliArgs::try_parse_from(["icydb", "canister", "refresh", "-c", "demo", "-e", "test"])
            .expect("canister refresh should parse");
    let CliCommand::Canister(CanisterCommand::Refresh(target)) = args.command else {
        panic!("expected canister refresh command");
    };

    assert_eq!(target.canister_name(), "demo");
    assert_eq!(target.environment(), "test");
}

#[test]
fn icp_query_command_targets_environment_and_hex_query_output() {
    let command = icp_query_command(
        "demo",
        "demo_rpg",
        SQL_QUERY_ENDPOINT.method(),
        "(\"SELECT 1\")",
    );
    let args = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert_eq!(command.get_program().to_string_lossy(), "icp");
    assert_eq!(
        args,
        vec![
            "canister",
            "call",
            "demo_rpg",
            "__icydb_query",
            "(\"SELECT 1\")",
            "--query",
            "--output",
            "hex",
            "--environment",
            "demo",
        ],
    );
}

#[test]
fn configured_endpoint_methods_match_generated_endpoint_names() {
    assert_eq!(SNAPSHOT_ENDPOINT.method(), "__icydb_snapshot");
    assert_eq!(METRICS_ENDPOINT.method(), "__icydb_metrics");
    assert_eq!(METRICS_RESET_ENDPOINT.method(), "__icydb_metrics_reset");
    assert_eq!(FIXTURES_LOAD_ENDPOINT.method(), "__icydb_fixtures_load");
    assert_eq!(SQL_QUERY_ENDPOINT.method(), "__icydb_query");
    assert_eq!(SQL_DDL_ENDPOINT.method(), "__icydb_ddl");
}

#[test]
fn metrics_candid_arg_renders_optional_window() {
    assert_eq!(metrics_candid_arg(None), "(null)");
    assert_eq!(metrics_candid_arg(Some(123)), "(opt (123 : nat64))");
}

#[test]
fn snapshot_report_rendering_uses_human_tables() {
    let text = render_snapshot_report(&icydb::db::StorageReport::default());

    assert!(text.contains("IcyDB storage snapshot"));
    assert!(text.contains("data stores\n  None"));
    assert!(text.contains("index stores\n  None"));
    assert!(text.contains("entities\n  None"));
}

#[test]
fn metrics_report_rendering_uses_human_summary() {
    let text = render_metrics_report(&icydb::metrics::EventReport::default());

    assert!(text.contains("IcyDB metrics"));
    assert!(text.contains("requested window start ms: none"));
    assert!(text.contains("counters: none"));
    assert!(text.contains("entities\n  None"));
}

#[test]
fn icp_update_command_targets_environment_without_query_flag() {
    let command = icp_update_command(
        "demo",
        "demo_rpg",
        SQL_DDL_ENDPOINT.method(),
        "(\"CREATE INDEX name_idx\")",
    );
    let args = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert_eq!(command.get_program().to_string_lossy(), "icp");
    assert_eq!(
        args,
        vec![
            "canister",
            "call",
            "demo_rpg",
            "__icydb_ddl",
            "(\"CREATE INDEX name_idx\")",
            "--output",
            "hex",
            "--environment",
            "demo",
        ],
    );
}

#[test]
fn fixtures_load_command_targets_fixed_generated_endpoint() {
    let command = fixtures_load_command("demo", "demo_rpg");
    let args = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert_eq!(command.get_program().to_string_lossy(), "icp");
    assert_eq!(
        args,
        vec![
            "canister",
            "call",
            "demo_rpg",
            "__icydb_fixtures_load",
            "()",
            "--environment",
            "demo",
        ],
    );
}

#[test]
fn sql_recovery_hint_points_stale_canister_to_targeted_refresh() {
    let message = sql_error_with_recovery_hint(
        "Canister has no query method '__icydb_query'.",
        DEFAULT_ENVIRONMENT,
        "demo_rpg",
    );

    assert!(
        message.contains("icydb canister refresh --environment demo --canister demo_rpg"),
        "stale canister errors should include a targeted refresh command"
    );
}

#[test]
fn sql_recovery_hint_leaves_unrelated_errors_unchanged() {
    let error = "SQL DDL execution is not supported in this release";

    assert_eq!(
        sql_error_with_recovery_hint(error, DEFAULT_ENVIRONMENT, "demo_rpg"),
        error,
    );
}

#[test]
fn sql_shell_call_kind_routes_supported_ddl_to_update_method() {
    assert_eq!(
        sql_shell_call_kind("CREATE INDEX name_idx ON Character (name);"),
        SqlShellCallKind::Ddl,
    );
    assert_eq!(
        sql_shell_call_kind("  create   index name_idx ON Character (name)  ; "),
        SqlShellCallKind::Ddl,
    );
    assert_eq!(
        sql_shell_call_kind("SELECT * FROM Character"),
        SqlShellCallKind::Query,
    );
    assert_eq!(
        sql_shell_call_kind("CREATE UNIQUE INDEX name_idx ON Character (name)"),
        SqlShellCallKind::Query,
    );
}

#[test]
fn ddl_response_rendering_includes_execution_metrics() {
    let response: Result<SqlQueryResult, icydb::Error> = Ok(SqlQueryResult::Ddl {
        entity: "Character".to_string(),
        mutation_kind: "add_non_unique_field_path_index".to_string(),
        target_index: "character_level_idx".to_string(),
        target_store: "demo::CharacterStore".to_string(),
        field_path: vec!["level".to_string()],
        status: "published".to_string(),
        rows_scanned: 7,
        index_keys_written: 7,
    });
    let candid_bytes = Encode!(&response).expect("DDL response should encode");
    let decoded = Decode!(
        candid_bytes.as_slice(),
        Result<SqlQueryResult, icydb::Error>
    )
    .expect("DDL response should decode")
    .expect("DDL response should succeed");

    assert_eq!(
        decoded.render_text(),
        "surface=ddl entity=Character mutation_kind=add_non_unique_field_path_index target_index=character_level_idx target_store=demo::CharacterStore field_path=level status=published rows_scanned=7 index_keys_written=7",
        "CLI DDL response rendering should surface rebuild metrics from the decoded canister payload",
    );
}

#[test]
fn hex_response_bytes_accepts_plain_or_labeled_icp_hex_output() {
    assert_eq!(
        hex_response_bytes("4449444c00017f").expect("plain hex should parse"),
        vec![0x44, 0x49, 0x44, 0x4c, 0x00, 0x01, 0x7f],
    );
    assert_eq!(
        hex_response_bytes("response (hex): 44 49 44 4c").expect("labeled hex should parse"),
        vec![0x44, 0x49, 0x44, 0x4c],
    );
}

#[test]
fn projection_shell_text_leaves_footer_without_embedded_trailing_blank_line() {
    let rendered = render_projection_shell_text(
        SqlQueryRowsOutput {
            entity: "Character".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec!["alice".to_string()]],
            row_count: 1,
        },
        None,
        None,
    );

    assert!(
        rendered.ends_with("1 row,"),
        "projection shell output should leave footer formatting to the command boundary: {rendered:?}",
    );
}

#[test]
fn grouped_shell_text_leaves_footer_without_embedded_trailing_blank_line() {
    let rendered = render_grouped_shell_text(
        SqlGroupedRowsOutput {
            entity: "Character".to_string(),
            columns: vec!["class_name".to_string(), "COUNT(*)".to_string()],
            rows: vec![vec!["Bard".to_string(), "5".to_string()]],
            row_count: 1,
            next_cursor: None,
        },
        None,
        None,
    );

    assert!(
        rendered.ends_with("1 row,"),
        "grouped shell output should leave footer formatting to the command boundary: {rendered:?}",
    );
}
