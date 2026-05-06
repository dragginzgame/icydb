use std::path::PathBuf;

use clap::Parser;
use icydb::db::sql::{SqlGroupedRowsOutput, SqlQueryRowsOutput};
use serde_json::json;

use crate::{
    cli::{CanisterCommand, CliArgs, CliCommand, DEFAULT_CANISTER, DevCommand, FixturesCommand},
    shell::{
        ShellConfig, ShellPerfAttribution, drain_complete_shell_statements,
        finalize_successful_command_output, is_shell_help_command,
        normalize_grouped_next_cursor_json, normalize_shell_statement_line, parse_perf_result,
        render_grouped_shell_text, render_perf_suffix, render_projection_shell_text,
        shell_help_text,
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
    assert_eq!(config.sql.as_deref(), Some("SELECT name FROM character;"));
}

#[test]
fn cli_args_accept_explicit_sql_option() {
    let args = CliArgs::try_parse_from([
        "icydb",
        "sql",
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
    assert_eq!(config.sql.as_deref(), Some("SELECT name FROM character;"));
}

#[test]
fn cli_args_default_sql_target_to_demo_rpg() {
    let args = CliArgs::try_parse_from(["icydb", "sql", "SELECT * FROM character;"])
        .expect("sql command should parse without explicit canister");
    let CliCommand::Sql(sql_args) = args.command else {
        panic!("expected sql command");
    };
    let config = ShellConfig::from_sql_args(sql_args);

    assert_eq!(config.canister, DEFAULT_CANISTER);
    assert_eq!(config.sql.as_deref(), Some("SELECT * FROM character;"));
}

#[test]
fn cli_args_group_canister_list_under_canister_keyword() {
    let args =
        CliArgs::try_parse_from(["icydb", "canister", "list"]).expect("canister list should parse");
    let CliCommand::Canister(CanisterCommand::List) = args.command else {
        panic!("expected canister list command");
    };
}

#[test]
fn cli_args_group_fixture_reload_under_fixtures_keyword() {
    let args = CliArgs::try_parse_from(["icydb", "fixtures", "reload", "--canister", "demo"])
        .expect("fixtures reload should parse");
    let CliCommand::Fixtures(FixturesCommand::Reload(target)) = args.command else {
        panic!("expected fixtures reload command");
    };

    assert_eq!(target.canister_name(), "demo");
}

#[test]
fn cli_args_group_dev_init_under_dev_keyword() {
    let args = CliArgs::try_parse_from(["icydb", "dev", "init", "--canister", "demo"])
        .expect("dev init should parse");
    let CliCommand::Dev(DevCommand::Init(target)) = args.command else {
        panic!("expected dev init command");
    };

    assert_eq!(target.canister_name(), "demo");
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
