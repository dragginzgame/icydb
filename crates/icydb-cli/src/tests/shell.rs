//! Module: SQL shell tests.
//! Responsibility: exercise shell input normalization, routing, decoding, and output rendering.
//! Does not own: top-level clap parsing or ICP process command construction.
//! Boundary: test-only assertions over shell helpers and decoded SQL payload text.

use crate::{
    cli::DEFAULT_ENVIRONMENT,
    shell::test_support::{
        SqlShellCallKind, candid_escape_string, drain_complete_shell_statements,
        finalize_successful_command_output, interactive_start_message, is_shell_exit_command,
        is_shell_help_command, normalize_shell_statement_line, render_grouped_shell_text,
        render_perf_suffix, render_projection_shell_text, shell_help_text, shell_perf_attribution,
        sql_error_with_recovery_hint, sql_shell_call_kind,
    },
};
use candid::{Decode, Encode};
use icydb::{
    db::{
        RowProjectionOutput,
        sql::{SqlGroupedRowsOutput, SqlQueryResult},
    },
    value::OutputValue,
};

#[test]
fn render_perf_suffix_skips_zero_instruction_segments() {
    let suffix = render_perf_suffix(Some(&shell_perf_attribution(2_400, 500, 0, 0, 1_900, 0)))
        .expect("non-zero perf attribution should render a footer");

    assert_eq!(suffix, "2.4Ki [cceeeeeeee]");
}

#[test]
fn render_perf_suffix_omits_empty_attribution() {
    assert!(
        render_perf_suffix(Some(&shell_perf_attribution(0, 0, 0, 0, 0, 0))).is_none(),
        "all-zero perf attribution should not render a footer",
    );
}

#[test]
fn render_perf_suffix_scales_bar_width_by_instruction_magnitude() {
    let suffix = render_perf_suffix(Some(&shell_perf_attribution(
        120_000_000,
        10_000_000,
        20_000_000,
        20_000_000,
        40_000_000,
        10_000_000,
    )))
    .expect("large perf attribution should render a footer");

    assert_eq!(suffix, "120.0Mi [ccppppsssseeeeeeeeedd????]");
}

#[test]
fn render_perf_suffix_omits_unknown_bucket_when_top_level_attribution_is_exhaustive() {
    let suffix = render_perf_suffix(Some(&shell_perf_attribution(
        10_000_000, 1_000_000, 2_000_000, 2_000_000, 3_000_000, 2_000_000,
    )))
    .expect("complete perf attribution should render a footer");

    assert_eq!(suffix, "10.0Mi [ccppppsssseeeeeedddd]");
}

#[test]
fn render_perf_suffix_surfaces_unattributed_remainder_as_unknown_bucket() {
    let suffix = render_perf_suffix(Some(&shell_perf_attribution(
        10_000_000, 1_000_000, 1_000_000, 1_000_000, 4_000_000, 1_000_000,
    )))
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
    for input in [
        "?", "help", "HELP", "\\?", "\\help", "\\HELP", "help;", " ? ",
    ] {
        assert!(
            is_shell_help_command(input),
            "input should be treated as shell help: {input:?}",
        );
    }
}

#[test]
fn exit_command_matches_supported_spellings_case_insensitively() {
    for input in ["\\q", "\\Q", "quit", "QUIT", "exit", "EXIT"] {
        assert!(
            is_shell_exit_command(input),
            "input should be treated as shell exit: {input:?}",
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
fn drain_complete_shell_statements_preserves_semicolons_after_escaped_quote() {
    let mut statement = String::from("SELECT 'it\\'s; ok' AS marker;\nSELECT 2;");
    let drained = drain_complete_shell_statements(&mut statement);

    assert_eq!(
        drained.into_iter().collect::<Vec<_>>(),
        vec![
            "SELECT 'it\\'s; ok' AS marker;".to_string(),
            "SELECT 2;".to_string()
        ],
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
    assert!(help.contains("CREATE INDEX character_level_idx ON character (level);"));
    assert!(help.contains("SHOW INDEXES FROM character;"));
    assert!(help.contains("DESCRIBE character;"));
    assert!(help.contains("DROP INDEX character_level_idx ON character;"));
}

#[test]
fn interactive_start_message_names_target_and_exit_controls() {
    let message = interactive_start_message("test", "demo_rpg");

    assert!(message.contains("'test:demo_rpg'"));
    assert!(message.contains("terminate statements with ';'"));
    assert!(message.contains("\\q, exit, or Ctrl-D"));
}

#[test]
fn sql_recovery_hint_points_stale_canister_to_targeted_refresh() {
    let message = sql_error_with_recovery_hint(
        "Canister has no query method 'icydb_query'.",
        DEFAULT_ENVIRONMENT,
        "demo_rpg",
    );

    assert!(
        message.contains("icydb canister refresh demo_rpg --environment demo"),
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
fn candid_escape_string_escapes_sql_for_wire_arg() {
    assert_eq!(
        candid_escape_string("SELECT \"name\\path\"\nFROM Character\tWHERE note = 'a\rb'"),
        "SELECT \\\"name\\\\path\\\"\\nFROM Character\\tWHERE note = 'a\\rb'",
    );
}

#[test]
fn sql_shell_call_kind_routes_sql_to_configured_endpoint_family() {
    for sql in [
        "CREATE INDEX name_idx ON Character (name);",
        "  create   index name_idx ON Character (name)  ; ",
        "CREATE INDEX IF NOT EXISTS name_idx ON Character (name);",
        "DROP INDEX name_idx ON Character;",
        "DROP INDEX name_idx;",
        "  drop   index name_idx ON Character  ; ",
        "DROP INDEX IF EXISTS name_idx ON Character;",
        "CREATE UNIQUE INDEX name_idx ON Character (name)",
        "ALTER TABLE Character ADD COLUMN nickname text",
        "ALTER TABLE Character ALTER COLUMN score SET DEFAULT 7",
    ] {
        assert_eq!(
            sql_shell_call_kind(sql).expect("SQL should parse"),
            SqlShellCallKind::Ddl,
        );
    }

    for sql in [
        "SELECT * FROM Character",
        "SHOW INDEXES FROM Character",
        "INSERT INTO Character (id, name) VALUES (1, 'Ada')",
        "DELETE FROM Character WHERE id = 1",
    ] {
        assert_eq!(
            sql_shell_call_kind(sql).expect("SQL should parse"),
            SqlShellCallKind::Query,
        );
    }

    assert_eq!(
        sql_shell_call_kind("UPDATE Character SET name = 'Ada' WHERE id = 1")
            .expect("SQL should parse"),
        SqlShellCallKind::Update,
    );
}

#[test]
fn ddl_response_rendering_includes_execution_metrics() {
    let response: Result<SqlQueryResult, icydb::Error> = Ok(SqlQueryResult::Ddl {
        entity: "Character".to_string(),
        mutation_kind: "add_field_path_index".to_string(),
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
        "surface=ddl entity=Character mutation_kind=add_field_path_index target_index=character_level_idx target_store=demo::CharacterStore field_path=level status=published rows_scanned=7 index_keys_written=7",
        "CLI DDL response rendering should surface rebuild metrics from the decoded canister payload",
    );
}

#[test]
fn ddl_no_op_response_rendering_includes_zero_execution_metrics() {
    let response: Result<SqlQueryResult, icydb::Error> = Ok(SqlQueryResult::Ddl {
        entity: "Character".to_string(),
        mutation_kind: "drop_secondary_index".to_string(),
        target_index: "character_missing_idx".to_string(),
        target_store: String::new(),
        field_path: Vec::new(),
        status: "no_op".to_string(),
        rows_scanned: 0,
        index_keys_written: 0,
    });
    let candid_bytes = Encode!(&response).expect("no-op DDL response should encode");
    let decoded = Decode!(
        candid_bytes.as_slice(),
        Result<SqlQueryResult, icydb::Error>
    )
    .expect("no-op DDL response should decode")
    .expect("no-op DDL response should succeed");

    assert_eq!(
        decoded.render_text(),
        "surface=ddl entity=Character mutation_kind=drop_secondary_index target_index=character_missing_idx target_store= field_path= status=no_op rows_scanned=0 index_keys_written=0",
        "CLI DDL response rendering should keep no-op status and zero work metrics visible",
    );
}

#[test]
fn projection_shell_text_leaves_footer_without_embedded_trailing_blank_line() {
    let rendered = render_projection_shell_text(
        RowProjectionOutput {
            entity: "Character".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec![OutputValue::Text("alice".to_string())]],
            row_count: 1,
        },
        None,
    );

    assert!(
        rendered.ends_with("1 row,"),
        "projection shell output should leave footer formatting to the command boundary: {rendered:?}",
    );
}

#[test]
fn projection_shell_text_renders_null_cells_as_sql_null() {
    let rendered = render_projection_shell_text(
        RowProjectionOutput {
            entity: "Character".to_string(),
            columns: vec!["nickname".to_string()],
            rows: vec![vec![OutputValue::Null]],
            row_count: 1,
        },
        None,
    );

    assert!(
        rendered.contains("NULL"),
        "projection shell output should render SQL NULL in uppercase: {rendered:?}",
    );
    assert!(
        !rendered.contains("null"),
        "projection shell output should not leak lowercase transport null cells: {rendered:?}",
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
    );

    assert!(
        rendered.ends_with("1 row,"),
        "grouped shell output should leave footer formatting to the command boundary: {rendered:?}",
    );
}

#[test]
fn grouped_shell_text_renders_null_cells_as_sql_null() {
    let rendered = render_grouped_shell_text(
        SqlGroupedRowsOutput {
            entity: "Character".to_string(),
            columns: vec!["class_name".to_string(), "COUNT(*)".to_string()],
            rows: vec![vec!["null".to_string(), "5".to_string()]],
            row_count: 1,
            next_cursor: None,
        },
        None,
    );

    assert!(
        rendered.contains("NULL"),
        "grouped shell output should render SQL NULL in uppercase: {rendered:?}",
    );
    assert!(
        !rendered.contains("null"),
        "grouped shell output should not leak lowercase transport null cells: {rendered:?}",
    );
}
