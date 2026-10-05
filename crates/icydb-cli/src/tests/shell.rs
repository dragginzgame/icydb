//! Module: SQL shell tests.
//! Responsibility: exercise shell input preservation, routing, decoding, and output rendering.
//! Does not own: top-level clap parsing or ICP process command construction.
//! Boundary: test-only assertions over shell helpers and decoded SQL payload text.

use crate::{
    cli::DEFAULT_ENVIRONMENT,
    shell::test_support::{
        SqlShellCallKind, candid_escape_string, drain_complete_shell_statements,
        finalize_successful_command_output, interactive_start_message, is_shell_exit_command,
        is_shell_help_command, render_sql_response, shell_help_text, sql_error_with_recovery_hint,
        sql_shell_call_kind,
    },
};
use candid::{Decode, Encode};
use icydb::{
    ConstraintValidationFindingOutput,
    db::{
        RowProjectionOutput,
        sql::{SqlConstraintValidationOutput, SqlGroupedRowsOutput, SqlQueryResult},
    },
    value::OutputValue,
};

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
fn drain_complete_shell_statements_ignores_empty_separators() {
    for (input, expected) in [
        ("  ;;  ", vec![]),
        ("SELECT 1;;   ", vec!["SELECT 1;"]),
        (
            "; SELECT 1;;;\nSELECT 2; ;;",
            vec!["SELECT 1;", "SELECT 2;"],
        ),
        ("SELECT ';;' AS marker;;;", vec!["SELECT ';;' AS marker;"]),
    ] {
        let mut statement = input.to_string();
        assert_eq!(
            drain_complete_shell_statements(&mut statement)
                .into_iter()
                .collect::<Vec<_>>(),
            expected,
        );
        assert!(statement.is_empty());
    }
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
fn drain_complete_shell_statements_accepts_literal_backslashes() {
    for literal in [r"C:\", r"C:\folder\", r"\", r"\'';quoted", "café\\"] {
        let sql = format!("UPDATE Character SET name = '{literal}' WHERE id = 1;");
        assert_eq!(
            sql_shell_call_kind(&sql),
            Ok(SqlShellCallKind::Update),
            "{sql}"
        );
        let mut statement = format!("{sql}\nSELECT 2;");
        let drained = drain_complete_shell_statements(&mut statement);
        assert_eq!(
            drained.into_iter().collect::<Vec<_>>(),
            [sql, "SELECT 2;".into()]
        );
        assert!(statement.is_empty());
    }
}

#[test]
fn drain_complete_shell_statements_preserves_semicolons_after_escaped_quote() {
    let mut statement = String::from("SELECT 'it''s; ok' AS marker;\nSELECT 2;");
    let drained = drain_complete_shell_statements(&mut statement);

    assert_eq!(
        drained.into_iter().collect::<Vec<_>>(),
        vec![
            "SELECT 'it''s; ok' AS marker;".to_string(),
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
    assert_eq!(statement, "\nSELECT");
}

#[test]
fn shell_help_text_names_current_commands_and_examples() {
    let help = shell_help_text();

    assert!(help.contains("? / help         show this help"));
    assert!(help.contains("\\q / quit / exit quit the interactive shell"));
    assert!(!help.contains("icydb-cli help"));
    assert!(help.contains("CREATE INDEX character_level_idx ON character (level);"));
    assert!(help.contains("SHOW INDEXES FROM character;"));
    assert!(help.contains("DESCRIBE character;"));
    assert!(help.contains("DESCRIBE character VERBOSE;"));
    assert!(help.contains("SHOW RELATIONS FROM character;"));
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
fn sql_recovery_hint_preserves_authoritative_deployed_method_absence() {
    let error = "Canister has no query method 'icydb_query'.";

    assert_eq!(
        sql_error_with_recovery_hint(error, DEFAULT_ENVIRONMENT, "demo_rpg"),
        error,
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
fn sql_recovery_hint_requires_environment_verification_before_disposable_refresh() {
    let error = "startup index rebuild failed: store 'token' not found";
    let rendered = sql_error_with_recovery_hint(error, "local", "sample-feed");

    assert!(rendered.contains("canister status sample-feed --environment local"));
    assert!(rendered.contains("Do not refresh unless"));
    assert!(!rendered.contains("run `icydb canister refresh"));
}

#[test]
fn candid_escape_string_escapes_sql_for_wire_arg() {
    assert_eq!(
        candid_escape_string("SELECT \"name\\path\"\nFROM Character\tWHERE note = 'a\rb'"),
        "SELECT \\\"name\\\\path\\\"\\nFROM Character\\tWHERE note = 'a\\rb'",
    );
}

#[test]
fn sql_shell_call_kind_routes_sql_to_fixed_endpoint_family() {
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
        constraint_validation: None,
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
        constraint_validation: None,
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
fn ddl_constraint_validation_page_roundtrips_typed_acknowledgement_state() {
    let finding: ConstraintValidationFindingOutput = serde_json::from_value(serde_json::json!({
        "accepted_schema_fingerprint": [17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17],
        "entity_tag": 29,
        "constraint_id": 41,
        "primary_key": [1, 2, 3],
        "field_ids": [7],
        "value_path": null,
        "error_code":
            icydb::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION.raw(),
    }))
    .expect("test constraint finding should decode");
    let expected = SqlConstraintValidationOutput {
        constraint_id: 41,
        activation_epoch: Some(7),
        page_sequence: Some(3),
        state: "forward".to_string(),
        revision_status: "tracking".to_string(),
        rows_scanned: 9,
        findings: vec![finding],
        complete: false,
    };
    let response: Result<SqlQueryResult, icydb::Error> = Ok(SqlQueryResult::Ddl {
        entity: "Character".to_string(),
        mutation_kind: "validate_constraint".to_string(),
        target_index: "adult_age".to_string(),
        target_store: "Character".to_string(),
        field_path: Vec::new(),
        status: "validation_findings".to_string(),
        rows_scanned: 9,
        index_keys_written: 0,
        constraint_validation: Some(expected.clone()),
    });
    let candid_bytes = Encode!(&response).expect("validation response should encode");
    let decoded = Decode!(
        candid_bytes.as_slice(),
        Result<SqlQueryResult, icydb::Error>
    )
    .expect("validation response should decode")
    .expect("validation response should succeed");
    let rendered = decoded.render_text();
    let SqlQueryResult::Ddl {
        constraint_validation,
        ..
    } = decoded
    else {
        panic!("validation response should remain DDL");
    };
    assert_eq!(constraint_validation, Some(expected));
    assert!(
        rendered.contains(
            "constraint_finding fingerprint=11111111111111111111111111111111 entity_tag=29 constraint_id=41 primary_key=010203 field_ids=7 class=invariant_violation code=E210"
        ),
        "CLI SQL rendering should retain the compact historical finding",
    );
}

#[test]
fn projection_shell_text_leaves_footer_without_embedded_trailing_blank_line() {
    let rendered = SqlQueryResult::Projection(RowProjectionOutput {
        entity: "Character".to_string(),
        columns: vec!["name".to_string()],
        rows: vec![vec![OutputValue::text("alice".to_string())]],
        row_count: 1,
    })
    .render_text();

    assert!(
        rendered.ends_with("1 row,"),
        "projection shell output should leave footer formatting to the command boundary: {rendered:?}",
    );
}

#[test]
fn projection_shell_text_renders_null_cells_as_sql_null() {
    let rendered = SqlQueryResult::Projection(RowProjectionOutput {
        entity: "Character".to_string(),
        columns: vec!["nickname".to_string()],
        rows: vec![vec![OutputValue::null()]],
        row_count: 1,
    })
    .render_text();

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
    let rendered = SqlQueryResult::Grouped(SqlGroupedRowsOutput {
        entity: "Character".to_string(),
        columns: vec!["class_name".to_string(), "COUNT(*)".to_string()],
        rows: vec![vec!["Bard".to_string(), "5".to_string()]],
        row_count: 1,
        next_cursor: None,
    })
    .render_text();

    assert!(
        rendered.ends_with("1 row,"),
        "grouped shell output should leave footer formatting to the command boundary: {rendered:?}",
    );
}

#[test]
fn grouped_shell_text_renders_null_cells_as_sql_null() {
    let rendered = SqlQueryResult::Grouped(SqlGroupedRowsOutput {
        entity: "Character".to_string(),
        columns: vec!["class_name".to_string(), "COUNT(*)".to_string()],
        rows: vec![vec!["NULL".to_string(), "5".to_string()]],
        row_count: 1,
        next_cursor: None,
    })
    .render_text();

    assert!(
        rendered.contains("NULL"),
        "grouped shell output should render SQL NULL in uppercase: {rendered:?}",
    );
    assert!(
        !rendered.contains("null"),
        "grouped shell output should not leak lowercase transport null cells: {rendered:?}",
    );
}

#[test]
fn sql_response_null_rendering_is_shared_by_query_and_returning() {
    let result = SqlQueryResult::Projection(RowProjectionOutput {
        entity: "User".into(),
        columns: vec!["missing".into(), "lower".into(), "upper".into()],
        rows: vec![vec![
            OutputValue::null(),
            OutputValue::text("null".into()),
            OutputValue::text("NULL".into()),
        ]],
        row_count: 1,
    });
    let response: Result<SqlQueryResult, icydb::Error> = Ok(result.clone());
    let bytes = Encode!(&response).expect("SQL response should encode");
    let expected = icydb::db::sql::render_projection_display_rows_lines(
        &["missing".into(), "lower".into(), "upper".into()],
        &[vec!["NULL".into(), "'null'".into(), "'NULL'".into()]],
        1,
    )
    .join("\n");

    for (sql, kind) in [
        ("SELECT nickname FROM User", SqlShellCallKind::Query),
        (
            "UPDATE User SET nickname = NULL RETURNING nickname",
            SqlShellCallKind::Update,
        ),
        (
            "DELETE FROM User RETURNING nickname",
            SqlShellCallKind::Query,
        ),
        (
            "INSERT INTO User (nickname) VALUES (NULL) RETURNING nickname",
            SqlShellCallKind::Query,
        ),
    ] {
        assert_eq!(sql_shell_call_kind(sql), Ok(kind), "{sql}");
        assert_eq!(render_sql_response(&bytes), Ok(expected.clone()), "{sql}");
    }
    assert_eq!(result.render_text(), expected);
}

#[test]
fn sql_response_preserves_preformatted_grouped_cells_and_cursor() {
    let result = SqlQueryResult::Grouped(SqlGroupedRowsOutput {
        entity: "User".into(),
        columns: vec!["key".into(), "value".into()],
        rows: vec![
            vec!["NULL".into(), "'null'".into()],
            vec!["'NULL'".into(), "NULL".into()],
            vec!["null".into(), "0.000".into()],
        ],
        row_count: 3,
        next_cursor: Some("opaque-cursor".into()),
    });
    let expected = result.render_text();
    let response: Result<SqlQueryResult, icydb::Error> = Ok(result);
    let bytes = Encode!(&response).expect("grouped response should encode");
    assert_eq!(render_sql_response(&bytes), Ok(expected));
}

#[test]
fn sql_response_keeps_endpoint_and_decode_failures_as_errors() {
    let error = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::QueryReadAdmission,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::QueryReadAdmission {
            reason: icydb::diagnostic::QueryReadAdmissionCode::PublicQueryRequiresLimit,
        }),
    ));
    let response: Result<SqlQueryResult, icydb::Error> = Err(error);
    let bytes = Encode!(&response).expect("endpoint failure should encode");
    assert!(render_sql_response(&bytes).is_err());
    assert!(render_sql_response(&[]).is_err());
}
