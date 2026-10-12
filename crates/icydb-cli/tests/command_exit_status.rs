//! Exercise SQL, canister and migration outcomes through the actual CLI process.
//! A local ICP fixture supplies current Candid responses without a live network.

#![cfg(unix)]

use std::{
    fmt::Write as _,
    fs,
    io::Write as _,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

use candid::{CandidType, Encode};
use icydb::{
    Error, ErrorOrigin,
    db::{
        RowProjectionOutput, SchemaMigrationPhase, SchemaMigrationStatusPage, sql::SqlQueryResult,
    },
    diagnostic::RuntimeBoundaryCode,
    value::OutputValue,
};

const STATEMENTS: [(&str, &str, bool); 3] = [
    ("SELECT name FROM Character", "icydb_query", true),
    (
        "CREATE INDEX name_idx ON Character (name)",
        "icydb_ddl",
        false,
    ),
    (
        "UPDATE Character SET name = 'Ada' WHERE id = 1",
        "icydb_update",
        false,
    ),
];

struct IcpFixture {
    directory: PathBuf,
}

impl IcpFixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "icydb-command-status-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&directory).unwrap();
        let executable = directory.join("icp");
        // Only the child receives this PATH. Unexpected calls fail closed;
        // the log proves the CLI reached the selected endpoint and call lane.
        fs::write(
            &executable,
            r#"#!/bin/sh
printf '%s\n' "$@" >> "$ICP_FIXTURE_LOG"
case "$1:$2" in
  build:fixture)
    [ "${ICP_FIXTURE_UPGRADE_FAILURE-}" != build ] || exit 1
    printf '%s\n' 'fresh build' > "$ICP_FIXTURE_BUILD_OUTPUT"
    ;;
  canister:install)
    [ "${ICP_FIXTURE_UPGRADE_FAILURE-}" != install ] || exit 1
    artifact=$ICP_FIXTURE_BUILD_OUTPUT
    shift 2
    while [ "$#" -gt 0 ]; do
      if [ "$1" = --wasm ]; then shift; artifact=$1; fi
      shift
    done
    read -r contents < "$artifact" || exit 1
    printf '%s\n' "$contents" > "$ICP_FIXTURE_DIRECTORY/installed"
    ;;
  canister:list) printf '%s\n' '{"canisters":["fixture"]}' ;;
  canister:status)
    if [ -n "${ICP_FIXTURE_STATUS_ERROR-}" ]; then
      printf '%s\n' "$ICP_FIXTURE_STATUS_ERROR" >&2
      exit 7
    fi
    case "${ICP_FIXTURE_REFRESH_CASE-}" in
      status-failure) printf '%s\n' 'connection refused' >&2; exit 1 ;;
      uninstalled) printf '%s\n' '{"module_hash":null}' ;;
      invalid-status) printf '%s\n' '{"status":"running"}' ;;
      invalid-json) printf '%s\n' '{' ;;
      invalid-module-hash) printf '%s\n' '{"module_hash":42}' ;;
      *)
        case "$4" in
          --json) printf '%s\n' '{"module_hash":"0x0123"}' ;;
          *) printf '%s\n' 'aaaaa-aa' ;;
        esac
        ;;
    esac
    ;;
  deploy:fixture) [ "${ICP_FIXTURE_REFRESH_CASE-}" != deploy-failure ] || exit 1 ;;
  canister:call)
    if [ "$ICP_FIXTURE_TRANSPORT_FAILURE" = 1 ] ||
       { [ "$ICP_FIXTURE_TRANSPORT_FAILURE" = update ] && [ "$4" = icydb_schema_migrate ]; }; then
      printf '%s\n' 'fixture transport failure' >&2
      exit 1
    fi
    case "$4" in
      icydb_fixtures_load)
        [ "${ICP_FIXTURE_REFRESH_CASE-}" != fixture-failure ] || exit 1
        printf '%s\n' "$ICP_FIXTURE_RESPONSE"
        ;;
      icydb_schema_migration) read -r reply < "$ICP_FIXTURE_DIRECTORY/status"; printf '%s\n' "$reply" ;;
      icydb_schema_migrate)
        step=0
        if [ -f "$ICP_FIXTURE_DIRECTORY/step" ]; then read -r step < "$ICP_FIXTURE_DIRECTORY/step"; fi
        if [ ! -f "$ICP_FIXTURE_DIRECTORY/reply-$step" ]; then exit 2; fi
        read -r reply < "$ICP_FIXTURE_DIRECTORY/reply-$step"
        printf '%s\n' "$reply"
        printf '%s\n' "$((step + 1))" > "$ICP_FIXTURE_DIRECTORY/step"
        ;;
      *)
    case "$5" in
      *'SELECT id FROM Character'*) printf '%s\n' "$ICP_FIXTURE_SUCCESS" ;;
      *) printf '%s\n' "$ICP_FIXTURE_RESPONSE" ;;
    esac
        ;;
    esac
    ;;
  *) exit 2 ;;
esac
"#,
        )
        .unwrap();
        fs::set_permissions(executable, fs::Permissions::from_mode(0o700)).unwrap();

        Self { directory }
    }

    fn command(&self, response: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_icydb"));
        command
            .current_dir(&self.directory)
            .env("PATH", &self.directory)
            .env("ICP_FIXTURE_LOG", self.directory.join("calls"))
            .env("ICP_FIXTURE_DIRECTORY", &self.directory)
            .env("ICP_FIXTURE_RESPONSE", response)
            .env("ICP_FIXTURE_SUCCESS", success_response())
            .env_remove("ICP_FIXTURE_STATUS_ERROR")
            .env_remove("ICP_FIXTURE_REFRESH_CASE")
            .env_remove("ICP_FIXTURE_TRANSPORT_FAILURE");

        command
    }

    fn sql_command(&self, response: &str) -> Command {
        let mut command = self.command(response);
        command.args(["sql", "-c", "fixture", "-e", "fixture"]);

        command
    }

    fn upgrade_command(&self, environment: &str, nested: bool) -> Command {
        let build_output = self.directory.join("build-output.wasm");
        let working_directory = if nested {
            self.directory.join("nested")
        } else {
            self.directory.clone()
        };
        fs::create_dir_all(&working_directory).unwrap();
        let mut command = self.command("");
        command
            .current_dir(working_directory)
            .env("ICP_FIXTURE_BUILD_OUTPUT", build_output)
            .env_remove("ICP_FIXTURE_UPGRADE_FAILURE")
            .args(["canister", "upgrade", "fixture", "-e", environment]);

        command
    }

    fn migration_command(&self, status: &str, replies: &[String], operation: &str) -> Command {
        fs::write(self.directory.join("status"), status).unwrap();
        for (index, reply) in replies.iter().enumerate() {
            fs::write(self.directory.join(format!("reply-{index}")), reply).unwrap();
        }
        let mut command = self.command("");
        command.args(["schema", "migration", operation, "fixture", "-e", "fixture"]);

        command
    }

    fn migration_calls(&self) -> usize {
        let log = fs::read_to_string(self.directory.join("calls")).unwrap_or_default();
        let arguments = log.lines().collect::<Vec<_>>();
        for call in arguments.split(|argument| *argument == "canister") {
            if call.first() == Some(&"call") {
                assert_eq!(
                    call.contains(&"--query"),
                    call[2] == "icydb_schema_migration"
                );
            }
        }
        log.lines()
            .filter(|line| *line == "icydb_schema_migrate")
            .count()
    }

    fn one_shot(&self, response: &str, sql: &str, explicit: bool) -> Output {
        let mut command = self.sql_command(response);
        if explicit {
            command.arg("--sql");
        }
        command.arg(sql).output().unwrap()
    }

    fn assert_call(&self, method: &str, query: bool) {
        let calls = fs::read_to_string(self.directory.join("calls")).unwrap();
        let arguments = calls.lines().collect::<Vec<_>>();
        assert!(arguments.contains(&method));
        assert_eq!(arguments.contains(&"--query"), query);
    }
}

impl Drop for IcpFixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "CLI fixture evidence retained at {}",
                self.directory.display()
            );
        } else {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}

fn response_hex<T: CandidType>(response: Result<T, Error>) -> String {
    let bytes = Encode!(&response).unwrap();
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut hex, "{byte:02x}").unwrap();
    }

    hex
}

fn success_response() -> String {
    response_hex(Ok(SqlQueryResult::Count {
        entity: "Character".into(),
        row_count: 1,
    }))
}

fn assert_failure(output: Output) {
    assert_eq!(output.status.code(), Some(1));
    assert!(
        output.stdout.is_empty(),
        "failed SQL must not emit success output"
    );
    assert!(
        !output.stderr.is_empty(),
        "failed SQL must report a diagnostic"
    );
}

#[test]
fn failed_canister_probes_preserve_diagnostics_and_stop_before_effects() {
    // These are opaque child payloads, never inputs to a stderr classifier.
    for diagnostic in [
        "replica rejected the caller: controller permission required",
        "fixture configuration failure",
        "connection refused by fixture transport",
    ] {
        for environment in ["local", "ic", "fixture"] {
            for arguments in [
                vec!["sql", "-c", "fixture", "--sql", "SELECT id FROM Character"],
                vec!["canister", "list"],
                vec!["canister", "refresh", "fixture"],
            ] {
                let fixture = IcpFixture::new();
                let output = fixture
                    .command(&success_response())
                    .env("ICP_FIXTURE_STATUS_ERROR", diagnostic)
                    .args(arguments)
                    .args(["-e", environment])
                    .output()
                    .unwrap();
                assert_eq!(output.status.code(), Some(1));
                let stderr = String::from_utf8(output.stderr).unwrap();
                assert!(stderr.contains(diagnostic), "child diagnostic must survive");
                let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
                let arguments = calls.lines().collect::<Vec<_>>();
                assert!(!arguments.contains(&"deploy"));
                assert!(!arguments.contains(&"call"));
                assert!(
                    arguments
                        .windows(2)
                        .any(|pair| pair == ["--environment", environment])
                );
            }
        }
    }
}

#[test]
fn missing_icp_executable_reports_command_context() {
    for arguments in [
        vec!["sql", "-c", "fixture", "--sql", "SELECT id FROM Character"],
        vec!["canister", "refresh", "fixture"],
    ] {
        let fixture = IcpFixture::new();
        fs::remove_file(fixture.directory.join("icp")).unwrap();
        let output = fixture.command("").args(arguments).output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains("icp"),
            "diagnostic must identify the missing executable"
        );
    }
}

#[test]
fn refresh_preserves_probe_admission_and_stops_after_failed_stages() {
    for (case, exit, deployed, reinstalled, fixtures_called) in [
        ("status-failure", 1, false, false, false),
        ("installed", 0, true, true, true),
        ("uninstalled", 0, true, false, true),
        ("invalid-status", 1, false, false, false),
        ("invalid-json", 1, false, false, false),
        ("invalid-module-hash", 1, false, false, false),
        ("deploy-failure", 1, true, true, false),
        ("fixture-failure", 1, true, true, true),
    ] {
        let fixture = IcpFixture::new();
        let output = fixture
            .command(&success_response())
            .env("ICP_FIXTURE_REFRESH_CASE", case)
            .args(["canister", "refresh", "fixture", "-e", "fixture"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(exit), "{case}");
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        let arguments = calls.lines().collect::<Vec<_>>();
        assert_eq!(arguments.contains(&"deploy"), deployed, "{case}");
        assert_eq!(arguments.contains(&"reinstall"), reinstalled, "{case}");
        assert_eq!(arguments.contains(&"--yes"), reinstalled, "{case}");
        assert_eq!(
            arguments.contains(&"icydb_fixtures_load"),
            fixtures_called,
            "{case}"
        );
        assert!(
            arguments
                .windows(2)
                .filter(|pair| pair[0] == "--environment")
                .all(|pair| pair[1] == "fixture")
        );
    }
}

#[test]
fn environment_selection_uses_default_then_inherited_value_then_explicit_flag() {
    for arguments in [
        vec!["canister", "status", "fixture"],
        vec!["sql", "-c", "fixture", "--sql", "SELECT id FROM Character"],
    ] {
        for (inherited, explicit, expected) in [
            (None, false, "local"),
            (Some("staging"), false, "staging"),
            (Some("staging"), true, "local"),
        ] {
            let fixture = IcpFixture::new();
            let mut command = fixture.command(&success_response());
            // Environment changes belong to the child, never the parallel test process.
            command.env_remove("ICP_ENVIRONMENT").args(&arguments);
            if let Some(inherited) = inherited {
                command.env("ICP_ENVIRONMENT", inherited);
            }
            if explicit {
                command.args(["-e", "local"]);
            }
            let output = command.output().unwrap();
            assert_eq!(output.status.code(), Some(0), "{output:?}");
            let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
            let arguments = calls.lines().collect::<Vec<_>>();
            let environments = arguments
                .windows(2)
                .filter(|pair| pair[0] == "--environment")
                .map(|pair| pair[1])
                .collect::<Vec<_>>();
            assert!(!environments.is_empty());
            assert!(
                environments
                    .iter()
                    .all(|environment| *environment == expected)
            );
        }
    }
}

#[test]
fn upgrade_installs_icp_build_output_for_selected_environment_and_working_directory() {
    for environment in ["local", "staging"] {
        for nested in [false, true] {
            let fixture = IcpFixture::new();
            let mut command = fixture.upgrade_command(environment, nested);
            let stale = command
                .get_current_dir()
                .unwrap()
                .join(".icp/local/canisters/fixture/fixture.wasm");
            fs::create_dir_all(stale.parent().unwrap()).unwrap();
            fs::write(stale, "stale build\n").unwrap();
            let output = command.output().unwrap();
            assert_eq!(output.status.code(), Some(0), "{output:?}");
            assert_eq!(
                fs::read_to_string(fixture.directory.join("installed")).unwrap(),
                "fresh build\n"
            );
            let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
            assert_eq!(
                calls,
                format!(
                    "build\nfixture\n--environment\n{environment}\ncanister\ninstall\nfixture\n--mode\nupgrade\n--environment\n{environment}\n"
                )
            );
        }
    }
}

#[test]
fn upgrade_preserves_explicit_wasm_override() {
    let fixture = IcpFixture::new();
    let artifact = fixture.directory.join("selected artifact.wasm");
    fs::write(&artifact, "explicit artifact\n").unwrap();
    let output = fixture
        .upgrade_command("staging", true)
        .arg("--wasm")
        .arg(&artifact)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        fs::read_to_string(fixture.directory.join("installed")).unwrap(),
        "explicit artifact\n"
    );
    let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
    assert!(
        calls
            .lines()
            .any(|argument| argument == artifact.to_str().unwrap())
    );
}

#[test]
fn upgrade_stops_after_failed_build_missing_override_or_failed_install() {
    for failure in ["build", "missing", "install"] {
        let fixture = IcpFixture::new();
        let mut command = fixture.upgrade_command("staging", true);
        if failure == "missing" {
            command.arg("--wasm").arg("missing.wasm");
        } else {
            command.env("ICP_FIXTURE_UPGRADE_FAILURE", failure);
        }
        assert_failure(command.output().unwrap());
        assert!(!fixture.directory.join("installed").exists());
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(
            calls.lines().any(|argument| argument == "install"),
            failure == "install"
        );
    }
}

#[test]
fn one_shot_sql_endpoint_errors_fail_for_every_call_lane_and_argument_form() {
    for boundary in [
        RuntimeBoundaryCode::SqlSurfacePolicyDenied,
        RuntimeBoundaryCode::SqlSurfaceControllerRequired,
    ] {
        for (sql, method, query) in STATEMENTS {
            for explicit in [false, true] {
                let fixture = IcpFixture::new();
                let response = response_hex::<SqlQueryResult>(Err(Error::from_runtime_boundary(
                    boundary,
                    ErrorOrigin::Interface,
                )));
                assert_failure(fixture.one_shot(&response, sql, explicit));
                fixture.assert_call(method, query);
            }
        }
    }
}

#[test]
fn one_shot_sql_success_keeps_stdout_and_success_status() {
    let result = SqlQueryResult::Count {
        entity: "Character".into(),
        row_count: 1,
    };
    let expected = format!("{}\n\n", result.render_text());
    let response = response_hex(Ok(result));
    for (sql, method, query) in STATEMENTS {
        for explicit in [false, true] {
            let fixture = IcpFixture::new();
            let output = fixture.one_shot(&response, sql, explicit);
            assert_eq!(output.status.code(), Some(0));
            assert_eq!(output.stdout, expected.as_bytes());
            assert!(output.stderr.is_empty());
            fixture.assert_call(method, query);
        }
    }
}

#[test]
fn one_shot_sql_null_display_matches_query_and_update_returning() {
    let result = SqlQueryResult::Projection(RowProjectionOutput {
        entity: "Character".into(),
        columns: vec!["missing".into(), "lower".into(), "upper".into()],
        rows: vec![vec![
            OutputValue::null(),
            OutputValue::text("null".into()),
            OutputValue::text("NULL".into()),
        ]],
        row_count: 1,
    });
    let expected = format!(
        "{}\n\n",
        icydb::db::sql::render_projection_display_rows_lines(
            &["missing".into(), "lower".into(), "upper".into()],
            &[vec!["NULL".into(), "'null'".into(), "'NULL'".into()]],
            1,
        )
        .join("\n"),
    );
    let response = response_hex(Ok(result));
    for (sql, method, query) in [
        ("SELECT name FROM Character", "icydb_query", true),
        (
            "UPDATE Character SET name = NULL WHERE id = 1 RETURNING name",
            "icydb_update",
            false,
        ),
    ] {
        for explicit in [false, true] {
            let fixture = IcpFixture::new();
            let output = fixture.one_shot(&response, sql, explicit);
            assert_eq!(output.status.code(), Some(0));
            assert_eq!(output.stdout, expected.as_bytes());
            assert!(output.stderr.is_empty());
            fixture.assert_call(method, query);
        }
    }
}

#[test]
fn one_shot_sql_transport_and_invalid_candid_fail_for_every_call_lane() {
    for (sql, method, query) in STATEMENTS {
        for transport_failure in [false, true] {
            let fixture = IcpFixture::new();
            let mut command = fixture.sql_command("00");
            if transport_failure {
                command.env("ICP_FIXTURE_TRANSPORT_FAILURE", "1");
            }
            assert_failure(command.args(["--sql", sql]).output().unwrap());
            fixture.assert_call(method, query);
        }
    }
}

#[test]
fn interactive_sql_eof_discards_unterminated_statements() {
    for (input, completed_queries) in [
        ("", 0),
        ("  \n\t\n", 0),
        ("SELECT name FROM Character\n", 0),
        ("DROP INDEX name_idx ON Character\n", 0),
        ("UPDATE Character SET name = 'Ada' WHERE id = 1\n", 0),
        ("UPDATE Character SET name = 'unfinished\n  ", 0),
        ("SELECT id FROM Character;\n", 1),
        (
            "SELECT id FROM Character;\nDROP INDEX name_idx ON Character\n",
            1,
        ),
        (
            "SELECT id FROM Character; SELECT id FROM Character;\nUPDATE Character SET name = 'unfinished\n",
            2,
        ),
    ] {
        let fixture = IcpFixture::new();
        let mut child = fixture
            .sql_command(&success_response())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(0), "{input:?}");
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(
            calls.lines().filter(|line| *line == "icydb_query").count(),
            completed_queries,
            "{input:?}",
        );
        assert!(
            !calls
                .lines()
                .any(|line| matches!(line, "icydb_ddl" | "icydb_update"))
        );
        let history =
            fs::read_to_string(fixture.directory.join(".cache/sql_history")).unwrap_or_default();
        assert!(!history.contains("DROP INDEX") && !history.contains("UPDATE"));
        if completed_queries == 0 {
            assert!(history.is_empty());
        } else {
            assert!(history.contains("SELECT id FROM Character;"));
        }
    }
}

#[test]
fn interactive_sql_history_failures_do_not_block_completed_statements() {
    for case in ["parent_file", "history_directory", "invalid_utf8"] {
        let fixture = IcpFixture::new();
        let history = fixture.directory.join(".cache/sql_history");
        match case {
            "parent_file" => fs::write(fixture.directory.join(".cache"), b"untouched").unwrap(),
            "history_directory" => fs::create_dir_all(&history).unwrap(),
            "invalid_utf8" => {
                fs::create_dir_all(history.parent().unwrap()).unwrap();
                fs::write(&history, [0xff]).unwrap();
            }
            _ => unreachable!(),
        }

        let mut child = fixture
            .sql_command(&success_response())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"SELECT id FROM Character;\nSELECT id FROM Character;\n")
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(0),
            "history case {case}: {:?}",
            String::from_utf8_lossy(&output.stderr),
        );
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(
            calls.lines().filter(|line| *line == "icydb_query").count(),
            2
        );
        assert!(String::from_utf8_lossy(&output.stderr).contains(".cache/sql_history"));
        if case == "parent_file" {
            assert_eq!(
                fs::read(fixture.directory.join(".cache")).unwrap(),
                b"untouched"
            );
        } else if case == "history_directory" {
            assert!(history.is_dir());
        } else {
            assert_eq!(fs::read(&history).unwrap(), [0xff]);
        }
    }
}

#[test]
fn interactive_sql_continues_to_success_after_endpoint_rejection() {
    for (sql, method, query) in STATEMENTS {
        let fixture = IcpFixture::new();
        let response = response_hex::<SqlQueryResult>(Err(Error::from_runtime_boundary(
            RuntimeBoundaryCode::SqlSurfacePolicyDenied,
            ErrorOrigin::Interface,
        )));
        let mut child = fixture
            .sql_command(&response)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let input = format!("{sql};\nSELECT id FROM Character;\n\\q\n");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(0));
        let successful_output = SqlQueryResult::Count {
            entity: "Character".into(),
            row_count: 1,
        }
        .render_text();
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains(&successful_output)
        );
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(
            calls.lines().filter(|line| *line == method).count(),
            1 + usize::from(query)
        );
        assert!(calls.lines().any(|line| line == "icydb_query"));
    }
}

const PROGRESS_PHASES: [SchemaMigrationPhase; 8] = [
    SchemaMigrationPhase::Idle,
    SchemaMigrationPhase::Prepared,
    SchemaMigrationPhase::Validating,
    SchemaMigrationPhase::ReadyToRewrite,
    SchemaMigrationPhase::RewritingRows,
    SchemaMigrationPhase::RebuildingIndexes,
    SchemaMigrationPhase::FinalValidation,
    SchemaMigrationPhase::Publishing,
];

fn migration_status_value(phase: SchemaMigrationPhase, rows: u64) -> serde_json::Value {
    let plan = (!matches!(
        phase,
        SchemaMigrationPhase::Unadopted | SchemaMigrationPhase::Adopted
    ))
    .then_some([3_u8; 32]);
    let accepted_head = if phase == SchemaMigrationPhase::Applied {
        serde_json::json!({"Exact": {"revision": 2, "fingerprint": vec![8_u8; 32]}})
    } else {
        serde_json::json!("Empty")
    };
    let receipt = (phase == SchemaMigrationPhase::Applied).then(|| {
        serde_json::json!({
            "database_identity": vec![1_u8; 32], "plan_digest": plan,
            "prior_head": "Empty", "accepted_head": accepted_head,
        })
    });
    let findings = if phase == SchemaMigrationPhase::Rejected {
        vec![serde_json::json!({"kind": "UniqueIndex", "entity_tag": 11, "primary_key": [7, 9]})]
    } else {
        Vec::new()
    };
    serde_json::json!({
        "database_identity": vec![1_u8; 32], "accepted_head": accepted_head,
        "plan_digest": plan, "phase": format!("{phase:?}"), "transitions": [],
        "rows_validated": rows, "rows_rewritten": 0, "indexes_rebuilt": 0,
        "findings": findings, "next_cursor": null, "terminal_receipt": receipt,
    })
}

fn migration_response(phase: SchemaMigrationPhase, rows: u64) -> String {
    let status: SchemaMigrationStatusPage =
        serde_json::from_value(migration_status_value(phase, rows)).unwrap();
    response_hex(Ok(status))
}

fn assert_migration_outcome(output: Output, phase: SchemaMigrationPhase, exit: i32) {
    assert_eq!(output.status.code(), Some(exit));
    assert_eq!(output.stderr.is_empty(), exit == 0);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout
            .lines()
            .any(|line| line == format!("  phase: {phase:?}"))
    );
    if phase == SchemaMigrationPhase::Rejected {
        assert!(stdout.contains("UniqueIndex: entity 11 key 0709"));
    }
}

#[test]
fn migration_run_requires_applied_for_existing_and_new_terminal_results() {
    for phase in [
        SchemaMigrationPhase::Applied,
        SchemaMigrationPhase::Rejected,
        SchemaMigrationPhase::Aborted,
    ] {
        let exit = i32::from(phase != SchemaMigrationPhase::Applied);
        for existing_terminal in [false, true] {
            let fixture = IcpFixture::new();
            let status = migration_response(
                if existing_terminal {
                    phase
                } else {
                    SchemaMigrationPhase::Idle
                },
                0,
            );
            let progress = if existing_terminal {
                Vec::new()
            } else if phase == SchemaMigrationPhase::Applied {
                PROGRESS_PHASES[1..].to_vec()
            } else {
                vec![
                    SchemaMigrationPhase::Prepared,
                    SchemaMigrationPhase::Validating,
                ]
            };
            let replies = progress
                .into_iter()
                .chain((!existing_terminal).then_some(phase))
                .enumerate()
                .map(|(index, phase)| migration_response(phase, u64::try_from(index).unwrap() + 1))
                .collect::<Vec<_>>();
            let output = fixture
                .migration_command(&status, &replies, "run")
                .output()
                .unwrap();
            assert_migration_outcome(output, phase, exit);
            assert_eq!(fixture.migration_calls(), replies.len());
        }
    }
}

#[test]
fn migration_advance_distinguishes_bounded_progress_from_unsuccessful_terminals() {
    for phase in PROGRESS_PHASES.into_iter().chain([
        SchemaMigrationPhase::Applied,
        SchemaMigrationPhase::Rejected,
        SchemaMigrationPhase::Aborted,
    ]) {
        let fixture = IcpFixture::new();
        let status = migration_response(SchemaMigrationPhase::Prepared, 0);
        let reply = migration_response(phase, 1);
        let output = fixture
            .migration_command(&status, &[reply], "advance")
            .output()
            .unwrap();
        let exit = i32::from(matches!(
            phase,
            SchemaMigrationPhase::Rejected | SchemaMigrationPhase::Aborted
        ));
        assert_migration_outcome(output, phase, exit);
        assert_eq!(fixture.migration_calls(), 1);
    }
}

#[test]
fn migration_abort_requires_aborted_and_preserves_paged_rejected_cleanup() {
    for (initial, terminal) in [
        (
            SchemaMigrationPhase::Prepared,
            SchemaMigrationPhase::Aborted,
        ),
        (
            SchemaMigrationPhase::Rejected,
            SchemaMigrationPhase::Aborted,
        ),
        (SchemaMigrationPhase::Applied, SchemaMigrationPhase::Applied),
        (SchemaMigrationPhase::Aborted, SchemaMigrationPhase::Aborted),
        (
            SchemaMigrationPhase::Prepared,
            SchemaMigrationPhase::Applied,
        ),
    ] {
        let fixture = IcpFixture::new();
        let status = migration_response(initial, 0);
        let mut replies = Vec::new();
        if matches!(
            initial,
            SchemaMigrationPhase::Prepared | SchemaMigrationPhase::Rejected
        ) {
            // The private staging cursor can advance without changing the public
            // page. Abort must keep cleaning until its terminal receipt arrives.
            replies.extend([status.clone(), status.clone()]);
        }
        replies.push(migration_response(terminal, 0));
        let output = fixture
            .migration_command(&status, &replies, "abort")
            .arg("--yes")
            .output()
            .unwrap();
        assert_migration_outcome(
            output,
            terminal,
            i32::from(terminal != SchemaMigrationPhase::Aborted),
        );
        assert_eq!(fixture.migration_calls(), replies.len());
    }
}

#[test]
fn migration_status_inspects_every_phase_without_claiming_operation_success() {
    for phase in PROGRESS_PHASES.into_iter().chain([
        SchemaMigrationPhase::Unadopted,
        SchemaMigrationPhase::Adopted,
        SchemaMigrationPhase::Applied,
        SchemaMigrationPhase::Rejected,
        SchemaMigrationPhase::Aborted,
    ]) {
        let fixture = IcpFixture::new();
        let status = migration_response(phase, 0);
        let output = fixture
            .migration_command(&status, &[], "status")
            .output()
            .unwrap();
        assert_migration_outcome(output, phase, 0);
        assert_eq!(fixture.migration_calls(), 0);
    }
}

#[test]
fn migration_loops_keep_identity_and_progress_failures_closed() {
    for operation in ["run", "abort"] {
        for field in ["database_identity", "plan_digest"] {
            let fixture = IcpFixture::new();
            let phase = if operation == "run" {
                SchemaMigrationPhase::Applied
            } else {
                SchemaMigrationPhase::Aborted
            };
            let mut value = migration_status_value(phase, 1);
            value[field] = serde_json::json!(vec![9_u8; 32]);
            let status: SchemaMigrationStatusPage = serde_json::from_value(value).unwrap();
            let initial = migration_response(SchemaMigrationPhase::Prepared, 0);
            let mut command =
                fixture.migration_command(&initial, &[response_hex(Ok(status))], operation);
            if operation == "abort" {
                command.arg("--yes");
            }
            assert_failure(command.output().unwrap());
            assert_eq!(fixture.migration_calls(), 1);
        }
    }
    let fixture = IcpFixture::new();
    let unchanged = migration_response(SchemaMigrationPhase::Validating, 0);
    assert_failure(
        fixture
            .migration_command(&unchanged, std::slice::from_ref(&unchanged), "run")
            .output()
            .unwrap(),
    );
    assert_eq!(fixture.migration_calls(), 1);
}

#[test]
fn migration_confirmation_missing_plans_and_adoption_keep_existing_contracts() {
    for operation in ["abort", "adopt"] {
        let fixture = IcpFixture::new();
        let status = migration_response(SchemaMigrationPhase::Unadopted, 0);
        assert_failure(
            fixture
                .migration_command(&status, &[], operation)
                .output()
                .unwrap(),
        );
        assert!(!fixture.directory.join("calls").exists());
    }
    for operation in ["run", "advance", "abort"] {
        let fixture = IcpFixture::new();
        let status = migration_response(SchemaMigrationPhase::Adopted, 0);
        let mut command = fixture.migration_command(&status, &[], operation);
        if operation == "abort" {
            command.arg("--yes");
        }
        assert_failure(command.output().unwrap());
        assert_eq!(fixture.migration_calls(), 0);
    }
    let fixture = IcpFixture::new();
    let initial = migration_response(SchemaMigrationPhase::Unadopted, 0);
    let reply = migration_response(SchemaMigrationPhase::Adopted, 0);
    let output = fixture
        .migration_command(&initial, &[reply], "adopt")
        .arg("--yes")
        .output()
        .unwrap();
    assert_migration_outcome(output, SchemaMigrationPhase::Adopted, 0);
    assert_eq!(fixture.migration_calls(), 1);
}

#[test]
fn migration_commands_preserve_remote_transport_and_invalid_reply_failures() {
    let remote_error =
        response_hex::<SchemaMigrationStatusPage>(Err(Error::from_runtime_boundary(
            RuntimeBoundaryCode::SchemaSurfaceControllerRequired,
            ErrorOrigin::Interface,
        )));
    for operation in ["status", "advance", "run", "abort", "adopt"] {
        for read_failure in [false, true] {
            for invalid_response in [false, true] {
                let fixture = IcpFixture::new();
                let bad_reply = if invalid_response {
                    "00"
                } else {
                    &remote_error
                };
                let good_status = migration_response(SchemaMigrationPhase::Prepared, 0);
                let initial = if read_failure {
                    bad_reply
                } else {
                    &good_status
                };
                let replies = [bad_reply.to_string()];
                let mut command = fixture.migration_command(initial, &replies, operation);
                if matches!(operation, "abort" | "adopt") {
                    command.arg("--yes");
                }
                // Status has no update leg; exercise its read failure only.
                if operation == "status" && !read_failure {
                    continue;
                }
                assert_failure(command.output().unwrap());
                assert_eq!(fixture.migration_calls(), usize::from(!read_failure));
            }
        }
        for failure in ["1", "update"] {
            if operation == "status" && failure == "update" {
                continue;
            }
            let fixture = IcpFixture::new();
            let initial = migration_response(SchemaMigrationPhase::Prepared, 0);
            let mut command = fixture.migration_command(&initial, &[], operation);
            command.env("ICP_FIXTURE_TRANSPORT_FAILURE", failure);
            if matches!(operation, "abort" | "adopt") {
                command.arg("--yes");
            }
            assert_failure(command.output().unwrap());
            assert_eq!(fixture.migration_calls(), usize::from(failure == "update"));
        }
    }
}
