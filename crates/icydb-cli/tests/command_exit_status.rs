//! Exercise SQL, canister and migration outcomes through the actual CLI process.
//! A local ICP fixture supplies current Wire responses without a live network.

#![cfg(unix)]

use std::{
    fmt::Write as _,
    fs,
    io::Write as _,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use icydb::{
    Error, ErrorOrigin,
    db::{
        EntityConstraintDescription, EntitySchemaDescription, RowProjectionOutput,
        SchemaMigrationPhase, SchemaMigrationStatusPage, sql::SqlQueryResult,
    },
    diagnostic::{DiagnosticFactTag, RuntimeBoundaryCode},
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
    #[expect(
        clippy::too_many_lines,
        reason = "keep the single executable fixture script with its temporary-path setup"
    )]
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
    if [ "$4" != icydb_fixtures_load ]; then
    [ "$5" = --args-file ] || exit 2
    [ "$7:$8" = --args-format:bin ] || exit 2
    argument=0
    if [ -f "$ICP_FIXTURE_DIRECTORY/argument-count" ]; then read -r argument < "$ICP_FIXTURE_DIRECTORY/argument-count"; fi
    captured="$ICP_FIXTURE_DIRECTORY/argument-$argument.bin"
    command -p cp "$6" "$captured" || exit 2
    printf '%s\n' "$4" "$captured" >> "$ICP_FIXTURE_DIRECTORY/arguments"
    printf '%s\n' "$((argument + 1))" > "$ICP_FIXTURE_DIRECTORY/argument-count"
    fi
    if [ "$ICP_FIXTURE_TRANSPORT_FAILURE" = 1 ] ||
       { [ "$ICP_FIXTURE_TRANSPORT_FAILURE" = update ] && [ "$4" = icydb_schema_migrate ]; }; then
      printf '%s\n' 'fixture transport failure' >&2
      exit 1
    fi
    case "$4" in
      icydb_query) printf '%s' "$ICP_WIRE_QUERY" ;;
      icydb_ddl) printf '%s' "$ICP_WIRE_DDL" ;;
      icydb_update) printf '%s' "$ICP_WIRE_UPDATE" ;;
      icydb_metrics) printf '%s' "$ICP_WIRE_METRICS" ;;
      icydb_metrics_reset) printf '%s' "$ICP_WIRE_METRICS_RESET" ;;
      icydb_snapshot) printf '%s' "$ICP_WIRE_SNAPSHOT" ;;
      icydb_schema) printf '%s' "$ICP_WIRE_SCHEMA" ;;
      icydb_schema_migration) printf '%s' "$ICP_WIRE_SCHEMA_MIGRATION" ;;
      icydb_schema_migrate) printf '%s' "$ICP_WIRE_SCHEMA_MIGRATE" ;;
      icydb_fixtures_load) printf '%s' "$ICP_WIRE_FIXTURES_LOAD" ;;
    esac
    case "$4" in
      icydb_fixtures_load)
        [ "${ICP_FIXTURE_REFRESH_CASE-}" != fixture-failure ] || exit 1
        printf '%s\n' "00"
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
    if command -p cmp -s "$6" "$ICP_FIXTURE_SUCCESS_ARG" || command -p cmp -s "$6" "$ICP_FIXTURE_SUCCESS_ARG_TERMINATED"; then
      printf '%s\n' "$ICP_FIXTURE_SUCCESS"
    else
      printf '%s\n' "$ICP_FIXTURE_RESPONSE"
    fi
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
        let success = self.directory.join("success-argument.bin");
        let terminated = self.directory.join("success-argument-terminated.bin");
        fs::write(
            &success,
            icydb::wire::Endpoint::Query
                .encode_request("SELECT id FROM Character")
                .unwrap(),
        )
        .unwrap();
        fs::write(
            &terminated,
            icydb::wire::Endpoint::Query
                .encode_request("SELECT id FROM Character;")
                .unwrap(),
        )
        .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_icydb"));
        command
            .current_dir(&self.directory)
            .env("PATH", &self.directory)
            .env("ICP_FIXTURE_LOG", self.directory.join("calls"))
            .env("ICP_FIXTURE_DIRECTORY", &self.directory)
            .env("ICP_FIXTURE_RESPONSE", response)
            .env("ICP_FIXTURE_SUCCESS", success_response())
            .env("ICP_FIXTURE_SUCCESS_ARG", &success)
            .env("ICP_FIXTURE_SUCCESS_ARG_TERMINATED", &terminated)
            .env_remove("ICP_FIXTURE_STATUS_ERROR")
            .env_remove("ICP_FIXTURE_REFRESH_CASE")
            .env_remove("ICP_FIXTURE_TRANSPORT_FAILURE");

        for (name, endpoint) in [
            ("QUERY", icydb::wire::Endpoint::Query),
            ("DDL", icydb::wire::Endpoint::Ddl),
            ("UPDATE", icydb::wire::Endpoint::Update),
            ("METRICS", icydb::wire::Endpoint::Metrics),
            ("METRICS_RESET", icydb::wire::Endpoint::MetricsReset),
            ("SNAPSHOT", icydb::wire::Endpoint::Snapshot),
            ("SCHEMA", icydb::wire::Endpoint::Schema),
            ("SCHEMA_MIGRATION", icydb::wire::Endpoint::SchemaMigration),
            ("SCHEMA_MIGRATE", icydb::wire::Endpoint::SchemaMigrate),
            ("FIXTURES_LOAD", icydb::wire::Endpoint::FixturesLoad),
        ] {
            let mut prefix = String::new();
            for byte in endpoint.encode_response(()).unwrap() {
                write!(&mut prefix, "{byte:02x}").unwrap();
            }
            command.env(format!("ICP_WIRE_{name}"), prefix);
        }
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

    fn diagnostic_command(&self, response: &str) -> Command {
        let mut command = self.command(response);
        command.args([
            "diagnostic",
            "E221",
            "--canister",
            "fixture",
            "-e",
            "fixture",
        ]);
        // Complete mutation-field facts preserve the same identity through
        // live resolution, offline metadata and numeric rendering.
        let half = u64::from_be_bytes([7; 8]);
        for (tag, value) in [
            (DiagnosticFactTag::AcceptedSchemaFingerprintMethod, 1),
            (DiagnosticFactTag::AcceptedSchemaFingerprintHigh, half),
            (DiagnosticFactTag::AcceptedSchemaFingerprintLow, half),
            (DiagnosticFactTag::EntityTag, 42),
            (DiagnosticFactTag::FieldId, 1),
            (DiagnosticFactTag::MutationOperation, 1),
        ] {
            command.args(["--fact", &format!("{}={value}", tag.raw())]);
        }
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
                assert!(call.windows(2).any(|pair| pair == ["--args-format", "bin"]));
            }
        }
        for bytes in self.call_arguments("icydb_schema_migration") {
            let request = icydb::wire::Endpoint::SchemaMigration
                .decode_request::<icydb::db::SchemaMigrationStatusRequest>(&bytes)
                .expect("CLI status arguments must carry the maintained request type");
            assert_eq!(request.cursor(), None);
        }
        let commands = self.call_arguments("icydb_schema_migrate");
        for bytes in &commands {
            icydb::wire::Endpoint::SchemaMigrate
                .decode_request::<icydb::db::SchemaMigrationCommand>(bytes)
                .expect("CLI mutation arguments must carry the maintained command type");
        }
        commands.len()
    }

    fn one_shot(&self, response: &str, sql: &str, explicit: bool) -> Output {
        let mut command = self.sql_command(response);
        if explicit {
            command.arg("--sql");
        }
        command.arg(sql).output().unwrap()
    }

    fn assert_call(&self, method: &str, query: bool) {
        let calls = fs::read_to_string(self.directory.join("calls")).unwrap_or_default();
        let arguments = calls.lines().collect::<Vec<_>>();
        assert!(arguments.contains(&method));
        assert_eq!(arguments.contains(&"--query"), query);
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["--args-format", "bin"])
        );
    }

    fn call_arguments(&self, method: &str) -> Vec<Vec<u8>> {
        let calls = fs::read_to_string(self.directory.join("arguments")).unwrap_or_default();
        calls
            .lines()
            .collect::<Vec<_>>()
            .as_chunks::<2>()
            .0
            .iter()
            .filter(|pair| pair[0] == method)
            .map(|pair| fs::read(pair[1]).unwrap())
            .collect()
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

#[test]
fn sql_process_passes_exact_typed_bytes_for_arbitrary_text() {
    for sql in [
        "SELECT name FROM Character".to_string(),
        "SELECT name FROM Character WHERE name = '\u{1b}\u{0085}\u{202e}界e\u{301}🦀'".to_string(),
        "SELECT name FROM Character\nWHERE note = 'a\\path\"quote\r\tb'".to_string(),
        format!(
            "SELECT name FROM Character WHERE name = '{}'",
            "x".repeat(96 * 1024)
        ),
    ] {
        let fixture = IcpFixture::new();
        let output = fixture.one_shot(&success_response(), &sql, true);
        assert!(output.status.success(), "{output:?}");
        fixture.assert_call("icydb_query", true);
        let arguments = fixture.call_arguments("icydb_query");
        assert_eq!(arguments.len(), 1);
        assert_eq!(
            icydb::wire::Endpoint::Query
                .decode_request::<String>(&arguments[0])
                .unwrap(),
            sql
        );
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        for pair in calls.lines().collect::<Vec<_>>().windows(2) {
            if pair[0] == "--args-file" {
                assert!(
                    !std::path::Path::new(pair[1]).exists(),
                    "argument file must be retired after the child completes"
                );
            }
        }
    }
}

#[test]
fn report_commands_preserve_endpoint_frames_and_query_update_selection() {
    use icydb::wire::Endpoint;

    for (arguments, endpoint, query, response) in [
        (
            vec!["metrics"],
            Endpoint::Metrics,
            true,
            response_hex(Ok(icydb::metrics::MetricsReport::default())),
        ),
        (
            vec!["metrics", "--reset"],
            Endpoint::MetricsReset,
            false,
            response_hex(Ok(())),
        ),
        (
            vec!["snapshot"],
            Endpoint::Snapshot,
            true,
            response_hex(Ok(icydb::db::StorageReport::default())),
        ),
        (
            vec!["schema", "show"],
            Endpoint::Schema,
            true,
            response_hex(Ok(Vec::<EntitySchemaDescription>::new())),
        ),
    ] {
        let fixture = IcpFixture::new();
        let output = fixture
            .command(&response)
            .args(arguments)
            .args(["fixture", "-e", "fixture"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
        assert!(!output.stdout.is_empty());
        let requests = fixture.call_arguments(endpoint.name());
        assert_eq!(requests, [endpoint.encode_request(()).unwrap()]);
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(calls.lines().any(|argument| argument == "--query"), query);
    }
}

fn response_hex<T: serde::Serialize>(response: Result<T, Error>) -> String {
    let bytes = icydb_schema::wire::encode_one(&response).unwrap();
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut hex, "{byte:02x}").unwrap();
    }

    hex
}

fn diagnostic_report(
    path: String,
    constraints: Vec<EntityConstraintDescription>,
) -> Vec<EntitySchemaDescription> {
    vec![EntitySchemaDescription::new(
        path,
        "LiveAccount".into(),
        42,
        1,
        [7; 16],
        "id".into(),
        vec!["id".into()],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        constraints,
        1,
        1,
    )]
}

fn diagnostic_artifact_json() -> serde_json::Value {
    serde_json::json!({
        "format": "icydb-diagnostic-schema", "version": 1,
        "provenance": { "kind": "source", "source": "schema/Account" },
        "entities": [{
            "fingerprint_method": 1, "fingerprint": vec![7; 16], "entity_tag": 42,
            "entity_name": "OfflineAccount", "entity_path": "schema::OfflineAccount",
            "fields": [{ "id": 1, "name": "id" }],
            "constraints": [], "indexes": [], "relations": []
        }]
    })
}

#[test]
fn diagnostic_invalid_live_reports_keep_source_metadata_resolution() {
    let constraints = ["id", "other"]
        .into_iter()
        .zip(1_u32..)
        .map(|(field, id)| {
            serde_json::from_value(serde_json::json!({
                "id": id, "name": format!("required_{field}"), "kind": "not_null",
                "origin": "ddl", "validation_state": "validated", "field_id": 1,
                "fields": [field], "semantics": "required"
            }))
            .unwrap()
        })
        .collect();
    let replies = [
        response_hex(Ok(diagnostic_report(
            "x".repeat(icydb_schema::MAX_SOURCE_KEY_BYTES + 1),
            Vec::new(),
        ))),
        response_hex(Ok(diagnostic_report(
            "schema::LiveAccount".into(),
            constraints,
        ))),
        response_hex::<Vec<EntitySchemaDescription>>(Err(Error::from_runtime_boundary(
            RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
            ErrorOrigin::Runtime,
        ))),
        "00".into(),
    ];
    for reply in replies {
        let fixture = IcpFixture::new();
        let artifact = fixture.directory.join("source.json");
        fs::write(&artifact, diagnostic_artifact_json().to_string()).unwrap();
        let output = fixture
            .diagnostic_command(&reply)
            .arg("--source-metadata")
            .arg(&artifact)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let report = String::from_utf8(output.stdout).unwrap();
        assert!(report.contains("entity_tag=42(OfflineAccount)"), "{report}");
        assert!(report.contains("field_id=1(id)"), "{report}");
        assert!(report.lines().any(|line| line.starts_with("note:")));
        fixture.assert_call("icydb_schema", true);
    }
}

#[test]
fn diagnostic_live_lookup_preserves_numeric_fallback_and_exact_artifact_priority() {
    let malformed = response_hex(Ok(diagnostic_report(
        "x".repeat(icydb_schema::MAX_SOURCE_KEY_BYTES + 1),
        Vec::new(),
    )));
    let fixture = IcpFixture::new();
    let output = fixture.diagnostic_command(&malformed).output().unwrap();
    assert!(output.status.success(), "{output:?}");
    let report = String::from_utf8(output.stdout).unwrap();
    assert!(report.contains("entity_tag=42"));
    assert!(!report.contains("LiveAccount"));
    assert!(report.lines().any(|line| line.starts_with("note:")));
    fixture.assert_call("icydb_schema", true);

    let fixture = IcpFixture::new();
    let artifact = fixture.directory.join("deployment.json");
    let mut json = diagnostic_artifact_json();
    json["provenance"] = serde_json::json!({
        "kind": "deployment", "environment": "fixture", "canister": "fixture"
    });
    fs::write(&artifact, json.to_string()).unwrap();
    let output = fixture
        .diagnostic_command(&malformed)
        .arg("--artifact")
        .arg(&artifact)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let report = String::from_utf8(output.stdout).unwrap();
    assert!(report.contains("entity_tag=42(OfflineAccount)"));
    assert!(!fixture.directory.join("calls").exists());
}

#[test]
fn diagnostic_valid_live_reports_and_invalid_explicit_artifacts_keep_their_boundaries() {
    let reply = response_hex(Ok(diagnostic_report(
        "schema::LiveAccount".into(),
        Vec::new(),
    )));
    let fixture = IcpFixture::new();
    let output = fixture.diagnostic_command(&reply).output().unwrap();
    assert!(output.status.success(), "{output:?}");
    let report = String::from_utf8(output.stdout).unwrap();
    assert!(report.contains("entity_tag=42(LiveAccount)"));
    assert!(!report.lines().any(|line| line.starts_with("note:")));
    fixture.assert_call("icydb_schema", true);

    for option in ["--artifact", "--source-metadata"] {
        let fixture = IcpFixture::new();
        let artifact = fixture.directory.join("invalid.json");
        fs::write(&artifact, "{}").unwrap();
        let output = fixture
            .diagnostic_command(&reply)
            .arg(option)
            .arg(&artifact)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!fixture.directory.join("calls").exists());
    }
}

#[test]
fn diagnostic_artifact_options_refuse_fifos_without_waiting_for_a_writer() {
    for option in ["--artifact", "--source-metadata"] {
        let fixture = IcpFixture::new();
        let fifo = fixture.directory.join("artifact.fifo");
        assert!(
            Command::new("/usr/bin/mkfifo")
                .args(["-m", "600"])
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        let link = fixture.directory.join("artifact.link");
        std::os::unix::fs::symlink(&fifo, &link).unwrap();
        for selected in [&fifo, &link] {
            let mut child = fixture
                .diagnostic_command("00")
                .arg(option)
                .arg(selected)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            // This deadline bounds a blocking-open regression in the fixture;
            // it is neither a performance assertion nor a filesystem deadline.
            let deadline = Instant::now() + Duration::from_secs(5);
            while child.try_wait().unwrap().is_none() {
                if Instant::now() >= deadline {
                    child.kill().unwrap();
                    let output = child.wait_with_output().unwrap();
                    panic!("diagnostic waited for a FIFO writer: {output:?}");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let output = child.wait_with_output().unwrap();
            assert!(!output.status.success(), "{output:?}");
            assert!(output.stdout.is_empty(), "{output:?}");
            assert!(!output.stderr.is_empty(), "{output:?}");
            assert!(!fixture.directory.join("calls").exists());
        }
    }
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
                vec!["canister", "refresh", "fixture", "--yes"],
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
        vec!["canister", "refresh", "fixture", "--yes"],
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
            .args(["canister", "refresh", "fixture", "--yes", "-e", "fixture"])
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
fn unsupported_sql_mutations_fail_before_any_icp_process() {
    for sql in [
        "INSERT INTO Character (id, name) VALUES (1, 'Ada')",
        " \t insert into Character (id) SELECT id FROM Other;",
        "DELETE FROM Character",
        " \n delete from Character WHERE id = 1;",
        "DELETE FROM Character RETURNING name",
        "INSERT INTO Character (id, name) VALUES (1, 'Ada') RETURNING name",
    ] {
        for explicit in [false, true] {
            let fixture = IcpFixture::new();
            let output = fixture.one_shot(&success_response(), sql, explicit);
            assert!(!output.status.success(), "{output:?}");
            assert!(output.stdout.is_empty());
            assert!(!output.stderr.is_empty());
            assert!(!fixture.directory.join("calls").exists());
        }
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
    let expected = "surface=count entity=Character row_count=1\n\n";
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
    let expected = concat!(
        "+---------+--------+--------+\n",
        "| missing | lower  | upper  |\n",
        "+---------+--------+--------+\n",
        "| NULL    | 'null' | 'NULL' |\n",
        "+---------+--------+--------+\n\n",
        "1 row,\n\n",
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
fn one_shot_sql_transport_and_invalid_wire_fail_for_every_call_lane() {
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
        let successful_output = "surface=count entity=Character row_count=1";
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains(successful_output)
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
fn migration_run_accepts_repeated_public_pages_until_applied() {
    let fixture = IcpFixture::new();
    let initial = migration_response(SchemaMigrationPhase::Idle, 0);
    // Private cursors can advance without changing any public field. Exercise
    // repeated pages across the complete maintained phase sequence.
    let replies = PROGRESS_PHASES
        .into_iter()
        .flat_map(|phase| std::iter::repeat_n(migration_response(phase, 0), 2))
        .chain(std::iter::once(migration_response(
            SchemaMigrationPhase::Applied,
            0,
        )))
        .collect::<Vec<_>>();
    let output = fixture
        .migration_command(&initial, &replies, "run")
        .output()
        .unwrap();
    assert_migration_outcome(output, SchemaMigrationPhase::Applied, 0);
    assert_eq!(fixture.migration_calls(), replies.len());
}

#[test]
fn migration_run_limits_calls_and_another_invocation_resumes_progress() {
    for phase in [
        SchemaMigrationPhase::Idle,
        SchemaMigrationPhase::FinalValidation,
    ] {
        let fixture = IcpFixture::new();
        let status = migration_response(phase, 0);
        let mut replies = vec![status.clone(); 1_024];
        replies.push(migration_response(SchemaMigrationPhase::Applied, 0));
        let mut command = fixture.migration_command(&status, &replies, "run");

        // A valid extra response detects an off-by-one call and lets a second
        // invocation prove that the ceiling does not discard resumable progress.
        assert_migration_outcome(command.output().unwrap(), phase, 1);
        assert_eq!(fixture.migration_calls(), 1_024);

        assert_migration_outcome(command.output().unwrap(), SchemaMigrationPhase::Applied, 0);
        assert_eq!(fixture.migration_calls(), 1_025);
    }
}

#[test]
fn migration_run_accepts_applied_on_its_last_allowed_call() {
    let fixture = IcpFixture::new();
    let status = migration_response(SchemaMigrationPhase::FinalValidation, 0);
    let mut replies = vec![status.clone(); 1_023];
    replies.push(migration_response(SchemaMigrationPhase::Applied, 0));
    let output = fixture
        .migration_command(&status, &replies, "run")
        .output()
        .unwrap();
    assert_migration_outcome(output, SchemaMigrationPhase::Applied, 0);
    assert_eq!(fixture.migration_calls(), 1_024);
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
fn migration_abort_limits_calls_and_another_invocation_resumes_cleanup() {
    for phase in [
        SchemaMigrationPhase::Prepared,
        SchemaMigrationPhase::Rejected,
    ] {
        let fixture = IcpFixture::new();
        let status = migration_response(phase, 0);
        let mut replies = vec![status.clone(); 1_024];
        replies.push(migration_response(SchemaMigrationPhase::Aborted, 0));
        let mut command = fixture.migration_command(&status, &replies, "abort");
        command.arg("--yes");

        // An additional valid response makes an off-by-one call observable without
        // leaving the unbounded implementation spinning on an infinite substitute.
        assert_migration_outcome(command.output().unwrap(), phase, 1);
        assert_eq!(fixture.migration_calls(), 1_024);

        assert_migration_outcome(command.output().unwrap(), SchemaMigrationPhase::Aborted, 0);
        assert_eq!(fixture.migration_calls(), 1_025);
    }
}

#[test]
fn migration_abort_accepts_terminal_receipt_on_its_last_allowed_call() {
    let fixture = IcpFixture::new();
    let status = migration_response(SchemaMigrationPhase::Rejected, 0);
    let mut replies = vec![status.clone(); 1_023];
    replies.push(migration_response(SchemaMigrationPhase::Aborted, 0));
    let output = fixture
        .migration_command(&status, &replies, "abort")
        .arg("--yes")
        .output()
        .unwrap();
    assert_migration_outcome(output, SchemaMigrationPhase::Aborted, 0);
    assert_eq!(fixture.migration_calls(), 1_024);
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
fn migration_loops_reject_changed_database_or_plan_after_repeated_pages() {
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
            let replies = [initial.clone(), initial.clone(), response_hex(Ok(status))];
            let mut command = fixture.migration_command(&initial, &replies, operation);
            if operation == "abort" {
                command.arg("--yes");
            }
            assert_failure(command.output().unwrap());
            assert_eq!(fixture.migration_calls(), replies.len());
        }
    }
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
                let mut replies = Vec::new();
                if operation == "run" && !read_failure {
                    replies.extend([good_status.clone(), good_status.clone()]);
                }
                replies.push(bad_reply.to_string());
                let mut command = fixture.migration_command(initial, &replies, operation);
                if matches!(operation, "abort" | "adopt") {
                    command.arg("--yes");
                }
                // Status has no update leg; exercise its read failure only.
                if operation == "status" && !read_failure {
                    continue;
                }
                assert_failure(command.output().unwrap());
                assert_eq!(
                    fixture.migration_calls(),
                    if read_failure { 0 } else { replies.len() }
                );
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
