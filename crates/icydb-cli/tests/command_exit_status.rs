//! Exercise SQL, canister and migration outcomes through the actual CLI process.
//! A local ICP fixture supplies current Wire responses without a live network.

#![cfg(unix)]

mod parse_errors;

use std::{
    fmt::Write as _,
    fs,
    io::Write as _,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

use ic_host_fs::testing::create_fixture_directory;
use ic_host_process::tool::{OutputLimit, OutputLimits, capture_command};
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
        let parent = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
        fs::create_dir_all(&parent).unwrap();
        // PID reuse must not collide with retained failures. The shared owner
        // allocates a fresh directory without reclaiming earlier evidence.
        let directory = create_fixture_directory(&parent, "icydb-command-status").unwrap();
        let executable = directory.join("icp");
        // Only the child receives this PATH. Unexpected calls fail closed;
        // the log proves the CLI reached the selected endpoint and call lane.
        fs::write(
            &executable,
            r#"#!/bin/sh
printf '%s\n' "$@" >> "$ICP_FIXTURE_LOG"
if [ -n "${ICP_FIXTURE_ARGV-}" ]; then
  printf '%s\000' "$@" >> "$ICP_FIXTURE_ARGV"
fi
case "$1:$2" in
  project:show)
    [ "$#" -eq 2 ] || exit 2
    case "${ICP_FIXTURE_PROJECT_CASE-}" in
      invalid-utf8) printf '\377' ;;
      oversized) command -p dd if=/dev/zero bs=1048576 count=17 2>/dev/null ;;
      *)
        if [ -n "${ICP_FIXTURE_PROJECT_RESPONSE+x}" ]; then
          printf '%s\n' "$ICP_FIXTURE_PROJECT_RESPONSE"
        else
          printf '%s\n' 'environments: {local: {name: local, network: {name: local}}, fixture: {name: fixture, network: {name: ic}}}'
        fi
        ;;
    esac
    exit "${ICP_FIXTURE_PROJECT_STATUS:-0}"
    ;;
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
  canister:list)
    printf '%s' "${ICP_FIXTURE_LIST_ERROR-}" >&2
    case "${ICP_FIXTURE_LIST_CAPTURE-}" in
      stdout) command -p dd if=/dev/zero bs=1048576 count=17 2>/dev/null ;;
      stderr) (command -p dd if=/dev/zero bs=1024 count=65 2>/dev/null) >&2 ;;
      signal) kill -TERM "$$" ;;
    esac
    if [ -n "${ICP_FIXTURE_LIST_RESPONSE+x}" ]; then
      printf '%s\n' "$ICP_FIXTURE_LIST_RESPONSE"
    else
      printf '%s\n' '{"canisters":["fixture"]}'
    fi
    exit "${ICP_FIXTURE_LIST_STATUS:-0}"
    ;;
  canister:status)
    if [ -n "${ICP_FIXTURE_STATUS_ERROR-}" ]; then
      printf '%s\n' "$ICP_FIXTURE_STATUS_ERROR" >&2
      exit 7
    fi
    case "${ICP_FIXTURE_REFRESH_CASE-}" in
      status-failure) printf '%s\n' 'connection refused' >&2; exit 1 ;;
      uninstalled|uninstalled-deploy-failure) printf '%s\n' '{"module_hash":null}' ;;
      invalid-status) printf '%s\n' '{"status":"running"}' ;;
      invalid-json) printf '%s\n' '{' ;;
      invalid-module-hash) printf '%s\n' '{"module_hash":42}' ;;
      *)
        case "$4" in
          --json)
            if [ -f "$ICP_FIXTURE_DIRECTORY/deployed" ] && [ -n "${ICP_FIXTURE_BALANCE_JSON+x}" ]; then
              printf '%s\n' "$ICP_FIXTURE_BALANCE_JSON"
              exit "${ICP_FIXTURE_BALANCE_STATUS:-0}"
            elif [ -n "${ICP_FIXTURE_CYCLES+x}" ]; then
              printf '{"module_hash":"0x0123","cycles":"%s"}\n' "$ICP_FIXTURE_CYCLES"
            else
              printf '%s\n' '{"module_hash":"0x0123"}'
            fi
            ;;
          --id-only) printf '%s\n' "${ICP_FIXTURE_STATUS_ID-aaaaa-aa}" ;;
          *) printf '%s\n' "${ICP_FIXTURE_STATUS_ID-aaaaa-aa}" ;;
        esac
        ;;
    esac
    ;;
  deploy:fixture)
    case "${ICP_FIXTURE_REFRESH_CASE-}" in
      deploy-failure|uninstalled-deploy-failure) exit 1 ;;
    esac
    printf '%s\n' deployed > "$ICP_FIXTURE_DIRECTORY/deployed"
    ;;
  canister:top-up)
    [ "$3:$4:$5:$6:$7" = --amount:100t:fixture:--environment:fixture ] || exit 2
    ;;
  canister:call)
    [ -f "$6" ] || exit 2
    printf '%s\n' "$6" >> "$ICP_FIXTURE_DIRECTORY/argument-paths"
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
    if [ "$4" = "${ICP_FIXTURE_CAPTURE_METHOD-}" ]; then
      printf '%s\n' "$4" >> "$ICP_FIXTURE_DIRECTORY/effects"
      case "$ICP_FIXTURE_CAPTURE_CASE" in
        stdout) command -p dd if=/dev/zero bs=1048576 count=17 2>/dev/null; exit 0 ;;
        stderr) (command -p dd if=/dev/zero bs=1024 count=65 2>/dev/null) >&2; exit 0 ;;
        wait)
          command -p sleep 10 &
          printf armed > "$ICP_FIXTURE_DIRECTORY/capture-ready"
          wait
          ;;
      esac
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
            icydb::__macro::RpcMethod::Query
                .encode_request(
                    "SELECT id FROM Character",
                    icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
                )
                .unwrap(),
        )
        .unwrap();
        fs::write(
            &terminated,
            icydb::__macro::RpcMethod::Query
                .encode_request(
                    "SELECT id FROM Character;",
                    icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
                )
                .unwrap(),
        )
        .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_icydb"));
        // Fixture controls belong to this invocation. Clear ambient overrides
        // without mutating the test process's environment; each test may then
        // set its own child-only controls after the defaults below.
        for (name, _) in std::env::vars_os() {
            let bytes = name.as_encoded_bytes();
            if bytes.starts_with(b"ICP_FIXTURE_")
                || bytes.starts_with(b"ICP_WIRE_")
                || name == "ICP_ENVIRONMENT"
            {
                command.env_remove(name);
            }
        }
        command
            .current_dir(&self.directory)
            .env("PATH", &self.directory)
            .env("ICP_FIXTURE_LOG", self.directory.join("calls"))
            .env("ICP_FIXTURE_DIRECTORY", &self.directory)
            .env("ICP_FIXTURE_RESPONSE", response)
            .env("ICP_FIXTURE_SUCCESS", success_response())
            .env("ICP_FIXTURE_SUCCESS_ARG", &success)
            .env("ICP_FIXTURE_SUCCESS_ARG_TERMINATED", &terminated);

        for (name, endpoint) in [
            ("QUERY", icydb::__macro::RpcMethod::Query),
            ("DDL", icydb::__macro::RpcMethod::Ddl),
            ("UPDATE", icydb::__macro::RpcMethod::Update),
            ("METRICS", icydb::__macro::RpcMethod::Metrics),
            ("METRICS_RESET", icydb::__macro::RpcMethod::MetricsReset),
            ("SNAPSHOT", icydb::__macro::RpcMethod::Snapshot),
            ("SCHEMA", icydb::__macro::RpcMethod::Schema),
            (
                "SCHEMA_MIGRATION",
                icydb::__macro::RpcMethod::SchemaMigration,
            ),
            ("SCHEMA_MIGRATE", icydb::__macro::RpcMethod::SchemaMigrate),
            ("FIXTURES_LOAD", icydb::__macro::RpcMethod::FixturesLoad),
        ] {
            let mut prefix = String::new();
            for byte in endpoint
                .encode_response((), icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
                .unwrap()
            {
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
            let request = icydb::__macro::RpcMethod::SchemaMigration
                .decode_request::<icydb::db::SchemaMigrationStatusRequest>(
                    &bytes,
                    icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
                )
                .expect("CLI status arguments must carry the maintained request type");
            assert_eq!(request.cursor(), None);
        }
        let commands = self.call_arguments("icydb_schema_migrate");
        for bytes in &commands {
            icydb::__macro::RpcMethod::SchemaMigrate
                .decode_request::<icydb::db::SchemaMigrationCommand>(
                    bytes,
                    icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
                )
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

// This watchdog bounds a blocking-file regression, not filesystem performance.
// The shared process owner drains output and cleans up the captured child.
fn file_admission_output(command: &mut Command) -> Output {
    let captured = capture_command(
        command.stdin(Stdio::null()),
        OutputLimits {
            stdout: OutputLimit::Terminate(64 * 1024),
            stderr: OutputLimit::Terminate(64 * 1024),
            timeout: Some(Duration::from_secs(5)),
        },
    )
    .expect("file admission must finish without waiting for a FIFO writer");
    Output {
        status: captured.status,
        stdout: captured.stdout,
        stderr: captured.stderr,
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
            icydb::__macro::RpcMethod::Query
                .decode_request::<String>(
                    &arguments[0],
                    icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES)
                )
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
    use icydb::__macro::RpcMethod;

    for (arguments, endpoint, query, response) in [
        (
            vec!["metrics"],
            RpcMethod::Metrics,
            true,
            response_hex(Ok(icydb::metrics::MetricsReport::default())),
        ),
        (
            vec!["metrics", "--reset"],
            RpcMethod::MetricsReset,
            false,
            response_hex(Ok(())),
        ),
        (
            vec!["snapshot"],
            RpcMethod::Snapshot,
            true,
            response_hex(Ok(icydb::db::StorageReport::default())),
        ),
        (
            vec!["schema", "show"],
            RpcMethod::Schema,
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
        assert_eq!(
            requests,
            [endpoint
                .encode_request((), icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES))
                .unwrap()]
        );
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
fn unsupported_artifact_formats_are_terminal_safe_before_calls_or_publication() {
    for format in [
        "",
        "other-format",
        "界-e\u{301}-\\n\t\r\n\x1b]52;c;data\x07\u{0085}\u{202e}\u{2028}",
    ] {
        for route in ["--artifact", "--source-metadata", "export"] {
            let fixture = IcpFixture::new();
            let artifact = fixture.directory.join("unsupported.json");
            let destination = fixture.directory.join("output.json");
            let mut json = diagnostic_artifact_json();
            json["format"] = format.into();
            fs::write(&artifact, json.to_string()).unwrap();
            let mut command = if route == "export" {
                let mut command = fixture.command("");
                command
                    .args(["schema", "diagnostic-source-metadata", "--artifact"])
                    .arg(&artifact)
                    .args(["--source", "fixture-source", "--output"])
                    .arg(&destination);
                command
            } else {
                let mut command = fixture.diagnostic_command("");
                command.arg(route).arg(&artifact);
                command
            };
            let output = command.output().unwrap();
            assert!(!output.status.success(), "{route}: {output:?}");
            assert!(output.stdout.is_empty());
            let stderr = assert_terminal_lines(&output.stderr);
            assert_eq!(
                stderr,
                format!(
                    "ERROR: unsupported diagnostic artifact format/version '{}'/1; expected 'icydb-diagnostic-schema'/1\n",
                    ic_host_text::escape::terminal_text(format),
                )
            );
            assert!(!fixture.directory.join("calls").exists());
            assert!(!destination.exists());
        }
    }
}

#[test]
fn diagnostic_lookup_labels_keep_terminal_lines_and_exact_schema_matching() {
    for route in ["--artifact", "--source-metadata", "live"] {
        for stale in [false, true] {
            let mut line_count = None;
            for label in [
                "界-e\u{301}-\\n",
                "界-e\u{301}-\\n\t\r\n\x1b]52;c;data\x07\u{0085}\u{202e}\u{2028}",
            ] {
                let fixture = IcpFixture::new();
                let path = format!("schema::{label}");
                let field = format!("field-{label}");
                let fingerprint = if stale { 8 } else { 7 };
                let constraint = serde_json::from_value(serde_json::json!({
                    "id": 3, "name": "required", "kind": "not_null",
                    "origin": "ddl", "validation_state": "validated", "field_id": 1,
                    "fields": [&field], "semantics": "required"
                }))
                .unwrap();
                let mut report =
                    serde_json::to_value(diagnostic_report(path.clone(), vec![constraint]))
                        .unwrap();
                report[0]["entity_name"] = label.into();
                report[0]["accepted_schema_fingerprint"] = serde_json::json!(vec![fingerprint; 16]);
                let report: Vec<EntitySchemaDescription> = serde_json::from_value(report).unwrap();
                let reply = response_hex(Ok(report));
                let artifact = fixture.directory.join("labels.json");
                let mut json = diagnostic_artifact_json();
                json["entities"][0]["entity_name"] = label.into();
                json["entities"][0]["entity_path"] = path.clone().into();
                json["entities"][0]["fields"][0]["name"] = field.clone().into();
                json["entities"][0]["fingerprint"] = serde_json::json!(vec![fingerprint; 16]);
                if route == "--artifact" {
                    json["provenance"] = serde_json::json!({"kind":"deployment","environment":"fixture","canister":"fixture"});
                }
                let bytes = serde_json::to_vec(&json).unwrap();
                fs::write(&artifact, &bytes).unwrap();
                let mut command = fixture.command(&reply);
                command.args(["diagnostic", "E221"]);
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
                if route == "live" {
                    command.args(["--canister", "fixture", "-e", "fixture"]);
                } else {
                    command.arg(route).arg(&artifact);
                }
                let output = command.output().unwrap();
                assert!(output.status.success(), "{route}: {output:?}");
                assert!(output.stderr.is_empty());
                let stdout = assert_terminal_lines(&output.stdout);
                if let Some(expected) = line_count {
                    assert_eq!(stdout.lines().count(), expected);
                } else {
                    line_count = Some(stdout.lines().count());
                }
                if stale {
                    assert!(!stdout.contains("accepted entity:"));
                    assert!(stdout.contains("names withheld"));
                    assert!(!stdout.contains("field-"));
                } else {
                    assert!(stdout.contains(&format!(
                        "accepted entity: {} ({})",
                        ic_host_text::escape::terminal_text(label),
                        ic_host_text::escape::terminal_text(&path)
                    )));
                    assert!(stdout.contains(&format!(
                        "field_id=1({})",
                        ic_host_text::escape::terminal_text(&field)
                    )));
                }
                assert_eq!(fs::read(&artifact).unwrap(), bytes);
                assert_eq!(fixture.directory.join("calls").exists(), route == "live");
            }
        }
    }
}

#[test]
fn diagnostic_lookup_input_errors_keep_terminal_controls_visible() {
    for case in [
        "code",
        "overflow",
        "fact-shape",
        "fact-tag",
        "fact-value",
        "json",
    ] {
        for label in [
            "界-e\u{301}-\\n",
            "界-e\u{301}-\\n\t\r\n\x1b[2J\u{0085}\u{202e}\u{2028}",
        ] {
            let fixture = IcpFixture::new();
            let mut command = fixture.command("");
            command.arg("diagnostic");
            let input = match case {
                "overflow" => "\r\n999999\t\u{2028}".to_string(),
                "fact-tag" => format!("{label}=1"),
                "fact-value" => format!("entity_tag={label}"),
                _ => label.to_string(),
            };
            if case == "json" {
                let path = fixture.directory.join("error.json");
                fs::write(
                    &path,
                    serde_json::json!({"code":label,"class":0,"origin":0,"facts":[]}).to_string(),
                )
                .unwrap();
                command.arg("--error-json").arg(path);
            } else if case.starts_with("fact-") {
                command.args(["E221", "--fact", &input]);
            } else {
                command.arg(&input);
            }
            let output = command.output().unwrap();
            assert!(!output.status.success(), "{case}: {output:?}");
            assert!(output.stdout.is_empty());
            let stderr = assert_terminal_lines(&output.stderr);
            if case == "json" {
                assert_eq!(output.status.code(), Some(1));
                assert_eq!(stderr.lines().count(), 1, "{case}: {stderr}");
            } else {
                assert_eq!(output.status.code(), Some(2));
            }
            if case != "json" {
                let displayed_input = if case == "fact-tag" { label } else { &input };
                assert!(
                    stderr.contains(ic_host_text::escape::terminal_text(displayed_input).as_ref())
                );
            }
            assert!(!fixture.directory.join("calls").exists());
        }
    }
}

#[test]
fn diagnostic_fact_count_remains_domain_admission_after_typed_parsing() {
    let fixture = IcpFixture::new();
    let mut command = fixture.command("");
    command.args(["diagnostic", "65535"]);
    for _ in 0..=icydb::diagnostic::MAX_PUBLIC_DIAGNOSTIC_FACTS {
        command.args(["--fact", "250=18446744073709551615"]);
    }
    assert_failure(command.output().unwrap());
    assert!(!fixture.directory.join("calls").exists());
}

#[test]
fn diagnostic_file_options_refuse_fifos_without_waiting_for_a_writer() {
    for option in ["--artifact", "--source-metadata", "--error-json"] {
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
            let mut command = if option == "--error-json" {
                let mut command = fixture.command("");
                command.arg("diagnostic");
                command
            } else {
                fixture.diagnostic_command("00")
            };
            let output = file_admission_output(command.arg(option).arg(selected));
            assert!(!output.status.success(), "{output:?}");
            assert!(output.stdout.is_empty(), "{output:?}");
            assert!(!output.stderr.is_empty(), "{output:?}");
            assert!(!fixture.directory.join("calls").exists());
        }
    }
}

#[test]
fn diagnostic_error_json_keeps_regular_aliases_stdin_and_the_existing_byte_limit() {
    let error = Error::from_runtime_boundary(
        RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
        ErrorOrigin::Runtime,
    );
    let mut expected_report = None;
    for oversized in [false, true] {
        let mut bytes = serde_json::to_vec(&error).unwrap();
        bytes.resize(64 * 1024 + usize::from(oversized), b' ');
        for route in ["file", "symlink", "stdin"] {
            let fixture = IcpFixture::new();
            let input = fixture.directory.join("error.json");
            fs::write(&input, &bytes).unwrap();
            let link = fixture.directory.join("error.link");
            std::os::unix::fs::symlink(&input, &link).unwrap();
            let mut command = fixture.command("");
            command.args(["diagnostic", "--error-json"]);
            let output = if route == "stdin" {
                let mut child = command
                    .arg("-")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                child.stdin.take().unwrap().write_all(&bytes).unwrap();
                child.wait_with_output().unwrap()
            } else {
                command
                    .arg(if route == "file" { &input } else { &link })
                    .output()
                    .unwrap()
            };
            assert_eq!(output.status.success(), !oversized, "{route}: {output:?}");
            if oversized {
                assert!(output.stdout.is_empty());
                assert!(!output.stderr.is_empty());
            } else {
                assert!(output.stderr.is_empty());
                assert!(!output.stdout.is_empty());
                if let Some(expected) = &expected_report {
                    assert_eq!(&output.stdout, expected);
                } else {
                    expected_report = Some(output.stdout);
                }
            }
            assert!(!fixture.directory.join("calls").exists());
        }
    }
}

#[test]
fn effective_project_network_reserves_fixture_cycles_from_nested_directories() {
    for nested in [false, true] {
        let fixture = IcpFixture::new();
        // ICP accepts flow-style environment declarations; the CLI must use
        // resolved project facts even when invoked below the project root.
        fs::write(
            fixture.directory.join("icp.yaml"),
            "canisters:\n  - name: fixture\n    build:\n      steps:\n        - type: script\n          commands: [\"false\"]\nenvironments:\n  - {name: fixture, network: local, canisters: [fixture]}\n",
        )
        .unwrap();
        let working_directory = if nested {
            let directory = fixture.directory.join("nested");
            fs::create_dir(&directory).unwrap();
            directory
        } else {
            fixture.directory.clone()
        };
        let output = fixture
            .command("")
            .current_dir(working_directory)
            .env("ICP_FIXTURE_CYCLES", "1_000")
            .env(
                "ICP_FIXTURE_PROJECT_RESPONSE",
                "environments: {fixture: {name: fixture, network: {name: local}}}",
            )
            .args(["canister", "refresh", "fixture", "--yes", "-e", "fixture"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        let calls = calls.lines().collect::<Vec<_>>();
        for method in ["show", "top-up", "icydb_fixtures_load"] {
            assert_eq!(calls.iter().filter(|line| **line == method).count(), 1);
        }
        let position = |method| calls.iter().position(|line| *line == method).unwrap();
        assert!(position("show") < position("top-up"));
        assert!(position("top-up") < position("icydb_fixtures_load"));
    }
}

#[test]
fn fixture_cycle_balances_use_json_and_keep_local_funding_thresholds() {
    for (response, funded) in [
        (r#"{"cycles":"0"}"#, true),
        (r#"{"cycles":"1_418_380_664_222"}"#, true),
        (r#"{"cycles":"50_000_000_000_000"}"#, false),
        (
            r#"{"cycles":"340282366920938463463374607431768211455"}"#,
            false,
        ),
        (r#"{"module_hash":"0x0123","controllers":[]}"#, false),
    ] {
        let fixture = IcpFixture::new();
        let output = fixture
            .command("")
            .env("ICP_FIXTURE_BALANCE_JSON", response)
            .env(
                "ICP_FIXTURE_PROJECT_RESPONSE",
                "environments: {fixture: {name: fixture, network: {name: local}}}",
            )
            .args(["canister", "refresh", "fixture", "--yes", "-e", "fixture"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(0), "{response}: {output:?}");
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        let calls = calls.lines().collect::<Vec<_>>();
        assert_eq!(calls.iter().filter(|line| **line == "status").count(), 2);
        assert_eq!(calls.iter().filter(|line| **line == "--json").count(), 2);
        assert_eq!(calls.contains(&"top-up"), funded, "{response}");
        assert_eq!(
            calls
                .iter()
                .filter(|line| **line == "icydb_fixtures_load")
                .count(),
            1
        );
    }
}

#[test]
fn refused_cycle_observations_stop_before_funding_and_fixture_calls() {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};

    for (response, status) in [
        (b"{".as_slice(), "0"),
        (br#"{"cycles":null}"#, "0"),
        (br#"{"cycles":0}"#, "0"),
        (br#"{"cycles":"-1"}"#, "0"),
        (br#"{"cycles":"1_00"}"#, "0"),
        (br#"{"cycles":"0","cycles":"1"}"#, "0"),
        (
            br#"{"cycles":"340282366920938463463374607431768211456"}"#,
            "0",
        ),
        (br#"{"cycles":"0"} {}"#, "0"),
        (br#"{"cycles":"\u001b[2J"}"#, "0"),
        (b"{\"cycles\":\"0\",\"extra\":\"\xff\"}", "0"),
        (br#"{"cycles":"0"}"#, "7"),
    ] {
        let fixture = IcpFixture::new();
        let output = fixture
            .command("")
            .env("ICP_FIXTURE_BALANCE_JSON", OsStr::from_bytes(response))
            .env("ICP_FIXTURE_BALANCE_STATUS", status)
            .env(
                "ICP_FIXTURE_PROJECT_RESPONSE",
                "environments: {fixture: {name: fixture, network: {name: local}}}",
            )
            .args(["canister", "refresh", "fixture", "--yes", "-e", "fixture"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{response:?}: {output:?}");
        assert!(output.stdout.is_empty());
        assert_terminal_lines(&output.stderr);
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        let calls = calls.lines().collect::<Vec<_>>();
        assert_eq!(calls.iter().filter(|line| **line == "deploy").count(), 1);
        assert_eq!(calls.iter().filter(|line| **line == "--json").count(), 2);
        assert!(!calls.contains(&"top-up"));
        assert!(!calls.contains(&"icydb_fixtures_load"));
    }
}

#[test]
fn effective_project_unknown_or_remote_network_never_enables_fixture_funding() {
    let local = "environments: {fixture: {name: fixture, network: {name: local}}}";
    for (environment, response, status, case) in [
        ("fixture", local, "7", ""),
        (
            "local",
            "environments: {local: {name: local, network: {name: local}}}",
            "7",
            "",
        ),
        ("fixture", "{}", "0", ""),
        (
            "fixture",
            "environments: {other: {name: other, network: {name: local}}}",
            "0",
            "",
        ),
        (
            "fixture",
            "environments: {fixture: {name: fixture, network: {name: ic}}}",
            "0",
            "",
        ),
        ("Fixture", local, "0", ""),
        ("fixture", local, "0", "invalid-utf8"),
        ("fixture", local, "0", "oversized"),
    ] {
        let fixture = IcpFixture::new();
        // A raw local hint cannot override failed/absent effective facts.
        fs::write(
            fixture.directory.join("icp.yaml"),
            format!("environments:\n  - name: {environment}\n    network: local\n"),
        )
        .unwrap();
        let output = fixture
            .command("")
            .env("ICP_FIXTURE_CYCLES", "1_000")
            .env("ICP_FIXTURE_PROJECT_RESPONSE", response)
            .env("ICP_FIXTURE_PROJECT_STATUS", status)
            .env("ICP_FIXTURE_PROJECT_CASE", case)
            .args(["canister", "refresh", "fixture", "--yes", "-e", environment])
            .output()
            .unwrap();
        assert!(output.status.success(), "{environment}/{case}: {output:?}");
        let stderr = assert_terminal_lines(&output.stderr);
        assert!(
            stderr.contains(&format!(
                "local ICP network is not confirmed for environment '{}'",
                ic_host_text::escape::terminal_text(environment)
            )),
            "{stderr}"
        );
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert!(!calls.lines().any(|line| line == "top-up"));
        assert_eq!(calls.lines().filter(|line| *line == "status").count(), 1);
        assert_eq!(calls.lines().filter(|line| *line == "show").count(), 1);
        assert_eq!(
            calls
                .lines()
                .filter(|line| *line == "icydb_fixtures_load")
                .count(),
            1
        );
    }
}

#[test]
fn manifest_file_admission_does_not_wait_for_fifo_writers() {
    for refresh in [false, true] {
        for symlink in [false, true] {
            let fixture = IcpFixture::new();
            let manifest = fixture.directory.join("icp.yaml");
            let fifo = if symlink {
                fixture.directory.join("manifest.fifo")
            } else {
                manifest.clone()
            };
            assert!(
                Command::new("/usr/bin/mkfifo")
                    .args(["-m", "600"])
                    .arg(&fifo)
                    .status()
                    .unwrap()
                    .success()
            );
            if symlink {
                std::os::unix::fs::symlink(&fifo, &manifest).unwrap();
            }
            let mut command = fixture.command("");
            if refresh {
                command.args(["canister", "refresh", "fixture", "--yes", "-e", "fixture"]);
            } else {
                command
                    .env("ICP_FIXTURE_LIST_STATUS", "7")
                    .args(["canister", "list", "-e", "fixture"]);
            }
            let output = file_admission_output(&mut command);
            let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
            assert!(!calls.lines().any(|line| line == "top-up"));
            if refresh {
                // Network discovery uses effective project facts, so refresh
                // no longer reads this raw manifest. Fixture loading continues.
                assert!(output.status.success(), "{output:?}");
                assert_eq!(
                    calls
                        .lines()
                        .filter(|line| *line == "icydb_fixtures_load")
                        .count(),
                    1
                );
            } else {
                assert_eq!(output.status.code(), Some(1), "{output:?}");
                assert!(output.stdout.is_empty());
                assert!(!output.stderr.is_empty());
                assert!(
                    !calls
                        .lines()
                        .any(|line| line == "status" || line == "call" || line == "deploy")
                );
            }
        }
    }
}

#[test]
fn manifest_file_admission_preserves_regular_symlinks_and_utf8_refusal() {
    for contents in [
        b"environments:\n  - name: fixture\n    canisters: [fixture]\n".as_slice(),
        b"\xff",
    ] {
        for symlink in [false, true] {
            let fixture = IcpFixture::new();
            let manifest = fixture.directory.join("icp.yaml");
            if symlink {
                let target = fixture.directory.join("project.yaml");
                fs::write(&target, contents).unwrap();
                std::os::unix::fs::symlink(target, &manifest).unwrap();
            } else {
                fs::write(&manifest, contents).unwrap();
            }
            let output = file_admission_output(
                fixture
                    .command("")
                    .env("ICP_FIXTURE_LIST_STATUS", "7")
                    .args(["canister", "list", "-e", "fixture"]),
            );
            if std::str::from_utf8(contents).is_ok() {
                assert!(output.status.success(), "{output:?}");
                assert!(
                    String::from_utf8(output.stdout)
                        .unwrap()
                        .contains("fixture")
                );
            } else {
                assert_eq!(output.status.code(), Some(1), "{output:?}");
                assert!(output.stdout.is_empty());
                assert!(!output.stderr.is_empty());
            }
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
fn malformed_canister_ids_stop_before_read_and_update_calls() {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};

    let oversized = [b'a'; 4096];
    for id in [
        b"not-a-principal".as_slice(),
        b"aaaaa-ab",
        b"aaaaa-aa\naaaaa-aa",
        b"\x1b[2J",
        b"\xff",
        oversized.as_slice(),
    ] {
        for arguments in [
            vec!["canister", "list"],
            vec!["metrics", "fixture"],
            vec!["metrics", "--reset", "fixture"],
            vec!["schema", "show", "fixture"],
        ] {
            let fixture = IcpFixture::new();
            let output = fixture
                .command(&success_response())
                .env("ICP_FIXTURE_STATUS_ID", OsStr::from_bytes(id))
                .args(arguments)
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(1));
            assert!(!output.stderr.is_empty());
            let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
            assert!(!calls.lines().any(|line| line == "call" || line == "deploy"));
        }
    }
}

#[test]
fn canister_id_admission_preserves_valid_and_absent_observations() {
    let maximum = icydb_schema::Principal::from_slice(&[42; 29]).to_string();
    for id in [
        "aaaaa-aa",
        maximum.as_str(),
        " \taaaaa-aa\r\n",
        "",
        " \t\r\n",
    ] {
        let fixture = IcpFixture::new();
        let output = fixture
            .command(&success_response())
            .env("ICP_FIXTURE_STATUS_ID", id)
            .args(["canister", "list"])
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(0),
            "canister id {id:?}: {output:?}"
        );
        if !id.trim().is_empty() {
            assert!(
                String::from_utf8(output.stdout)
                    .unwrap()
                    .contains(id.trim())
            );
        }
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert!(!calls.lines().any(|line| line == "call" || line == "deploy"));
    }
}

#[test]
fn ambient_fixture_controls_cannot_change_canister_observations() {
    // A child test process supplies ambient state without racing other tests
    // through process-global environment mutation. It runs the real admission
    // control, including explicit per-case overrides and the fake ICP process.
    for (name, value) in [
        ("ICP_FIXTURE_LIST_RESPONSE", "{}"),
        ("ICP_FIXTURE_LIST_STATUS", "7"),
        ("ICP_FIXTURE_STATUS_ERROR", "ambient probe failure"),
        ("ICP_FIXTURE_REFRESH_CASE", "invalid-json"),
    ] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                stringify!(canister_id_admission_preserves_valid_and_absent_observations),
                "--nocapture",
            ])
            .env(name, value)
            .output()
            .unwrap();
        assert!(output.status.success(), "ambient {name}: {output:?}");
    }
}

#[test]
fn malformed_inventory_cannot_fall_back_to_the_manifest() {
    for response in [
        "{}",
        "null",
        r#"[["fixture"]]"#,
        r#"{"canisters":null}"#,
        r#"{"canisters":[],"canisters":["fixture"]}"#,
        r#"{"canisters":42}"#,
        r#"{"canisters":["fixture",42]}"#,
    ] {
        let fixture = IcpFixture::new();
        fs::write(
            fixture.directory.join("icp.yaml"),
            "environments:\n  - name: local\n    canisters: [fixture]\n",
        )
        .unwrap();
        let output = fixture
            .command(&success_response())
            .env("ICP_FIXTURE_LIST_RESPONSE", response)
            .args(["canister", "list", "-e", "local"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert!(!calls.lines().any(|line| line == "status"));
    }
}

#[test]
fn upgrade_path_messages_are_safe_and_install_uses_original_path_bytes() {
    for exists in [false, true] {
        let mut line_counts = None;
        for label in [
            "界-\\n.wasm",
            "界-\\n\r\n\x1b[2J\u{0085}\u{202e}\u{2028}.wasm",
        ] {
            let fixture = IcpFixture::new();
            let selected = fixture.directory.join(label);
            if exists {
                fs::write(&selected, b"selected override\n").unwrap();
            }
            let arguments = fixture.directory.join("observed-argv");
            let output = fixture
                .upgrade_command("fixture", false)
                .env("ICP_FIXTURE_ARGV", &arguments)
                .arg("--wasm")
                .arg(&selected)
                .output()
                .unwrap();
            assert_eq!(output.status.success(), exists, "{output:?}");
            let stdout = assert_terminal_lines(&output.stdout);
            let stderr = assert_terminal_lines(&output.stderr);
            let counts = (stdout.lines().count(), stderr.lines().count());
            if let Some(expected) = line_counts {
                assert_eq!(counts, expected);
            } else {
                line_counts = Some(counts);
            }
            let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
            assert_eq!(calls.lines().any(|line| line == "install"), exists);
            if exists {
                assert_eq!(
                    fs::read(fixture.directory.join("installed")).unwrap(),
                    b"selected override\n"
                );
                let bytes = fs::read(arguments).unwrap();
                assert!(
                    bytes
                        .split(|byte| *byte == 0)
                        .any(|argument| argument == selected.as_os_str().as_encoded_bytes())
                );
            } else {
                assert!(stderr.contains(
                    ic_host_text::escape::terminal_text(&selected.display().to_string()).as_ref()
                ));
            }
        }
    }
}

#[test]
fn history_path_warnings_are_safe_without_losing_completed_statements() {
    let mut line_count = None;
    for label in [
        "界-\\n-history",
        "界-\\n-history\r\n\x1b[2J\u{0085}\u{202e}\u{2028}",
    ] {
        let fixture = IcpFixture::new();
        let history = fixture.directory.join(label);
        fs::create_dir(&history).unwrap();
        let mut child = fixture
            .sql_command(&success_response())
            .arg("--history-file")
            .arg(&history)
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
        assert!(output.status.success(), "{output:?}");
        let stderr = assert_terminal_lines(&output.stderr);
        if let Some(expected) = line_count {
            assert_eq!(stderr.lines().count(), expected);
        } else {
            line_count = Some(stderr.lines().count());
        }
        assert!(stderr.contains(
            ic_host_text::escape::terminal_text(&history.display().to_string()).as_ref()
        ));
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(
            calls.lines().filter(|line| *line == "icydb_query").count(),
            2
        );
        assert!(history.is_dir());
    }
}

#[test]
fn artifact_path_messages_preserve_safe_lines_and_create_only_payloads() {
    for case in ["missing", "malformed", "wrong-owner", "publish", "occupied"] {
        let mut line_counts = None;
        let mut expected_bytes = None;
        for label in ["界-\\n", "界-\\n\r\n\x1b[2J\u{0085}\u{202e}\u{2028}"] {
            let fixture = IcpFixture::new();
            let artifact = fixture.directory.join(format!("{label}-input.json"));
            let destination = fixture.directory.join(format!("{label}-output.json"));
            let mut json = diagnostic_artifact_json();
            if case != "wrong-owner" {
                json["provenance"] = serde_json::json!({"kind":"deployment","environment":"fixture","canister":"fixture"});
            }
            match case {
                "missing" => {}
                "malformed" => fs::write(&artifact, "{").unwrap(),
                _ => fs::write(&artifact, json.to_string()).unwrap(),
            }
            if case == "occupied" {
                fs::write(&destination, b"untouched").unwrap();
            }
            let output = fixture
                .command("")
                .args(["schema", "diagnostic-source-metadata", "--artifact"])
                .arg(&artifact)
                .args(["--source", "fixture-source", "--output"])
                .arg(&destination)
                .output()
                .unwrap();
            assert_eq!(
                output.status.success(),
                case == "publish",
                "{case}: {output:?}"
            );
            let stdout = assert_terminal_lines(&output.stdout);
            let stderr = assert_terminal_lines(&output.stderr);
            let counts = (stdout.lines().count(), stderr.lines().count());
            if let Some(expected) = line_counts {
                assert_eq!(counts, expected);
            } else {
                line_counts = Some(counts);
            }
            if case == "publish" {
                assert!(
                    stdout.contains(
                        ic_host_text::escape::terminal_text(&destination.display().to_string())
                            .as_ref()
                    )
                );
                let bytes = fs::read(&destination).unwrap();
                if let Some(expected) = &expected_bytes {
                    assert_eq!(&bytes, expected);
                } else {
                    expected_bytes = Some(bytes);
                }
            } else {
                assert!(output.stdout.is_empty());
                if case == "occupied" {
                    assert_eq!(fs::read(&destination).unwrap(), b"untouched");
                } else {
                    assert!(!destination.exists());
                }
            }
            assert!(!fixture.directory.join("calls").exists());
        }
    }
}

#[test]
fn target_labels_are_terminal_safe_and_icp_arguments_keep_original_bytes() {
    let hostile = "界-e\u{301}-\\n\t\r\n\x1b]52;c;data\x07\u{0085}\u{202e}\u{2028}";
    for case in [
        "list",
        "missing-id",
        "status-error",
        "sql-error",
        "report-error",
        "reset",
        "status",
    ] {
        let mut line_counts = None;
        for label in ["界-e\u{301}-\\n", hostile] {
            let fixture = IcpFixture::new();
            let environment = format!("env-{label}");
            let canister = format!("app-{label}");
            let arguments = fixture.directory.join("observed-argv");
            let mut command = fixture.command(&response_hex(Ok(())));
            command.env("ICP_FIXTURE_ARGV", &arguments).env(
                "ICP_FIXTURE_LIST_RESPONSE",
                serde_json::json!({"canisters": [&canister]}).to_string(),
            );
            match case {
                "list" => {
                    command.args(["canister", "list", "-e", &environment]);
                }
                "status" => {
                    command.args(["canister", "status", &canister, "-e", &environment]);
                }
                "reset" => {
                    command.args(["metrics", "--reset", &canister, "-e", &environment]);
                }
                "report-error" => {
                    command.env("ICP_FIXTURE_TRANSPORT_FAILURE", "1").args([
                        "metrics",
                        &canister,
                        "-e",
                        &environment,
                    ]);
                }
                _ => {
                    command.args([
                        "sql",
                        "-c",
                        &canister,
                        "-e",
                        &environment,
                        "--sql",
                        "SELECT id FROM Character",
                    ]);
                    match case {
                        "missing-id" => {
                            command.env("ICP_FIXTURE_STATUS_ID", "");
                        }
                        "status-error" => {
                            command.env("ICP_FIXTURE_STATUS_ERROR", "opaque observation");
                        }
                        "sql-error" => {
                            command.env("ICP_FIXTURE_TRANSPORT_FAILURE", "1");
                        }
                        _ => unreachable!(),
                    }
                }
            }
            let output = command.output().unwrap();
            assert_eq!(
                output.status.success(),
                matches!(case, "list" | "reset" | "status"),
                "{case}: {output:?}"
            );
            let stdout = assert_terminal_lines(&output.stdout);
            let stderr = assert_terminal_lines(&output.stderr);
            let counts = (stdout.lines().count(), stderr.lines().count());
            if let Some(expected) = line_counts {
                assert_eq!(counts, expected, "labels cannot add report lines: {case}");
            } else {
                line_counts = Some(counts);
            }
            let display = format!("{stdout}{stderr}");
            for target in [&environment, &canister] {
                assert!(
                    display.contains(ic_host_text::escape::terminal_text(target).as_ref()),
                    "{case}: {display:?}"
                );
                let bytes = fs::read(&arguments).unwrap();
                assert!(
                    bytes
                        .split(|byte| *byte == 0)
                        .any(|argument| argument == target.as_bytes()),
                    "ICP must receive the original target: {case}"
                );
            }
        }
    }
}

fn assert_terminal_lines(bytes: &[u8]) -> &str {
    let text = std::str::from_utf8(bytes).unwrap();
    for line in text.split('\n') {
        assert_eq!(ic_host_text::escape::terminal_text(line), line);
    }
    text
}

#[test]
fn inventory_failures_escape_target_metadata_and_preserve_cause_lines() {
    for case in ["nonzero", "missing", "permission"] {
        let mut line_count = None;
        for environment in [
            "env-界-\\n",
            "env-界-\\n\t\r\n\x1b[2J\u{0085}\u{202e}\u{2028}",
        ] {
            let fixture = IcpFixture::new();
            match case {
                "missing" => fs::remove_file(fixture.directory.join("icp")).unwrap(),
                "permission" => fs::set_permissions(
                    fixture.directory.join("icp"),
                    fs::Permissions::from_mode(0o600),
                )
                .unwrap(),
                _ => {}
            }
            let output = fixture
                .command("")
                .env("ICP_FIXTURE_LIST_STATUS", "7")
                .args(["canister", "list", "-e", environment])
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(1), "{case}: {output:?}");
            assert!(output.stdout.is_empty());
            let stderr = assert_terminal_lines(&output.stderr);
            assert!(stderr.contains(ic_host_text::escape::terminal_text(environment).as_ref()));
            if let Some(expected) = line_count {
                assert_eq!(stderr.lines().count(), expected);
            } else {
                line_count = Some(stderr.lines().count());
            }
        }
    }
}

#[test]
fn lifecycle_environment_labels_preserve_owned_lines_and_command_targets() {
    for case in ["deploy", "upgrade", "refresh"] {
        let mut line_counts = None;
        for environment in ["env-界-\\n", "env-界-\\n\x1b[2J\r\n\u{202e}"] {
            let fixture = IcpFixture::new();
            let arguments = fixture.directory.join("observed-argv");
            let mut command = if case == "upgrade" {
                fixture.upgrade_command(environment, false)
            } else {
                let mut command = fixture.command("");
                command.args(["canister", case, "fixture", "-e", environment]);
                if case == "refresh" {
                    command.arg("--yes");
                }
                command
            };
            let output = command
                .env("ICP_FIXTURE_ARGV", &arguments)
                .output()
                .unwrap();
            assert!(output.status.success(), "{case}: {output:?}");
            let stdout = assert_terminal_lines(&output.stdout);
            let stderr = assert_terminal_lines(&output.stderr);
            let counts = (stdout.lines().count(), stderr.lines().count());
            if let Some(expected) = line_counts {
                assert_eq!(counts, expected);
            } else {
                line_counts = Some(counts);
            }
            assert!(stderr.contains(ic_host_text::escape::terminal_text(environment).as_ref()));
            let bytes = fs::read(arguments).unwrap();
            assert!(
                bytes
                    .split(|byte| *byte == 0)
                    .any(|argument| argument == environment.as_bytes())
            );
        }
    }
}

#[test]
fn manifest_fallback_keeps_yaml_lists_comments_and_literal_name_bytes() {
    for manifest in [
        "environments:\n  - name: fixture\n    canisters: [fixture] # local selection\n",
        "environments:\n  - name: fixture\n    canisters:\n      - fixture\n",
        "environments: [{name: fixture, canisters: [fixture]}]\n",
        "environments:\n  - name: fixture\n    canisters: ['one,two', 'literal''quote']\n",
    ] {
        let fixture = IcpFixture::new();
        fs::write(fixture.directory.join("icp.yaml"), manifest).unwrap();
        let arguments = fixture.directory.join("observed-argv");
        let output = fixture
            .command("")
            .env("ICP_FIXTURE_LIST_STATUS", "7")
            .env("ICP_FIXTURE_ARGV", &arguments)
            .args(["canister", "list", "-e", "fixture"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{manifest}: {output:?}");
        let stdout = std::str::from_utf8(&output.stdout).unwrap();
        let argv = fs::read(arguments).unwrap();
        let expected = if manifest.contains("one,two") {
            vec!["one,two", "literal'quote"]
        } else {
            vec!["fixture"]
        };
        let expected_probes = expected.len();
        for name in expected {
            assert!(stdout.contains(name), "{manifest}: {stdout:?}");
            assert!(
                argv.split(|byte| *byte == 0)
                    .any(|argument| argument == name.as_bytes())
            );
        }
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(calls.lines().filter(|line| *line == "list").count(), 1);
        assert_eq!(
            calls.lines().filter(|line| *line == "status").count(),
            expected_probes
        );
        assert!(!calls.lines().any(|line| line == "call" || line == "deploy"));
    }
}

#[test]
fn manifest_yaml_refusal_retains_inventory_cause_and_stops_before_status() {
    let diagnostic = "opaque inventory observation";
    for manifest in [
        "environments: [",
        "environments: [{name: fixture, canisters: [one], canisters: [two]}]",
        "environments: [{name: fixture, canisters: {one: true}}]",
        "'\u{202e}': one\n'\u{202e}': two\nenvironments: []",
    ] {
        let fixture = IcpFixture::new();
        fs::write(fixture.directory.join("icp.yaml"), manifest).unwrap();
        let output = fixture
            .command("")
            .env("ICP_FIXTURE_LIST_STATUS", "7")
            .env("ICP_FIXTURE_LIST_ERROR", diagnostic)
            .args(["canister", "list", "-e", "fixture"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let stderr = assert_terminal_lines(&output.stderr);
        assert!(stderr.contains(diagnostic));
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(calls.lines().filter(|line| *line == "list").count(), 1);
        assert!(
            !calls
                .lines()
                .any(|line| line == "status" || line == "call" || line == "deploy")
        );
    }
}

#[test]
fn inventory_fallback_retains_child_diagnostics_and_existing_outcomes() {
    // These are opaque child payloads, not inputs to a failure classifier.
    let diagnostic = "inventory observation: opaque-π";
    for environment in ["local", "ic", "fixture"] {
        for manifest in [
            None,
            Some(Vec::new()),
            Some(vec![0xff]),
            Some(
                format!("environments:\n  - name: {environment}\n    canisters: [fixture]\n")
                    .into_bytes(),
            ),
        ] {
            for capture in ["", "signal"] {
                let fixture = IcpFixture::new();
                if let Some(contents) = &manifest {
                    fs::write(fixture.directory.join("icp.yaml"), contents).unwrap();
                }
                let valid_manifest = manifest
                    .as_ref()
                    .is_some_and(|contents| std::str::from_utf8(contents).is_ok());
                let output = fixture
                    .command("")
                    .env("ICP_FIXTURE_LIST_STATUS", "7")
                    .env("ICP_FIXTURE_LIST_ERROR", diagnostic)
                    .env("ICP_FIXTURE_LIST_CAPTURE", capture)
                    .args(["canister", "list", "-e", environment])
                    .output()
                    .unwrap();
                assert_eq!(
                    output.status.success(),
                    valid_manifest,
                    "environment={environment} manifest={manifest:?} capture={capture:?}: {output:?}"
                );
                let stderr = std::str::from_utf8(&output.stderr).unwrap();
                assert!(
                    stderr.contains(diagnostic),
                    "child bytes must survive: environment={environment} manifest={manifest:?} capture={capture:?}: {output:?}"
                );
                let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
                assert_eq!(calls.lines().filter(|line| *line == "list").count(), 1);
                assert_eq!(
                    calls.lines().filter(|line| *line == "status").count(),
                    usize::from(valid_manifest && !manifest.as_ref().unwrap().is_empty())
                );
                assert!(!calls.lines().any(|line| line == "call" || line == "deploy"));
                if !valid_manifest {
                    assert!(output.stdout.is_empty());
                }
            }
        }
    }
}

#[test]
fn inventory_fallback_diagnostics_are_control_free() {
    let fixture = IcpFixture::new();
    fs::write(fixture.directory.join("icp.yaml"), "").unwrap();
    let output = fixture
        .command("")
        .env("ICP_FIXTURE_LIST_STATUS", "7")
        .env("ICP_FIXTURE_LIST_ERROR", "opaque\x1b[2J\r\n\tmarker")
        .args(["canister", "list", "-e", "fixture"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("marker"));
    assert!(
        !stderr
            .chars()
            .any(|character| character.is_control() && character != '\n')
    );
}

#[test]
fn inventory_capture_failures_never_become_manifest_success() {
    for capture in ["stdout", "stderr"] {
        let fixture = IcpFixture::new();
        fs::write(fixture.directory.join("icp.yaml"), "").unwrap();
        let output = fixture
            .command("")
            .env("ICP_FIXTURE_LIST_CAPTURE", capture)
            .args(["canister", "list", "-e", "fixture"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(calls.lines().filter(|line| *line == "list").count(), 1);
        assert!(
            !calls
                .lines()
                .any(|line| line == "status" || line == "call" || line == "deploy")
        );
    }
}

#[test]
fn inventory_non_missing_spawn_failure_cannot_use_manifest_fallback() {
    let fixture = IcpFixture::new();
    fs::write(fixture.directory.join("icp.yaml"), "").unwrap();
    fs::set_permissions(
        fixture.directory.join("icp"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let output = fixture
        .command("")
        .args(["canister", "list", "-e", "fixture"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
    assert!(!fixture.directory.join("calls").exists());
}

#[test]
fn inventory_missing_executable_retains_target_across_manifest_fallback() {
    let environment = "inventory-target-π";
    for manifest in [None, Some(Vec::new()), Some(vec![0xff])] {
        let fixture = IcpFixture::new();
        fs::remove_file(fixture.directory.join("icp")).unwrap();
        if let Some(contents) = &manifest {
            fs::write(fixture.directory.join("icp.yaml"), contents).unwrap();
        }
        let output = fixture
            .command("")
            .args(["canister", "list", "-e", environment])
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            manifest == Some(Vec::new()),
            "{output:?}"
        );
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains(environment),
            "selected target must survive: {stderr:?}"
        );
        assert!(!fixture.directory.join("calls").exists());
    }
}

#[test]
fn missing_canister_guidance_retains_secondary_inventory_failure() {
    let diagnostic = "secondary inventory observation: opaque-π";
    let fixture = IcpFixture::new();
    let output = fixture
        .sql_command("")
        .env("ICP_FIXTURE_STATUS_ID", "")
        .env("ICP_FIXTURE_LIST_STATUS", "7")
        .env("ICP_FIXTURE_LIST_ERROR", diagnostic)
        .args(["--sql", "SELECT id FROM Character"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(diagnostic),
        "secondary child bytes must survive: {stderr:?}"
    );
    let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
    assert_eq!(calls.lines().filter(|line| *line == "status").count(), 1);
    assert_eq!(calls.lines().filter(|line| *line == "list").count(), 1);
    assert!(!calls.lines().any(|line| line == "call" || line == "deploy"));
}

#[test]
fn inventory_preserves_empty_success_and_failed_command_fallback() {
    for (response, status, probes_canister) in [
        (r#"{"canisters":[]}"#, "0", false),
        (r#"{"canisters":["fixture"],"extra":true}"#, "0", true),
        ("unavailable", "7", true),
    ] {
        let fixture = IcpFixture::new();
        fs::write(
            fixture.directory.join("icp.yaml"),
            "environments:\n  - name: local\n    canisters: [fixture]\n",
        )
        .unwrap();
        let output = fixture
            .command(&success_response())
            .env("ICP_FIXTURE_LIST_RESPONSE", response)
            .env("ICP_FIXTURE_LIST_STATUS", status)
            .args(["canister", "list", "-e", "local"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(0));
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(calls.lines().any(|line| line == "status"), probes_canister);
    }
}

#[test]
fn external_probe_diagnostics_cannot_inject_terminal_controls() {
    let fixture = IcpFixture::new();
    let output = fixture
        .command(&success_response())
        .env("ICP_FIXTURE_STATUS_ERROR", "bad\x1b[2J\rmessage")
        .args(["canister", "list", "-e", "local"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains('\x1b'));
    assert!(!stderr.contains('\r'));
    assert!(stderr.contains("message"));
}

#[test]
fn external_hex_diagnostics_cannot_inject_terminal_controls() {
    for byte in [7_u8, 8, 27, 127] {
        for (sql, method, query) in STATEMENTS {
            let fixture = IcpFixture::new();
            let response = format!("{}x", char::from(byte));
            let output = fixture.one_shot(&response, sql, true);
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            let stderr = String::from_utf8(output.stderr).unwrap();
            assert!(!stderr.chars().any(|c| c.is_control() && c != '\n'));
            fixture.assert_call(method, query);
            assert_eq!(fixture.call_arguments(method).len(), 1);
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
        ("uninstalled-deploy-failure", 1, true, false, false),
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
        if case.ends_with("deploy-failure") {
            let label = if reinstalled {
                "icp deploy reinstall"
            } else {
                "icp deploy"
            };
            let stderr = assert_terminal_lines(&output.stderr);
            assert!(
                stderr.contains(&format!("ERROR: {label} failed with")),
                "{stderr}"
            );
        }
        let calls = fs::read_to_string(fixture.directory.join("calls")).unwrap();
        assert_eq!(
            calls.lines().filter(|line| *line == "deploy").count(),
            usize::from(deployed)
        );
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
        vec!["diagnostic", "E7", "--canister", "fixture"],
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
    for ((sql, method, query), label) in STATEMENTS.into_iter().zip(["query", "DDL", "update"]) {
        for transport_failure in [false, true] {
            let fixture = IcpFixture::new();
            let mut command = fixture.sql_command("00");
            if transport_failure {
                command.env("ICP_FIXTURE_TRANSPORT_FAILURE", "1");
            }
            let output = command.args(["--sql", sql]).output().unwrap();
            let stderr = assert_terminal_lines(&output.stderr);
            if transport_failure {
                assert!(
                    stderr.contains(&format!("IcyDB SQL {label} method '{method}' failed")),
                    "{stderr}"
                );
            }
            assert_eq!(
                stderr.contains("update outcome unknown"),
                !query,
                "{stderr}"
            );
            assert_failure(output);
            fixture.assert_call(method, query);
            assert_eq!(fixture.call_arguments(method).len(), 1);
        }
    }
}

#[test]
fn mutation_capture_overflow_never_retries_or_retains_arguments() {
    for mode in ["stdout", "stderr"] {
        for (arguments, method) in [
            (
                vec![
                    "sql",
                    "-c",
                    "fixture",
                    "--sql",
                    "CREATE INDEX name_idx ON Character (name)",
                ],
                "icydb_ddl",
            ),
            (
                vec![
                    "sql",
                    "-c",
                    "fixture",
                    "--sql",
                    "UPDATE Character SET name = 'Ada' WHERE id = 1",
                ],
                "icydb_update",
            ),
            (vec!["metrics", "--reset", "fixture"], "icydb_metrics_reset"),
            (
                vec!["canister", "refresh", "fixture", "--yes"],
                "icydb_fixtures_load",
            ),
        ] {
            let fixture = IcpFixture::new();
            let output = fixture
                .command("")
                .env("ICP_FIXTURE_CAPTURE_METHOD", method)
                .env("ICP_FIXTURE_CAPTURE_CASE", mode)
                .args(arguments)
                .args(["-e", "fixture"])
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            assert!(!output.stderr.is_empty());
            let effects = fs::read_to_string(fixture.directory.join("effects")).unwrap();
            assert_eq!(effects.lines().collect::<Vec<_>>(), [method]);
            for path in fs::read_to_string(fixture.directory.join("argument-paths"))
                .unwrap()
                .lines()
            {
                assert!(!std::path::Path::new(path).exists());
            }
        }
    }
}

#[test]
fn mutation_capture_signals_stop_once_and_remove_arguments() {
    for signal in ["-INT", "-TERM", "-HUP"] {
        let fixture = IcpFixture::new();
        let mut child = fixture
            .sql_command(&success_response())
            .env("ICP_FIXTURE_CAPTURE_METHOD", "icydb_update")
            .env("ICP_FIXTURE_CAPTURE_CASE", "wait")
            .args(["--sql", "UPDATE Character SET name = 'Ada' WHERE id = 1"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let ready = fixture.directory.join("capture-ready");
        let deadline = Instant::now() + Duration::from_secs(15);
        while fs::read(&ready).ok().as_deref() != Some(b"armed") && Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let armed = fs::read(&ready).ok().as_deref() == Some(b"armed");
        if armed {
            assert!(
                Command::new("/bin/kill")
                    .args([signal, &child.id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
        } else {
            child.kill().ok();
        }
        while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let completed = child.try_wait().unwrap().is_some();
        if !completed {
            child.kill().ok();
        }
        let output = child.wait_with_output().unwrap();
        assert!(
            armed && completed,
            "{signal}: capture must observe cancellation after complete readiness; \
             armed={armed}, completed={completed}, output={output:?}"
        );
        assert_eq!(output.status.code(), Some(1), "{signal}: {output:?}");
        assert!(output.stdout.is_empty(), "{signal}: {output:?}");
        assert!(!output.stderr.is_empty(), "{signal}: {output:?}");
        fixture.assert_call("icydb_update", false);
        assert_eq!(fixture.call_arguments("icydb_update").len(), 1);
        for path in fs::read_to_string(fixture.directory.join("argument-paths"))
            .unwrap()
            .lines()
        {
            assert!(!std::path::Path::new(path).exists());
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
    assert_eq!(output.status.code(), Some(exit), "{phase:?}: {output:?}");
    assert_eq!(output.stderr.is_empty(), exit == 0, "{phase:?}: {output:?}");
    let stdout = std::str::from_utf8(&output.stdout)
        .unwrap_or_else(|error| panic!("{phase:?}: {error}: {output:?}"));
    assert!(
        stdout
            .lines()
            .any(|line| line == format!("  phase: {phase:?}")),
        "{phase:?}: {output:?}"
    );
    if phase == SchemaMigrationPhase::Rejected {
        assert!(
            stdout.contains("UniqueIndex: entity 11 key 0709"),
            "{output:?}"
        );
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
        let output = fixture
            .migration_command(&status, &[], operation)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_terminal_lines(&output.stderr);
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
