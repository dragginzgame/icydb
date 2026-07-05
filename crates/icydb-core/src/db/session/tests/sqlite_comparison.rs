//! Optional SQLite CLI comparison harness for the 0.196 audit line.
//!
//! These tests keep the always-on path dependency-free at runtime: manifest and
//! artifact-shape checks run normally, while the actual SQLite comparison is
//! ignored and skips cleanly when `sqlite3` is not installed.

use super::*;
use serde::Serialize;
use std::{
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

const DEFAULT_OUTPUT_STEM: &str = "/tmp/icydb-sqlite-comparison/sqlite_comparison_harness";
const HARNESS_DATE: &str = "2026-07-05";
const SAMPLE_COUNT: usize = 5;

const SQLITE_DEFAULT_SETUP_PRAGMAS: &[&str] = &[];
const SQLITE_DEFAULT_QUERY_PRAGMAS: &[&str] = &[];
const SQLITE_WAL_NORMAL_SETUP_PRAGMAS: &[&str] =
    &["PRAGMA journal_mode=WAL;", "PRAGMA synchronous=NORMAL;"];
const SQLITE_WAL_NORMAL_QUERY_PRAGMAS: &[&str] = &["PRAGMA synchronous=NORMAL;"];
const SQLITE_WAL_FULL_SETUP_PRAGMAS: &[&str] =
    &["PRAGMA journal_mode=WAL;", "PRAGMA synchronous=FULL;"];
const SQLITE_WAL_FULL_QUERY_PRAGMAS: &[&str] = &["PRAGMA synchronous=FULL;"];
const SQLITE_UNSAFE_SETUP_PRAGMAS: &[&str] = &["PRAGMA synchronous=OFF;"];
const SQLITE_UNSAFE_QUERY_PRAGMAS: &[&str] = &["PRAGMA synchronous=OFF;"];

const SQLITE_MODES: &[SqliteMode] = &[
    SqliteMode {
        key: "sqlite_default",
        label: "SQLite default mode",
        setup_pragmas: SQLITE_DEFAULT_SETUP_PRAGMAS,
        query_pragmas: SQLITE_DEFAULT_QUERY_PRAGMAS,
        unsafe_speed_reference: false,
    },
    SqliteMode {
        key: "sqlite_wal_normal",
        label: "SQLite WAL synchronous=NORMAL",
        setup_pragmas: SQLITE_WAL_NORMAL_SETUP_PRAGMAS,
        query_pragmas: SQLITE_WAL_NORMAL_QUERY_PRAGMAS,
        unsafe_speed_reference: false,
    },
    SqliteMode {
        key: "sqlite_wal_full",
        label: "SQLite WAL synchronous=FULL",
        setup_pragmas: SQLITE_WAL_FULL_SETUP_PRAGMAS,
        query_pragmas: SQLITE_WAL_FULL_QUERY_PRAGMAS,
        unsafe_speed_reference: false,
    },
    SqliteMode {
        key: "sqlite_unsafe_sync_off",
        label: "SQLite synchronous=OFF unsafe speed reference",
        setup_pragmas: SQLITE_UNSAFE_SETUP_PRAGMAS,
        query_pragmas: SQLITE_UNSAFE_QUERY_PRAGMAS,
        unsafe_speed_reference: true,
    },
];

const COMPARISON_ROWS: &[ComparisonFixtureRow] = &[
    ComparisonFixtureRow {
        id: 10,
        name: "alice",
        age: 20,
    },
    ComparisonFixtureRow {
        id: 20,
        name: "bravo",
        age: 30,
    },
    ComparisonFixtureRow {
        id: 30,
        name: "mira",
        age: 30,
    },
    ComparisonFixtureRow {
        id: 40,
        name: "nora",
        age: 40,
    },
    ComparisonFixtureRow {
        id: 50,
        name: "zane",
        age: 50,
    },
    ComparisonFixtureRow {
        id: 60,
        name: "milo",
        age: 60,
    },
];

const COMPARISON_SCENARIOS: &[ComparisonScenario] = &[
    ComparisonScenario {
        key: "primary_order_limit",
        sql: "SELECT id, name FROM SessionSqlEntity ORDER BY id ASC LIMIT 3",
        route_family: "primary_order",
        expectation: "deterministic primary-key ordered page",
    },
    ComparisonScenario {
        key: "filter_order_limit",
        sql: "SELECT name, age FROM SessionSqlEntity WHERE age >= 30 ORDER BY age ASC, id ASC LIMIT 4",
        route_family: "materialized_or_secondary_order",
        expectation: "filtered ordered page with duplicate age tie-breaker",
    },
    ComparisonScenario {
        key: "name_range_order",
        sql: "SELECT name FROM SessionSqlEntity WHERE name >= 'm' ORDER BY name ASC, id ASC LIMIT 3",
        route_family: "range_order",
        expectation: "range-filtered ordered page",
    },
    ComparisonScenario {
        key: "count_filtered",
        sql: "SELECT COUNT(*) FROM SessionSqlEntity WHERE age >= 30",
        route_family: "aggregate_count",
        expectation: "overlapping filtered count",
    },
    ComparisonScenario {
        key: "missing_primary_key",
        sql: "SELECT id FROM SessionSqlEntity WHERE id = '000000000000000000000003E7'",
        route_family: "primary_lookup",
        expectation: "missing primary-key lookup",
    },
];

const FAIRNESS_NOTES: &[&str] = &[
    "SQLite runs through the local sqlite3 CLI and is not using Internet Computer stable memory.",
    "IcyDB runs natively against the in-process session fixture with warm query/schema caches after the first signature query.",
    "SQLite timings include CLI process startup and are exploratory diagnostics, not headline performance claims.",
    "SQLite synchronous=OFF is recorded only as an unsafe speed reference and must not be used as the main comparison baseline.",
    "The harness compares overlapping STRICT-typed SQL behavior and does not force SQLite dynamic typing semantics onto IcyDB.",
];

#[derive(Clone, Copy, Serialize)]
struct SqliteMode {
    key: &'static str,
    label: &'static str,
    setup_pragmas: &'static [&'static str],
    query_pragmas: &'static [&'static str],
    unsafe_speed_reference: bool,
}

#[derive(Clone, Copy)]
struct ComparisonFixtureRow {
    id: u128,
    name: &'static str,
    age: u64,
}

#[derive(Clone, Copy, Serialize)]
struct ComparisonScenario {
    key: &'static str,
    sql: &'static str,
    route_family: &'static str,
    expectation: &'static str,
}

#[derive(Serialize)]
struct ComparisonReport {
    audit_line: &'static str,
    date: &'static str,
    harness: &'static str,
    sqlite_version: String,
    icydb_build_profile: &'static str,
    sample_count: usize,
    fairness_notes: &'static [&'static str],
    sqlite_modes: &'static [SqliteMode],
    scenarios: Vec<ScenarioReport>,
}

#[derive(Serialize)]
struct ScenarioReport {
    key: &'static str,
    sql: &'static str,
    route_family: &'static str,
    expectation: &'static str,
    icydb_result_signature: String,
    icydb_timing: TimingSummary,
    sqlite_results: Vec<SqliteScenarioResult>,
}

#[derive(Serialize)]
struct SqliteScenarioResult {
    mode_key: &'static str,
    mode_label: &'static str,
    unsafe_speed_reference: bool,
    result_signature: String,
    matches_icydb: bool,
    timing: TimingSummary,
}

#[derive(Clone, Serialize)]
struct TimingSummary {
    #[serde(rename = "samples_ns")]
    samples: Vec<u128>,
    #[serde(rename = "median_ns")]
    median: u128,
    #[serde(rename = "min_ns")]
    min: u128,
    #[serde(rename = "max_ns")]
    max: u128,
}

#[test]
fn sqlite_comparison_manifest_covers_required_modes_and_scenarios() {
    assert_eq!(SQLITE_MODES.len(), 4);
    assert!(SQLITE_MODES.iter().any(|mode| mode.key == "sqlite_default"));
    assert!(
        SQLITE_MODES
            .iter()
            .any(|mode| mode.key == "sqlite_wal_normal")
    );
    assert!(
        SQLITE_MODES
            .iter()
            .any(|mode| mode.key == "sqlite_wal_full")
    );
    assert!(
        SQLITE_MODES
            .iter()
            .any(|mode| mode.key == "sqlite_unsafe_sync_off" && mode.unsafe_speed_reference)
    );

    let scenario_keys: Vec<&str> = COMPARISON_SCENARIOS
        .iter()
        .map(|scenario| scenario.key)
        .collect();

    for required in [
        "primary_order_limit",
        "filter_order_limit",
        "name_range_order",
        "count_filtered",
        "missing_primary_key",
    ] {
        assert!(scenario_keys.contains(&required));
    }

    assert!(
        FAIRNESS_NOTES
            .iter()
            .any(|note| note.contains("stable memory"))
    );
    assert!(
        FAIRNESS_NOTES
            .iter()
            .any(|note| note.contains("unsafe speed reference"))
    );
}

#[test]
fn sqlite_comparison_report_serializes_required_fairness_metadata() {
    let report = ComparisonReport {
        audit_line: "0.196",
        date: HARNESS_DATE,
        harness: "optional_sqlite_cli_comparison",
        sqlite_version: "shape-test".to_string(),
        icydb_build_profile: "cargo test native",
        sample_count: SAMPLE_COUNT,
        fairness_notes: FAIRNESS_NOTES,
        sqlite_modes: SQLITE_MODES,
        scenarios: vec![ScenarioReport {
            key: COMPARISON_SCENARIOS[0].key,
            sql: COMPARISON_SCENARIOS[0].sql,
            route_family: COMPARISON_SCENARIOS[0].route_family,
            expectation: COMPARISON_SCENARIOS[0].expectation,
            icydb_result_signature: "id\tname".to_string(),
            icydb_timing: TimingSummary::from_samples(vec![1, 2, 3]),
            sqlite_results: vec![SqliteScenarioResult {
                mode_key: SQLITE_MODES[0].key,
                mode_label: SQLITE_MODES[0].label,
                unsafe_speed_reference: SQLITE_MODES[0].unsafe_speed_reference,
                result_signature: "id\tname".to_string(),
                matches_icydb: true,
                timing: TimingSummary::from_samples(vec![2, 3, 4]),
            }],
        }],
    };

    let json = serde_json::to_string_pretty(&report).expect("report JSON should serialize");
    assert!(json.contains("\"fairness_notes\""));
    assert!(json.contains("\"sqlite_modes\""));
    assert!(json.contains("\"sqlite_results\""));
    assert!(json.contains("\"unsafe_speed_reference\""));

    let markdown = comparison_report_markdown(&report);
    assert!(markdown.contains("SQLite Comparison Harness"));
    assert!(markdown.contains("primary_order_limit"));
    assert!(markdown.contains("unsafe speed reference"));
}

#[test]
#[ignore = "optional SQLite CLI comparison harness; run manually with sqlite3 installed"]
fn sqlite_comparison_harness_runs_when_sqlite3_available() {
    let sqlite_path = sqlite3_path();

    if sqlite_version(&sqlite_path).is_err() {
        eprintln!(
            "skipping optional SQLite comparison harness: `{}` is not available",
            sqlite_path.display()
        );
        return;
    }

    let output_stem = env::var("ICYDB_SQLITE_COMPARISON_OUTPUT_STEM")
        .map_or_else(|_| PathBuf::from(DEFAULT_OUTPUT_STEM), PathBuf::from);
    let report = run_comparison_harness(&sqlite_path).expect("comparison harness should run");

    for scenario in &report.scenarios {
        for sqlite in &scenario.sqlite_results {
            assert!(
                sqlite.matches_icydb,
                "{} diverged in {}: IcyDB=`{}` SQLite=`{}`",
                scenario.key,
                sqlite.mode_key,
                scenario.icydb_result_signature,
                sqlite.result_signature
            );
        }
    }

    write_report_artifacts(&report, &output_stem).expect("comparison artifacts should write");
    eprintln!(
        "wrote optional SQLite comparison artifacts to {}.json and {}.md",
        output_stem.display(),
        output_stem.display()
    );
}

impl TimingSummary {
    fn from_samples(mut samples_ns: Vec<u128>) -> Self {
        samples_ns.sort_unstable();
        let median_ns = samples_ns[samples_ns.len() / 2];
        let min_ns = samples_ns[0];
        let max_ns = samples_ns[samples_ns.len() - 1];

        Self {
            samples: samples_ns,
            median: median_ns,
            min: min_ns,
            max: max_ns,
        }
    }
}

fn sqlite3_path() -> PathBuf {
    env::var("ICYDB_SQLITE3").map_or_else(|_| PathBuf::from("sqlite3"), PathBuf::from)
}

fn run_comparison_harness(sqlite_path: &Path) -> Result<ComparisonReport, String> {
    let sqlite_version = sqlite_version(sqlite_path)?;
    let scenarios = COMPARISON_SCENARIOS
        .iter()
        .map(|scenario| run_comparison_scenario(sqlite_path, scenario))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ComparisonReport {
        audit_line: "0.196",
        date: HARNESS_DATE,
        harness: "optional_sqlite_cli_comparison",
        sqlite_version,
        icydb_build_profile: "cargo test native",
        sample_count: SAMPLE_COUNT,
        fairness_notes: FAIRNESS_NOTES,
        sqlite_modes: SQLITE_MODES,
        scenarios,
    })
}

fn run_comparison_scenario(
    sqlite_path: &Path,
    scenario: &'static ComparisonScenario,
) -> Result<ScenarioReport, String> {
    let (icydb_result_signature, icydb_timing) = run_icydb_scenario(scenario)?;
    let sqlite_results = SQLITE_MODES
        .iter()
        .map(|mode| run_sqlite_scenario(sqlite_path, scenario, mode, &icydb_result_signature))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ScenarioReport {
        key: scenario.key,
        sql: scenario.sql,
        route_family: scenario.route_family,
        expectation: scenario.expectation,
        icydb_result_signature,
        icydb_timing,
        sqlite_results,
    })
}

fn run_icydb_scenario(scenario: &ComparisonScenario) -> Result<(String, TimingSummary), String> {
    reset_session_sql_store();
    let session = sql_session();
    seed_sqlite_comparison_rows(&session);

    let baseline = icydb_signature_for(&session, scenario.sql)?;
    let timing = time_samples(|| {
        let signature = icydb_signature_for(&session, scenario.sql)?;
        if signature == baseline {
            Ok(())
        } else {
            Err(format!(
                "IcyDB result signature changed during timing for {}: baseline=`{baseline}` sample=`{signature}`",
                scenario.key
            ))
        }
    })?;

    Ok((baseline, timing))
}

fn run_sqlite_scenario(
    sqlite_path: &Path,
    scenario: &ComparisonScenario,
    mode: &SqliteMode,
    icydb_signature: &str,
) -> Result<SqliteScenarioResult, String> {
    let db_path = sqlite_db_path(mode);
    reset_sqlite_database(&db_path)?;
    setup_sqlite_database(sqlite_path, &db_path, mode)?;

    let baseline = sqlite_signature_for(sqlite_path, &db_path, mode, scenario.sql)?;
    let timing = time_samples(|| {
        let signature = sqlite_signature_for(sqlite_path, &db_path, mode, scenario.sql)?;
        if signature == baseline {
            Ok(())
        } else {
            Err(format!(
                "SQLite result signature changed during timing for {} in {}: baseline=`{baseline}` sample=`{signature}`",
                scenario.key, mode.key
            ))
        }
    })?;

    Ok(SqliteScenarioResult {
        mode_key: mode.key,
        mode_label: mode.label,
        unsafe_speed_reference: mode.unsafe_speed_reference,
        result_signature: baseline.clone(),
        matches_icydb: baseline == icydb_signature,
        timing,
    })
}

fn seed_sqlite_comparison_rows(session: &DbSession<SessionSqlCanister>) {
    insert_session_fixture_rows(
        session,
        COMPARISON_ROWS.iter().copied(),
        |row| SessionSqlEntity {
            id: Ulid::from_u128(row.id),
            name: row.name.to_string(),
            age: row.age,
        },
        "sqlite comparison",
    );
}

fn icydb_signature_for(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<String, String> {
    statement_projection_rows::<SessionSqlEntity>(session, sql)
        .map(|rows| result_signature(rows.as_slice()))
        .map_err(|err| format!("IcyDB query failed for `{sql}`: {err}"))
}

fn sqlite_signature_for(
    sqlite_path: &Path,
    db_path: &Path,
    mode: &SqliteMode,
    sql: &str,
) -> Result<String, String> {
    let mut script = String::new();
    for pragma in mode.query_pragmas {
        writeln!(script, "{pragma}").expect("writing query pragma should succeed");
    }
    writeln!(script, "{sql};").expect("writing query SQL should succeed");

    sqlite_output(sqlite_path, db_path, script.as_str())
}

fn result_signature(rows: &[Vec<Value>]) -> String {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(value_signature)
                .collect::<Vec<_>>()
                .join("\t")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn value_signature(value: &Value) -> String {
    match value {
        Value::Text(value) => value.clone(),
        Value::Nat64(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Ulid(value) => value.to_string(),
        Value::Null => "NULL".to_string(),
        _ => format!("{value:?}"),
    }
}

fn setup_sqlite_database(
    sqlite_path: &Path,
    db_path: &Path,
    mode: &SqliteMode,
) -> Result<(), String> {
    let mut script = String::new();
    for pragma in mode.setup_pragmas {
        writeln!(script, "{pragma}").expect("writing setup pragma should succeed");
    }
    script.push_str(
        "CREATE TABLE SessionSqlEntity (\n\
         id TEXT PRIMARY KEY,\n\
         name TEXT NOT NULL,\n\
         age INTEGER NOT NULL\n\
         ) STRICT;\n\
         CREATE INDEX session_sql_entity_name_age_id ON SessionSqlEntity(name, age, id);\n\
         CREATE INDEX session_sql_entity_age_id ON SessionSqlEntity(age, id);\n",
    );

    for row in COMPARISON_ROWS {
        writeln!(
            script,
            "INSERT INTO SessionSqlEntity(id, name, age) VALUES ('{}', '{}', {});",
            Ulid::from_u128(row.id),
            sqlite_string_literal(row.name),
            row.age
        )
        .expect("writing fixture insert should succeed");
    }

    sqlite_output(sqlite_path, db_path, script.as_str()).map(|_| ())
}

fn sqlite_output(sqlite_path: &Path, db_path: &Path, script: &str) -> Result<String, String> {
    let output = Command::new(sqlite_path)
        .arg("-batch")
        .arg("-noheader")
        .arg("-cmd")
        .arg(".mode tabs")
        .arg("-cmd")
        .arg(".nullvalue NULL")
        .arg(db_path)
        .arg(script)
        .output()
        .map_err(|err| format!("failed to run `{}`: {err}", sqlite_path.display()))?;

    if !output.status.success() {
        return Err(format!(
            "`{}` failed with status {:?}: {}",
            sqlite_path.display(),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(normalize_sqlite_output(&String::from_utf8_lossy(
        &output.stdout,
    )))
}

fn normalize_sqlite_output(output: &str) -> String {
    output.trim_end_matches(['\n', '\r']).replace("\r\n", "\n")
}

fn sqlite_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn sqlite_version(sqlite_path: &Path) -> Result<String, String> {
    let output = Command::new(sqlite_path)
        .arg("--version")
        .output()
        .map_err(|err| format!("failed to run `{}`: {err}", sqlite_path.display()))?;

    if !output.status.success() {
        return Err(format!(
            "`{}` --version failed with status {:?}: {}",
            sqlite_path.display(),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn time_samples(mut run: impl FnMut() -> Result<(), String>) -> Result<TimingSummary, String> {
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        let start = Instant::now();
        run()?;
        samples.push(start.elapsed().as_nanos());
    }

    Ok(TimingSummary::from_samples(samples))
}

fn sqlite_db_path(mode: &SqliteMode) -> PathBuf {
    env::temp_dir()
        .join("icydb-sqlite-comparison")
        .join(format!("{}-{}.sqlite3", std::process::id(), mode.key))
}

fn reset_sqlite_database(db_path: &Path) -> Result<(), String> {
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create SQLite comparison directory `{}`: {err}",
                parent.display()
            )
        })?;
    }

    for path in [
        db_path.to_path_buf(),
        db_path.with_extension("sqlite3-wal"),
        db_path.with_extension("sqlite3-shm"),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(format!(
                    "failed to remove stale SQLite comparison file `{}`: {err}",
                    path.display()
                ));
            }
        }
    }

    Ok(())
}

fn write_report_artifacts(report: &ComparisonReport, output_stem: &Path) -> Result<(), String> {
    if let Some(parent) = output_stem.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create SQLite comparison output directory `{}`: {err}",
                parent.display()
            )
        })?;
    }

    let json_path = output_stem.with_extension("json");
    let markdown_path = output_stem.with_extension("md");
    let json = serde_json::to_string_pretty(report)
        .map_err(|err| format!("failed to serialize SQLite comparison JSON: {err}"))?;
    fs::write(&json_path, json).map_err(|err| {
        format!(
            "failed to write SQLite comparison JSON `{}`: {err}",
            json_path.display()
        )
    })?;
    fs::write(&markdown_path, comparison_report_markdown(report)).map_err(|err| {
        format!(
            "failed to write SQLite comparison Markdown `{}`: {err}",
            markdown_path.display()
        )
    })?;

    Ok(())
}

fn comparison_report_markdown(report: &ComparisonReport) -> String {
    let mut out = String::new();
    writeln!(out, "# SQLite Comparison Harness").expect("writing markdown should succeed");
    writeln!(out).expect("writing markdown should succeed");
    writeln!(out, "- Audit line: {}", report.audit_line).expect("writing markdown should succeed");
    writeln!(out, "- Date: {}", report.date).expect("writing markdown should succeed");
    writeln!(out, "- SQLite version: {}", report.sqlite_version)
        .expect("writing markdown should succeed");
    writeln!(out, "- IcyDB profile: {}", report.icydb_build_profile)
        .expect("writing markdown should succeed");
    writeln!(out, "- Samples per timing cell: {}", report.sample_count)
        .expect("writing markdown should succeed");
    writeln!(out).expect("writing markdown should succeed");
    writeln!(out, "## Fairness Notes").expect("writing markdown should succeed");
    for note in report.fairness_notes {
        writeln!(out, "- {note}").expect("writing markdown should succeed");
    }
    writeln!(out).expect("writing markdown should succeed");
    writeln!(out, "## Results").expect("writing markdown should succeed");
    writeln!(
        out,
        "| Scenario | Route Family | IcyDB Median ns | SQLite Mode | SQLite Median ns | Match | Unsafe | Expectation |"
    )
    .expect("writing markdown should succeed");
    writeln!(out, "| --- | --- | ---: | --- | ---: | --- | --- | --- |")
        .expect("writing markdown should succeed");

    for scenario in &report.scenarios {
        for sqlite in &scenario.sqlite_results {
            writeln!(
                out,
                "| {} | {} | {} | {} | {} | {} | {} | {} |",
                scenario.key,
                scenario.route_family,
                scenario.icydb_timing.median,
                sqlite.mode_key,
                sqlite.timing.median,
                sqlite.matches_icydb,
                sqlite.unsafe_speed_reference,
                scenario.expectation,
            )
            .expect("writing markdown should succeed");
        }
    }

    out
}
