//! Module: CLI config report rendering and sync diagnostics.
//! Responsibility: render resolved `icydb.toml` summaries and ICP sync issues.
//! Does not own: config discovery, endpoint gates, or command execution.
//! Boundary: receives resolved config inputs and returns user-facing text.

use std::{collections::BTreeSet, fmt::Write as _, path::Path};

use icydb_config_build::GeneratedSqlUpdatePolicy;

use crate::table::{ColumnAlign, append_indented_table};

use super::ResolvedConfig;

type CanisterConfigRow = [String; 5];
type CheckedCanisterConfigRow = [String; 6];

const CANISTER_CONFIG_HEADERS: [&str; 5] =
    ["canister", "SQL surfaces", "metrics", "snapshot", "schema"];
const CHECKED_CANISTER_CONFIG_HEADERS: [&str; 6] = [
    "canister",
    "SQL surfaces",
    "metrics",
    "snapshot",
    "schema",
    "ICP environment",
];
const CANISTER_CONFIG_ALIGNMENTS: [ColumnAlign; 5] = [
    ColumnAlign::Left,
    ColumnAlign::Left,
    ColumnAlign::Left,
    ColumnAlign::Left,
    ColumnAlign::Left,
];
const CHECKED_CANISTER_CONFIG_ALIGNMENTS: [ColumnAlign; 6] = [
    ColumnAlign::Left,
    ColumnAlign::Left,
    ColumnAlign::Left,
    ColumnAlign::Left,
    ColumnAlign::Left,
    ColumnAlign::Left,
];

pub(super) fn render_config_report(
    start_dir: &Path,
    environment: Option<&str>,
    known_canisters: &[String],
    resolved: &ResolvedConfig,
) -> String {
    let known = known_canister_set(known_canisters);
    let config = resolved.config();
    let mut report = String::new();

    append_config_report_header(&mut report, start_dir, environment, resolved.config_path());
    append_configured_canisters(&mut report, config, environment, &known);

    report
}

fn append_config_report_header(
    report: &mut String,
    start_dir: &Path,
    environment: Option<&str>,
    config_path: Option<&Path>,
) {
    report.push_str("IcyDB config summary\n");
    match config_path {
        Some(path) => {
            let _ = writeln!(report, "Config file: {}", path.display());
        }
        None => report.push_str("Config file: not found\n"),
    }
    let _ = writeln!(report, "Search started at: {}", start_dir.display());
    match environment {
        Some(environment) => {
            let _ = writeln!(report, "ICP sync check: environment '{environment}'");
        }
        None => report.push_str("ICP sync check: not run; pass --environment <name>\n"),
    }
    report.push('\n');
}

fn append_configured_canisters(
    report: &mut String,
    config: &icydb_config_build::GeneratedIcydbConfig,
    environment: Option<&str>,
    known: &BTreeSet<&str>,
) {
    report.push_str("Configured canisters\n");
    if config.canisters().is_empty() {
        report.push_str("  None\n");
    } else if environment.is_some() {
        let rows = config
            .canisters()
            .iter()
            .map(|(name, canister)| checked_canister_config_row(name, *canister, known))
            .collect::<Vec<_>>();
        append_checked_canister_table(report, rows.as_slice());
    } else {
        let rows = config
            .canisters()
            .iter()
            .map(|(name, canister)| canister_config_row(name, *canister))
            .collect::<Vec<_>>();
        append_canister_table(report, rows.as_slice());
    }
}

pub(super) fn config_sync_issues(
    environment: Option<&str>,
    known_canisters: &[String],
    resolved: &ResolvedConfig,
) -> Vec<String> {
    let known = known_canister_set(known_canisters);
    let config = resolved.config();
    let mut issues = Vec::new();

    if resolved.config_path().is_none() {
        issues.push("no icydb.toml was found".to_string());
    }

    let Some(environment) = environment else {
        return issues;
    };

    for name in config.canisters().keys() {
        if !known.contains(name.as_str()) {
            issues.push(format!(
                "canisters.{name} is not in ICP environment '{environment}'"
            ));
        }
    }

    issues
}

fn known_canister_set(known_canisters: &[String]) -> BTreeSet<&str> {
    known_canisters.iter().map(String::as_str).collect()
}

fn canister_config_row(
    name: &str,
    canister: icydb_config_build::GeneratedCanisterConfig,
) -> CanisterConfigRow {
    [
        name.to_string(),
        sql_surface_status(
            canister.sql_readonly(),
            canister.sql_ddl(),
            canister.sql_fixtures(),
            canister.sql_update_policy(),
        ),
        metrics_surface_status(canister.metrics(), canister.metrics_extended()).to_string(),
        enabled_status(canister.snapshot()).to_string(),
        enabled_status(canister.schema()).to_string(),
    ]
}

fn checked_canister_config_row(
    name: &str,
    canister: icydb_config_build::GeneratedCanisterConfig,
    known: &BTreeSet<&str>,
) -> CheckedCanisterConfigRow {
    let icp_status = status_text(known.contains(name)).to_string();
    let [name, sql, metrics, snapshot, schema] = canister_config_row(name, canister);

    [name, sql, metrics, snapshot, schema, icp_status]
}

fn append_canister_table(report: &mut String, rows: &[CanisterConfigRow]) {
    append_indented_table(
        report,
        "  ",
        &CANISTER_CONFIG_HEADERS,
        rows,
        &CANISTER_CONFIG_ALIGNMENTS,
    );
}

fn append_checked_canister_table(report: &mut String, rows: &[CheckedCanisterConfigRow]) {
    append_indented_table(
        report,
        "  ",
        &CHECKED_CANISTER_CONFIG_HEADERS,
        rows,
        &CHECKED_CANISTER_CONFIG_ALIGNMENTS,
    );
}

const fn status_text(ok: bool) -> &'static str {
    if ok { "ok" } else { "mismatch" }
}

fn sql_surface_status(
    readonly: bool,
    ddl: bool,
    fixtures: bool,
    update_policy: Option<GeneratedSqlUpdatePolicy>,
) -> String {
    let mut surfaces = Vec::new();
    if readonly {
        surfaces.push("readonly");
    }
    if ddl {
        surfaces.push("ddl");
    }
    if fixtures {
        surfaces.push("fixtures");
    }
    if let Some(policy) = update_policy {
        surfaces.push(match policy {
            GeneratedSqlUpdatePolicy::PublicPrimaryKeyOnly => "update:primary_key",
            GeneratedSqlUpdatePolicy::PublicBoundedDeterministic => "update:bounded",
        });
    }

    if surfaces.is_empty() {
        "off".to_string()
    } else {
        surfaces.join(", ")
    }
}

const fn metrics_surface_status(metrics: bool, extended: bool) -> &'static str {
    match (metrics, extended) {
        (true, true) => "enabled, extended",
        (true, false) => "enabled",
        (false, true) => "extended",
        (false, false) => "off",
    }
}

const fn enabled_status(enabled: bool) -> &'static str {
    if enabled { "enabled" } else { "off" }
}
