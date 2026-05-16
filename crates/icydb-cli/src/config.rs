use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
};

use crate::{
    cli::{ConfigArgs, ConfigInitArgs},
    icp::known_canisters,
};

const CONFIG_FILE_NAME: &str = "icydb.toml";
const CONFIG_PATH_ENV: &str = "ICYDB_CONFIG_PATH";

struct ConfigContext {
    environment: Option<String>,
    known_canisters: Vec<String>,
    start_dir: PathBuf,
    resolved: icydb_config_build::ResolvedIcydbConfig,
}

/// Create a default IcyDB config file at the repository/workspace config root.
pub(crate) fn init_config(args: ConfigInitArgs) -> Result<(), String> {
    let start_dir = resolve_start_dir(args.start_dir())?;
    let path = resolved_config_path(start_dir.as_path())
        .unwrap_or_else(|| init_config_path(start_dir.as_path()));

    if path.exists() && !args.force() {
        return Err(format!(
            "IcyDB config already exists at '{}'; pass --force to replace it",
            path.display()
        ));
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("create config directory '{}': {err}", parent.display()))?;
    }
    fs::write(path.as_path(), render_default_config(&args))
        .map_err(|err| format!("write IcyDB config '{}': {err}", path.display()))?;

    println!("Wrote IcyDB config: {}", path.display());

    Ok(())
}

/// Resolve, validate, and display the IcyDB config visible from one directory.
pub(crate) fn show_config(args: ConfigArgs) -> Result<(), String> {
    let context = load_config_context(args)?;

    print!(
        "{}",
        render_config_report(
            context.start_dir.as_path(),
            context.environment.as_deref(),
            context.known_canisters.as_slice(),
            &context.resolved,
        )
    );

    Ok(())
}

/// Resolve, validate, and fail when the config is not synced with ICP metadata.
pub(crate) fn check_config(args: ConfigArgs) -> Result<(), String> {
    let context = load_config_context(args)?;
    let issues = config_sync_issues(
        context.environment.as_deref(),
        context.known_canisters.as_slice(),
        &context.resolved,
    );
    if issues.is_empty() {
        println!("IcyDB config check passed");
        if context.environment.is_none() {
            println!("ICP sync check not run; pass --environment <name>");
        }

        return Ok(());
    }

    let mut message = String::from("IcyDB config check failed");
    for issue in issues {
        message.push_str("\n- ");
        message.push_str(issue.as_str());
    }

    Err(message)
}

fn load_config_context(args: ConfigArgs) -> Result<ConfigContext, String> {
    let environment = args.environment().map(str::to_string);
    let known_canisters = if let Some(environment) = &environment {
        known_canisters(environment.as_str())?
    } else {
        Vec::new()
    };
    let start_dir = resolve_start_dir(args.start_dir())?;
    let resolved = icydb_config_build::load_resolved_icydb_toml(start_dir.as_path(), &[])
        .map_err(|err| err.to_string())?;

    Ok(ConfigContext {
        environment,
        known_canisters,
        start_dir,
        resolved,
    })
}

fn resolve_start_dir(start_dir: Option<&Path>) -> Result<PathBuf, String> {
    let path = start_dir.map_or_else(|| PathBuf::from("."), Path::to_path_buf);

    path.canonicalize()
        .map_err(|err| format!("resolve config start directory '{}': {err}", path.display()))
}

fn init_config_path(start_dir: &Path) -> PathBuf {
    workspace_root(start_dir)
        .unwrap_or_else(|| start_dir.to_path_buf())
        .join(CONFIG_FILE_NAME)
}

fn resolved_config_path(start_dir: &Path) -> Option<PathBuf> {
    if let Some(explicit) = env::var_os(CONFIG_PATH_ENV) {
        return Some(PathBuf::from(explicit));
    }

    for ancestor in start_dir.ancestors() {
        let candidate = ancestor.join(CONFIG_FILE_NAME);
        if candidate.exists() {
            return Some(candidate);
        }
        if is_workspace_root(ancestor) {
            break;
        }
    }

    None
}

fn workspace_root(start_dir: &Path) -> Option<PathBuf> {
    start_dir
        .ancestors()
        .find(|ancestor| is_workspace_root(ancestor))
        .map(Path::to_path_buf)
}

fn is_workspace_root(path: &Path) -> bool {
    fs::read_to_string(path.join("Cargo.toml")).is_ok_and(|source| source.contains("[workspace]"))
}

fn render_default_config(args: &ConfigInitArgs) -> String {
    format!(
        "\
[canisters.{canister}.sql]
readonly = {readonly}
ddl = {ddl}
fixtures = {fixtures}

[canisters.{canister}.metrics]
enabled = {metrics}
reset = {metrics_reset}

[canisters.{canister}.snapshot]
enabled = {snapshot}
",
        canister = args.canister_name(),
        readonly = args.readonly(),
        ddl = args.ddl(),
        fixtures = args.fixtures(),
        metrics = args.metrics(),
        metrics_reset = args.metrics_reset(),
        snapshot = args.snapshot(),
    )
}

pub(crate) fn render_config_report(
    start_dir: &Path,
    environment: Option<&str>,
    known_canisters: &[String],
    resolved: &icydb_config_build::ResolvedIcydbConfig,
) -> String {
    let known = known_canisters
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let config = resolved.config();
    let mut report = String::new();

    report.push_str("IcyDB config summary\n");
    match resolved.config_path() {
        Some(path) => report.push_str(format!("Config file: {}\n", path.display()).as_str()),
        None => report.push_str("Config file: not found\n"),
    }
    report.push_str(format!("Search started at: {}\n", start_dir.display()).as_str());
    match environment {
        Some(environment) => {
            report.push_str(format!("ICP sync check: environment '{environment}'\n").as_str());
        }
        None => report.push_str("ICP sync check: not run; pass --environment <name>\n"),
    }
    report.push('\n');

    report.push_str("Configured canisters\n");
    if config.canisters().is_empty() {
        report.push_str("  None\n");
    } else if environment.is_some() {
        let rows = config
            .canisters()
            .iter()
            .map(|(name, canister)| {
                (
                    name.as_str(),
                    sql_surface_status(
                        canister.sql_readonly(),
                        canister.sql_ddl(),
                        canister.sql_fixtures(),
                    ),
                    metrics_surface_status(canister.metrics(), canister.metrics_reset()),
                    enabled_status(canister.snapshot()),
                    Some(status_text(known.contains(name.as_str()))),
                )
            })
            .collect::<Vec<_>>();
        append_canister_table(&mut report, rows.as_slice());
    } else {
        let rows = config
            .canisters()
            .iter()
            .map(|(name, canister)| {
                (
                    name.as_str(),
                    sql_surface_status(
                        canister.sql_readonly(),
                        canister.sql_ddl(),
                        canister.sql_fixtures(),
                    ),
                    metrics_surface_status(canister.metrics(), canister.metrics_reset()),
                    enabled_status(canister.snapshot()),
                    None,
                )
            })
            .collect::<Vec<_>>();
        append_canister_table(&mut report, rows.as_slice());
    }

    report
}

pub(crate) fn config_sync_issues(
    environment: Option<&str>,
    known_canisters: &[String],
    resolved: &icydb_config_build::ResolvedIcydbConfig,
) -> Vec<String> {
    let known = known_canisters
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
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

fn append_canister_table(report: &mut String, rows: &[(&str, &str, &str, &str, Option<&str>)]) {
    let canister_width = table_width("canister", rows.iter().map(|(name, _, _, _, _)| *name));
    let sql_width = table_width("SQL surfaces", rows.iter().map(|(_, sql, _, _, _)| *sql));
    let metrics_width = table_width("metrics", rows.iter().map(|(_, _, metrics, _, _)| *metrics));
    let snapshot_width = table_width(
        "snapshot",
        rows.iter().map(|(_, _, _, snapshot, _)| *snapshot),
    );
    let include_in_env = rows.iter().any(|(_, _, _, _, in_env)| in_env.is_some());

    if include_in_env {
        report.push_str(
            format!(
                "  {canister:<canister_width$}  {sql:<sql_width$}  {metrics:<metrics_width$}  {snapshot:<snapshot_width$}  {in_env}\n",
                canister = "canister",
                sql = "SQL surfaces",
                metrics = "metrics",
                snapshot = "snapshot",
                in_env = "ICP environment",
            )
            .as_str(),
        );
    } else {
        report.push_str(
            format!(
                "  {canister:<canister_width$}  {sql:<sql_width$}  {metrics:<metrics_width$}  {snapshot}\n",
                canister = "canister",
                sql = "SQL surfaces",
                metrics = "metrics",
                snapshot = "snapshot",
            )
            .as_str(),
        );
    }
    for (canister, sql, metrics, snapshot, in_env) in rows {
        if let Some(in_env) = in_env {
            report.push_str(
                format!(
                    "  {canister:<canister_width$}  {sql:<sql_width$}  {metrics:<metrics_width$}  {snapshot:<snapshot_width$}  {in_env}\n"
                )
                .as_str(),
            );
        } else {
            report.push_str(
                format!(
                    "  {canister:<canister_width$}  {sql:<sql_width$}  {metrics:<metrics_width$}  {snapshot}\n"
                )
                .as_str(),
            );
        }
    }
}

fn table_width<'a>(heading: &str, values: impl Iterator<Item = &'a str>) -> usize {
    values.map(str::len).max().unwrap_or(0).max(heading.len())
}

const fn status_text(ok: bool) -> &'static str {
    if ok { "ok" } else { "mismatch" }
}

const fn sql_surface_status(readonly: bool, ddl: bool, fixtures: bool) -> &'static str {
    match (readonly, ddl, fixtures) {
        (true, true, true) => "readonly, ddl, fixtures",
        (true, true, false) => "readonly, ddl",
        (true, false, true) => "readonly, fixtures",
        (true, false, false) => "readonly",
        (false, true, true) => "ddl, fixtures",
        (false, true, false) => "ddl",
        (false, false, true) => "fixtures",
        (false, false, false) => "off",
    }
}

const fn metrics_surface_status(metrics: bool, reset: bool) -> &'static str {
    match (metrics, reset) {
        (true, true) => "enabled, reset",
        (true, false) => "enabled",
        (false, true) => "reset",
        (false, false) => "off",
    }
}

const fn enabled_status(enabled: bool) -> &'static str {
    if enabled { "enabled" } else { "off" }
}
