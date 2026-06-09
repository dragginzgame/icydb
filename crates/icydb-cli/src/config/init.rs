//! Module: CLI config initialization.
//! Responsibility: create a default `icydb.toml` in the resolved config root.
//! Does not own: config validation, report rendering, or endpoint surface gates.
//! Boundary: receives parsed init args and writes one user-facing config file.

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::cli::ConfigInitArgs;

use super::resolution::{
    CONFIG_FILE_NAME, resolve_start_dir, resolved_config_path, workspace_root,
};

/// Create a default IcyDB config file at the repository/workspace config root.
pub(crate) fn init_config(args: ConfigInitArgs) -> Result<(), String> {
    let start_dir = resolve_start_dir(args.start_dir())?;
    let path = resolved_config_path(start_dir.as_path())
        .unwrap_or_else(|| init_config_path(start_dir.as_path()));

    if path.exists() && !args.force() {
        return Err(config_exists_message(path.as_path()));
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

fn init_config_path(start_dir: &Path) -> PathBuf {
    workspace_root(start_dir)
        .unwrap_or_else(|| start_dir.to_path_buf())
        .join(CONFIG_FILE_NAME)
}

fn config_exists_message(path: &Path) -> String {
    format!(
        "IcyDB config already exists at '{}'; pass --force to replace it",
        path.display()
    )
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
extended = {metrics_extended}

[canisters.{canister}.snapshot]
enabled = {snapshot}

[canisters.{canister}.schema]
enabled = {schema}
",
        canister = args.canister_name(),
        readonly = args.readonly(),
        ddl = args.ddl(),
        fixtures = args.fixtures(),
        metrics = args.metrics(),
        metrics_extended = args.metrics_extended(),
        snapshot = args.snapshot(),
        schema = args.schema(),
    )
}
