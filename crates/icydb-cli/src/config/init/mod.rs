//! Module: CLI config initialization.
//! Responsibility: create a default `icydb.toml` in the resolved config root.
//! Does not own: config validation, report rendering, or endpoint surface gates.
//! Boundary: receives parsed init args and writes one user-facing config file.

mod placement;
mod render;

use std::{
    fs::{self, OpenOptions},
    io::{ErrorKind, Write as _},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::cli::ConfigInitArgs;

use self::{
    placement::{ConfigInitPlacement, resolve_config_init_placement},
    render::render_default_config,
};
use super::resolution::resolve_start_dir;

/// Create a default IcyDB config file at the repository/workspace config root.
pub(crate) fn init_config(args: ConfigInitArgs) -> Result<(), String> {
    init_config_with_existing_config_path(args, existing_config_path)
}

#[cfg(test)]
pub(crate) fn init_config_without_existing_config(args: ConfigInitArgs) -> Result<(), String> {
    init_config_with_existing_config_path(args, |_| None)
}

#[cfg(test)]
pub(crate) fn init_config_with_existing_config_for_test(
    args: ConfigInitArgs,
    existing_config: PathBuf,
) -> Result<(), String> {
    init_config_with_existing_config_path(args, |_| Some(existing_config))
}

fn init_config_with_existing_config_path(
    args: ConfigInitArgs,
    existing_config_path: impl FnOnce(&Path) -> Option<PathBuf>,
) -> Result<(), String> {
    let start_dir = resolve_start_dir(args.start_dir())?;
    let placement = resolve_config_init_placement(start_dir.as_path(), existing_config_path)?;
    let path = placement.path();

    match placement {
        ConfigInitPlacement::ExistingConfig(_) if !args.force() => {
            return Err(config_exists_message(path));
        }
        _ if path.exists() && !args.force() => {
            return Err(config_exists_message(path));
        }
        _ => {}
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("create config directory '{}': {err}", parent.display()))?;
    }
    let contents = render_default_config(&args)?;
    if args.force() {
        replace_config(path, contents.as_bytes())?;
    } else {
        create_config(path, contents.as_bytes())?;
    }

    println!("Wrote IcyDB config: {}", path.display());

    Ok(())
}

fn existing_config_path(start_dir: &Path) -> Option<PathBuf> {
    icydb_config::resolve_existing_icydb_toml(start_dir)
}

fn config_exists_message(path: &Path) -> String {
    format!(
        "IcyDB config already exists at '{}'; pass --force to replace it",
        path.display()
    )
}

fn create_config(path: &Path, contents: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|err| {
            if err.kind() == ErrorKind::AlreadyExists {
                config_exists_message(path)
            } else {
                format!("create IcyDB config '{}': {err}", path.display())
            }
        })?;
    file.write_all(contents)
        .map_err(|err| format!("write IcyDB config '{}': {err}", path.display()))
}

fn replace_config(path: &Path, contents: &[u8]) -> Result<(), String> {
    let temp_path = temp_config_path(path)?;
    let write_result = write_temp_config(temp_path.as_path(), contents);
    if let Err(err) = write_result {
        let _ = fs::remove_file(temp_path.as_path());
        return Err(err);
    }

    fs::rename(temp_path.as_path(), path).map_err(|err| {
        let _ = fs::remove_file(temp_path.as_path());
        format!(
            "replace IcyDB config '{}' with '{}': {err}",
            path.display(),
            temp_path.display()
        )
    })
}

fn write_temp_config(path: &Path, contents: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|err| format!("create temporary IcyDB config '{}': {err}", path.display()))?;
    file.write_all(contents)
        .map_err(|err| format!("write temporary IcyDB config '{}': {err}", path.display()))
}

fn temp_config_path(path: &Path) -> Result<PathBuf, String> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(icydb_config::ICYDB_CONFIG_FILE_NAME);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("resolve temporary config timestamp: {err}"))?
        .as_nanos();

    for attempt in 0..100 {
        let temp_name = format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            nanos + attempt
        );
        let candidate = parent.join(temp_name);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(format!(
        "find temporary IcyDB config path next to '{}'",
        path.display()
    ))
}
