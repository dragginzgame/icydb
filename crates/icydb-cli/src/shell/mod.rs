mod input;
mod perf;
mod render;

use std::{
    collections::VecDeque,
    path::PathBuf,
    process::{Command, Stdio},
};

use candid::Decode;
use rustyline::DefaultEditor;

use crate::{
    cli::{DEFAULT_CANISTER, SqlArgs},
    icp::require_created_canister,
    shell::{
        input::{ShellInput, read_statement},
        render::{ShellSqlQueryPerfResult, render_shell_text_from_perf_result},
    },
};

#[cfg(test)]
pub(crate) use crate::shell::{
    input::{
        drain_complete_shell_statements, is_shell_help_command, normalize_shell_statement_line,
        shell_help_text,
    },
    perf::{
        ShellPerfAttribution, normalize_grouped_next_cursor_json, parse_perf_result,
        render_perf_suffix,
    },
    render::{
        finalize_successful_command_output, render_grouped_shell_text, render_projection_shell_text,
    },
};

///
/// ShellConfig
///
/// ShellConfig carries the small amount of runtime configuration needed by the
/// dev SQL shell binary.
///

pub(crate) struct ShellConfig {
    pub(crate) canister: String,
    pub(crate) environment: String,
    pub(crate) history_file: PathBuf,
    pub(crate) sql: Option<String>,
}

impl ShellConfig {
    pub(crate) fn from_sql_args(args: SqlArgs) -> Self {
        let sql = args
            .sql
            .or_else(|| (!args.trailing_sql.is_empty()).then(|| args.trailing_sql.join(" ")));
        Self {
            canister: args
                .canister
                .unwrap_or_else(|| DEFAULT_CANISTER.to_string()),
            environment: args.environment,
            history_file: args.history_file,
            sql,
        }
    }
}

/// Run a one-shot SQL statement or the interactive SQL shell.
pub(crate) fn run_sql_command(args: SqlArgs) -> Result<(), String> {
    let config = ShellConfig::from_sql_args(args);

    if let Some(sql) = config.sql {
        if input::is_shell_help_command(sql.as_str()) {
            print!(
                "{}",
                render::finalize_successful_command_output(input::shell_help_text())
            );

            return Ok(());
        }

        let output = execute_sql(
            config.environment.as_str(),
            config.canister.as_str(),
            sql.as_str(),
        )?;
        print!(
            "{}",
            render::finalize_successful_command_output(output.as_str())
        );
    } else {
        require_created_canister(config.environment.as_str(), config.canister.as_str())?;
        run_interactive_shell(&config)?;
    }

    Ok(())
}

fn run_interactive_shell(config: &ShellConfig) -> Result<(), String> {
    // Phase 1: prepare the line editor and persistent history file.
    let mut editor = DefaultEditor::new().map_err(|err| err.to_string())?;
    let mut pending_sql = VecDeque::<String>::new();
    let mut partial_statement = String::new();
    if let Some(parent) = config.history_file.parent() {
        std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    if config.history_file.exists() {
        editor
            .load_history(config.history_file.as_path())
            .map_err(|err| err.to_string())?;
    }

    eprintln!(
        "[icydb sql] interactive mode on '{}:{}' (terminate statements with ';', use \\q, exit, or Ctrl-D to quit)",
        config.environment, config.canister
    );

    // Phase 2: collect one semicolon-terminated statement, then execute it.
    loop {
        match read_statement(&mut editor, &mut pending_sql, &mut partial_statement)? {
            ShellInput::Exit => break,
            ShellInput::Help => {
                print!(
                    "{}",
                    render::finalize_successful_command_output(input::shell_help_text())
                );
            }
            ShellInput::Sql(sql) => {
                editor
                    .add_history_entry(sql.as_str())
                    .map_err(|err| err.to_string())?;
                editor
                    .append_history(config.history_file.as_path())
                    .map_err(|err| err.to_string())?;

                match execute_sql(
                    config.environment.as_str(),
                    config.canister.as_str(),
                    sql.as_str(),
                ) {
                    Ok(output) => {
                        print!(
                            "{}",
                            render::finalize_successful_command_output(output.as_str())
                        );
                    }
                    Err(err) => println!("ERROR: {err}"),
                }
            }
        }
    }

    Ok(())
}

fn execute_sql(environment: &str, canister: &str, sql: &str) -> Result<String, String> {
    require_created_canister(environment, canister)?;

    let escaped_sql = candid_escape_string(sql);
    let candid_bytes = icp_query(
        environment,
        canister,
        "query_with_perf",
        escaped_sql.as_str(),
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<ShellSqlQueryPerfResult, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    // Phase 2: decode the Candid response and render through the canonical
    // SQL facade, with shell-only footer/cell tweaks layered on top.
    match response {
        Ok(result) => Ok(render_shell_text_from_perf_result(result)),
        Err(err) => Ok(format!("ERROR: {err}")),
    }
}

fn icp_query(
    environment: &str,
    canister: &str,
    method: &str,
    escaped_sql: &str,
) -> Result<Vec<u8>, String> {
    let candid_arg = format!("(\"{escaped_sql}\")");
    let output = icp_query_command(environment, canister, method, candid_arg.as_str())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;

    hex_response_bytes(stdout.as_str())
}

pub(crate) fn icp_query_command(
    environment: &str,
    canister: &str,
    method: &str,
    candid_arg: &str,
) -> Command {
    let mut command = Command::new("icp");
    command
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(method)
        .arg(candid_arg)
        .arg("--query")
        .arg("--output")
        .arg("hex")
        .arg("--environment")
        .arg(environment);

    command
}

pub(crate) fn hex_response_bytes(output: &str) -> Result<Vec<u8>, String> {
    let candidate = output
        .rsplit_once("response (hex):")
        .map_or(output, |(_, value)| value)
        .trim();
    let hex = candidate.split_whitespace().collect::<String>();
    if hex.is_empty() {
        return Err("icp canister call returned an empty hex response".to_string());
    }
    if hex.len() % 2 != 0 {
        return Err("icp canister call returned odd-length hex response".to_string());
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }

    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        other => Err(format!(
            "icp canister call returned non-hex byte '{}'",
            char::from(other)
        )),
    }
}

fn candid_escape_string(sql: &str) -> String {
    let mut escaped = String::with_capacity(sql.len());
    for ch in sql.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(ch),
        }
    }

    escaped
}
