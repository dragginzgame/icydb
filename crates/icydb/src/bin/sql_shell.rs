use std::{
    env,
    path::PathBuf,
    process::{Command, Stdio},
};

use icydb::{
    Error,
    db::sql::{SqlGroupedRowsOutput, SqlQueryResult, SqlQueryRowsOutput, render_grouped_lines},
};
use rustyline::{DefaultEditor, error::ReadlineError};
use serde_json::Value;

fn main() {
    let config = ShellConfig::parse(env::args().skip(1).collect());

    match config.sql {
        Some(sql) => {
            let output = execute_sql(config.canister.as_str(), sql.as_str())
                .unwrap_or_else(|err| panic!("execute SQL statement: {err}"));
            println!("{output}");
        }
        None => {
            run_interactive_shell(&config)
                .unwrap_or_else(|err| panic!("run interactive SQL shell: {err}"));
        }
    }
}

///
/// ShellConfig
///
/// ShellConfig carries the small amount of runtime configuration needed by the
/// dev SQL shell binary.
///

struct ShellConfig {
    canister: String,
    history_file: PathBuf,
    sql: Option<String>,
}

impl ShellConfig {
    fn parse(args: Vec<String>) -> Self {
        let mut canister = env::var("SQLQ_CANISTER").unwrap_or_else(|_| "demo_rpg".to_string());
        let mut history_file = env::var("SQLQ_HISTORY_FILE")
            .map_or_else(|_| PathBuf::from(".cache/sql_history"), PathBuf::from);
        let mut sql = None;

        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--canister" | "-c" => {
                    let value = args
                        .get(index + 1)
                        .unwrap_or_else(|| panic!("--canister requires one value"));
                    canister.clone_from(value);
                    index += 2;
                }
                "--history-file" => {
                    let value = args
                        .get(index + 1)
                        .unwrap_or_else(|| panic!("--history-file requires one value"));
                    history_file = PathBuf::from(value);
                    index += 2;
                }
                "--sql" => {
                    let value = args
                        .get(index + 1)
                        .unwrap_or_else(|| panic!("--sql requires one value"));
                    sql = Some(value.clone());
                    index += 2;
                }
                _ => {
                    sql = Some(args[index..].join(" "));
                    break;
                }
            }
        }

        Self {
            canister,
            history_file,
            sql,
        }
    }
}

fn run_interactive_shell(config: &ShellConfig) -> Result<(), String> {
    // Phase 1: prepare the line editor and persistent history file.
    let mut editor = DefaultEditor::new().map_err(|err| err.to_string())?;
    if let Some(parent) = config.history_file.parent() {
        std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    if config.history_file.exists() {
        editor
            .load_history(config.history_file.as_path())
            .map_err(|err| err.to_string())?;
    }

    eprintln!(
        "[sql.sh] interactive mode on '{}' (terminate statements with ';', use \\q, exit, or Ctrl-D to quit)",
        config.canister
    );

    // Phase 2: collect one semicolon-terminated statement, then execute it.
    loop {
        let Some(sql) = read_statement(&mut editor)? else {
            break;
        };
        editor
            .add_history_entry(sql.as_str())
            .map_err(|err| err.to_string())?;
        editor
            .append_history(config.history_file.as_path())
            .map_err(|err| err.to_string())?;

        match execute_sql(config.canister.as_str(), sql.as_str()) {
            Ok(output) => println!("{output}"),
            Err(err) => println!("ERROR: {err}"),
        }
    }

    Ok(())
}

fn read_statement(editor: &mut DefaultEditor) -> Result<Option<String>, String> {
    let mut statement = String::new();
    let mut prompt = "icydb> ";

    loop {
        match editor.readline(prompt) {
            Ok(line) => {
                // Ignore top-level blank input so pressing Enter on an empty
                // prompt simply reprompts instead of executing empty SQL.
                if statement.trim().is_empty() && line.trim().is_empty() {
                    prompt = "icydb> ";
                    continue;
                }

                if statement.trim().is_empty() && matches!(line.as_str(), "\\q" | "quit" | "exit") {
                    return Ok(None);
                }

                if !statement.is_empty() {
                    statement.push('\n');
                }
                statement.push_str(line.as_str());

                if statement.trim_end().ends_with(';') {
                    return Ok(Some(statement));
                }

                prompt = "    -> ";
            }
            Err(ReadlineError::Interrupted) => {
                statement.clear();
                prompt = "icydb> ";
            }
            Err(ReadlineError::Eof) => {
                if statement.trim().is_empty() {
                    println!();
                    return Ok(None);
                }

                return Ok(Some(statement));
            }
            Err(err) => return Err(err.to_string()),
        }
    }
}

fn execute_sql(canister: &str, sql: &str) -> Result<String, String> {
    let escaped_sql = candid_escape_string(sql);

    // Phase 1: prefer the perf-aware query surface when it exists on the
    // deployed canister, but keep one clean fallback for older deployments.
    let raw_json = match dfx_query(canister, "query_with_perf", escaped_sql.as_str()) {
        Ok(raw_json) => raw_json,
        Err(_) => dfx_query(canister, "query", escaped_sql.as_str())?,
    };

    // Phase 2: decode the dfx JSON envelope and render through the canonical
    // SQL facade, with shell-only footer/cell tweaks layered on top.
    render_shell_text_from_dfx_json(raw_json.as_str())
}

fn dfx_query(canister: &str, method: &str, escaped_sql: &str) -> Result<String, String> {
    let candid_arg = format!("(\"{escaped_sql}\")");
    let output = Command::new("dfx")
        .arg("canister")
        .arg("call")
        .arg(canister)
        .arg(method)
        .arg(candid_arg)
        .arg("--output")
        .arg("json")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| err.to_string())?;

    if output.status.success() {
        return String::from_utf8(output.stdout).map_err(|err| err.to_string());
    }

    Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
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

fn render_shell_text_from_dfx_json(input: &str) -> Result<String, String> {
    let envelope: Value = serde_json::from_str(input).map_err(|err| err.to_string())?;
    let payload = find_result_payload(&envelope)
        .ok_or_else(|| "find Ok/Err result payload in dfx json envelope".to_string())?;

    if let Some(ok) = payload.get("Ok") {
        if let Some((result, instructions)) = parse_perf_result(ok) {
            return Ok(render_shell_text(result, Some(instructions)));
        }

        let result: SqlQueryResult =
            serde_json::from_value(ok.clone()).map_err(|err| err.to_string())?;
        return Ok(render_shell_text(result, None));
    }

    let err: Error = serde_json::from_value(
        payload
            .get("Err")
            .cloned()
            .ok_or_else(|| "Err payload missing".to_string())?,
    )
    .map_err(|err| err.to_string())?;

    Ok(format!("ERROR: {err}"))
}

fn render_shell_text(result: SqlQueryResult, instructions: Option<u64>) -> String {
    match result {
        SqlQueryResult::Projection(rows) => render_projection_shell_text(rows, instructions),
        SqlQueryResult::Grouped(rows) => render_grouped_shell_text(rows, instructions),
        other => other.render_text(),
    }
}

fn render_projection_shell_text(mut rows: SqlQueryRowsOutput, instructions: Option<u64>) -> String {
    uppercase_null_cells(rows.rows.as_mut_slice());

    let mut lines =
        icydb::db::sql::render_projection_lines(rows.entity.as_str(), &rows.as_projection_rows());
    append_perf_suffix(lines.as_mut_slice(), instructions);

    lines.join("\n")
}

fn render_grouped_shell_text(mut rows: SqlGroupedRowsOutput, instructions: Option<u64>) -> String {
    uppercase_null_cells(rows.rows.as_mut_slice());

    let mut lines = render_grouped_lines(&rows);
    append_perf_suffix(lines.as_mut_slice(), instructions);

    lines.join("\n")
}

fn uppercase_null_cells(rows: &mut [Vec<String>]) {
    for row in rows {
        for cell in row {
            if cell == "null" {
                *cell = "NULL".to_string();
            }
        }
    }
}

fn append_perf_suffix(lines: &mut [String], instructions: Option<u64>) {
    let Some(instructions) = instructions else {
        return;
    };
    let Some(last_line) = lines.last_mut() else {
        return;
    };
    if last_line.ends_with(" in set") {
        last_line.push_str(" (");
        last_line.push_str(format_instruction_count(instructions).as_str());
        last_line.push(')');
    }
}

fn format_instruction_count(instructions: u64) -> String {
    if instructions >= 1_000_000 {
        let tenths = (instructions.saturating_mul(10) + 500_000) / 1_000_000;
        return format!("{}.{}M instructions", tenths / 10, tenths % 10);
    }

    if instructions >= 1_000 {
        let tenths = (instructions.saturating_mul(10) + 500) / 1_000;
        return format!("{}.{}K instructions", tenths / 10, tenths % 10);
    }

    format!("{instructions} instructions")
}

fn parse_perf_result(value: &Value) -> Option<(SqlQueryResult, u64)> {
    let Value::Object(map) = value else {
        return None;
    };
    let result_value = map.get("result")?;
    let instructions = parse_instruction_count(map.get("instructions")?)?;
    let result = serde_json::from_value::<SqlQueryResult>(result_value.clone()).ok()?;

    Some((result, instructions))
}

fn parse_instruction_count(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn find_result_payload(value: &Value) -> Option<&Value> {
    match value {
        Value::Object(map) => {
            if map.contains_key("Ok") || map.contains_key("Err") {
                return Some(value);
            }

            map.values().find_map(find_result_payload)
        }
        Value::Array(items) => items.iter().find_map(find_result_payload),
        _ => None,
    }
}
