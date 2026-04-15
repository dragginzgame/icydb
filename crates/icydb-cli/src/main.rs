use std::{
    env,
    path::PathBuf,
    process::{Command, Stdio},
    time::Instant,
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
            if is_shell_help_command(sql.as_str()) {
                print!("{}", finalize_successful_command_output(shell_help_text()));

                return;
            }

            let output = execute_sql(config.canister.as_str(), sql.as_str())
                .unwrap_or_else(|err| panic!("execute SQL statement: {err}"));
            print!("{}", finalize_successful_command_output(output.as_str()));
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

///
/// ShellPerfAttribution
///
/// ShellPerfAttribution carries the hard-cut dev-shell perf footer payload.
/// The shell keeps this formatting-only shape local so the canister payload can
/// evolve independently from the rendered footer string.
///

struct ShellPerfAttribution {
    total: u64,
    planner: u64,
    store: u64,
    executor: u64,
    pure_covering_decode: u64,
    pure_covering_row_assembly: u64,
    decode: u64,
    compiler: u64,
}

impl ShellPerfAttribution {
    // Sum the current top-level SQL query perf contract exactly as emitted by
    // query_with_perf: compiler, planner, store, executor, then public decode.
    const fn attributed_total(&self) -> u64 {
        self.compiler
            .saturating_add(self.planner)
            .saturating_add(self.store)
            .saturating_add(self.executor)
            .saturating_add(self.decode)
    }

    // Preserve one visible fallback bucket for legacy or future payloads whose
    // total exceeds the current top-level query perf contract.
    const fn residual_total(&self) -> u64 {
        self.total.saturating_sub(self.attributed_total())
    }

    const fn pure_covering_executor_residual(&self) -> u64 {
        self.executor
            .saturating_sub(self.pure_covering_decode)
            .saturating_sub(self.pure_covering_row_assembly)
    }
}

///
/// ShellLocalRenderAttribution
///
/// ShellLocalRenderAttribution records the CLI-only render time spent turning
/// decoded SQL result payloads into the local table/footer text shown in the
/// shell. This stays separate from the canister-side `c/p/s/e/d` instruction
/// contract because it measures native shell formatting work rather than query
/// engine execution.
///

struct ShellLocalRenderAttribution {
    render_micros: u128,
}

///
/// ShellInput
///
/// ShellInput classifies one top-level interactive shell action before the CLI
/// decides whether to execute SQL, print local help text, or exit the shell.
///

enum ShellInput {
    Sql(String),
    Help,
    Exit,
}

fn is_shell_help_command(input: &str) -> bool {
    matches!(
        input.trim().trim_end_matches(';').trim(),
        "?" | "help" | "\\?" | "\\help"
    )
}

const fn shell_help_text() -> &'static str {
    "meta commands:
  ? / help         show this help
  \\q / quit / exit quit the interactive shell

perf footer legend:
  c = compile     parse, lower, and compile the SQL surface
  p = planner     resolve visible indexes and build the structural access plan
  s = store       physical data/index-store traversal and physical payload decode
  e = executor    residual filter, order, group, aggregate, and projection logic
  d = decode      package the public SQL result payload for the shell
  {pc=.../...}    pure covering decode / pure covering row assembly
  {er=...}        remaining executor work outside the explicit pure covering subpath
  {r=...}         local shell render time for table/footer formatting

examples:
  SELECT name FROM character;
  EXPLAIN EXECUTION SELECT name FROM character;"
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
        match read_statement(&mut editor)? {
            ShellInput::Exit => break,
            ShellInput::Help => {
                print!("{}", finalize_successful_command_output(shell_help_text()));
            }
            ShellInput::Sql(sql) => {
                editor
                    .add_history_entry(sql.as_str())
                    .map_err(|err| err.to_string())?;
                editor
                    .append_history(config.history_file.as_path())
                    .map_err(|err| err.to_string())?;

                match execute_sql(config.canister.as_str(), sql.as_str()) {
                    Ok(output) => print!("{}", finalize_successful_command_output(output.as_str())),
                    Err(err) => println!("ERROR: {err}"),
                }
            }
        }
    }

    Ok(())
}

fn read_statement(editor: &mut DefaultEditor) -> Result<ShellInput, String> {
    let mut statement = String::new();
    let mut prompt = "icydb> ";

    loop {
        match editor.readline(prompt) {
            Ok(line) => {
                // Normalize recalled or freshly typed lines before they enter
                // the statement buffer so history recall does not reintroduce
                // trailing spaces or duplicate terminators.
                let normalized_line = normalize_shell_statement_line(line.as_str());

                // Ignore top-level blank input so pressing Enter on an empty
                // prompt simply reprompts instead of executing empty SQL.
                if statement.trim().is_empty() && normalized_line.is_empty() {
                    prompt = "icydb> ";
                    continue;
                }

                if statement.trim().is_empty()
                    && matches!(normalized_line.as_str(), "\\q" | "quit" | "exit")
                {
                    return Ok(ShellInput::Exit);
                }

                if statement.trim().is_empty() && is_shell_help_command(normalized_line.as_str()) {
                    return Ok(ShellInput::Help);
                }

                if !statement.is_empty() {
                    statement.push('\n');
                }
                statement.push_str(normalized_line.as_str());

                if statement.trim_end().ends_with(';') {
                    return Ok(ShellInput::Sql(statement));
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
                    return Ok(ShellInput::Exit);
                }

                return Ok(ShellInput::Sql(statement));
            }
            Err(err) => return Err(err.to_string()),
        }
    }
}

// Trim shell-facing line noise while preserving the SQL text itself, so
// history recall does not force users to remove stray whitespace or `;;`.
fn normalize_shell_statement_line(line: &str) -> String {
    let trimmed = line.trim();
    let without_extra_semicolons = trimmed.trim_end_matches(';');

    if without_extra_semicolons.len() == trimmed.len() {
        return trimmed.to_string();
    }

    format!("{without_extra_semicolons};")
}

fn execute_sql(canister: &str, sql: &str) -> Result<String, String> {
    let escaped_sql = candid_escape_string(sql);
    let raw_json = dfx_query(canister, "query_with_perf", escaped_sql.as_str())?;

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
        let (result, attribution) = parse_perf_result(ok)?;
        let render_start = Instant::now();
        let rendered = render_shell_text(result, Some(attribution), None);
        let render_attribution = ShellLocalRenderAttribution {
            render_micros: render_start.elapsed().as_micros(),
        };
        let rendered = append_shell_render_suffix(rendered, Some(&render_attribution));

        return Ok(rendered);
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

fn render_shell_text(
    result: SqlQueryResult,
    attribution: Option<ShellPerfAttribution>,
    render_attribution: Option<ShellLocalRenderAttribution>,
) -> String {
    match result {
        SqlQueryResult::Projection(rows) => {
            render_projection_shell_text(rows, attribution, render_attribution)
        }
        SqlQueryResult::Grouped(rows) => {
            render_grouped_shell_text(rows, attribution, render_attribution)
        }
        other => other.render_text(),
    }
}

fn render_projection_shell_text(
    mut rows: SqlQueryRowsOutput,
    attribution: Option<ShellPerfAttribution>,
    render_attribution: Option<ShellLocalRenderAttribution>,
) -> String {
    uppercase_null_cells(rows.rows.as_mut_slice());

    let mut lines =
        icydb::db::sql::render_projection_lines(rows.entity.as_str(), &rows.as_projection_rows());
    append_perf_suffix(
        lines.as_mut_slice(),
        attribution.as_ref(),
        render_attribution.as_ref(),
    );

    lines.join("\n")
}

fn render_grouped_shell_text(
    mut rows: SqlGroupedRowsOutput,
    attribution: Option<ShellPerfAttribution>,
    render_attribution: Option<ShellLocalRenderAttribution>,
) -> String {
    uppercase_null_cells(rows.rows.as_mut_slice());

    let mut lines = render_grouped_lines(&rows);
    append_perf_suffix(
        lines.as_mut_slice(),
        attribution.as_ref(),
        render_attribution.as_ref(),
    );

    lines.join("\n")
}

// Keep successful command output visually isolated so the next prompt or shell
// continuation appears after one blank separator line.
fn finalize_successful_command_output(rendered: &str) -> String {
    let mut finalized = String::with_capacity(rendered.len().saturating_add(2));
    finalized.push_str(rendered);
    finalized.push('\n');
    finalized.push('\n');

    finalized
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

fn append_perf_suffix(
    lines: &mut [String],
    attribution: Option<&ShellPerfAttribution>,
    render_attribution: Option<&ShellLocalRenderAttribution>,
) {
    let Some(last) = lines.last_mut() else {
        return;
    };
    let perf_suffix = render_perf_suffix(attribution);
    let pure_covering_suffix = render_pure_covering_suffix(attribution);
    let executor_residual_suffix = render_executor_residual_suffix(attribution);
    let render_suffix = render_shell_render_suffix(render_attribution);
    if perf_suffix.is_none()
        && pure_covering_suffix.is_none()
        && executor_residual_suffix.is_none()
        && render_suffix.is_none()
    {
        return;
    }

    let mut suffixes = Vec::new();
    if let Some(perf_suffix) = perf_suffix {
        suffixes.push(perf_suffix);
    }
    if let Some(pure_covering_suffix) = pure_covering_suffix {
        suffixes.push(pure_covering_suffix);
    }
    if let Some(executor_residual_suffix) = executor_residual_suffix {
        suffixes.push(executor_residual_suffix);
    }
    if let Some(render_suffix) = render_suffix {
        suffixes.push(render_suffix);
    }

    *last = format!("{last} {}", suffixes.join(" "));
}

fn append_shell_render_suffix(
    rendered: String,
    render_attribution: Option<&ShellLocalRenderAttribution>,
) -> String {
    let Some(render_suffix) = render_shell_render_suffix(render_attribution) else {
        return rendered;
    };
    let mut lines = rendered.lines().map(str::to_string).collect::<Vec<_>>();
    let Some(last) = lines.last_mut() else {
        return rendered;
    };
    *last = format!("{last} {render_suffix}");

    lines.join("\n")
}

fn render_perf_suffix(attribution: Option<&ShellPerfAttribution>) -> Option<String> {
    let attribution = attribution?;
    if attribution.total == 0 {
        return None;
    }

    let total = format_instructions(attribution.total);
    let Some(bar) = render_perf_composition_bar(attribution) else {
        return Some(total);
    };

    Some(format!("{total} {bar}"))
}

fn render_shell_render_suffix(
    render_attribution: Option<&ShellLocalRenderAttribution>,
) -> Option<String> {
    let render_attribution = render_attribution?;
    if render_attribution.render_micros == 0 {
        return None;
    }

    Some(format!(
        "{{r={}}}",
        format_render_duration(render_attribution.render_micros)
    ))
}

fn render_pure_covering_suffix(attribution: Option<&ShellPerfAttribution>) -> Option<String> {
    let attribution = attribution?;
    if attribution.pure_covering_decode == 0 && attribution.pure_covering_row_assembly == 0 {
        return None;
    }

    Some(format!(
        "{{pc={}/{}}}",
        format_instructions(attribution.pure_covering_decode),
        format_instructions(attribution.pure_covering_row_assembly),
    ))
}

fn render_executor_residual_suffix(attribution: Option<&ShellPerfAttribution>) -> Option<String> {
    let attribution = attribution?;
    let residual = attribution.pure_covering_executor_residual();
    if residual == 0 {
        return None;
    }

    Some(format!("{{er={}}}", format_instructions(residual)))
}

fn format_render_duration(render_micros: u128) -> String {
    if render_micros >= 1_000 {
        let millis_tenths = (render_micros + 50) / 100;
        let whole = millis_tenths / 10;
        let fractional = millis_tenths % 10;

        return format!("{whole}.{fractional}ms");
    }

    format!("{render_micros}us")
}

// Render one compact fixed-order composition bar for compiler/planner/store/executor/decode shares.
fn render_perf_composition_bar(attribution: &ShellPerfAttribution) -> Option<String> {
    let phases = [
        ('c', attribution.compiler),
        ('p', attribution.planner),
        ('s', attribution.store),
        ('e', attribution.executor),
        ('d', attribution.decode),
        ('?', attribution.residual_total()),
    ];
    let phase_total = phases.iter().map(|(_, value)| *value).sum::<u64>();
    if phase_total == 0 {
        return None;
    }

    let width = perf_composition_bar_width(attribution.total);
    let mut allocated = phases
        .iter()
        .map(|(label, value)| PerfBarBucket::new(*label, *value, width, phase_total))
        .collect::<Vec<_>>();
    let assigned = allocated.iter().map(PerfBarBucket::count).sum::<usize>();
    let mut remaining = width.saturating_sub(assigned);

    // Phase 1: distribute the largest rounding remainders first so the bar
    // stays stable while still summing to the configured width exactly.
    allocated.sort_by(|left, right| {
        right
            .remainder
            .cmp(&left.remainder)
            .then_with(|| right.value.cmp(&left.value))
            .then_with(|| left.label.cmp(&right.label))
    });
    for bucket in &mut allocated {
        if remaining == 0 {
            break;
        }
        if bucket.value == 0 {
            continue;
        }

        bucket.count = bucket.count.saturating_add(1);
        remaining = remaining.saturating_sub(1);
    }

    // Phase 2: restore the canonical c/p/s/e/d order in the rendered shell surface.
    allocated.sort_by_key(|bucket| match bucket.label {
        'c' => 0,
        'p' => 1,
        's' => 2,
        'e' => 3,
        'd' => 4,
        '?' => 5,
        _ => 6,
    });

    let mut rendered = String::with_capacity(width.saturating_add(2));
    rendered.push('[');
    for bucket in allocated {
        for _ in 0..bucket.count {
            rendered.push(bucket.label);
        }
    }
    rendered.push(']');

    Some(rendered)
}

// Scale the composition bar by powers of ten so larger queries get a little
// more resolution without letting the footer sprawl indefinitely.
fn perf_composition_bar_width(total_instructions: u64) -> usize {
    let mut width = 10usize;
    let mut threshold = 1_000_000u64;
    while total_instructions >= threshold && width < 50 {
        width = width.saturating_add(5).min(50);
        threshold = threshold.saturating_mul(10);
    }

    width
}

///
/// PerfBarBucket
///
/// Rounded per-phase allocation bucket used while building one shell perf
/// composition bar.
/// This keeps width allocation explicit so the final `[cped...]` footer stays
/// deterministic even when integer rounding leaves leftover cells.
///

struct PerfBarBucket {
    label: char,
    value: u64,
    count: usize,
    remainder: u128,
}

impl PerfBarBucket {
    fn new(label: char, value: u64, width: usize, total: u64) -> Self {
        let scaled = u128::from(value).saturating_mul(width as u128);
        let total = u128::from(total);

        Self {
            label,
            value,
            count: usize::try_from(scaled / total).unwrap_or(usize::MAX),
            remainder: scaled % total,
        }
    }

    const fn count(&self) -> usize {
        self.count
    }
}

fn format_instructions(instructions: u64) -> String {
    if instructions >= 1_000_000 {
        return format_scaled_instructions(instructions, 1_000_000, "Mi");
    }

    if instructions >= 1_000 {
        return format_scaled_instructions(instructions, 1_000, "Ki");
    }

    format!("{instructions}i")
}

fn format_scaled_instructions(instructions: u64, scale: u64, suffix: &str) -> String {
    let scaled_tenths =
        ((u128::from(instructions) * 10) + (u128::from(scale) / 2)) / u128::from(scale);
    let whole = scaled_tenths / 10;
    let fractional = scaled_tenths % 10;

    format!("{whole}.{fractional}{suffix}")
}

fn parse_perf_result(value: &Value) -> Result<(SqlQueryResult, ShellPerfAttribution), String> {
    let result_value = value
        .get("result")
        .ok_or_else(|| "perf result missing result payload".to_string())?;
    let mut result_value = result_value.clone();

    normalize_grouped_next_cursor_json(&mut result_value);

    let result =
        serde_json::from_value::<SqlQueryResult>(result_value).map_err(|err| err.to_string())?;

    Ok((
        result,
        ShellPerfAttribution {
            total: parse_perf_u64(value, "instructions")?,
            planner: parse_perf_u64(value, "planner_instructions")?,
            store: parse_perf_u64_or_default(value, "store_instructions")?,
            executor: parse_perf_u64(value, "executor_instructions")?,
            pure_covering_decode: parse_perf_u64_or_default(
                value,
                "pure_covering_decode_instructions",
            )?,
            pure_covering_row_assembly: parse_perf_u64_or_default(
                value,
                "pure_covering_row_assembly_instructions",
            )?,
            decode: parse_perf_u64_or_default(value, "decode_instructions")?,
            compiler: parse_perf_u64(value, "compiler_instructions")?,
        },
    ))
}

fn normalize_grouped_next_cursor_json(value: &mut Value) {
    let Some(grouped) = value.get_mut("Grouped") else {
        return;
    };
    let Some(grouped_object) = grouped.as_object_mut() else {
        return;
    };
    let Some(next_cursor) = grouped_object.get_mut("next_cursor") else {
        return;
    };

    match next_cursor {
        Value::Array(values) if values.is_empty() => *next_cursor = Value::Null,
        Value::Array(values) if values.len() == 1 => {
            *next_cursor = values.pop().unwrap_or(Value::Null);
        }
        _ => {}
    }
}

fn parse_perf_u64(value: &Value, field: &str) -> Result<u64, String> {
    let field_value = value
        .get(field)
        .ok_or_else(|| format!("perf result missing {field}"))?;

    match field_value {
        Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| format!("perf field {field} is not a u64")),
        Value::String(text) => text
            .parse()
            .map_err(|_| format!("perf field {field} is not a u64")),
        _ => Err(format!("perf field {field} has unsupported type")),
    }
}

fn parse_perf_u64_or_default(value: &Value, field: &str) -> Result<u64, String> {
    if value.get(field).is_none() {
        return Ok(0);
    }

    parse_perf_u64(value, field)
}

fn find_result_payload(value: &Value) -> Option<&Value> {
    if matches!(value, Value::Object(map) if map.contains_key("Ok") || map.contains_key("Err")) {
        return Some(value);
    }

    if let Some(result) = value.get("result") {
        return Some(result);
    }

    match value {
        Value::Array(items) => items.iter().find_map(find_result_payload),
        Value::Object(map) => map.values().find_map(find_result_payload),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ShellPerfAttribution, finalize_successful_command_output, is_shell_help_command,
        normalize_grouped_next_cursor_json, normalize_shell_statement_line, parse_perf_result,
        render_perf_suffix, shell_help_text,
    };
    use icydb::db::sql::{SqlGroupedRowsOutput, SqlQueryRowsOutput};
    use serde_json::json;

    #[test]
    fn parse_perf_result_accepts_candid_option_none_for_grouped_next_cursor() {
        let value = json!({
            "result": {
                "Grouped": {
                    "entity": "Character",
                    "columns": ["class_name", "COUNT(*)"],
                    "rows": [["Bard", "5"]],
                    "row_count": 1,
                    "next_cursor": []
                }
            },
            "instructions": "1",
            "planner_instructions": "1",
            "store_instructions": "1",
            "executor_instructions": "1",
            "decode_instructions": "1",
            "compiler_instructions": "1"
        });

        let (result, _) = parse_perf_result(&value).expect("grouped perf result should decode");
        let grouped = match result {
            icydb::db::sql::SqlQueryResult::Grouped(grouped) => grouped,
            other => panic!("expected grouped result, got {other:?}"),
        };

        assert_eq!(grouped.next_cursor, None);
    }

    #[test]
    fn normalize_grouped_next_cursor_json_converts_candid_some_to_plain_string() {
        let mut value = json!({
            "Grouped": {
                "entity": "Character",
                "columns": ["class_name", "COUNT(*)"],
                "rows": [["Bard", "5"]],
                "row_count": 1,
                "next_cursor": ["cursor-token"]
            }
        });

        normalize_grouped_next_cursor_json(&mut value);

        assert_eq!(
            value["Grouped"]["next_cursor"],
            json!("cursor-token"),
            "grouped next_cursor should normalize from candid option encoding",
        );
    }

    #[test]
    fn render_perf_suffix_skips_zero_instruction_segments() {
        let suffix = render_perf_suffix(Some(&ShellPerfAttribution {
            total: 2_400,
            planner: 0,
            store: 0,
            executor: 1_900,
            pure_covering_decode: 0,
            pure_covering_row_assembly: 0,
            decode: 0,
            compiler: 500,
        }))
        .expect("non-zero perf attribution should render a footer");

        assert_eq!(suffix, "2.4Ki [cceeeeeeee]");
    }

    #[test]
    fn render_perf_suffix_omits_empty_attribution() {
        assert!(
            render_perf_suffix(Some(&ShellPerfAttribution {
                total: 0,
                planner: 0,
                store: 0,
                executor: 0,
                pure_covering_decode: 0,
                pure_covering_row_assembly: 0,
                decode: 0,
                compiler: 0,
            }))
            .is_none(),
            "all-zero perf attribution should not render a footer",
        );
    }

    #[test]
    fn render_perf_suffix_scales_bar_width_by_instruction_magnitude() {
        let suffix = render_perf_suffix(Some(&ShellPerfAttribution {
            total: 120_000_000,
            planner: 20_000_000,
            store: 20_000_000,
            executor: 40_000_000,
            pure_covering_decode: 0,
            pure_covering_row_assembly: 0,
            decode: 10_000_000,
            compiler: 10_000_000,
        }))
        .expect("large perf attribution should render a footer");

        assert_eq!(suffix, "120.0Mi [ccppppsssseeeeeeeeedd????]");
    }

    #[test]
    fn render_perf_suffix_omits_unknown_bucket_when_top_level_attribution_is_exhaustive() {
        let suffix = render_perf_suffix(Some(&ShellPerfAttribution {
            total: 10_000_000,
            planner: 2_000_000,
            store: 2_000_000,
            executor: 3_000_000,
            pure_covering_decode: 0,
            pure_covering_row_assembly: 0,
            decode: 2_000_000,
            compiler: 1_000_000,
        }))
        .expect("complete perf attribution should render a footer");

        assert_eq!(suffix, "10.0Mi [ccppppsssseeeeeedddd]");
    }

    #[test]
    fn render_perf_suffix_surfaces_unattributed_remainder_as_unknown_bucket() {
        let suffix = render_perf_suffix(Some(&ShellPerfAttribution {
            total: 10_000_000,
            planner: 1_000_000,
            store: 1_000_000,
            executor: 4_000_000,
            pure_covering_decode: 0,
            pure_covering_row_assembly: 0,
            decode: 1_000_000,
            compiler: 1_000_000,
        }))
        .expect("residual perf attribution should render a footer");

        assert_eq!(suffix, "10.0Mi [ccppsseeeeeeeedd????]");
    }

    #[test]
    fn successful_command_output_keeps_one_blank_separator_line() {
        assert_eq!(
            finalize_successful_command_output("surface=explain"),
            "surface=explain\n\n",
        );
    }

    #[test]
    fn help_command_matches_supported_spellings() {
        for input in ["?", "help", "\\?", "\\help", "help;", " ? "] {
            assert!(
                is_shell_help_command(input),
                "input should be treated as shell help: {input:?}",
            );
        }
    }

    #[test]
    fn normalize_shell_statement_line_trims_surrounding_whitespace() {
        assert_eq!(
            normalize_shell_statement_line("   SELECT * FROM character   "),
            "SELECT * FROM character",
        );
    }

    #[test]
    fn normalize_shell_statement_line_collapses_repeated_trailing_semicolons() {
        assert_eq!(normalize_shell_statement_line("  query();;   "), "query();",);
    }

    #[test]
    fn normalize_shell_statement_line_preserves_semicolon_only_terminator_lines() {
        assert_eq!(normalize_shell_statement_line("  ;; "), ";");
    }

    #[test]
    fn shell_help_text_mentions_current_perf_legend() {
        let help = shell_help_text();

        assert!(help.contains("? / help         show this help"));
        assert!(help.contains("\\q / quit / exit quit the interactive shell"));
        assert!(!help.contains("icydb-cli help"));
        assert!(help.contains("c = compile"));
        assert!(help.contains("p = planner"));
        assert!(help.contains("s = store"));
        assert!(help.contains("e = executor"));
        assert!(help.contains("d = decode"));
        assert!(help.contains("{pc=.../...}"));
        assert!(help.contains("{er=...}"));
        assert!(help.contains("{r=...}"));
    }

    #[test]
    fn projection_shell_text_leaves_footer_without_embedded_trailing_blank_line() {
        let rendered = super::render_projection_shell_text(
            SqlQueryRowsOutput {
                entity: "Character".to_string(),
                columns: vec!["name".to_string()],
                rows: vec![vec!["alice".to_string()]],
                row_count: 1,
            },
            None,
            None,
        );

        assert!(
            rendered.ends_with("1 row,"),
            "projection shell output should leave footer formatting to the command boundary: {rendered:?}",
        );
    }

    #[test]
    fn grouped_shell_text_leaves_footer_without_embedded_trailing_blank_line() {
        let rendered = super::render_grouped_shell_text(
            SqlGroupedRowsOutput {
                entity: "Character".to_string(),
                columns: vec!["class_name".to_string(), "COUNT(*)".to_string()],
                rows: vec![vec!["Bard".to_string(), "5".to_string()]],
                row_count: 1,
                next_cursor: None,
            },
            None,
            None,
        );

        assert!(
            rendered.ends_with("1 row,"),
            "grouped shell output should leave footer formatting to the command boundary: {rendered:?}",
        );
    }
}
