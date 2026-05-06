use std::collections::VecDeque;

use rustyline::{DefaultEditor, error::ReadlineError};

///
/// ShellInput
///
/// ShellInput classifies one top-level interactive shell action before the CLI
/// decides whether to execute SQL, print local help text, or exit the shell.
///

pub(super) enum ShellInput {
    Sql(String),
    Help,
    Exit,
}

pub(crate) fn is_shell_help_command(input: &str) -> bool {
    matches!(
        input.trim().trim_end_matches(';').trim(),
        "?" | "help" | "\\?" | "\\help"
    )
}

pub(crate) const fn shell_help_text() -> &'static str {
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

pub(super) fn read_statement(
    editor: &mut DefaultEditor,
    pending_sql: &mut VecDeque<String>,
    partial_statement: &mut String,
) -> Result<ShellInput, String> {
    // Drain any previously pasted complete statements before blocking for more
    // terminal input so one bracketed paste can execute multiple SQL commands.
    if let Some(sql) = pending_sql.pop_front() {
        return Ok(ShellInput::Sql(sql));
    }

    let mut prompt = if partial_statement.trim().is_empty() {
        "icydb> "
    } else {
        "    -> "
    };

    loop {
        match editor.readline(prompt) {
            Ok(line) => {
                // Normalize recalled or freshly typed lines before they enter
                // the statement buffer so history recall does not reintroduce
                // trailing spaces or duplicate terminators.
                let normalized_line = normalize_shell_statement_line(line.as_str());

                // Ignore top-level blank input so pressing Enter on an empty
                // prompt simply reprompts instead of executing empty SQL.
                if partial_statement.trim().is_empty() && normalized_line.is_empty() {
                    prompt = "icydb> ";
                    continue;
                }

                if partial_statement.trim().is_empty()
                    && matches!(normalized_line.as_str(), "\\q" | "quit" | "exit")
                {
                    return Ok(ShellInput::Exit);
                }

                if partial_statement.trim().is_empty()
                    && is_shell_help_command(normalized_line.as_str())
                {
                    return Ok(ShellInput::Help);
                }

                if !partial_statement.is_empty() {
                    partial_statement.push('\n');
                }
                partial_statement.push_str(normalized_line.as_str());

                // Split one pasted batch into every complete top-level
                // semicolon-terminated statement while preserving any trailing
                // incomplete remainder for the continuation prompt.
                pending_sql.extend(drain_complete_shell_statements(partial_statement));

                if let Some(sql) = pending_sql.pop_front() {
                    return Ok(ShellInput::Sql(sql));
                }

                prompt = "    -> ";
            }
            Err(ReadlineError::Interrupted) => {
                partial_statement.clear();
                pending_sql.clear();
                prompt = "icydb> ";
            }
            Err(ReadlineError::Eof) => {
                if partial_statement.trim().is_empty() {
                    println!();
                    return Ok(ShellInput::Exit);
                }

                let sql = partial_statement.trim().to_string();
                partial_statement.clear();

                return Ok(ShellInput::Sql(sql));
            }
            Err(err) => return Err(err.to_string()),
        }
    }
}

// Split every complete top-level SQL statement from one shell buffer while
// preserving quoted semicolons and any trailing incomplete remainder.
pub(crate) fn drain_complete_shell_statements(statement: &mut String) -> VecDeque<String> {
    let mut complete = VecDeque::<String>::new();
    let mut start = 0usize;
    let mut in_single_quote = false;
    let chars = statement.char_indices().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let (offset, ch) = chars[index];
        if ch == '\'' {
            let next_is_quote = chars.get(index + 1).is_some_and(|(_, next)| *next == '\'');
            if in_single_quote && next_is_quote {
                index += 2;
                continue;
            }

            in_single_quote = !in_single_quote;
            index += 1;
            continue;
        }

        if ch == ';' && !in_single_quote {
            let end = offset + ch.len_utf8();
            let candidate = statement[start..end].trim();
            if !candidate.is_empty() {
                complete.push_back(candidate.to_string());
            }
            start = end;
        }

        index += 1;
    }

    let remainder = statement[start..].trim().to_string();
    statement.clear();
    statement.push_str(remainder.as_str());

    complete
}

// Trim shell-facing line noise while preserving the SQL text itself, so
// history recall does not force users to remove stray whitespace or `;;`.
pub(crate) fn normalize_shell_statement_line(line: &str) -> String {
    let trimmed = line.trim();
    let without_extra_semicolons = trimmed.trim_end_matches(';');

    if without_extra_semicolons.len() == trimmed.len() {
        return trimmed.to_string();
    }

    format!("{without_extra_semicolons};")
}
