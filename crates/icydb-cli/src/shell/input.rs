//! Module: shell input parsing.
//! Responsibility: classify interactive shell input and split complete SQL statements.
//! Does not own: SQL execution, endpoint routing, or rendered output.
//! Boundary: exposes parsed shell actions to the shell runner and test-only helpers via the parent module.

use std::collections::VecDeque;

use rustyline::{DefaultEditor, error::ReadlineError};

const SHELL_PROMPT: &str = "icydb> ";
const SHELL_CONTINUATION_PROMPT: &str = "    -> ";

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

enum ShellTopLevelInput {
    Blank,
    Help,
    Exit,
    Sql,
}

pub(super) fn is_shell_help_command(input: &str) -> bool {
    let normalized = input
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_lowercase();

    matches!(normalized.as_str(), "?" | "help" | "\\?" | "\\help")
}

pub(super) fn is_shell_exit_command(input: &str) -> bool {
    let normalized = input.to_ascii_lowercase();

    matches!(normalized.as_str(), "\\q" | "quit" | "exit")
}

pub(super) const fn shell_help_text() -> &'static str {
    "meta commands:
  ? / help         show this help
  \\q / quit / exit quit the interactive shell

SQL strings:
  Escape a quote by doubling it (''); backslashes are literal.
  Multiline strings retain their whitespace and semicolons.

examples:
  SELECT name FROM character;
  EXPLAIN EXECUTION SELECT name FROM character;
  CREATE INDEX character_level_idx ON character (level);
  SHOW INDEXES FROM character;
  DESCRIBE character;
  DESCRIBE character VERBOSE;
  SHOW RELATIONS FROM character;
  DROP INDEX character_level_idx ON character;"
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

    let mut prompt = shell_prompt(partial_statement);

    loop {
        match editor.readline(prompt) {
            Ok(line) => {
                if partial_statement_is_empty(partial_statement) {
                    match top_level_shell_input(line.as_str()) {
                        // Ignore top-level blank input so pressing Enter on an
                        // empty prompt simply reprompts instead of executing empty SQL.
                        ShellTopLevelInput::Blank => {
                            prompt = SHELL_PROMPT;
                            continue;
                        }
                        ShellTopLevelInput::Exit => return Ok(ShellInput::Exit),
                        ShellTopLevelInput::Help => return Ok(ShellInput::Help),
                        ShellTopLevelInput::Sql => {}
                    }
                }

                // Split one pasted batch into every complete top-level
                // semicolon-terminated statement while preserving any trailing
                // incomplete remainder for the continuation prompt.
                // Line-edge bytes may belong to an open literal. Classify meta
                // commands separately; never normalize text sent to SQL.
                append_shell_statement_line(partial_statement, line.as_str());
                pending_sql.extend(drain_complete_shell_statements(partial_statement));

                if let Some(sql) = pending_sql.pop_front() {
                    return Ok(ShellInput::Sql(sql));
                }

                prompt = shell_prompt(partial_statement);
            }
            Err(ReadlineError::Interrupted) => {
                clear_shell_input_state(pending_sql, partial_statement);
                prompt = shell_prompt(partial_statement);
            }
            Err(ReadlineError::Eof) => return Ok(shell_input_at_eof(partial_statement)),
            Err(err) => return Err(err.to_string()),
        }
    }
}

fn partial_statement_is_empty(partial_statement: &str) -> bool {
    partial_statement.trim().is_empty()
}

fn shell_prompt(partial_statement: &str) -> &'static str {
    if partial_statement_is_empty(partial_statement) {
        SHELL_PROMPT
    } else {
        SHELL_CONTINUATION_PROMPT
    }
}

fn append_shell_statement_line(partial_statement: &mut String, line: &str) {
    if !partial_statement.is_empty() {
        partial_statement.push('\n');
    }
    partial_statement.push_str(line);
}

fn clear_shell_input_state(pending_sql: &mut VecDeque<String>, partial_statement: &mut String) {
    partial_statement.clear();
    pending_sql.clear();
}

fn shell_input_at_eof(partial_statement: &mut String) -> ShellInput {
    if partial_statement_is_empty(partial_statement) {
        println!();
        return ShellInput::Exit;
    }

    ShellInput::Sql(std::mem::take(partial_statement))
}

fn top_level_shell_input(line: &str) -> ShellTopLevelInput {
    let line = line.trim();
    if line.is_empty() {
        return ShellTopLevelInput::Blank;
    }
    if is_shell_exit_command(line) {
        return ShellTopLevelInput::Exit;
    }
    if is_shell_help_command(line) {
        return ShellTopLevelInput::Help;
    }

    ShellTopLevelInput::Sql
}

// Split every complete top-level SQL statement from one shell buffer while
// preserving quoted semicolons and any trailing incomplete remainder.
pub(super) fn drain_complete_shell_statements(statement: &mut String) -> VecDeque<String> {
    let mut complete = VecDeque::<String>::new();
    let mut start = 0usize;
    let mut in_single_quote = false;
    let chars = statement.char_indices().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let (offset, ch) = chars[index];
        // Match the SQL lexer: doubled quotes escape a quote; a backslash
        // has no effect on string or statement boundaries.
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
            if candidate != ";" {
                complete.push_back(candidate.to_string());
            }
            start = end;
        }

        index += 1;
    }

    // The unfinished suffix may end inside a literal, including on spaces or
    // a blank line. Remove only completed statements, then discard a suffix
    // only when it contains no SQL text at all.
    statement.drain(..start);
    if partial_statement_is_empty(statement) {
        statement.clear();
    }

    complete
}

#[cfg(test)]
mod tests {
    use super::{
        SHELL_CONTINUATION_PROMPT, SHELL_PROMPT, ShellInput, ShellTopLevelInput,
        append_shell_statement_line, clear_shell_input_state, drain_complete_shell_statements,
        shell_input_at_eof, shell_prompt, top_level_shell_input,
    };
    use crate::shell::route::{SqlShellCallKind, sql_shell_call_kind};
    use std::slice;

    #[test]
    fn statement_lines_preserve_multiline_literal_contents() {
        let lines = [
            "UPDATE character SET bio = 'Line one   ",
            "    indented line two;;",
            "",
            "  café; it''s SQL  ",
            "last line' WHERE id = 7;",
        ];
        let mut partial = String::new();
        for (index, line) in lines.iter().enumerate() {
            append_shell_statement_line(&mut partial, line);
            let complete = drain_complete_shell_statements(&mut partial);
            if index + 1 == lines.len() {
                let expected = lines.join("\n");
                assert_eq!(
                    complete.into_iter().collect::<Vec<_>>().as_slice(),
                    slice::from_ref(&expected),
                );
                assert_eq!(sql_shell_call_kind(&expected), Ok(SqlShellCallKind::Update));
                assert!(partial.is_empty());
            } else {
                assert!(complete.is_empty());
                assert_eq!(partial, lines[..=index].join("\n"));
            }
        }
    }

    #[test]
    fn pasted_statements_preserve_an_unfinished_literal() {
        let first = "SELECT * FROM character;";
        let remainder = "\nUPDATE character SET bio = 'unfinished   ";
        let mut partial = format!("{first}{remainder}");
        assert_eq!(
            drain_complete_shell_statements(&mut partial)
                .into_iter()
                .collect::<Vec<_>>(),
            [first],
        );
        assert_eq!(partial, remainder);
        assert_eq!(shell_prompt(&partial), SHELL_CONTINUATION_PROMPT);

        let next = "    next line;;' WHERE id = 7;";
        append_shell_statement_line(&mut partial, next);
        let expected = format!("UPDATE character SET bio = 'unfinished   \n{next}");
        assert_eq!(
            drain_complete_shell_statements(&mut partial)
                .into_iter()
                .collect::<Vec<_>>()
                .as_slice(),
            slice::from_ref(&expected),
        );
        assert_eq!(sql_shell_call_kind(&expected), Ok(SqlShellCallKind::Update));
        assert_eq!(shell_prompt(&partial), SHELL_PROMPT);
    }

    #[test]
    fn shell_meta_commands_ignore_outer_whitespace() {
        for line in ["", "  ", "\t"] {
            assert!(matches!(
                top_level_shell_input(line),
                ShellTopLevelInput::Blank
            ));
        }
        for line in ["  EXIT  ", "\t\\q\t", " Quit "] {
            assert!(matches!(
                top_level_shell_input(line),
                ShellTopLevelInput::Exit
            ));
        }
        for line in ["  help;;;  ", "  ?  ", "\\help\t"] {
            assert!(matches!(
                top_level_shell_input(line),
                ShellTopLevelInput::Help
            ));
        }
        assert!(matches!(
            top_level_shell_input("SELECT * FROM character;"),
            ShellTopLevelInput::Sql
        ));
    }

    #[test]
    fn eof_preserves_sql_text_for_the_parser() {
        for sql in [
            "  UPDATE character SET bio = 'last  ' WHERE id = 7  ",
            "UPDATE character SET bio = 'unfinished   \n  ",
        ] {
            let mut partial = sql.to_string();
            let ShellInput::Sql(actual) = shell_input_at_eof(&mut partial) else {
                panic!("EOF should deliver the pending SQL");
            };
            assert_eq!(actual, sql);
            assert!(partial.is_empty());
        }
        assert!(matches!(
            shell_input_at_eof(&mut String::from(" \t")),
            ShellInput::Exit
        ));
    }

    #[test]
    fn interruption_clears_pasted_and_unfinished_input() {
        let mut pending = ["SELECT * FROM character;".into()].into();
        let mut partial = String::from("UPDATE character SET bio = 'unfinished");
        clear_shell_input_state(&mut pending, &mut partial);
        assert!(pending.is_empty());
        assert!(partial.is_empty());
        assert_eq!(shell_prompt(&partial), SHELL_PROMPT);
        append_shell_statement_line(&mut partial, "SELECT * FROM character;");
        assert_eq!(drain_complete_shell_statements(&mut partial).len(), 1);
    }
}
