//! Module: interactive SQL shell.
//! Responsibility: run the line-editor loop for interactive SQL input.
//! Does not own: SQL execution semantics, call routing, or shell text rendering rules.
//! Boundary: coordinates shell input, history persistence, and command output.

use std::collections::VecDeque;

use rustyline::DefaultEditor;

use crate::shell::{
    ShellConfig, execute_sql,
    input::{self, ShellInput, read_statement},
    render,
};

pub(super) fn run_interactive_shell(config: &ShellConfig) -> Result<(), String> {
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
