//! Module: CLI binary entrypoint.
//! Responsibility: wire parsed process args into the command dispatcher.
//! Does not own: argument definitions, command execution, or output rendering.
//! Boundary: starts the CLI process and maps command errors to exit status.

mod cli;
mod commands;
mod diagnostic;
mod icp;
mod observability;
mod shell;
mod table;

use clap::{
    Parser,
    error::{ContextKind, ContextValue},
};
use std::process::ExitCode;

use crate::{cli::CliArgs, commands::run_cli};

fn main() -> ExitCode {
    let args = CliArgs::try_parse().unwrap_or_else(|mut error| {
        // Clap owns usage and diagnostics. Escape rejected external tokens,
        // including conflicts and unknown arguments, before its renderer sees them.
        let mut escaped_token = false;
        for kind in [
            ContextKind::InvalidValue,
            ContextKind::InvalidArg,
            ContextKind::InvalidSubcommand,
            ContextKind::PriorArg,
            ContextKind::TrailingArg,
        ] {
            let Some(context) = error.get(kind) else {
                continue;
            };
            let escaped = match context {
                ContextValue::String(value) => {
                    ContextValue::String(ic_host_text::escape::terminal_text(value).into_owned())
                }
                ContextValue::Strings(values) => ContextValue::Strings(
                    values
                        .iter()
                        .map(|value| ic_host_text::escape::terminal_text(value).into_owned())
                        .collect(),
                ),
                _ => continue,
            };
            if &escaped != context {
                error.insert(kind, escaped);
                escaped_token = true;
            }
        }
        if escaped_token {
            // Preformatted tips can contain another raw copy of the rejected
            // token mixed with trusted styling. Omit those tips only when input
            // needs escaping; retain normal tips and Clap's usage/help rendering.
            error.remove(ContextKind::Suggested);
        }
        error.exit()
    });
    match run_cli(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ERROR: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests;
