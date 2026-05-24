//! Module: CLI binary entrypoint.
//! Responsibility: wire parsed process args into the command dispatcher.
//! Does not own: argument definitions, command execution, or output rendering.
//! Boundary: starts the CLI process and maps command errors to exit status.

mod cli;
mod commands;
mod config;
mod icp;
mod observability;
mod shell;
mod table;

use clap::Parser;
use std::process::ExitCode;

use crate::{cli::CliArgs, commands::run_cli};

fn main() -> ExitCode {
    match run_cli(CliArgs::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ERROR: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests;
