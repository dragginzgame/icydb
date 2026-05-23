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

#[cfg(test)]
mod tests;

use clap::Parser;

use crate::{cli::CliArgs, commands::run_cli};

fn main() {
    if let Err(err) = run_cli(CliArgs::parse()) {
        eprintln!("ERROR: {err}");
        std::process::exit(1);
    }
}
