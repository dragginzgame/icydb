mod cli;
mod commands;
mod config;
mod icp;
mod shell;

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
