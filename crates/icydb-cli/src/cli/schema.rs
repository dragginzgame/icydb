//! Module: CLI schema command arguments.
//! Responsibility: define live schema observability clap surfaces.
//! Does not own: schema endpoint execution, endpoint publication, or report rendering.
//! Boundary: exposes parsed schema command values to the observability owner.

use clap::Subcommand;

use crate::cli::CanisterTarget;

///
/// SchemaCommand
///
/// SchemaCommand owns accepted live-schema observability.
///

#[derive(Debug, Subcommand)]
pub(crate) enum SchemaCommand {
    /// Read accepted schema metadata from an IcyDB canister.
    Show(CanisterTarget),
}
