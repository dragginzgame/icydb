//! Module: CLI schema command arguments.
//! Responsibility: define live schema observability clap surfaces.
//! Does not own: schema endpoint execution, config gating, or report rendering.
//! Boundary: exposes parsed schema command values to the observability owner.

use clap::Subcommand;

use super::CanisterTarget;

///
/// SchemaCommand
///
/// SchemaCommand owns live schema observability. `show` reads the accepted
/// schema report; `check` compares the generated proposal compiled into the
/// deployed canister with the accepted runtime catalog.
///

#[derive(Debug, Subcommand)]
pub(crate) enum SchemaCommand {
    /// Read accepted schema metadata from an IcyDB canister.
    Show(CanisterTarget),
    /// Compare generated schema metadata with accepted live schema metadata.
    Check(CanisterTarget),
}
