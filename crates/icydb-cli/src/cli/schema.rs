//! Module: CLI schema command arguments.
//! Responsibility: define live schema observability clap surfaces.
//! Does not own: schema endpoint execution, endpoint publication, or report rendering.
//! Boundary: exposes parsed schema command values to the observability owner.

use clap::{Args, Subcommand};

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

    /// Inspect or operate an explicit deployed source migration.
    #[command(subcommand)]
    Migration(SchemaMigrationCommand),
}

/// Deployed-only source migration operations.
#[derive(Debug, Subcommand)]
pub(crate) enum SchemaMigrationCommand {
    /// Read one bounded migration status page.
    Status(CanisterTarget),
    /// Advance one bounded migration step.
    Advance(CanisterTarget),
    /// Repeatedly advance while deployed identity and plan remain exact.
    Run(CanisterTarget),
    /// Abort a migration before irreversible rewriting begins.
    Abort(ConfirmedMigrationTarget),
    /// Adopt an exact existing version-1 generated schema.
    Adopt(ConfirmedMigrationTarget),
}

/// Destructive migration target requiring explicit confirmation.
#[derive(Args, Debug)]
pub(crate) struct ConfirmedMigrationTarget {
    #[command(flatten)]
    target: CanisterTarget,

    /// Confirm the controller operation explicitly.
    #[arg(long)]
    yes: bool,
}

impl ConfirmedMigrationTarget {
    pub(crate) const fn target(&self) -> &CanisterTarget {
        &self.target
    }

    pub(crate) const fn confirmed(&self) -> bool {
        self.yes
    }
}
