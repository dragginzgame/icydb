//! Module: CLI schema command arguments.
//! Responsibility: define live schema observability clap surfaces.
//! Does not own: schema endpoint execution, endpoint publication, or report rendering.
//! Boundary: exposes parsed schema command values to the observability owner.

use std::path::{Path, PathBuf};

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

    /// Export bounded accepted-schema identity for offline diagnostics.
    DiagnosticArtifact(DiagnosticArtifactArgs),

    /// Bind an accepted diagnostic artifact to generated/source metadata.
    DiagnosticSourceMetadata(DiagnosticSourceMetadataArgs),

    /// Inspect or operate an explicit deployed source migration.
    #[command(subcommand)]
    Migration(SchemaMigrationCommand),
}

/// Target and new output path for one diagnostic schema artifact.
#[derive(Args, Debug)]
pub(crate) struct DiagnosticArtifactArgs {
    #[command(flatten)]
    target: CanisterTarget,

    /// New artifact path; an existing file is never overwritten.
    #[arg(short, long, value_name = "PATH")]
    output: PathBuf,
}

impl DiagnosticArtifactArgs {
    pub(crate) const fn target(&self) -> &CanisterTarget {
        &self.target
    }

    pub(crate) fn output(&self) -> &Path {
        self.output.as_path()
    }
}

/// Exact accepted artifact and source label for one host-only metadata binding.
#[derive(Args, Debug)]
pub(crate) struct DiagnosticSourceMetadataArgs {
    /// Existing exact accepted-schema diagnostic artifact.
    #[arg(long, value_name = "PATH")]
    artifact: PathBuf,

    /// Generated package or source path that owns this metadata.
    #[arg(long, value_name = "SOURCE")]
    source: String,

    /// New metadata path; an existing file is never overwritten.
    #[arg(short, long, value_name = "PATH")]
    output: PathBuf,
}

impl DiagnosticSourceMetadataArgs {
    pub(crate) fn artifact(&self) -> &Path {
        self.artifact.as_path()
    }

    pub(crate) const fn source(&self) -> &str {
        self.source.as_str()
    }

    pub(crate) fn output(&self) -> &Path {
        self.output.as_path()
    }
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
