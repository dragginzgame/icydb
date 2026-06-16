//! Module: CLI metrics command arguments.
//! Responsibility: define generated metrics endpoint clap surfaces.
//! Does not own: metrics endpoint execution, config gating, or rendering.
//! Boundary: exposes parsed metrics command values to the observability owner.

use clap::Args;

use crate::cli::CanisterTarget;

///
/// MetricsArgs
///
/// MetricsArgs owns the generated metrics endpoint command surface. The reset
/// switch keeps normal read usage short while still making the destructive
/// operation explicit.
///

#[derive(Args, Debug)]
pub(crate) struct MetricsArgs {
    #[command(flatten)]
    target: CanisterTarget,

    /// Only include metrics windows starting at this millisecond timestamp.
    #[arg(long, conflicts_with = "reset", value_name = "MILLIS")]
    window_start_ms: Option<u64>,

    /// Read the opt-in extended metrics report instead of the compact report.
    #[arg(long, conflicts_with = "reset")]
    extended: bool,

    /// Reset in-memory metrics instead of reading the metrics report.
    #[arg(long)]
    reset: bool,
}

impl MetricsArgs {
    pub(crate) const fn target(&self) -> &CanisterTarget {
        &self.target
    }

    pub(crate) const fn window_start_ms(&self) -> Option<u64> {
        self.window_start_ms
    }

    pub(crate) const fn extended(&self) -> bool {
        self.extended
    }

    pub(crate) const fn reset(&self) -> bool {
        self.reset
    }
}
