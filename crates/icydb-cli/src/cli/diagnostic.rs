//! Module: CLI diagnostic command arguments.
//! Responsibility: define compact diagnostic-code lookup clap surfaces.
//! Does not own: diagnostic registry rendering or canister error transport.
//! Boundary: exposes parsed diagnostic-code input to the command dispatcher.

use clap::Args;

///
/// DiagnosticArgs
///
/// DiagnosticArgs owns host-side lookup of compact IcyDB error codes. The code
/// is intentionally a string so users can paste either `E7` or `7`.
///

#[derive(Args, Debug)]
pub(crate) struct DiagnosticArgs {
    /// Compact IcyDB error code, for example E7, 7, E190, or 190.
    #[arg(value_name = "CODE")]
    code: String,
}

impl DiagnosticArgs {
    pub(crate) fn code(&self) -> &str {
        &self.code
    }
}
