//! Test-only helpers for the parent module.

use super::*;

pub(in crate::icp) fn fixtures_load_command(
    environment: &str,
    canister: &str,
    argument_file: &std::path::Path,
) -> Command {
    fixtures::fixtures_load_command(environment, canister, argument_file)
}

pub(in crate::icp) fn top_up_command(environment: &str, canister: &str, amount: &str) -> Command {
    fixtures::top_up_command(environment, canister, amount)
}
