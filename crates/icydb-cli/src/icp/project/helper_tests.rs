//! Test-only helpers for the parent module.

use super::*;

pub(in crate::icp) fn parse_manifest_canisters(
    contents: &str,
    environment: &str,
) -> Result<Vec<String>, String> {
    manifest::parse_canisters(contents, environment)
}
