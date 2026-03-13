//!
//! Blank demo canister used in tests to exercise provisioning flows.
//! Lives in `crates/canisters` solely as a showcase for ops helpers.
//!
//! Test-only helper: this canister is intended for local/dev flows and is not
//! a public-facing deployment target.
//!

use ic_cdk::{export_candid, update};

//
// ENDPOINTS
//

/// main test endpoint for things that can fail
#[update]
fn test() -> Result<(), String> {
    if !cfg!(debug_assertions) {
        return Err("test-only canister".to_string());
    }

    Ok(())
}

export_candid!();
