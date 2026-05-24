//! Module: runtime
//! Responsibility: isolate IC runtime calls behind host-testable helpers.
//! Does not own: storage policy, endpoint generation, or public IC crate facades.
//! Boundary: internal core modules -> runtime -> IC/system clock APIs.

use std::time::SystemTime;

/// Read the current IC local performance counter when diagnostics run on wasm.
#[must_use]
#[cfg(target_arch = "wasm32")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "wasm diagnostics uses the non-const IC performance counter"
)]
pub(crate) fn performance_counter(counter_type: u32) -> u64 {
    ic_cdk::api::performance_counter(counter_type)
}

/// Return the current UNIX epoch time in milliseconds.
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    reason = "millisecond epoch values fit into u64 for IcyDB timestamps"
)]
pub(crate) fn now_millis() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        return (ic_cdk::api::time() / 1_000_000) as u64;
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as u64,
            Err(_) => 0,
        }
    }
}
