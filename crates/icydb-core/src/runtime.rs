//! Module: runtime
//! Responsibility: isolate IC runtime calls behind host-testable helpers.
//! Does not own: storage policy, endpoint generation, or public IC crate facades.
//! Boundary: internal core modules -> runtime -> IC/system clock APIs.

#[cfg(not(target_arch = "wasm32"))]
use std::time::SystemTime;

/// Read the current IC local instruction counter.
#[must_use]
#[cfg(target_arch = "wasm32")]
pub(crate) fn local_instruction_counter() -> u64 {
    ic_cdk::api::performance_counter(1)
}

/// Return zero when local instruction accounting runs outside the IC.
#[must_use]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) const fn local_instruction_counter() -> u64 {
    0
}

/// Return the current UNIX epoch time in milliseconds.
#[must_use]
#[cfg_attr(
    not(target_arch = "wasm32"),
    expect(
        clippy::cast_possible_truncation,
        reason = "millisecond epoch values fit into u64 for IcyDB timestamps"
    )
)]
pub(crate) fn now_millis() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        ic_cdk::api::time() / 1_000_000
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as u64,
            Err(_) => 0,
        }
    }
}
