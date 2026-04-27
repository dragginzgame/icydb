//! Module: diagnostics::local_instructions
//! Responsibility: shared local instruction counter helpers.
//! Does not own: phase attribution taxonomy or diagnostic DTO shaping.
//! Boundary: provides one counter read/measure primitive for callers that own
//! their own buckets and labels.

/// Read the current local instruction counter for diagnostic phase timing.
#[must_use]
#[expect(
    clippy::missing_const_for_fn,
    reason = "wasm diagnostics uses the non-const IC performance counter"
)]
pub(in crate::db) fn read_local_instruction_counter() -> u64 {
    #[cfg(all(feature = "diagnostics", target_arch = "wasm32"))]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(all(feature = "diagnostics", target_arch = "wasm32")))]
    {
        0
    }
}

/// Measure one caller-owned phase with the shared local instruction counter.
///
/// The helper deliberately keeps the result payload generic because each
/// caller owns its own error/result shape and attribution bucket.
pub(in crate::db) fn measure_local_instruction_delta<T>(run: impl FnOnce() -> T) -> (u64, T) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}
