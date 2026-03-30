//! Module: db::executor::pipeline::timing
//! Responsibility: execution-time measurement helpers used by load pipeline entrypoints.
//! Does not own: metrics sink ownership or performance counter semantics.
//! Boundary: provides wasm-safe elapsed time measurement for execution observability.

/// Capture the pipeline execution start marker.
#[cfg(target_arch = "wasm32")]
#[must_use]
pub(in crate::db::executor) fn start_execution_timer() -> u64 {
    canic::cdk::utils::time::now_millis()
}

/// Convert elapsed pipeline duration to microseconds.
#[cfg(target_arch = "wasm32")]
#[must_use]
pub(in crate::db::executor) fn elapsed_execution_micros(execution_started_at_ms: u64) -> u64 {
    canic::cdk::utils::time::now_millis()
        .saturating_sub(execution_started_at_ms)
        .saturating_mul(1_000)
}

/// Capture the pipeline execution start marker.
#[cfg(not(target_arch = "wasm32"))]
#[must_use]
pub(in crate::db::executor) fn start_execution_timer() -> std::time::Instant {
    std::time::Instant::now()
}

/// Convert elapsed pipeline duration to microseconds.
#[cfg(not(target_arch = "wasm32"))]
#[must_use]
pub(in crate::db::executor) fn elapsed_execution_micros(
    execution_started_at: std::time::Instant,
) -> u64 {
    u64::try_from(execution_started_at.elapsed().as_micros()).unwrap_or(u64::MAX)
}
