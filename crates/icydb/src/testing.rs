//! Typed startup observation for native canister tests (`testing` feature).
//!
//! Testkit owns PocketIC provisioning, lifecycle and bounded progression. This
//! module only maps IcyDB's existing startup state into that polling contract.

use std::{task::Poll, time::Duration};

use crate::db::DatabaseStartupState;

pub use ic_testkit::{pic::TickUntilError, pocket_ic::PocketIc};

/// Wait for IcyDB startup using a caller-supplied typed readiness observation.
///
/// `observe` must read the generated canister's `startup_state()` through the
/// application's own authorized observation endpoint. Transport and terminal
/// startup failures must remain errors; neither is retried. Do not pass an
/// application mutation here: readiness polling must not repeat its effects.
///
/// The caller owns the async runtime. The initial observation runs before progression,
/// even with zero rounds.
/// While recovering, Testkit advances simulated time by one second and ticks
/// once per round, allowing entropy replies and watchdog retries to progress.
/// Readiness on the last allowed round succeeds. Select `max_rounds` for the
/// fixture's recovery workload; elapsed time does not determine readiness.
///
/// This works after installation or upgrade and does not install, restart or
/// configure a server, register timers, or drive recovery inside the canister.
///
/// # Errors
/// Returns Testkit's typed observation/progression failure or exhausted round
/// budget. The observation's original error is preserved in `TickUntilError::Failed`.
pub async fn drive_startup<E>(
    pic: &PocketIc,
    max_rounds: u32,
    mut observe: impl AsyncFnMut(&PocketIc) -> Result<DatabaseStartupState, E>,
) -> Result<(), TickUntilError<E>> {
    ic_testkit::pic::tick_until(pic, max_rounds, Duration::from_secs(1), async |pic| {
        startup_poll(observe(pic).await)
    })
    .await
}

// Only Recovering permits another progression round. In particular, a failed
// observation must not be interpreted as pending merely because startup ran.
fn startup_poll<E>(observation: Result<DatabaseStartupState, E>) -> Poll<Result<(), E>> {
    match observation {
        Ok(DatabaseStartupState::Ready) => Poll::Ready(Ok(())),
        Ok(DatabaseStartupState::Recovering) => Poll::Pending,
        Err(error) => Poll::Ready(Err(error)),
    }
}

#[cfg(test)]
mod tests;
