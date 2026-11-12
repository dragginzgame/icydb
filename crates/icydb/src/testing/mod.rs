//! Typed startup observation for native canister tests (`testing` feature).
//!
//! This module maps typed IcyDB readiness observations without depending on a
//! simulator. Testkit owns PocketIC provisioning, lifecycle and bounded polling.

use std::task::Poll;

use crate::db::DatabaseStartupState;

/// Project an authorized typed startup observation into a readiness predicate.
///
/// Read the generated canister's `startup_state()` through the application's
/// own authorized read-only endpoint. Only [`DatabaseStartupState::Recovering`]
/// permits another polling round. Ready completes successfully; transport and
/// terminal startup errors stop immediately with their original typed value.
/// An ordinary operation's startup-pending error is still an error here.
///
/// Compose this predicate with Testkit's `pic::tick_until` using the existing
/// `PocketIcInstance` owner, a caller-selected round budget and one-second
/// simulated advances. Testkit owns initial polling, progression, last-round
/// completion and typed exhaustion; this function performs no polling or effects.
///
/// Never use a mutation as the observation: readiness polling must not repeat
/// application effects. The same composition works after installation or upgrade.
pub fn startup_poll<E>(observation: Result<DatabaseStartupState, E>) -> Poll<Result<(), E>> {
    match observation {
        Ok(DatabaseStartupState::Ready) => Poll::Ready(Ok(())),
        Ok(DatabaseStartupState::Recovering) => Poll::Pending,
        Err(error) => Poll::Ready(Err(error)),
    }
}

#[cfg(test)]
mod tests;
