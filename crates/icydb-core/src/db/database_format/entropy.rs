//! Module: db::database_format::entropy
//! Responsibility: obtain boot entropy and derive database cursor keys.
//! Does not own: startup scheduling or durable control publication.
//! Boundary: secure boot seed -> format-admission cursor authentication.

use crate::{db::DatabaseIncarnationId, error::InternalError};
use sha2::{Digest, Sha256};
use std::cell::RefCell;

thread_local! {
    static BOOT_ENTROPY: RefCell<BootEntropy> = const { RefCell::new(BootEntropy::Empty) };
}

enum BootEntropy {
    Empty,
    Pending,
    Ready([u8; 32]),
}

impl BootEntropy {
    const fn begin(&mut self) -> bool {
        if matches!(self, Self::Empty) {
            *self = Self::Pending;
            true
        } else {
            false
        }
    }

    fn finish(&mut self, seed: Option<[u8; 32]>) {
        if matches!(self, Self::Pending) {
            *self = match seed {
                Some(seed) if seed != [0; 32] => Self::Ready(seed),
                _ => Self::Empty,
            };
        }
    }

    const fn seed(&self) -> Option<[u8; 32]> {
        match self {
            Self::Ready(seed) => Some(*seed),
            Self::Empty | Self::Pending => None,
        }
    }
}

// Cancellation must release the single-flight claim as well as ordinary errors.
struct EntropyRequest;

impl Drop for EntropyRequest {
    fn drop(&mut self) {
        BOOT_ENTROPY.with_borrow_mut(|state| state.finish(None));
    }
}

/// Obtain a secure seed, leaving asynchronous failures to the startup watchdog.
pub(super) fn require_boot_entropy() -> Result<[u8; 32], InternalError> {
    #[cfg(target_arch = "wasm32")]
    if !ic_cdk::api::in_replicated_execution() {
        return Err(InternalError::recovery_pending());
    }

    if BOOT_ENTROPY.with_borrow_mut(BootEntropy::begin) {
        request_entropy();
    }
    BOOT_ENTROPY
        .with_borrow(BootEntropy::seed)
        .ok_or_else(InternalError::recovery_pending)
}

#[cfg(not(target_arch = "wasm32"))]
fn request_entropy() {
    let _request = EntropyRequest;
    let mut seed = [0; 32];
    let result = getrandom::fill(&mut seed).ok().map(|()| seed);
    BOOT_ENTROPY.with_borrow_mut(|state| state.finish(result));
}

#[cfg(target_arch = "wasm32")]
fn request_entropy() {
    let request = EntropyRequest;
    ic_cdk::futures::spawn_migratory(async move {
        let _request = request;
        let seed = match ic_cdk::call::Call::bounded_wait(
            candid::Principal::management_canister(),
            "raw_rand",
        )
        .await
        {
            Ok(response) => response.candid::<[u8; 32]>().ok(),
            Err(_) => None,
        };
        BOOT_ENTROPY.with_borrow_mut(|state| state.finish(seed));
    });
}

/// Bind independent database keys to one unpredictable boot seed.
/// Neither the incarnation nor its generation source is used as secret entropy.
pub(super) fn cursor_key(seed: [u8; 32], incarnation: DatabaseIncarnationId) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"icydb.cursor-authentication.boot.v1");
    hash.update(seed);
    hash.update(incarnation.to_bytes());
    hash.finalize().into()
}

#[cfg(test)]
/// Simulate a completed entropy request or a still-pending request.
pub(in crate::db) fn set_boot_entropy_for_tests(seed: Option<[u8; 32]>) {
    BOOT_ENTROPY.with_borrow_mut(|state| {
        *state = seed.map_or(BootEntropy::Pending, BootEntropy::Ready);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boot_entropy_retries_failures_and_deduplicates_pending_requests() {
        let mut state = BootEntropy::Empty;
        assert!(state.begin());
        assert!(!state.begin());
        assert!(state.seed().is_none());
        state.finish(None);
        assert!(state.begin());
        state.finish(Some([0; 32]));
        assert!(state.begin());
        state.finish(Some([7; 32]));
        assert_eq!(state.seed(), Some([7; 32]));
        assert!(!state.begin());
        state.finish(None);
        assert_eq!(state.seed(), Some([7; 32]));
    }

    #[test]
    fn cancelled_entropy_request_releases_pending_claim() {
        set_boot_entropy_for_tests(None);
        drop(EntropyRequest);
        assert!(BOOT_ENTROPY.with_borrow_mut(BootEntropy::begin));
        BOOT_ENTROPY.with_borrow_mut(|state| state.finish(Some([9; 32])));
    }

    #[test]
    fn cursor_keys_bind_boot_entropy_and_database_identity() {
        let incarnation = DatabaseIncarnationId::for_tests(1);
        let key = cursor_key([1; 32], incarnation);
        assert_ne!(key, [0; 32]);
        assert_eq!(key, cursor_key([1; 32], incarnation));
        assert_ne!(key, cursor_key([2; 32], incarnation));
        assert_ne!(
            key,
            cursor_key([1; 32], DatabaseIncarnationId::for_tests(2))
        );
    }
}
