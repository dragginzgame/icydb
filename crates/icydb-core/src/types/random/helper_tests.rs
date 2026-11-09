//! Test-only helpers for the parent module.

use super::*;

pub(in crate::types) fn seed_from(seed: [u8; 32]) {
    RNG.with_borrow_mut(|rng| {
        *rng = Some(Generator::from_seed(seed, u64::MAX));
    });
}

pub(in crate::types) fn clear_for_tests() {
    RNG.with_borrow_mut(|rng| *rng = None);
}
