//! Module: types::random
//! Owns the process-local seeded RNG used by core generated scalar values.

#[cfg(test)]
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::{ChaCha20Rng, rand_core::Rng};
use std::cell::RefCell;
use thiserror::Error as ThisError;

thread_local! {
    static RNG: RefCell<Option<ChaCha20Rng>> = const { RefCell::new(None) };
}

#[derive(Debug, ThisError)]
pub(crate) enum RandomError {
    #[error("randomness is not initialized")]
    NotInitialized,
}

#[cfg(test)]
pub(crate) fn seed_from(seed: [u8; 32]) {
    RNG.with_borrow_mut(|rng| {
        *rng = Some(ChaCha20Rng::from_seed(seed));
    });
}

pub(crate) fn next_u128() -> Result<u128, RandomError> {
    RNG.with_borrow_mut(|rng| match rng.as_mut() {
        Some(rand) => {
            let hi = u128::from(rand.next_u64());
            let lo = u128::from(rand.next_u64());
            Ok((hi << 64) | lo)
        }
        None => Err(RandomError::NotInitialized),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEED: [u8; 32] = [17; 32];

    #[test]
    fn next_u128_fails_when_rng_is_unseeded() {
        RNG.with_borrow_mut(|rng| *rng = None);

        assert!(matches!(next_u128(), Err(RandomError::NotInitialized)));
    }

    #[test]
    fn seed_from_makes_next_u128_deterministic() {
        seed_from(SEED);
        let first = next_u128().expect("seeded rng should produce a value");
        let second = next_u128().expect("seeded rng should produce a second value");

        seed_from(SEED);
        assert_eq!(
            next_u128().expect("seeded rng should replay first value"),
            first
        );
        assert_eq!(
            next_u128().expect("seeded rng should replay second value"),
            second
        );
    }
}
