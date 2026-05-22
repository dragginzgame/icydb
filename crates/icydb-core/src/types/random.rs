//! Module: types::random
//! Owns the process-local seeded RNG used by core generated scalar values.

#[cfg(any(test, not(target_arch = "wasm32")))]
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

#[cfg(test)]
pub(crate) fn clear_for_tests() {
    RNG.with_borrow_mut(|rng| *rng = None);
}

#[cfg(test)]
pub(crate) fn seed_if_uninitialized_for_tests(seed: [u8; 32]) {
    RNG.with_borrow_mut(|rng| {
        if rng.is_none() {
            *rng = Some(ChaCha20Rng::from_seed(seed));
        }
    });
}

fn seed_from_system() -> Result<ChaCha20Rng, RandomError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).map_err(|_| RandomError::NotInitialized)?;
        Ok(ChaCha20Rng::from_seed(seed))
    }

    #[cfg(target_arch = "wasm32")]
    {
        Err(RandomError::NotInitialized)
    }
}

pub(crate) fn next_u128() -> Result<u128, RandomError> {
    RNG.with_borrow_mut(|rng| {
        if rng.is_none() {
            *rng = Some(seed_from_system()?);
        }

        let rand = rng.as_mut().ok_or(RandomError::NotInitialized)?;
        let hi = u128::from(rand.next_u64());
        let lo = u128::from(rand.next_u64());
        Ok((hi << 64) | lo)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEED: [u8; 32] = [17; 32];

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn next_u128_seeds_from_system_when_unseeded() {
        clear_for_tests();

        assert!(next_u128().is_ok());
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn next_u128_fails_when_rng_is_unseeded_without_native_entropy() {
        clear_for_tests();

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
