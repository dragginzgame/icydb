//! Module: types::random
//! Owns the process-local seeded RNG used by core generated scalar values.

#[cfg(any(test, not(target_arch = "wasm32")))]
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::{ChaCha20Rng, rand_core::Rng};
use std::cell::RefCell;

thread_local! {
    static RNG: RefCell<Option<ChaCha20Rng>> = const { RefCell::new(None) };
}

#[derive(Debug)]
pub(in crate::types) enum RandomError {
    NotInitialized,
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

pub(in crate::types) fn next_u128() -> Result<u128, RandomError> {
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
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::types) use helper_tests::clear_for_tests;
#[cfg(test)]
pub(in crate::types) use helper_tests::seed_from;
