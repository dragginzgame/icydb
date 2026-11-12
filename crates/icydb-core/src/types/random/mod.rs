//! Module: types::random
//! Owns the process-local seeded RNG used by core generated scalar values.

use ic_entropy::Generator;
use std::cell::RefCell;

thread_local! {
    static RNG: RefCell<Option<Generator>> = const { RefCell::new(None) };
}

#[derive(Debug)]
pub(in crate::types) enum RandomError {
    NotInitialized,
}

fn seed_from_system() -> Result<Generator, RandomError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).map_err(|_| RandomError::NotInitialized)?;
        // Preserve the existing long-lived native stream; no background reseed policy.
        Ok(Generator::from_seed(seed, u64::MAX))
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
        rand.next_u128().map_err(|_| RandomError::NotInitialized)
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
