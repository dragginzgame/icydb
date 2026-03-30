use rand_chacha::{
    ChaCha20Rng,
    rand_core::{Rng, SeedableRng},
};
use std::cell::RefCell;
use thiserror::Error;

thread_local! {
    static RNG: RefCell<Option<ChaCha20Rng>> = const { RefCell::new(None) };
}

///
/// RngError
///
/// Errors raised when the shared deterministic RNG has not been seeded yet.
///

#[derive(Debug, Error)]
pub enum RngError {
    #[error("Randomness is not initialized. Please try again later")]
    NotInitialized,
}

/// Seed the shared RNG from one 32-byte seed.
pub fn seed_from(seed: [u8; 32]) {
    RNG.with_borrow_mut(|rng| {
        *rng = Some(ChaCha20Rng::from_seed(seed));
    });
}

/// Return whether the shared RNG is currently seeded.
#[must_use]
pub fn is_seeded() -> bool {
    RNG.with_borrow(Option::is_some)
}

fn with_rng<T>(f: impl FnOnce(&mut ChaCha20Rng) -> T) -> Result<T, RngError> {
    RNG.with_borrow_mut(|rng| match rng.as_mut() {
        Some(rand) => Ok(f(rand)),
        None => Err(RngError::NotInitialized),
    })
}

/// Fill one caller-provided buffer using the shared RNG.
pub fn fill_bytes(dest: &mut [u8]) -> Result<(), RngError> {
    with_rng(|rand| rand.fill_bytes(dest))
}

/// Return one owned buffer of random bytes from the shared RNG.
pub fn random_bytes(size: usize) -> Result<Vec<u8>, RngError> {
    let mut buf = vec![0u8; size];
    fill_bytes(&mut buf)?;
    Ok(buf)
}

/// Return one random `u8` from the shared RNG.
pub fn next_u8() -> Result<u8, RngError> {
    Ok((next_u16()? & 0xFF) as u8)
}

/// Return one random `u16` from the shared RNG.
#[expect(clippy::cast_possible_truncation)]
pub fn next_u16() -> Result<u16, RngError> {
    with_rng(|rand| rand.next_u32() as u16)
}

/// Return one random `u32` from the shared RNG.
pub fn next_u32() -> Result<u32, RngError> {
    with_rng(Rng::next_u32)
}

/// Return one random `u64` from the shared RNG.
pub fn next_u64() -> Result<u64, RngError> {
    with_rng(Rng::next_u64)
}

/// Return one random `u128` from the shared RNG.
pub fn next_u128() -> Result<u128, RngError> {
    with_rng(|rand| {
        let hi = u128::from(rand.next_u64());
        let lo = u128::from(rand.next_u64());
        (hi << 64) | lo
    })
}
