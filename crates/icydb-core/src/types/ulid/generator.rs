//! Module: types::ulid::generator
//! Owns the process-local monotonic ULID generator used by runtime key
//! generation.

use crate::runtime::now_millis;
#[cfg(not(target_arch = "wasm32"))]
use crate::types::random;
use crate::types::{Ulid, UlidError};
use std::cell::RefCell;

thread_local! {
    static GENERATOR: RefCell<Generator> = RefCell::new(Generator::default());
}

/// Generate a ULID using the global monotonic generator.
pub(crate) fn generate() -> Result<Ulid, UlidError> {
    GENERATOR.with(|g| g.borrow_mut().generate())
}

///
/// Generator
///
/// hacked from <https://github.com/dylanhart/ulid-rs/blob/master/src/generator.rs>
/// as the ulid crate doesn't support a no-std generator
///

pub(crate) struct Generator {
    previous: Ulid,
    #[cfg(target_arch = "wasm32")]
    sequence: u64,
}

impl Default for Generator {
    fn default() -> Self {
        Self {
            previous: Ulid::nil(),
            #[cfg(target_arch = "wasm32")]
            sequence: 0,
        }
    }
}

impl Generator {
    // generate
    /// Monotonic ULID generation; increments within the same millisecond.
    pub(crate) fn generate(&mut self) -> Result<Ulid, UlidError> {
        let last_ts = self.previous.timestamp_ms();
        let ts = now_millis();

        // maybe time went backward, or it is the same ms.
        // increment instead of generating a new random so that it is monotonic
        if ts <= last_ts {
            if let Some(next) = self.previous.increment() {
                let ulid = next;
                self.previous = ulid;

                return Ok(self.previous);
            }

            return Err(UlidError::GeneratorOverflow);
        }

        #[cfg(not(target_arch = "wasm32"))]
        let component = Self::next_component(ts)?;
        #[cfg(target_arch = "wasm32")]
        let component = self.next_component(ts)?;
        let ulid = Ulid::from_parts(ts, component);

        self.previous = ulid;

        Ok(ulid)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn next_component(_ts: u64) -> Result<u128, UlidError> {
        random::next_u128().map_err(|_| UlidError::RandomnessUnavailable)
    }

    // IC canisters cannot synchronously request fresh entropy while building generated keys.
    // Use the ULID timestamp plus a process-local sequence for deterministic uniqueness.
    #[cfg(target_arch = "wasm32")]
    fn next_component(&mut self, ts: u64) -> Result<u128, UlidError> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(UlidError::GeneratorOverflow)?;

        Ok((u128::from(ts & 0xFFFF) << 64) | u128::from(self.sequence))
    }
}

///
/// TESTS
///

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_monotonic_generation() {
        random::seed_from([0x22; 32]);

        let mut g = Generator::default();
        let a = g.generate().unwrap();
        let b = g.generate().unwrap();

        assert!(a < b);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn generation_uses_native_entropy_when_randomness_is_uninitialized() {
        random::clear_for_tests();
        let mut generator = Generator::default();

        assert!(generator.generate().is_ok());
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn generation_uses_deterministic_component_without_native_entropy() {
        let mut generator = Generator::default();

        assert!(generator.generate().is_ok());
    }
}
