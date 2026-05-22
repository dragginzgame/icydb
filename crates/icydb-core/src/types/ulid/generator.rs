//! Module: types::ulid::generator
//! Owns the process-local monotonic ULID generator used by runtime key
//! generation.

use crate::types::{Ulid, UlidError, random};
use canic_cdk::utils::time::now_millis;
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
}

impl Default for Generator {
    fn default() -> Self {
        Self {
            previous: Ulid::nil(),
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

        // generate
        let rand = random::next_u128().map_err(|_| UlidError::RandomnessUnavailable)?;
        let ulid = Ulid::from_parts(ts, rand);

        self.previous = ulid;

        Ok(ulid)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_monotonic_generation() {
        random::seed_from([0x22; 32]);

        let mut g = Generator::default();
        let a = g.generate().unwrap();
        let b = g.generate().unwrap();

        assert!(a < b);
    }

    #[test]
    fn generation_fails_when_randomness_is_uninitialized() {
        random::clear_for_tests();
        let mut generator = Generator::default();

        assert!(matches!(
            generator.generate(),
            Err(UlidError::RandomnessUnavailable),
        ));
    }
}
