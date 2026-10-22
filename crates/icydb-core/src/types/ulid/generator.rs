//! Module: types::ulid::generator
//! Owns the process-local monotonic ULID generator used by runtime key
//! generation.

#[cfg(not(target_arch = "wasm32"))]
use crate::types::random;
use crate::{error::InternalError, types::Ulid};
use std::cell::RefCell;

thread_local! {
    static GENERATOR: RefCell<Generator> = RefCell::new(Generator::default());
}

#[derive(Debug)]
enum UlidGenerationError {
    GeneratorOverflow,

    #[cfg(not(target_arch = "wasm32"))]
    RandomnessUnavailable,
}

/// Generate a ULID at the supplied time using the global monotonic generator.
pub(crate) fn generate(timestamp_ms: u64) -> Result<Ulid, InternalError> {
    GENERATOR
        .with(|g| g.borrow_mut().generate(timestamp_ms))
        .map_err(|_| InternalError::executor_internal())
}

///
/// Generator
///
/// hacked from <https://github.com/dylanhart/ulid-rs/blob/master/src/generator.rs>
/// as the ulid crate doesn't support a no-std generator
///

struct Generator {
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
    fn generate(&mut self, ts: u64) -> Result<Ulid, UlidGenerationError> {
        let previous = ulid::Ulid::from_bytes(self.previous.to_bytes());
        let last_ts = previous.timestamp_ms();

        // maybe time went backward, or it is the same ms.
        // increment instead of generating a new random so that it is monotonic
        if ts <= last_ts {
            if let Ok(next) = previous.increment() {
                let ulid = Ulid::from_bytes(next.to_bytes());
                self.previous = ulid;

                return Ok(self.previous);
            }

            return Err(UlidGenerationError::GeneratorOverflow);
        }

        #[cfg(not(target_arch = "wasm32"))]
        let component = Self::next_component(ts)?;
        #[cfg(target_arch = "wasm32")]
        let component = self.next_component(ts)?;
        let ulid = Ulid::from_bytes(ulid::Ulid::from_parts(ts, component).to_bytes());

        self.previous = ulid;

        Ok(ulid)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn next_component(_ts: u64) -> Result<u128, UlidGenerationError> {
        random::next_u128().map_err(|_| UlidGenerationError::RandomnessUnavailable)
    }

    // IC canisters cannot synchronously request fresh entropy while building generated keys.
    // Use the ULID timestamp plus a process-local sequence for deterministic uniqueness.
    #[cfg(target_arch = "wasm32")]
    fn next_component(&mut self, ts: u64) -> Result<u128, UlidGenerationError> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(UlidGenerationError::GeneratorOverflow)?;

        Ok((u128::from(ts & 0xFFFF) << 64) | u128::from(self.sequence))
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
        #[cfg(not(target_arch = "wasm32"))]
        random::seed_from([0x22; 32]);

        let mut generator = Generator::default();
        let first = generator.generate(1000).unwrap();
        let same_time = generator.generate(1000).unwrap();
        let backward = generator.generate(999).unwrap();
        let later = generator.generate(1001).unwrap();

        assert!(first < same_time && same_time < backward && backward < later);
        for value in [first, same_time, backward] {
            assert_eq!(
                ulid::Ulid::from_bytes(value.to_bytes()).timestamp_ms(),
                1000
            );
        }
        assert_eq!(
            ulid::Ulid::from_bytes(later.to_bytes()).timestamp_ms(),
            1001
        );
        assert_eq!(
            ulid::Ulid::from_bytes(first.to_bytes())
                .increment()
                .unwrap()
                .to_bytes(),
            same_time.to_bytes()
        );
    }

    #[test]
    fn generation_overflow_preserves_previous() {
        let mut generator = Generator {
            previous: Ulid::MAX,
            #[cfg(target_arch = "wasm32")]
            sequence: 0,
        };

        assert!(matches!(
            generator.generate(1000),
            Err(UlidGenerationError::GeneratorOverflow)
        ));
        assert_eq!(generator.previous, Ulid::MAX);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn generation_uses_entropy_only_for_newer_timestamps() {
        let seed = [0x23; 32];
        random::seed_from(seed);
        let first_component = random::next_u128().unwrap();
        let next_component = random::next_u128().unwrap();
        random::seed_from(seed);
        let mut generator = Generator::default();

        assert_eq!(
            generator.generate(1000).unwrap().to_bytes(),
            ulid::Ulid::from_parts(1000, first_component).to_bytes()
        );
        generator.generate(1000).unwrap();
        generator.generate(999).unwrap();
        assert_eq!(
            generator.generate(1001).unwrap().to_bytes(),
            ulid::Ulid::from_parts(1001, next_component).to_bytes()
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn generation_uses_native_entropy_when_randomness_is_uninitialized() {
        random::clear_for_tests();
        let mut generator = Generator::default();

        assert!(generator.generate(1000).is_ok());
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn generation_uses_deterministic_component_without_native_entropy() {
        let mut generator = Generator::default();

        assert_eq!(
            generator.generate(66_536).unwrap().to_bytes(),
            ulid::Ulid::from_parts(66_536, (1000_u128 << 64) | 1).to_bytes()
        );
        generator.generate(66_536).unwrap();
        assert_eq!(
            generator.generate(66_537).unwrap().to_bytes(),
            ulid::Ulid::from_parts(66_537, (1001_u128 << 64) | 2).to_bytes()
        );
    }
}
