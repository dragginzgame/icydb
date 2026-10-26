//! Module: runtime
//! Responsibility: isolate IC runtime calls behind host-testable helpers.
//! Does not own: storage policy, endpoint generation, or public IC crate facades.
//! Boundary: internal core modules -> runtime -> IC/system clock APIs.

#[cfg(not(target_arch = "wasm32"))]
use std::time::SystemTime;

/// Read the current IC local instruction counter.
#[must_use]
#[cfg(target_arch = "wasm32")]
pub(crate) fn local_instruction_counter() -> u64 {
    ic_platform::ic0::performance_counter(1)
}

/// Return zero when local instruction accounting runs outside the IC.
#[must_use]
#[cfg(all(not(test), not(target_arch = "wasm32")))]
pub(crate) const fn local_instruction_counter() -> u64 {
    0
}

#[cfg(all(test, not(target_arch = "wasm32")))]
thread_local! {
    static TEST_INSTRUCTIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Read the scripted native test counter; this is never a timing measurement.
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn local_instruction_counter() -> u64 {
    TEST_INSTRUCTIONS.with(std::cell::Cell::get)
}

/// Scope synthetic instruction samples to one test thread, restoring on unwind.
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn with_instruction_counter_for_tests<R>(
    initial: u64,
    run: impl FnOnce(&std::cell::Cell<u64>) -> R,
) -> R {
    struct RestoreCounter<'a>(&'a std::cell::Cell<u64>, u64);

    impl Drop for RestoreCounter<'_> {
        fn drop(&mut self) {
            self.0.set(self.1);
        }
    }

    TEST_INSTRUCTIONS.with(|counter| {
        let _restore = RestoreCounter(counter, counter.replace(initial));
        run(counter)
    })
}

/// Return the current UNIX epoch time in milliseconds.
#[must_use]
#[cfg_attr(
    not(target_arch = "wasm32"),
    expect(
        clippy::cast_possible_truncation,
        reason = "millisecond epoch values fit into u64 for IcyDB timestamps"
    )
)]
pub(crate) fn now_millis() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        ic_platform::ic0::time() / 1_000_000
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as u64,
            Err(_) => 0,
        }
    }
}
