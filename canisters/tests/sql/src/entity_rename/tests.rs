use super::{entity_creation_startup_count, typed_fixture_invariant_error};
use ic_timers::MeasurementCounter;

#[test]
fn startup_counts_preserve_exact_values_and_typed_refusal() {
    for (total, overflowed, expected) in [
        (0, false, Ok(0)),
        (1, false, Ok(1)),
        (u128::from(u64::MAX), false, Ok(u64::MAX)),
        (
            u128::from(u64::MAX) + 1,
            false,
            Err(typed_fixture_invariant_error()),
        ),
        (u128::MAX, false, Err(typed_fixture_invariant_error())),
        (u128::MAX, true, Err(typed_fixture_invariant_error())),
    ] {
        let counter = MeasurementCounter::from_parts(total, overflowed).unwrap();
        assert_eq!(entity_creation_startup_count(counter), expected);
    }
}
