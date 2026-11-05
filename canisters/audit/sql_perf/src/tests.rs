use super::startup_watchdog_count;
use ic_timers::MeasurementCounter;

#[test]
fn startup_watchdog_counts_require_exact_u64_values() {
    for (total, overflowed, expected) in [
        (0, false, Some(0)),
        (1, false, Some(1)),
        (u128::from(u64::MAX), false, Some(u64::MAX)),
        (u128::from(u64::MAX) + 1, false, None),
        (u128::MAX, false, None),
        (u128::MAX, true, None),
    ] {
        let counter = MeasurementCounter::from_parts(total, overflowed).unwrap();
        assert_eq!(startup_watchdog_count(counter).ok(), expected);
    }
}
