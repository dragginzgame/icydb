use crate::testing::run_isolated;
use std::cell::Cell;

thread_local! {
    static FIXTURE_VALUE: Cell<u32> = const { Cell::new(0) };
}

#[test]
fn isolated_fixtures_preserve_thread_local_boundaries_and_return_values() {
    FIXTURE_VALUE.set(99);
    assert_eq!(
        run_isolated(|| {
            assert_eq!(FIXTURE_VALUE.get(), 0);
            FIXTURE_VALUE.set(1);
            7
        }),
        7
    );
    assert_eq!(FIXTURE_VALUE.get(), 99);
    run_isolated(|| assert_eq!(FIXTURE_VALUE.get(), 0));
    FIXTURE_VALUE.set(0);
}

#[test]
fn isolated_fixture_preserves_the_original_panic_payload() {
    let failure = std::panic::catch_unwind(|| {
        run_isolated(|| std::panic::panic_any(17_u32));
    })
    .unwrap_err();
    assert_eq!(failure.downcast_ref::<u32>(), Some(&17));
}
