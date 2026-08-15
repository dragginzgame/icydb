#![allow(clippy::missing_const_for_fn, dead_code)]

use std::cell::Cell;

std::thread_local! {
    static INIT_CALLS: Cell<u8> = const { Cell::new(0) };
    static POST_UPGRADE_CALLS: Cell<u8> = const { Cell::new(0) };
    static REENTER_ONCE: Cell<bool> = const { Cell::new(true) };
}

mod __icydb_generated {
    pub(crate) fn __icydb_startup_init() {
        if super::REENTER_ONCE.replace(false) {
            crate::__icydb_lifecycle_participant::post_upgrade();
        }
        super::INIT_CALLS.set(super::INIT_CALLS.get() + 1);
    }

    pub(crate) fn __icydb_startup_post_upgrade() {
        super::POST_UPGRADE_CALLS.set(super::POST_UPGRADE_CALLS.get() + 1);
    }
}

icydb::__icydb_start_participant_lifecycle!();

const _: fn() -> () = crate::__icydb_lifecycle_participant::init;
const _: fn() -> () = crate::__icydb_lifecycle_participant::post_upgrade;

#[test]
fn participant_traps_running_reentry_retries_and_ignores_completed_duplicates() {
    let first = std::panic::catch_unwind(crate::__icydb_lifecycle_participant::init);
    assert!(first.is_err(), "running re-entry must trap");
    assert_eq!(INIT_CALLS.get(), 0);
    assert_eq!(POST_UPGRADE_CALLS.get(), 0);

    crate::__icydb_lifecycle_participant::init();
    assert_eq!(INIT_CALLS.get(), 1);
    assert_eq!(POST_UPGRADE_CALLS.get(), 0);

    crate::__icydb_lifecycle_participant::init();
    crate::__icydb_lifecycle_participant::post_upgrade();
    assert_eq!(INIT_CALLS.get(), 1);
    assert_eq!(POST_UPGRADE_CALLS.get(), 0);
}
