use super::*;

#[test]
fn boot_entropy_retries_failures_and_deduplicates_pending_requests() {
    let mut state = BootEntropy::Empty;
    assert!(state.begin());
    assert!(!state.begin());
    assert!(state.seed().is_none());
    state.finish(None);
    assert!(state.begin());
    state.finish(Some([0; 32]));
    assert!(state.begin());
    state.finish(Some([7; 32]));
    assert_eq!(state.seed(), Some([7; 32]));
    assert!(!state.begin());
    state.finish(None);
    assert_eq!(state.seed(), Some([7; 32]));
}

#[test]
fn cancelled_entropy_request_releases_pending_claim() {
    set_boot_entropy_for_tests(None);
    drop(EntropyRequest);
    assert!(BOOT_ENTROPY.with_borrow_mut(BootEntropy::begin));
    BOOT_ENTROPY.with_borrow_mut(|state| state.finish(Some([9; 32])));
}

#[test]
fn host_entropy_admission_retries_and_refusals_preserve_the_boot_seed() {
    BOOT_ENTROPY.with_borrow_mut(|state| *state = BootEntropy::Empty);
    assert!(provide_boot_entropy([0; 32]).is_err());
    assert!(BOOT_ENTROPY.with_borrow(BootEntropy::seed).is_none());
    provide_boot_entropy([11; 32]).unwrap();
    provide_boot_entropy([11; 32]).unwrap();
    assert!(provide_boot_entropy([12; 32]).is_err());
    assert!(provide_boot_entropy([0; 32]).is_err());
    assert_eq!(BOOT_ENTROPY.with_borrow(BootEntropy::seed), Some([11; 32]));
}

#[test]
fn cursor_keys_bind_boot_entropy_and_database_identity() {
    let incarnation = DatabaseIncarnationId::for_tests(1);
    let key = cursor_key([1; 32], incarnation);
    assert_ne!(key, [0; 32]);
    assert_eq!(key, cursor_key([1; 32], incarnation));
    assert_ne!(key, cursor_key([2; 32], incarnation));
    assert_ne!(
        key,
        cursor_key([1; 32], DatabaseIncarnationId::for_tests(2))
    );
}
