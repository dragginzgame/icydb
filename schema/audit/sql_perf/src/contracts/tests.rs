//! Independent finite-value and field-order controls for native audit records.

use crate::contracts::{
    ApplicationStartupHook, ApplicationStartupSnapshot, SchemaApplicationPerfResult,
    StartupObservationPerfResult, StartupWatchdogPerfSnapshot,
};
use icydb::{
    db::DatabaseStartupState,
    wire::{decode_one, encode_one},
};

#[test]
fn schema_and_startup_records_preserve_native_field_order() {
    let schema = SchemaApplicationPerfResult {
        local_instructions: u64::MAX,
        reconcile_checks: 17,
        first_create: 2,
        exact_match: 15,
    };
    let bytes = encode_one(&schema).unwrap();
    assert_eq!(
        bytes,
        encode_one((u64::MAX, 17_u64, 2_u64, 15_u64)).unwrap()
    );
    assert_eq!(
        decode_one::<SchemaApplicationPerfResult>(&bytes).unwrap(),
        schema
    );
    let startup = StartupObservationPerfResult {
        state: DatabaseStartupState::Ready,
        local_instructions: 1_u64 << 40,
    };
    assert_eq!(
        encode_one(&startup).unwrap(),
        encode_one((DatabaseStartupState::Ready, 1_u64 << 40)).unwrap()
    );
}

#[test]
fn watchdog_totals_preserve_u128_and_optional_samples() {
    let snapshot = StartupWatchdogPerfSnapshot {
        scheduler_samples: 1,
        scheduler_total_instructions: u128::MAX,
        scheduler_maximum_instructions: Some(u64::MAX),
        work_samples: 2,
        work_total_instructions: 1_u128 << 100,
        work_latest_instructions: None,
        work_maximum_instructions: Some(3),
        work_started: 4,
        work_completed: 5,
        succeeded: 6,
        no_work: 7,
        retryable_failures: 8,
        invariant_failures: 9,
    };
    let bytes = encode_one(&snapshot).unwrap();
    assert_eq!(
        decode_one::<StartupWatchdogPerfSnapshot>(&bytes).unwrap(),
        snapshot
    );
    for length in 0..bytes.len() {
        assert!(decode_one::<StartupWatchdogPerfSnapshot>(&bytes[..length]).is_err());
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(decode_one::<StartupWatchdogPerfSnapshot>(&trailing).is_err());
}

#[test]
fn application_startup_preserves_hook_and_readiness_fields() {
    assert_eq!(encode_one(ApplicationStartupHook::Init).unwrap(), [0]);
    assert_eq!(
        encode_one(ApplicationStartupHook::PostUpgrade).unwrap(),
        [1]
    );
    let snapshot = ApplicationStartupSnapshot {
        hook: ApplicationStartupHook::PostUpgrade,
        engine_registered_before_hook: true,
        observations: u32::MAX,
        recovering_observations: 2,
        ready_observations: 3,
        restorations: 4,
        retry_scheduled: false,
        failure: None,
    };
    let bytes = encode_one(&snapshot).unwrap();
    assert_eq!(
        decode_one::<ApplicationStartupSnapshot>(&bytes).unwrap(),
        snapshot
    );
}
