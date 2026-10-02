//! Maintained bounds and review limits for durable mutation-job scale tests.
//!
//! Runtime owners enforce these limits. This module supplies scale inputs and
//! checks their agreement with the current authorities; it does not execute
//! mutations, publish progress, or select recovery behavior.

/// Matching rows in each collection-scale tier and scoring fixture.
pub const DURABLE_MUTATION_JOB_FIXTURE_ROWS: u32 = 10_001;

/// Current engine-owned maximum authoritative keys examined by one step.
pub const DURABLE_MUTATION_JOB_FORWARD_KEY_LIMIT: u32 = 4_096;

/// Current engine-owned maximum fixed updates staged by one Forward step.
pub const DURABLE_MUTATION_JOB_FORWARD_ROW_LIMIT: u32 = 240;

/// Current engine-owned maximum authoritative keys examined by one Verify step.
pub const DURABLE_MUTATION_JOB_VERIFY_KEY_LIMIT: u32 = 4_096;

/// Existing exact one-shot update admission ceiling used by the incident control.
pub const DURABLE_MUTATION_JOB_EAGER_UPDATE_ROW_LIMIT: u32 = 4_096;

/// Current maximum engine continuation retained inside one job.
pub const DURABLE_MUTATION_JOB_CONTINUATION_BYTES: u32 = 2 * 1_024;

/// Current maximum canonical accepted mutation intent.
pub const DURABLE_MUTATION_JOB_INTENT_BYTES: u32 = 16 * 1_024;

/// Current maximum retained replay receipt.
pub const DURABLE_MUTATION_JOB_RECEIPT_BYTES: u32 = 8 * 1_024;

/// Current maximum complete encoded mutation-job record.
pub const DURABLE_MUTATION_JOB_RECORD_BYTES: u32 = 64 * 1_024;

/// Shared current progress-store job capacity across all retained job families.
pub const DURABLE_MUTATION_JOB_GLOBAL_CAPACITY: u32 = 64;

/// Maximum shared occupancy admitted for non-integrity job families.
pub const DURABLE_PROGRESS_NON_INTEGRITY_CAPACITY: u32 = 56;

/// Exact shared slots reserved for Deep integrity work.
pub const DURABLE_PROGRESS_INTEGRITY_RESERVATION: u32 = 8;

/// Current idempotency-key byte limit for retained mutation jobs.
pub const DURABLE_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES: u32 = 256;

/// Current marker envelope for atomic mutation-progress custody.
pub const CURRENT_MUTATION_PROGRESS_MARKER_VERSION: u8 = 1;

/// Exact maximum current mutation-progress contribution to one marker payload.
pub const CURRENT_MUTATION_PROGRESS_MAX_MARKER_PAYLOAD_BYTES: u32 = 37_797;

const _: () = {
    assert!(DURABLE_MUTATION_JOB_FIXTURE_ROWS > 10_000);
    assert!(DURABLE_MUTATION_JOB_FIXTURE_ROWS > DURABLE_MUTATION_JOB_EAGER_UPDATE_ROW_LIMIT);
    assert!(
        DURABLE_PROGRESS_NON_INTEGRITY_CAPACITY + DURABLE_PROGRESS_INTEGRITY_RESERVATION
            == DURABLE_MUTATION_JOB_GLOBAL_CAPACITY
    );
};

/// Maximum reviewed instruction cost for one byte- and count-packed 240-update Forward step.
pub const DURABLE_FORWARD_INSTRUCTION_REVIEW_CEILING: u64 = 7_500_000_000;

/// Maximum reviewed instruction cost for one 4,096-key Verify step.
pub const DURABLE_VERIFY_INSTRUCTION_REVIEW_CEILING: u64 = 500_000_000;

/// Maximum reviewed instruction cost for state, replay, or acknowledgement.
pub const DURABLE_CONTROL_INSTRUCTION_REVIEW_CEILING: u64 = 2_000_000;

/// Minimum mutating calls needed when every matching row needs the patch.
#[must_use]
pub const fn minimum_forward_advances(rows: u32) -> u32 {
    rows.div_ceil(DURABLE_MUTATION_JOB_FORWARD_ROW_LIMIT)
}

/// Minimum clean Verify calls needed to prove exhaustion.
#[must_use]
pub const fn minimum_verify_advances(rows: u32) -> u32 {
    rows.div_ceil(DURABLE_MUTATION_JOB_VERIFY_KEY_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_fixture_requires_multiple_bounded_advances() {
        assert_eq!(
            minimum_forward_advances(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
            42
        );
        assert_eq!(
            minimum_verify_advances(DURABLE_MUTATION_JOB_FIXTURE_ROWS),
            3
        );
    }

    #[test]
    fn runtime_limits_match_the_scale_contract() {
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_CONTINUATION_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_CONTINUATION_BYTES)
                .expect("continuation byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_INTENT_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_INTENT_BYTES)
                .expect("intent byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_RECEIPT_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_RECEIPT_BYTES)
                .expect("receipt byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_RECORD_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_RECORD_BYTES)
                .expect("record byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES,
            usize::try_from(DURABLE_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES)
                .expect("idempotency-key byte limit should fit usize"),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_STEP_KEYS_SCANNED,
            u64::from(DURABLE_MUTATION_JOB_FORWARD_KEY_LIMIT),
        );
        assert_eq!(
            icydb::db::MAX_MUTATION_JOB_STEP_ROWS_UPDATED,
            u64::from(DURABLE_MUTATION_JOB_FORWARD_ROW_LIMIT),
        );
    }
}
