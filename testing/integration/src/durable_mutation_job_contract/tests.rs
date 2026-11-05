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
