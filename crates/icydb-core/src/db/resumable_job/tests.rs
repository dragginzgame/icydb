use super::*;

fn proof() -> ReadSetRevisionProof {
    ReadSetRevisionProof::from_parts(
        [1; 16],
        7,
        1,
        [2; 32],
        vec![ReadSetStoreRevision::new(
            ReadSetStoreIdentity::from_bytes([3; 32]),
            11,
            13,
        )],
    )
    .expect("bounded canonical proof should admit")
}

fn job_id() -> ResumableJobId {
    ResumableJobId::try_from_bytes([4; 32]).expect("nonzero job identity should admit")
}

#[test]
fn current_resumable_job_payload_round_trips_state_and_replay_receipt() {
    let record = ResumableJobRecord::new(job_id(), proof(), vec![1, 2, 3])
        .expect("initial resumable record should admit");
    let request = ResumableJobAdvanceRequest::new(
        job_id(),
        0,
        ResumableJobIdempotencyKey::new("page-0").expect("bounded idempotency key should admit"),
    );
    let advance = ResumableJobAdvance::new(
        Some("opaque-continuation".to_string()),
        vec![4, 5],
        vec![6, 7],
    )
    .expect("bounded advance should admit");
    let (advanced, _) = record
        .apply_advance(&request, advance)
        .expect("current request should advance");

    let bytes =
        encode_resumable_job_payload(&advanced).expect("current resumable payload should encode");
    assert!(!bytes.starts_with(b"DIDL"));
    assert_eq!(
        decode_resumable_job_payload(&bytes).expect("current resumable payload should decode"),
        advanced,
    );
}

#[test]
fn resumable_job_payload_rejects_truncation_and_trailing_bytes() {
    let record = ResumableJobRecord::new(job_id(), proof(), Vec::new())
        .expect("initial resumable record should admit");
    let bytes =
        encode_resumable_job_payload(&record).expect("current resumable payload should encode");

    for end in 0..bytes.len() {
        assert_eq!(
            decode_resumable_job_payload(&bytes[..end]),
            Err(ResumableJobError::CorruptProgressStore),
            "truncation at byte {end} must retain corruption classification",
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        decode_resumable_job_payload(&trailing),
        Err(ResumableJobError::CorruptProgressStore),
    );
}
