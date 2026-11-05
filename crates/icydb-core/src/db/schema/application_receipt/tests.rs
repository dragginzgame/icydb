use super::{
    SchemaApplicationRecord, SchemaChangeActivation, SchemaChangeJob, SchemaChangeOutcome,
    SchemaChangeReceipt, derive_schema_change_job_id,
};
use icydb_schema::{
    ExpectedAcceptedHead, ExpectedSchemaFingerprint, SchemaProposalDigest, SchemaSubmissionKey,
    TargetDatabaseIdentity, TargetStoreIdentity,
};

#[test]
fn schema_change_job_identity_covers_the_complete_idempotency_tuple() {
    let database = TargetDatabaseIdentity::from_bytes([0x11; 32]);
    let submission = SchemaSubmissionKey::try_new("job-id").expect("submission key should admit");
    let digest = SchemaProposalDigest::from_bytes([0x22; 32]);
    let empty =
        derive_schema_change_job_id(database, &submission, digest, &ExpectedAcceptedHead::Empty)
            .expect("empty-head identity should derive");
    let exact = derive_schema_change_job_id(
        database,
        &submission,
        digest,
        &ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
        },
    )
    .expect("exact-head identity should derive");

    assert_ne!(empty, exact);
}

#[test]
fn pending_and_terminal_record_state_is_exact() {
    let database = TargetDatabaseIdentity::from_bytes([0x11; 32]);
    let submission = SchemaSubmissionKey::try_new("state").expect("submission key should admit");
    let digest = SchemaProposalDigest::from_bytes([0x22; 32]);
    let head = ExpectedAcceptedHead::Empty;
    let job = SchemaChangeJob::new(
        derive_schema_change_job_id(database, &submission, digest, &head)
            .expect("job identity should derive"),
    );
    let pending = SchemaChangeReceipt::new(
        database,
        submission.clone(),
        digest,
        head.clone(),
        SchemaChangeOutcome::Pending {
            job,
            candidate_head: ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
            },
        },
    )
    .expect("pending receipt should admit");
    assert!(
        SchemaApplicationRecord::new(pending.clone(), Vec::new()).is_err(),
        "pending records require exact activation ownership",
    );
    let activation = SchemaChangeActivation::new(TargetStoreIdentity::from_bytes([0x44; 32]), 1, 1)
        .expect("activation should admit");
    SchemaApplicationRecord::new(pending, vec![activation])
        .expect("pending record with activation should admit");

    let terminal = SchemaChangeReceipt::new(
        database,
        submission,
        digest,
        head,
        SchemaChangeOutcome::Aborted {
            accepted_head: ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x55; 32]),
            },
        },
    )
    .expect("terminal receipt should admit");
    assert!(
        SchemaApplicationRecord::new(
            terminal,
            vec![
                SchemaChangeActivation::new(TargetStoreIdentity::from_bytes([0x44; 32]), 1, 1,)
                    .expect("activation should admit"),
            ],
        )
        .is_err(),
        "terminal records cannot retain activation state",
    );
}

#[test]
fn schema_change_receipt_outcome_heads_have_exact_temporal_closure() {
    let database = TargetDatabaseIdentity::from_bytes([0x11; 32]);
    let digest = SchemaProposalDigest::from_bytes([0x22; 32]);
    let prior = ExpectedAcceptedHead::Exact {
        revision: 7,
        fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
    };
    let changed = ExpectedAcceptedHead::Exact {
        revision: 8,
        fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
    };

    SchemaChangeReceipt::new(
        database,
        SchemaSubmissionKey::try_new("noop").expect("submission key should admit"),
        digest,
        prior.clone(),
        SchemaChangeOutcome::NoOp {
            accepted_head: prior.clone(),
        },
    )
    .expect("no-op must retain the exact prior head");
    assert!(
        SchemaChangeReceipt::new(
            database,
            SchemaSubmissionKey::try_new("invalid-noop").expect("submission key should admit"),
            digest,
            prior.clone(),
            SchemaChangeOutcome::NoOp {
                accepted_head: changed.clone(),
            },
        )
        .is_err(),
    );
    assert!(
        SchemaChangeReceipt::new(
            database,
            SchemaSubmissionKey::try_new("invalid-applied").expect("submission key should admit"),
            digest,
            prior.clone(),
            SchemaChangeOutcome::Applied {
                accepted_head: prior.clone(),
            },
        )
        .is_err(),
    );
    SchemaChangeReceipt::new(
        database,
        SchemaSubmissionKey::try_new("applied").expect("submission key should admit"),
        digest,
        prior.clone(),
        SchemaChangeOutcome::Applied {
            accepted_head: changed.clone(),
        },
    )
    .expect("applied receipt must identify a different exact head");
    assert!(
        SchemaChangeReceipt::new(
            database,
            SchemaSubmissionKey::try_new("invalid-abort").expect("submission key should admit"),
            digest,
            prior.clone(),
            SchemaChangeOutcome::Aborted {
                accepted_head: prior.clone(),
            },
        )
        .is_err(),
    );
    SchemaChangeReceipt::new(
        database,
        SchemaSubmissionKey::try_new("aborted").expect("submission key should admit"),
        digest,
        prior,
        SchemaChangeOutcome::Aborted {
            accepted_head: changed,
        },
    )
    .expect("aborted receipt must retain the post-abort accepted head");
}
