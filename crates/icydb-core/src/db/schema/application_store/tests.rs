use super::{
    APPLICATION_RECORD_HEADER_BYTES, APPLICATION_RECORD_MAGIC, APPLICATION_RECORD_VERSION,
    ApplicationRecordKey, MAX_SCHEMA_APPLICATION_RECORDS, SchemaApplicationRecordOp,
    SchemaApplicationStore, SchemaControlBytes, crc32c, decode_application_record,
    encode_application_record,
};
use crate::db::schema::migration_lineage::{
    AcceptedEntitySourceVersion, PendingEntitySourceLineage,
};
use crate::{
    db::schema::migration_lineage::{
        AcceptedEntitySourceLineage, AcceptedEntitySourceLineageCatalog,
    },
    db::{
        commit::{
            CommitMarker, DatabaseControlOp, decode_commit_marker_payload,
            encode_commit_marker_payload,
        },
        schema::{
            EntitySourceLineageCatalogOp, SchemaApplicationRecord, SchemaChangeActivation,
            SchemaChangeJob, SchemaChangeOutcome, SchemaChangeReceipt, derive_schema_change_job_id,
        },
    },
    testing::test_memory,
    types::EntityTag,
};
use icydb_schema::{
    EntitySourceDigest, ExpectedAcceptedHead, ExpectedSchemaFingerprint,
    MAX_SCHEMA_SUBMISSION_KEY_BYTES, SchemaProposalDigest, SchemaSubmissionKey,
    TargetDatabaseIdentity, TargetStoreIdentity,
};

fn submission_key(value: &str) -> SchemaSubmissionKey {
    SchemaSubmissionKey::try_new(value).expect("test submission key should admit")
}

fn pending_record(value: &str) -> SchemaApplicationRecord {
    let database_identity = TargetDatabaseIdentity::from_bytes([0x11; 32]);
    let submission_key = submission_key(value);
    let proposal_digest = SchemaProposalDigest::from_bytes([0x22; 32]);
    let prior_head = ExpectedAcceptedHead::Empty;
    let job_id = derive_schema_change_job_id(
        database_identity,
        &submission_key,
        proposal_digest,
        &prior_head,
    )
    .expect("test job identity should derive");
    let receipt = SchemaChangeReceipt::new(
        database_identity,
        submission_key,
        proposal_digest,
        prior_head,
        SchemaChangeOutcome::Pending {
            job: SchemaChangeJob::new(job_id),
            candidate_head: ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
            },
        },
    )
    .expect("pending receipt should admit");
    SchemaApplicationRecord::new(
        receipt,
        vec![
            SchemaChangeActivation::new(TargetStoreIdentity::from_bytes([0x44; 32]), 7, 9)
                .expect("activation should admit"),
        ],
    )
    .expect("pending record should admit")
}

fn no_op_record(value: &str) -> SchemaApplicationRecord {
    SchemaApplicationRecord::new(
        SchemaChangeReceipt::new(
            TargetDatabaseIdentity::from_bytes([0x11; 32]),
            submission_key(value),
            SchemaProposalDigest::from_bytes([0x22; 32]),
            ExpectedAcceptedHead::Empty,
            SchemaChangeOutcome::NoOp {
                accepted_head: ExpectedAcceptedHead::Empty,
            },
        )
        .expect("no-op receipt should admit"),
        Vec::new(),
    )
    .expect("no-op record should admit")
}

fn applied_record(pending: &SchemaApplicationRecord) -> SchemaApplicationRecord {
    let receipt = pending.receipt();
    SchemaApplicationRecord::new(
        SchemaChangeReceipt::new(
            receipt.database_identity(),
            receipt.submission_key().clone(),
            receipt.proposal_digest(),
            receipt.prior_head().clone(),
            SchemaChangeOutcome::Applied {
                accepted_head: ExpectedAcceptedHead::Exact {
                    revision: 1,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
                },
            },
        )
        .expect("applied receipt should admit"),
        Vec::new(),
    )
    .expect("applied record should admit")
}

fn aborted_record(pending: &SchemaApplicationRecord) -> SchemaApplicationRecord {
    let receipt = pending.receipt();
    SchemaApplicationRecord::new(
        SchemaChangeReceipt::new(
            receipt.database_identity(),
            receipt.submission_key().clone(),
            receipt.proposal_digest(),
            receipt.prior_head().clone(),
            SchemaChangeOutcome::Aborted {
                accepted_head: ExpectedAcceptedHead::Exact {
                    revision: 2,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([0x66; 32]),
                },
            },
        )
        .expect("aborted receipt should admit"),
        Vec::new(),
    )
    .expect("aborted record should admit")
}

fn empty_store(memory_id: u64) -> SchemaApplicationStore {
    SchemaApplicationStore::open(std::rc::Rc::new(test_memory(memory_id)))
        .expect("application store should initialize")
}

fn rewrite_application_payload(encoded: &[u8], rewrite: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut payload = encoded[APPLICATION_RECORD_HEADER_BYTES..].to_vec();
    rewrite(&mut payload);
    let mut rewritten = APPLICATION_RECORD_MAGIC.to_vec();
    rewritten.push(APPLICATION_RECORD_VERSION);
    rewritten.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("test payload should fit u32")
            .to_le_bytes(),
    );
    rewritten.extend_from_slice(&crc32c(&payload).to_le_bytes());
    rewritten.extend_from_slice(&payload);
    rewritten
}

#[test]
fn application_record_codec_is_canonical_and_checksum_bound() {
    let pending = pending_record("codec");
    let records = [
        no_op_record("codec-no-op"),
        pending.clone(),
        applied_record(&pending),
        aborted_record(&pending),
    ];
    for record in records {
        let key = ApplicationRecordKey::from_receipt(record.receipt()).expect("key should derive");
        let encoded = encode_application_record(&record).expect("record should encode");
        assert_eq!(
            encode_application_record(&record).expect("repeat encoding should remain stable"),
            encoded,
        );
        assert_eq!(
            decode_application_record(&encoded, key).expect("record should decode"),
            record,
        );
    }

    let key = ApplicationRecordKey::from_receipt(pending.receipt()).expect("key should derive");
    let mut corrupted = encode_application_record(&pending).expect("record should encode");
    let last = corrupted
        .last_mut()
        .expect("encoded record should contain a payload");
    *last ^= 0x80;
    assert!(decode_application_record(&corrupted, key).is_err());
}

#[test]
fn pending_lineage_record_is_bounded_and_terminal_cannot_retain_it() {
    let binding = PendingEntitySourceLineage::new(
        AcceptedEntitySourceVersion::try_new(1).unwrap(),
        EntitySourceDigest::from_bytes([3; 32]),
        EntitySourceDigest::from_bytes([4; 32]),
    )
    .unwrap();
    let record = pending_record("source-proof")
        .with_pending_source_lineage(Some(binding.clone()))
        .unwrap();
    let key = ApplicationRecordKey::from_receipt(record.receipt()).unwrap();
    let bytes = encode_application_record(&record).unwrap();
    assert_eq!(decode_application_record(&bytes, key).unwrap(), record);
    for len in 0..bytes.len() {
        assert!(decode_application_record(&bytes[..len], key).is_err());
    }
    for offset in [68, 64, 32, 0] {
        let corrupt = rewrite_application_payload(&bytes, |payload| {
            let start = payload.len() - 1 - offset;
            match offset {
                68 => payload[start] = 2,
                64 => payload[start - 3..=start].fill(0),
                32 | 0 => payload[start - 31..=start].fill(0),
                _ => unreachable!(),
            }
        });
        assert!(decode_application_record(&corrupt, key).is_err());
    }
    assert!(
        applied_record(&record)
            .with_pending_source_lineage(Some(binding))
            .is_err()
    );
    assert!(
        SchemaApplicationRecord::new(
            record.receipt().clone(),
            vec![record.activations()[0].clone(); 2]
        )
        .is_err()
    );
}

#[test]
fn application_record_v1_golden_bytes_remain_stable() {
    let pending = pending_record("golden");
    let record = applied_record(&pending);
    let encoded = encode_application_record(&record).expect("golden record should encode");

    let mut payload = vec![0x11; 32];
    payload.extend_from_slice(&6_u32.to_be_bytes());
    payload.extend_from_slice(b"golden");
    payload.extend_from_slice(&[0x22; 32]);
    payload.push(0);
    payload.push(2);
    payload.push(1);
    payload.extend_from_slice(&1_u64.to_be_bytes());
    payload.extend_from_slice(&[0x33; 32]);
    payload.extend_from_slice(&0_u32.to_be_bytes());
    payload.push(0); // terminal receipts retain no pending source binding
    let mut expected = b"ICYSAR01".to_vec();
    expected.push(1);
    expected.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("golden payload should fit u32")
            .to_le_bytes(),
    );
    expected.extend_from_slice(&crc32c(&payload).to_le_bytes());
    expected.extend_from_slice(&payload);

    assert_eq!(encoded, expected);
}

#[test]
fn application_record_decode_rejects_truncation_tags_lengths_and_trailing_bytes() {
    let record = pending_record("malformed");
    let key = ApplicationRecordKey::from_receipt(record.receipt()).expect("key should derive");
    let encoded = encode_application_record(&record).expect("record should encode");
    for len in 0..encoded.len() {
        assert!(
            decode_application_record(&encoded[..len], key).is_err(),
            "truncated application record at {len} must fail closed",
        );
    }

    let mut bad_magic = encoded.clone();
    bad_magic[0] ^= 1;
    assert!(decode_application_record(&bad_magic, key).is_err());

    let mut bad_version = encoded.clone();
    bad_version[APPLICATION_RECORD_MAGIC.len()] = 2;
    let error = decode_application_record(&bad_version, key)
        .expect_err("noncurrent application record version must fail closed");
    assert_eq!(
        error.class,
        crate::error::ErrorClass::IncompatiblePersistedFormat
    );

    let oversized_key = rewrite_application_payload(&encoded, |payload| {
        payload[32..36].copy_from_slice(
            &u32::try_from(MAX_SCHEMA_SUBMISSION_KEY_BYTES + 1)
                .expect("submission-key bound should fit u32")
                .to_be_bytes(),
        );
    });
    assert!(decode_application_record(&oversized_key, key).is_err());

    let unknown_outcome = rewrite_application_payload(&encoded, |payload| {
        let outcome_offset = 32 + 4 + "malformed".len() + 32 + 1;
        payload[outcome_offset] = u8::MAX;
    });
    assert!(decode_application_record(&unknown_outcome, key).is_err());

    let trailing = rewrite_application_payload(&encoded, |payload| payload.push(0));
    assert!(decode_application_record(&trailing, key).is_err());
}

#[test]
fn marker_round_trip_binds_application_receipt_and_lineage_effect() {
    let record = pending_record("lineage-marker");
    let lineage =
        AcceptedEntitySourceLineageCatalog::try_new(std::collections::BTreeMap::from([(
            (
                TargetStoreIdentity::from_bytes([0x44; 32]),
                EntityTag::new(7),
            ),
            AcceptedEntitySourceLineage::unadopted(ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
            })
            .expect("lineage should admit"),
        )]))
        .expect("lineage catalog should admit");
    let application =
        SchemaApplicationRecordOp::insert(&record).expect("application operation should prepare");
    let lineage = EntitySourceLineageCatalogOp::replace(None, &lineage)
        .expect("lineage operation should prepare");
    let marker = CommitMarker::from_parts_with_database_control(
        [0x55; 16],
        Vec::new(),
        vec![
            DatabaseControlOp::SchemaApplication(application.clone()),
            DatabaseControlOp::EntitySourceLineage(lineage.clone()),
        ],
    )
    .expect("marker should admit");
    let encoded = encode_commit_marker_payload(&marker).expect("marker should encode");
    let decoded = decode_commit_marker_payload(&encoded).expect("marker should decode");
    decoded
        .schema_application()
        .expect("application operation should remain");
    assert_eq!(
        decoded
            .entity_source_lineage()
            .expect("lineage effect should remain")
            .after_bytes(),
        lineage.after_bytes(),
    );
    assert!(
        CommitMarker::from_parts_with_database_control(
            [0x56; 16],
            Vec::new(),
            vec![
                DatabaseControlOp::EntitySourceLineage(lineage),
                DatabaseControlOp::SchemaApplication(application),
            ],
        )
        .is_err(),
        "database-control operations must remain in canonical target order",
    );
}

#[test]
fn application_store_compare_and_replace_is_idempotent_and_reopen_safe() {
    let pending = pending_record("replace");
    let applied = applied_record(&pending);
    let insert = SchemaApplicationRecordOp::insert(&pending).expect("insert should prepare");
    let replace =
        SchemaApplicationRecordOp::replace(&pending, &applied).expect("replace should prepare");
    let memory = std::rc::Rc::new(test_memory(220));
    let mut store = SchemaApplicationStore::open(memory.clone()).expect("store should initialize");

    store.apply(&insert).expect("insert should apply");
    store
        .apply(&insert)
        .expect("exact replay should be idempotent");
    assert_eq!(
        store
            .load(
                pending.receipt().database_identity(),
                pending.receipt().submission_key(),
            )
            .expect("record should load"),
        Some(pending.clone()),
    );
    store.apply(&replace).expect("replacement should apply");

    let reopened = SchemaApplicationStore::open(memory).expect("store should reopen");
    assert_eq!(
        reopened
            .load(
                applied.receipt().database_identity(),
                applied.receipt().submission_key(),
            )
            .expect("terminal record should load"),
        Some(applied),
    );
}

#[test]
fn application_store_rejects_wrong_compare_value_and_corrupt_record() {
    let first = pending_record("first");
    let second = pending_record("second");
    let first_insert =
        SchemaApplicationRecordOp::insert(&first).expect("first insert should prepare");
    let wrong_replace = SchemaApplicationRecordOp::replace(&second, &applied_record(&second))
        .expect("unrelated replacement should prepare");
    let mut store = empty_store(221);
    store
        .apply(&first_insert)
        .expect("first insert should apply");
    assert!(store.apply(&wrong_replace).is_err());

    let key = ApplicationRecordKey::from_receipt(first.receipt()).expect("key should derive");
    store
        .map
        .insert(key, SchemaControlBytes(vec![0xFF; 32]))
        .expect("fixture insert");
    assert!(store.load_key(key).is_err());
}

#[test]
fn application_record_replacement_preserves_submission_and_terminal_identity() {
    let pending = pending_record("transition");
    let applied = applied_record(&pending);
    let aborted = aborted_record(&pending);
    SchemaApplicationRecordOp::replace(&pending, &aborted)
        .expect("pending application should admit explicit abort");
    assert!(
        SchemaApplicationRecordOp::replace(&applied, &applied).is_err(),
        "terminal application records must be immutable",
    );
    assert!(
        SchemaApplicationRecordOp::replace(&aborted, &applied).is_err(),
        "an aborted application must remain terminal",
    );

    let receipt = pending.receipt();
    let current_head = SchemaApplicationRecord::new(
        SchemaChangeReceipt::new(
            receipt.database_identity(),
            receipt.submission_key().clone(),
            receipt.proposal_digest(),
            receipt.prior_head().clone(),
            SchemaChangeOutcome::Applied {
                accepted_head: ExpectedAcceptedHead::Exact {
                    revision: 2,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([0x55; 32]),
                },
            },
        )
        .expect("terminal receipt should admit"),
        Vec::new(),
    )
    .expect("terminal record should admit");
    let operation = SchemaApplicationRecordOp::replace(&pending, &current_head)
        .expect("a proven promotion may include unrelated accepted publications");
    SchemaApplicationRecordOp::from_encoded(
        operation.key(),
        operation.before_bytes().map(<[u8]>::to_vec),
        operation.after_bytes().to_vec(),
    )
    .expect("the same transition must survive marker decoding");
    for (digest, prior_head) in [
        (
            SchemaProposalDigest::from_bytes([0x99; 32]),
            receipt.prior_head().clone(),
        ),
        (
            receipt.proposal_digest(),
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x88; 32]),
            },
        ),
    ] {
        let altered_submission = SchemaApplicationRecord::new(
            SchemaChangeReceipt::new(
                receipt.database_identity(),
                receipt.submission_key().clone(),
                digest,
                prior_head,
                current_head.receipt().outcome().clone(),
            )
            .unwrap(),
            Vec::new(),
        )
        .unwrap();
        assert!(SchemaApplicationRecordOp::replace(&pending, &altered_submission).is_err());
    }
    assert!(SchemaApplicationRecordOp::replace(&pending, &pending).is_err());
}

#[test]
fn commit_marker_round_trips_exact_schema_application_effect() {
    let record = pending_record("marker");
    let operation =
        SchemaApplicationRecordOp::insert(&record).expect("marker effect should prepare");
    let marker =
        CommitMarker::from_parts_with_schema_application([0x55; 16], Vec::new(), Some(operation))
            .expect("marker should admit");
    let encoded = encode_commit_marker_payload(&marker).expect("marker should encode");
    let decoded = decode_commit_marker_payload(&encoded).expect("marker should decode");
    let decoded = decoded
        .schema_application()
        .expect("schema application effect should remain present");

    assert_eq!(
        decoded.key(),
        ApplicationRecordKey::from_receipt(record.receipt()).expect("key should derive"),
    );
    assert_eq!(
        decode_application_record(decoded.after_bytes(), decoded.key())
            .expect("marker record should decode"),
        record,
    );
}

#[test]
fn application_store_capacity_preserves_pending_evidence() {
    let mut store = empty_store(223);
    for ordinal in 0..MAX_SCHEMA_APPLICATION_RECORDS {
        let record = pending_record(&format!("capacity-{ordinal}"));
        let operation = SchemaApplicationRecordOp::insert(&record).expect("insert should prepare");
        store.apply(&operation).expect("bounded record should fit");
    }
    let overflow = pending_record("capacity-overflow");
    let operation = SchemaApplicationRecordOp::insert(&overflow).expect("overflow should prepare");

    assert!(store.preflight(&operation).is_err());
    assert_eq!(
        (store.record_count()).expect("storage operation should succeed"),
        MAX_SCHEMA_APPLICATION_RECORDS,
    );
}

#[test]
fn application_store_capacity_recycles_one_deterministic_terminal_receipt() {
    let mut store = empty_store(224);
    let mut terminal_keys = Vec::new();
    for ordinal in 0..MAX_SCHEMA_APPLICATION_RECORDS {
        let pending = pending_record(&format!("terminal-{ordinal}"));
        let terminal = if ordinal % 2 == 0 {
            applied_record(&pending)
        } else {
            aborted_record(&pending)
        };
        terminal_keys.push(
            ApplicationRecordKey::from_receipt(terminal.receipt())
                .expect("terminal key should derive"),
        );
        let operation =
            SchemaApplicationRecordOp::insert(&terminal).expect("terminal insert should prepare");
        store.apply(&operation).expect("terminal record should fit");
    }
    terminal_keys.sort_unstable();
    let retired = terminal_keys[0];
    let next = pending_record("capacity-recycled");
    let operation =
        SchemaApplicationRecordOp::insert(&next).expect("pending insert should prepare");

    store
        .apply(&operation)
        .expect("one terminal receipt should make bounded room");

    assert_eq!(
        (store.record_count()).expect("storage operation should succeed"),
        MAX_SCHEMA_APPLICATION_RECORDS,
    );
    assert!(
        store
            .load_key(retired)
            .expect("retired key should remain readable")
            .is_none(),
    );
    assert_eq!(
        store
            .load(
                next.receipt().database_identity(),
                next.receipt().submission_key(),
            )
            .expect("new pending record should load"),
        Some(next),
    );
}

#[test]
fn application_store_retention_preserves_readiness_and_pending_receipts() {
    struct Generated;
    impl crate::traits::Path for Generated {
        const PATH: &'static str = "retention::Generated";
    }
    impl crate::traits::CanisterKind for Generated {
        const COMMIT_STABLE_KEY: &'static str = "icydb.retention.commit.v1";
        const STARTUP_STABLE_KEY: &'static str = "icydb.retention.startup.v1";
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.retention.integrity.v1";

        fn generated_schema_application_record_key()
        -> Result<Option<[u8; 32]>, crate::error::InternalError> {
            ApplicationRecordKey::from_receipt(pending_record("protected").receipt())
                .map(|key| Some(key.to_bytes()))
        }
    }

    crate::db::commit::select_commit_memory_allocation_for::<Generated>(
        225,
        "icydb.retention.commit.v1",
    );
    let memory = std::rc::Rc::new(test_memory(225));
    let mut store = SchemaApplicationStore::open(memory.clone()).unwrap();
    let protected = applied_record(&pending_record("protected"));
    let protected_key = ApplicationRecordKey::from_receipt(protected.receipt()).unwrap();
    store
        .apply(&SchemaApplicationRecordOp::insert(&protected).unwrap())
        .unwrap();
    for ordinal in 1..MAX_SCHEMA_APPLICATION_RECORDS {
        let pending = pending_record(&format!("retained-{ordinal}"));
        store
            .apply(&SchemaApplicationRecordOp::insert(&pending).unwrap())
            .unwrap();
    }
    let next = pending_record("overflow");
    let insert = SchemaApplicationRecordOp::insert(&next).unwrap();
    assert_eq!(
        store.preflight(&insert).unwrap_err().class,
        crate::error::ErrorClass::Conflict
    );
    assert_eq!(
        store.apply(&insert).unwrap_err().class,
        crate::error::ErrorClass::Conflict
    );
    assert_eq!(
        (store.record_count()).expect("storage operation should succeed"),
        MAX_SCHEMA_APPLICATION_RECORDS
    );

    // Only the explicitly completed pending record becomes eligible.
    let pending = pending_record("retained-1");
    let completed = applied_record(&pending);
    store
        .apply(&SchemaApplicationRecordOp::replace(&pending, &completed).unwrap())
        .unwrap();
    assert_eq!(
        store.preflight(&insert).unwrap(),
        super::SchemaApplicationRecordPreflight::Ready {
            retired_terminal: Some(
                ApplicationRecordKey::from_receipt(completed.receipt()).unwrap()
            ),
        },
    );
    store.apply(&insert).unwrap();
    store.apply(&insert).unwrap();
    assert_eq!(
        (store.record_count()).expect("storage operation should succeed"),
        MAX_SCHEMA_APPLICATION_RECORDS
    );
    assert_eq!(
        store
            .load_key(ApplicationRecordKey::from_receipt(protected.receipt()).unwrap())
            .unwrap(),
        Some(protected)
    );
    assert!(
        store
            .load_key(ApplicationRecordKey::from_receipt(completed.receipt()).unwrap())
            .unwrap()
            .is_none()
    );
    assert_eq!(store.load_key(insert.key()).unwrap(), Some(next));
    drop(store);
    let reopened = SchemaApplicationStore::open(memory).unwrap();
    assert!(reopened.load_key(protected_key).unwrap().is_some());
}
