use super::*;
use crate::db::commit::{
    CommitMarker, DatabaseControlOp, decode_commit_marker_payload, encode_commit_marker_payload,
};

#[test]
fn cursor_codecs_preserve_exact_identity_prefix_and_shape_payloads() {
    let store = TargetStoreIdentity::from_bytes([7; 32]);
    let entity = EntityTag::new(42);
    let row = PersistedSchemaMigrationRowCursor {
        store,
        entity,
        primary_key: vec![8, 9],
    };
    let index = PersistedSchemaMigrationIndexCursor {
        store,
        entity,
        index: 31,
        key: vec![8, 9],
    };
    let mut expected_prefix = vec![1];
    expected_prefix.extend_from_slice(&[7; 32]);
    expected_prefix.extend_from_slice(&42u64.to_be_bytes());
    let mut expected_row = expected_prefix.clone();
    expected_row.extend_from_slice(&2u32.to_be_bytes());
    expected_row.extend_from_slice(&[8, 9]);
    let mut expected_index = expected_prefix;
    expected_index.extend_from_slice(&31u64.to_be_bytes());
    expected_index.extend_from_slice(&2u32.to_be_bytes());
    expected_index.extend_from_slice(&[8, 9]);
    let mut writer = MigrationWriter::new();
    encode_row_cursor(&mut writer, Some(&row)).unwrap();
    assert_eq!(writer.finish().unwrap(), expected_row);
    let mut writer = MigrationWriter::new();
    encode_index_cursor(&mut writer, Some(&index)).unwrap();
    assert_eq!(writer.finish().unwrap(), expected_index);
    let mut reader = MigrationReader::new(&expected_row);
    assert_eq!(decode_row_cursor(&mut reader).unwrap(), Some(row));
    reader.finish().unwrap();
    let mut reader = MigrationReader::new(&expected_index);
    assert_eq!(decode_index_cursor(&mut reader).unwrap(), Some(index));
    reader.finish().unwrap();
    for end in 0..expected_row.len() {
        assert!(decode_row_cursor(&mut MigrationReader::new(&expected_row[..end])).is_err());
    }
    for end in 0..expected_index.len() {
        assert!(decode_index_cursor(&mut MigrationReader::new(&expected_index[..end])).is_err());
    }
}

#[test]
fn cursor_codecs_preserve_absence_and_reject_unknown_tags() {
    let mut writer = MigrationWriter::new();
    encode_row_cursor(&mut writer, None).unwrap();
    assert_eq!(writer.finish().unwrap(), [0]);
    let mut writer = MigrationWriter::new();
    encode_index_cursor(&mut writer, None).unwrap();
    assert_eq!(writer.finish().unwrap(), [0]);
    assert_eq!(
        decode_row_cursor(&mut MigrationReader::new(&[0])).unwrap(),
        None
    );
    assert_eq!(
        decode_index_cursor(&mut MigrationReader::new(&[0])).unwrap(),
        None
    );
    for tag in 2..=u8::MAX {
        assert!(decode_row_cursor(&mut MigrationReader::new(&[tag])).is_err());
        assert!(decode_index_cursor(&mut MigrationReader::new(&[tag])).is_err());
    }
}

fn record() -> SchemaMigrationRecord {
    SchemaMigrationRecord::prepared(
        TargetDatabaseIdentity::from_bytes([1; 32]),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([2; 32]),
        },
        ExpectedAcceptedHead::Exact {
            revision: 2,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
        },
        SchemaProposalDigest::from_bytes([4; 32]),
        SchemaMigrationPlanDigest::from_bytes([5; 32]),
        vec![
            PersistedSchemaMigrationTransition::try_new(
                EntitySourceKey::try_new("User").expect("entity source should admit"),
                1,
                2,
            )
            .expect("transition should admit"),
        ],
        vec![
            PersistedSchemaMigrationEntity::try_new(
                TargetStoreIdentity::from_bytes([6; 32]),
                EntityTag::new(7),
                EntitySourceDigest::from_bytes([8; 32]),
            )
            .expect("entity identity should admit"),
        ],
        Vec::new(),
    )
    .expect("record should admit")
}

#[test]
fn migration_record_codec_is_canonical_bounded_and_checksum_bound() {
    let record = record();
    let encoded = encode_schema_migration_record(&record).expect("record should encode");
    assert_eq!(
        decode_schema_migration_record(&encoded).expect("record should decode"),
        record,
    );

    let mut corrupted = encoded;
    *corrupted.last_mut().expect("record has checksum") ^= 0x80;
    assert!(decode_schema_migration_record(&corrupted).is_err());
    assert!(
        decode_schema_migration_record(&vec![0; MAX_SCHEMA_MIGRATION_RECORD_BYTES + 1]).is_err()
    );
}

#[test]
fn migration_record_codec_retains_exact_transform_finding_identity() {
    let prepared = record();
    let finding = PersistedSchemaMigrationFinding::try_new_transform(
        TargetStoreIdentity::from_bytes([6; 32]),
        EntityTag::new(7),
        vec![0x81, 0x02],
        Some(FieldId::new(2)),
        Some(FieldId::new(3)),
        PersistedSchemaMigrationTransformReason::NegativeToUnsigned,
    )
    .expect("transform finding should admit");
    let progress = PersistedSchemaMigrationProgress::default()
        .with_validation_page(
            Some(
                PersistedSchemaMigrationRowCursor::try_new(
                    TargetStoreIdentity::from_bytes([6; 32]),
                    EntityTag::new(7),
                    vec![0x81, 0x02],
                )
                .expect("cursor should admit"),
            ),
            1,
            vec![finding],
        )
        .expect("progress should admit");
    let rejected = prepared
        .transition(PersistedSchemaMigrationPhase::Rejected, progress)
        .expect("prepared validation may reject");
    let encoded = encode_schema_migration_record(&rejected).expect("record should encode");
    let decoded = decode_schema_migration_record(&encoded).expect("record should decode");
    let [decoded_finding] = decoded.progress().findings() else {
        panic!("one transform finding should survive");
    };
    assert_eq!(decoded_finding.source_field(), Some(FieldId::new(2)));
    assert_eq!(decoded_finding.target_field(), Some(FieldId::new(3)));
    assert_eq!(
        decoded_finding.transform_reason(),
        Some(PersistedSchemaMigrationTransformReason::NegativeToUnsigned),
    );
}

#[test]
fn migration_record_codec_retains_row_level_value_contract_findings() {
    let finding = PersistedSchemaMigrationFinding::try_new_transform(
        TargetStoreIdentity::from_bytes([6; 32]),
        EntityTag::new(7),
        vec![0x81, 0x02],
        None,
        None,
        PersistedSchemaMigrationTransformReason::ValueContract,
    )
    .expect("whole-row value-contract findings should admit");
    let progress = PersistedSchemaMigrationProgress::default()
        .with_validation_page(None, 1, vec![finding])
        .unwrap();
    let rejected = record()
        .transition(PersistedSchemaMigrationPhase::Rejected, progress)
        .unwrap();
    let bytes = encode_schema_migration_record(&rejected).unwrap();
    let decoded = decode_schema_migration_record(&bytes).unwrap();
    assert_eq!(decoded, rejected);
    let [finding] = decoded.progress().findings() else {
        panic!("one whole-row finding should survive");
    };
    assert_eq!(finding.source_field(), None);
    assert_eq!(finding.target_field(), None);
    assert_eq!(
        finding.transform_reason(),
        Some(PersistedSchemaMigrationTransformReason::ValueContract),
    );
    // Build checksum-valid invalid records to qualify the decoding boundary,
    // independently of the constructor's earlier shape refusal.
    for reason in [
        PersistedSchemaMigrationTransformReason::Overflow,
        PersistedSchemaMigrationTransformReason::NullSource,
    ] {
        let mut invalid = rejected.clone();
        invalid.progress.findings[0].transform_reason = Some(reason);
        let bytes = encode_schema_migration_record(&invalid).unwrap();
        assert!(decode_schema_migration_record(&bytes).is_err());
    }
    let mut invalid = rejected;
    invalid.progress.findings[0].source_field = Some(FieldId::new(2));
    let bytes = encode_schema_migration_record(&invalid).unwrap();
    assert!(decode_schema_migration_record(&bytes).is_err());
}

#[test]
fn transform_finding_admission_preserves_exact_field_requirements() {
    let finding = |source, target, reason| {
        PersistedSchemaMigrationFinding::try_new_transform(
            TargetStoreIdentity::from_bytes([6; 32]),
            EntityTag::new(7),
            vec![0x81, 0x02],
            source,
            target,
            reason,
        )
    };
    for reason in [
        PersistedSchemaMigrationTransformReason::Overflow,
        PersistedSchemaMigrationTransformReason::NegativeToUnsigned,
        PersistedSchemaMigrationTransformReason::PrecisionLoss,
        PersistedSchemaMigrationTransformReason::NullSource,
    ] {
        assert!(finding(None, None, reason).is_err());
        assert!(finding(None, Some(FieldId::new(3)), reason).is_ok());
    }
    for (source, target) in [
        (Some(FieldId::new(0)), Some(FieldId::new(3))),
        (None, Some(FieldId::new(0))),
        (Some(FieldId::new(2)), None),
    ] {
        assert!(
            finding(
                source,
                target,
                PersistedSchemaMigrationTransformReason::ValueContract,
            )
            .is_err()
        );
    }
}

#[test]
fn migration_phase_machine_is_closed_and_abort_boundary_is_exact() {
    let prepared = record();
    let validating = prepared
        .transition(
            PersistedSchemaMigrationPhase::Validating,
            PersistedSchemaMigrationProgress::default(),
        )
        .expect("validation should start");
    assert!(validating.phase().blocks_ordinary_operations());
    assert!(validating.phase().abortable());
    let aborted = validating
        .transition(
            PersistedSchemaMigrationPhase::Aborted,
            PersistedSchemaMigrationProgress::default(),
        )
        .expect("validation should remain abortable");
    assert!(aborted.phase().terminal());
    assert!(
        validating
            .transition(
                PersistedSchemaMigrationPhase::RewritingRows,
                PersistedSchemaMigrationProgress::default(),
            )
            .is_err(),
        "validation cannot skip the ready boundary",
    );
    let ready = validating
        .transition(
            PersistedSchemaMigrationPhase::ReadyToRewrite,
            PersistedSchemaMigrationProgress::default(),
        )
        .expect("validated migration should become ready");
    let rewriting = ready
        .transition(
            PersistedSchemaMigrationPhase::RewritingRows,
            PersistedSchemaMigrationProgress::default(),
        )
        .expect("rewrite should start once ready");
    assert!(!rewriting.phase().abortable());
    assert!(
        rewriting
            .transition(
                PersistedSchemaMigrationPhase::Aborted,
                PersistedSchemaMigrationProgress::default(),
            )
            .is_err(),
    );
}

#[test]
fn migration_record_operation_binds_immutable_identity_and_monotonic_phase() {
    let prepared = record();
    let validating = prepared
        .transition(
            PersistedSchemaMigrationPhase::Validating,
            PersistedSchemaMigrationProgress::default(),
        )
        .expect("validation should start");
    SchemaMigrationRecordOp::insert(&prepared).expect("prepare insert should admit");
    SchemaMigrationRecordOp::replace(&prepared, &validating)
        .expect("phase replacement should admit");
    assert!(SchemaMigrationRecordOp::insert(&validating).is_err());
    assert!(SchemaMigrationRecordOp::replace(&validating, &prepared).is_err());
}

#[test]
fn terminal_handoff_record_rejects_active_reset_wrong_database_and_carried_progress() {
    let original = record();
    let mut next = original.clone();
    next.submission_digest = SchemaProposalDigest::from_bytes([0x71; 32]);
    for phase in [
        PersistedSchemaMigrationPhase::Applied,
        PersistedSchemaMigrationPhase::Aborted,
    ] {
        let mut terminal = original.clone();
        terminal.phase = phase;
        let operation = SchemaMigrationRecordOp::replace(&terminal, &next)
            .expect("fresh terminal handoff should admit");
        let marker = CommitMarker::from_parts_with_database_control(
            [0x72; 16],
            Vec::new(),
            vec![DatabaseControlOp::SchemaMigration(operation)],
        )
        .expect("handoff marker should admit");
        let encoded = encode_commit_marker_payload(&marker).expect("handoff marker should encode");
        decode_commit_marker_payload(&encoded).expect("handoff marker should decode");
        let mut invalid = next.clone();
        invalid.database_identity = TargetDatabaseIdentity::from_bytes([0x73; 32]);
        assert!(SchemaMigrationRecordOp::replace(&terminal, &invalid).is_err());
        invalid = next.clone();
        invalid.progress.rows_validated = 1;
        assert!(SchemaMigrationRecordOp::replace(&terminal, &invalid).is_err());
        assert!(SchemaMigrationRecordOp::replace(&terminal, &original).is_err());
    }
    assert!(SchemaMigrationRecordOp::replace(&original, &next).is_err());
}

#[test]
fn migration_record_operation_round_trips_inside_current_marker() {
    let prepared = record();
    let operation = SchemaMigrationRecordOp::insert(&prepared)
        .expect("prepared migration insertion should admit");
    let marker = CommitMarker::from_parts_with_database_control(
        [0x41; 16],
        Vec::new(),
        vec![DatabaseControlOp::SchemaMigration(operation.clone())],
    )
    .expect("migration marker should admit");
    let encoded = encode_commit_marker_payload(&marker).expect("marker should encode");
    let decoded = decode_commit_marker_payload(&encoded).expect("marker should decode");
    let [DatabaseControlOp::SchemaMigration(decoded)] = decoded.database_control() else {
        panic!("marker should retain exactly one migration operation");
    };
    assert_eq!(decoded.before_bytes(), operation.before_bytes());
    assert_eq!(decoded.after_bytes(), operation.after_bytes());
}
