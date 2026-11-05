use super::*;
use crate::{
    db::{
        index::{IndexId, UserIndexPrefixCardinalityKey},
        registry::StoreAllocationIdentity,
        schema::enum_catalog::{AcceptedSchemaFingerprint, AcceptedSchemaRevision},
    },
    error::ErrorClass,
};

fn allocations(offset: u8) -> StoreAllocationIdentities {
    StoreAllocationIdentities::new_journaled(
        StoreAllocationIdentity::new((100 + offset).into(), "test.cardinality.data.v1"),
        StoreAllocationIdentity::new((101 + offset).into(), "test.cardinality.index.v1"),
        StoreAllocationIdentity::new((102 + offset).into(), "test.cardinality.schema.v1"),
        StoreAllocationIdentity::new((103 + offset).into(), "test.cardinality.journal.v1"),
    )
}

fn source(seed: u8) -> CardinalitySourceIdentity {
    CardinalitySourceIdentity::derive(
        DatabaseIncarnationId::for_tests(seed),
        allocations(seed),
        Some(
            CardinalityAcceptedRootIdentity::new(
                AcceptedSchemaRevision::new(u64::from(seed) + 1),
                AcceptedSchemaFingerprint::new([seed; 32]),
            )
            .unwrap(),
        ),
        [
            IndexId::new_with_generation(EntityTag::new(7), 2, 9),
            IndexId::new_with_generation(EntityTag::new(7), 1, 9),
        ],
        FoldWatermark::new(
            crate::db::journal::JournalSequence::new(u64::from(seed) + 3),
            u64::from(seed) + 4,
        ),
    )
    .unwrap()
}

#[test]
fn generation_header_round_trips_the_complete_current_identity() {
    let header = CardinalityGenerationHeader::new(
        CardinalityGenerationId::INITIAL,
        CardinalityGenerationState::Building,
        CardinalityCountSlot::B,
        source(1),
    );
    let encoded = header.encode();
    assert_eq!(encoded.len(), 192);
    assert!(encoded.len() <= 256);
    assert_eq!(
        CardinalityGenerationHeader::decode(&encoded).unwrap(),
        header
    );
    assert_eq!(header.validate_source(source(1)), Ok(()));
}

#[test]
fn generation_header_rejects_noncurrent_and_malformed_forms() {
    let encoded = CardinalityGenerationHeader::new(
        CardinalityGenerationId::INITIAL,
        CardinalityGenerationState::Ready,
        CardinalityCountSlot::A,
        source(2),
    )
    .encode();
    for end in 0..encoded.len() {
        assert!(CardinalityGenerationHeader::decode(&encoded[..end]).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(CardinalityGenerationHeader::decode(&trailing).is_err());
    let mut future = encoded.clone();
    future[8] = 2;
    assert_eq!(
        CardinalityGenerationHeader::decode(&future)
            .unwrap_err()
            .class(),
        ErrorClass::IncompatiblePersistedFormat,
    );
    let mut corrupt = encoded;
    corrupt[40] ^= 0x80;
    assert!(CardinalityGenerationHeader::decode(&corrupt).is_err());
}

#[test]
fn source_validation_reports_each_stale_identity_dimension() {
    let header = CardinalityGenerationHeader::new(
        CardinalityGenerationId::INITIAL,
        CardinalityGenerationState::Ready,
        CardinalityCountSlot::A,
        source(3),
    );
    let mut expected = source(3);
    expected.database_incarnation = DatabaseIncarnationId::for_tests(99);
    assert_eq!(
        header.validate_source(expected),
        Err(CardinalitySourceMismatch::DatabaseIncarnation)
    );
    let mut expected = source(3);
    expected.store_allocation_fingerprint[0] ^= 1;
    assert_eq!(
        header.validate_source(expected),
        Err(CardinalitySourceMismatch::StoreAllocation)
    );
    let mut expected = source(3);
    expected.accepted_root = None;
    assert_eq!(
        header.validate_source(expected),
        Err(CardinalitySourceMismatch::AcceptedRoot)
    );
    let mut expected = source(3);
    expected.accepted_index_count += 1;
    assert_eq!(
        header.validate_source(expected),
        Err(CardinalitySourceMismatch::AcceptedIndexSet)
    );
    let mut expected = source(3);
    expected.fold_watermark = FoldWatermark::initial();
    assert_eq!(
        header.validate_source(expected),
        Err(CardinalitySourceMismatch::FoldWatermark)
    );
}

#[test]
fn allocation_and_index_set_fingerprints_are_canonical_and_strict() {
    assert_ne!(
        store_allocation_fingerprint(allocations(0)).unwrap(),
        store_allocation_fingerprint(allocations(1)).unwrap()
    );
    let first = IndexId::new_with_generation(EntityTag::new(1), 2, 3);
    let second = IndexId::new_with_generation(EntityTag::new(1), 1, 3);
    assert_eq!(
        accepted_index_set_fingerprint([first, second]).unwrap(),
        accepted_index_set_fingerprint([second, first]).unwrap()
    );
    assert!(accepted_index_set_fingerprint([first, first]).is_err());
    assert!(
        CardinalitySourceIdentity::derive(
            DatabaseIncarnationId::for_tests(1),
            allocations(0),
            None,
            [first],
            FoldWatermark::initial(),
        )
        .is_err()
    );
}

#[test]
fn count_key_and_value_forms_are_exact_and_collision_checked() {
    let entity_digest = CardinalityCountDigest::for_entity(EntityTag::new(42));
    assert_eq!(
        entity_digest.as_bytes(),
        [
            0x41, 0x00, 0x22, 0x00, 0x2d, 0xea, 0x85, 0xb1, 0xac, 0x3d, 0x8a, 0x83, 0x2b, 0x54,
            0x68, 0x37, 0x7c, 0x6e, 0xdf, 0x17, 0x2d, 0xc2, 0x9a, 0x37, 0x10, 0x64, 0x85, 0x85,
            0xcf, 0x6d, 0x2a, 0x23,
        ],
        "entity count-key preimage is persisted format",
    );
    let prefix = UserIndexPrefixCardinalityKey::new(
        IndexId::new_with_generation(EntityTag::new(42), 3, 7),
        vec![vec![1, 2], vec![3]],
    );
    let prefix_digest = CardinalityCountDigest::for_user_index_prefix(
        prefix.index_id(),
        prefix.prefix_components(),
    )
    .unwrap();
    assert_eq!(
        prefix_digest.as_bytes(),
        [
            0xe1, 0xb8, 0x75, 0xac, 0x03, 0x55, 0x4c, 0x2f, 0x1c, 0xce, 0xdb, 0xc0, 0x78, 0x09,
            0x42, 0x42, 0x94, 0x9a, 0xae, 0xa3, 0x59, 0x47, 0x71, 0x41, 0xcd, 0x66, 0xe9, 0xe9,
            0xa6, 0xfc, 0xf9, 0xd7,
        ],
        "user-index prefix count-key preimage is persisted format",
    );
    let borrowed = [
        prefix.prefix_components()[0].as_slice(),
        prefix.prefix_components()[1].as_slice(),
    ];
    assert_eq!(
        CardinalityCountDigest::for_user_index_prefix(prefix.index_id(), &borrowed).unwrap(),
        prefix_digest,
    );
    // Qualify every permitted prefix width and the maximum component size.
    let maximum = vec![vec![0xab; IndexKey::MAX_COMPONENT_SIZE]; MAX_INDEX_FIELDS];
    let borrowed: Vec<_> = maximum.iter().map(Vec::as_slice).collect();
    for width in 1..=MAX_INDEX_FIELDS {
        assert_eq!(
            CardinalityCountDigest::for_user_index_prefix(prefix.index_id(), &maximum[..width])
                .unwrap(),
            CardinalityCountDigest::for_user_index_prefix(prefix.index_id(), &borrowed[..width])
                .unwrap(),
        );
    }
    assert_ne!(entity_digest, prefix_digest);
    let record =
        CardinalityCountRecord::new(CardinalityGenerationId::INITIAL, prefix_digest, u64::MAX)
            .unwrap();
    let encoded = record.encode();
    assert_eq!(encoded.len(), 57);
    assert!(encoded.len() <= 64);
    assert_eq!(CardinalityCountRecord::decode(&encoded).unwrap(), record);
    assert!(CardinalityCountRecord::decode(&encoded[..encoded.len() - 1]).is_err());
    let mut trailing = encoded.to_vec();
    trailing.push(0);
    assert!(CardinalityCountRecord::decode(&trailing).is_err());
    let mut future = encoded;
    future[8] = 2;
    assert_eq!(
        CardinalityCountRecord::decode(&future).unwrap_err().class(),
        ErrorClass::IncompatiblePersistedFormat,
    );
    assert_eq!(
        record.validate_identity(CardinalityGenerationId::INITIAL, prefix_digest),
        Ok(u64::MAX)
    );
    assert_eq!(
        record.validate_identity(CardinalityGenerationId::try_new(2).unwrap(), prefix_digest),
        Err(CardinalityCountRecordMismatch::Generation)
    );
    assert_eq!(
        record.validate_identity(CardinalityGenerationId::INITIAL, entity_digest),
        Err(CardinalityCountRecordMismatch::DigestCollision)
    );
    assert!(
        CardinalityCountRecord::new(CardinalityGenerationId::INITIAL, entity_digest, 0).is_err()
    );
}

#[test]
fn count_key_rejects_empty_oversized_and_overwide_prefixes() {
    let index = IndexId::new_with_generation(EntityTag::new(1), 1, 1);
    for components in [
        Vec::new(),
        vec![Vec::new()],
        vec![vec![0; IndexKey::MAX_COMPONENT_SIZE + 1]],
        vec![vec![1], vec![2], vec![3], vec![4], vec![5]],
    ] {
        let borrowed: Vec<_> = components.iter().map(Vec::as_slice).collect();
        let borrowed_error =
            CardinalityCountDigest::for_user_index_prefix(index, &borrowed).unwrap_err();
        let owned_error =
            CardinalityCountDigest::for_user_index_prefix(index, &components).unwrap_err();
        assert_eq!(borrowed_error.class(), owned_error.class());
        let prefix = UserIndexPrefixCardinalityKey::new(index, components);
        assert!(
            CardinalityCountDigest::for_user_index_prefix(
                prefix.index_id(),
                prefix.prefix_components(),
            )
            .is_err()
        );
    }
}

#[test]
fn build_cursor_round_trips_maximum_row_and_index_checkpoints() {
    let row = CardinalityBuildCursor::new(
        CardinalityGenerationId::INITIAL,
        CardinalityCountSlot::A,
        source(4),
        CardinalityBuildPhase::Rows,
        Some(CardinalityBuildCheckpoint::Row(
            RawDataStoreKey::from_persisted_bytes(vec![1; RawDataStoreKey::MAX_STORED_SIZE_USIZE]),
        )),
        CardinalityBuildTotals::new(1, 2, 3, 4),
    )
    .unwrap();
    let row_encoded = row.encode().unwrap();
    assert_eq!(CardinalityBuildCursor::decode(&row_encoded).unwrap(), row);

    let index = CardinalityBuildCursor::new(
        CardinalityGenerationId::try_new(2).unwrap(),
        CardinalityCountSlot::B,
        source(5),
        CardinalityBuildPhase::Indexes,
        Some(CardinalityBuildCheckpoint::Index(
            RawIndexStoreKey::from_persisted_bytes(vec![1; IndexKey::MAX_STORED_SIZE_USIZE]),
        )),
        CardinalityBuildTotals::new(u64::MAX, u64::MAX, u64::MAX, u64::MAX),
    )
    .unwrap();
    let index_encoded = index.encode().unwrap();
    assert!(index_encoded.len() <= MAX_CARDINALITY_BUILD_CURSOR_BYTES);
    assert_eq!(
        CardinalityBuildCursor::decode(&index_encoded).unwrap(),
        index
    );
}

#[test]
fn build_cursor_rejects_phase_mismatch_corruption_and_noncurrent_version() {
    assert!(
        CardinalityBuildCursor::new(
            CardinalityGenerationId::INITIAL,
            CardinalityCountSlot::A,
            source(6),
            CardinalityBuildPhase::Rows,
            Some(CardinalityBuildCheckpoint::Index(
                RawIndexStoreKey::from_persisted_bytes(vec![1; IndexKey::MIN_STORED_SIZE_USIZE]),
            )),
            CardinalityBuildTotals::default(),
        )
        .is_err()
    );
    let cursor = CardinalityBuildCursor::new(
        CardinalityGenerationId::INITIAL,
        CardinalityCountSlot::A,
        source(6),
        CardinalityBuildPhase::Rows,
        None,
        CardinalityBuildTotals::default(),
    )
    .unwrap();
    let encoded = cursor.encode().unwrap();
    for end in 0..encoded.len() {
        assert!(CardinalityBuildCursor::decode(&encoded[..end]).is_err());
    }
    let mut future = encoded.clone();
    future[8] = 2;
    assert_eq!(
        CardinalityBuildCursor::decode(&future).unwrap_err().class(),
        ErrorClass::IncompatiblePersistedFormat,
    );
    let mut corrupt = encoded;
    corrupt[50] ^= 1;
    assert!(CardinalityBuildCursor::decode(&corrupt).is_err());
}

#[test]
fn build_cursor_rejects_authenticated_checkpoint_length_mismatches() {
    let cursor = CardinalityBuildCursor::new(
        CardinalityGenerationId::INITIAL,
        CardinalityCountSlot::A,
        source(6),
        CardinalityBuildPhase::Rows,
        None,
        CardinalityBuildTotals::default(),
    )
    .unwrap();
    let encoded = cursor.encode().unwrap();
    // Valid fingerprints must not mask truncated, oversized or unused
    // checkpoint bytes inside the admitted record envelope.
    for (declared_len, extra) in [(1_u32, None), (u32::MAX, None), (0, Some(0))] {
        let mut malformed = encoded[..CARDINALITY_BUILD_CURSOR_FIXED_BODY_BYTES].to_vec();
        malformed[CARDINALITY_BUILD_CURSOR_FIXED_BODY_BYTES - 4..]
            .copy_from_slice(&declared_len.to_be_bytes());
        malformed.extend(extra);
        let checksum = fingerprint(CARDINALITY_BUILD_CURSOR_FINGERPRINT_DOMAIN, &malformed);
        malformed.extend_from_slice(&checksum);
        assert_eq!(
            CardinalityBuildCursor::decode(&malformed)
                .unwrap_err()
                .class(),
            ErrorClass::Corruption,
        );
    }
}

#[test]
fn generation_identity_and_count_arithmetic_are_checked() {
    assert!(CardinalityGenerationId::try_new(0).is_err());
    assert_eq!(
        CardinalityGenerationId::INITIAL
            .checked_next()
            .unwrap()
            .get(),
        2
    );
    assert!(
        CardinalityGenerationId::try_new(u64::MAX)
            .unwrap()
            .checked_next()
            .is_err()
    );
}
