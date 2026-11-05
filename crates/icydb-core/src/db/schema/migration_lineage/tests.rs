use super::*;

fn exact_head(byte: u8) -> ExpectedAcceptedHead {
    ExpectedAcceptedHead::Exact {
        revision: u64::from(byte),
        fingerprint: ExpectedSchemaFingerprint::from_bytes([byte; 32]),
    }
}

#[test]
fn accepted_head_encoders_preserve_bytes_and_prewrite_refusals() {
    use crate::error::{ErrorClass, ErrorOrigin};

    for (revision, fingerprint) in [(7, [9; 32]), (0, [9; 32]), (7, [0; 32])] {
        let head = ExpectedAcceptedHead::Exact {
            revision,
            fingerprint: ExpectedSchemaFingerprint::from_bytes(fingerprint),
        };
        let mut expected = vec![42];
        expected.extend_from_slice(&revision.to_be_bytes());
        expected.extend_from_slice(&fingerprint);
        let mut migration = LineageWriter::new();
        migration.push_u8(42);
        encode_head(&mut migration, &head).unwrap();
        assert_eq!(migration.finish().unwrap(), expected);

        let mut lineage = LineageWriter::new();
        lineage.push_u8(42);
        let result = encode_lineage_head(&mut lineage, &head);
        if revision == 0 || fingerprint == [0; 32] {
            let error = result.unwrap_err();
            assert_eq!(error.class(), ErrorClass::InvariantViolation);
            assert_eq!(error.origin(), ErrorOrigin::Store);
            assert_eq!(lineage.finish().unwrap(), vec![42]);
        } else {
            result.unwrap();
            assert_eq!(lineage.finish().unwrap(), expected);
        }
    }

    for encode in [encode_head, encode_lineage_head] {
        let mut writer = LineageWriter::new();
        writer.push_u8(42);
        let error = encode(&mut writer, &ExpectedAcceptedHead::Empty).unwrap_err();
        assert_eq!(error.class(), ErrorClass::InvariantViolation);
        assert_eq!(error.origin(), ErrorOrigin::Store);
        assert_eq!(writer.finish().unwrap(), vec![42]);
    }
}

#[test]
fn lineage_codec_round_trips_adopted_and_unadopted_current_form() {
    let store = TargetStoreIdentity::from_bytes([7; 32]);
    let mut catalog = AcceptedEntitySourceLineageCatalog::default();
    catalog
        .insert(
            store,
            EntityTag::new(1),
            AcceptedEntitySourceLineage::unadopted(exact_head(3))
                .expect("unadopted lineage should admit"),
        )
        .expect("entry should insert");
    catalog
        .insert(
            store,
            EntityTag::new(2),
            AcceptedEntitySourceLineage::adopted(
                exact_head(3),
                AcceptedEntitySourceVersion::try_new(4).expect("version should admit"),
                EntitySourceDigest::from_bytes([9; 32]),
            )
            .expect("adopted lineage should admit"),
        )
        .expect("entry should insert");

    let encoded =
        encode_entity_source_lineage_catalog(&catalog).expect("lineage catalog should encode");
    assert_eq!(
        decode_entity_source_lineage_catalog(&encoded).expect("lineage catalog should decode"),
        catalog,
    );
}

#[test]
fn lineage_codec_rejects_checksum_length_version_and_noncanonical_order() {
    let encoded =
        encode_entity_source_lineage_catalog(&AcceptedEntitySourceLineageCatalog::default())
            .expect("empty lineage should encode");
    for offset in [0, 8, 9, encoded.len() - 1] {
        let mut malformed = encoded.clone();
        malformed[offset] ^= 0x80;
        assert!(decode_entity_source_lineage_catalog(&malformed).is_err());
    }

    let store = TargetStoreIdentity::from_bytes([7; 32]);
    let mut catalog = AcceptedEntitySourceLineageCatalog::default();
    for entity in [1, 2] {
        catalog
            .insert(
                store,
                EntityTag::new(entity),
                AcceptedEntitySourceLineage::unadopted(exact_head(3))
                    .expect("lineage should admit"),
            )
            .expect("entry should insert");
    }
    let mut out_of_order =
        encode_entity_source_lineage_catalog(&catalog).expect("lineage catalog should encode");
    let body_len = out_of_order.len() - LINEAGE_CHECKSUM_BYTES;
    let record_bytes = 32 + 8 + 8 + 32 + 1;
    let first = LINEAGE_HEADER_BYTES;
    let second = first + record_bytes;
    for offset in 0..record_bytes {
        out_of_order.swap(first + offset, second + offset);
    }
    let checksum = crc32c(&out_of_order[..body_len]).to_be_bytes();
    out_of_order[body_len..].copy_from_slice(&checksum);
    assert!(decode_entity_source_lineage_catalog(&out_of_order).is_err());
}

#[test]
fn lineage_catalog_enforces_exact_entry_and_identity_bounds() {
    assert!(
        AcceptedEntitySourceLineage::adopted(
            exact_head(1),
            AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
            EntitySourceDigest::from_bytes([0; 32]),
        )
        .is_err(),
    );
    let store = TargetStoreIdentity::from_bytes([5; 32]);
    let lineage =
        AcceptedEntitySourceLineage::unadopted(exact_head(1)).expect("lineage should admit");
    let mut catalog = AcceptedEntitySourceLineageCatalog::default();
    assert!(
        catalog
            .insert(store, EntityTag::new(0), lineage.clone())
            .is_err()
    );
    for entity in 1..=MAX_ENTITY_SOURCE_LINEAGE_ENTRIES {
        catalog
            .insert(
                store,
                EntityTag::new(u64::try_from(entity).expect("entity should fit")),
                lineage.clone(),
            )
            .expect("boundary entry should insert");
    }
    assert_eq!(catalog.entries().len(), MAX_ENTITY_SOURCE_LINEAGE_ENTRIES);
    assert!(encode_entity_source_lineage_catalog(&catalog).is_ok());
    assert!(
        catalog
            .insert(
                store,
                EntityTag::new(
                    u64::try_from(MAX_ENTITY_SOURCE_LINEAGE_ENTRIES + 1)
                        .expect("entity should fit"),
                ),
                lineage,
            )
            .is_err(),
    );
}
