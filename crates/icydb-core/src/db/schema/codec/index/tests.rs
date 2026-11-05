use super::*;
use crate::db::schema::AcceptedFieldKind;

#[test]
fn index_key_decode_rejects_width_before_reading_component_payloads() {
    let mut writer = SnapshotWriter::new();
    writer.push_len(MAX_INDEX_FIELDS + 1).unwrap();
    writer.push_bytes(&[0xa1, 0xb2, 0xc3, 0xd4, 0xe5]);
    let bytes = writer.finish().unwrap();
    let mut reader = SnapshotReader::new(&bytes);
    let error = decode_index_key(&mut reader).unwrap_err();
    assert_eq!(
        error.diagnostic(),
        InternalError::store_corruption().diagnostic()
    );
    assert_eq!(reader.read_u8().unwrap(), 0xa1);
}

fn path() -> PersistedIndexFieldPathSnapshot {
    PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["name".into()],
        AcceptedFieldKind::Text { max_len: None },
        false,
    )
}

#[test]
fn canonical_key_items_preserve_field_expression_and_mixed_order() {
    let field = PersistedIndexKeyItemSnapshot::FieldPath(path());
    let expression =
        PersistedIndexKeyItemSnapshot::Expression(Box::new(PersistedIndexExpressionSnapshot::new(
            PersistedIndexExpressionOp::Lower,
            path(),
            AcceptedFieldKind::Text { max_len: None },
            AcceptedFieldKind::Text { max_len: None },
            "expr:v1:LOWER(name)".into(),
        )));
    for items in [
        vec![field.clone()],
        vec![expression.clone()],
        vec![field.clone(), expression.clone(), field],
        vec![expression; MAX_INDEX_FIELDS],
    ] {
        let key = PersistedIndexKeySnapshot::new(items);
        let mut writer = SnapshotWriter::new();
        encode_index_key(&mut writer, &key).unwrap();
        let bytes = writer.finish().unwrap();
        let mut reader = SnapshotReader::new(&bytes);
        assert_eq!(decode_index_key(&mut reader).unwrap(), key);
        reader.finish().unwrap();
        // Every proper prefix rejects; no partial key can escape admission.
        for end in 0..bytes.len() {
            assert!(decode_index_key(&mut SnapshotReader::new(&bytes[..end])).is_err());
        }
        let mut unknown_item = bytes;
        unknown_item[4] = u8::MAX;
        assert!(decode_index_key(&mut SnapshotReader::new(&unknown_item)).is_err());
    }
}

#[test]
fn canonical_key_item_width_rejects_before_allocation_or_emission() {
    let item = PersistedIndexKeyItemSnapshot::FieldPath(path());
    let mut writer = SnapshotWriter::new();
    assert!(
        encode_index_key(
            &mut writer,
            &PersistedIndexKeySnapshot::new(vec![item; MAX_INDEX_FIELDS + 1])
        )
        .is_err()
    );
    let count = u32::try_from(MAX_INDEX_FIELDS + 1).unwrap().to_be_bytes();
    assert!(decode_index_key(&mut SnapshotReader::new(&count)).is_err());
}
