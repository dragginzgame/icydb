//! Borrowed views preserve the complete codec contract and raw byte ownership.

use super::*;
use crate::db::index::decode_index_key_frame;

#[test]
fn borrowed_frames_preserve_components_and_complete_primary_key_bytes() {
    let composite = PrimaryKeyValue::Composite(
        CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Nat64(u64::MAX),
        ])
        .unwrap(),
    );
    for kind in [IndexKeyKind::User, IndexKeyKind::System] {
        for count in 0..=MAX_INDEX_FIELDS {
            for primary in [
                PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
                composite,
            ] {
                let components = (0..count)
                    .map(|index| encode_component(&Value::Text(format!("component_{index}"))))
                    .collect::<Vec<_>>();
                let key = IndexKey::new_from_components_with_primary_key_value(
                    &index_id(),
                    kind,
                    &components,
                    &primary,
                )
                .unwrap();
                let raw = key.to_raw().unwrap();
                let frame = decode_index_key_frame(&raw).unwrap();
                assert_eq!(frame.key_kind(), kind);
                assert_eq!(frame.index_id(), index_id());
                assert_eq!(frame.components().len(), count);
                for (borrowed, expected) in frame.components().iter().zip(&components) {
                    assert_eq!(*borrowed, expected);
                    let offset = borrowed.as_ptr().addr() - raw.as_bytes().as_ptr().addr();
                    assert!(std::ptr::eq(
                        borrowed.as_ptr(),
                        raw.as_bytes()[offset..offset + borrowed.len()].as_ptr()
                    ));
                }
                let (value, bytes) = frame.primary_key_value_and_bytes().unwrap();
                assert_eq!(value, primary);
                assert_eq!(bytes, key.primary_key.as_slice());
                let offset = raw.as_bytes().len() - bytes.len();
                assert!(std::ptr::eq(
                    bytes.as_ptr(),
                    raw.as_bytes()[offset..].as_ptr()
                ));
            }
        }
    }
    let raw = maximum_size_key(1, 0xff).to_raw().unwrap();
    assert_eq!(raw.as_bytes().len(), IndexKey::MAX_STORED_SIZE_USIZE);
    assert_eq!(
        decode_index_key_frame(&raw).unwrap().components().len(),
        MAX_INDEX_FIELDS
    );
}

#[test]
fn borrowed_frames_preserve_strict_framing_and_primary_suffix_refusal() {
    let key = IndexKey::new_from_components_with_primary_key_value(
        &index_id(),
        IndexKeyKind::User,
        &[b"component".to_vec()],
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
    )
    .unwrap();
    let raw = key.to_raw().unwrap();
    let mut cases = (0..raw.as_bytes().len())
        .map(|length| raw.as_bytes()[..length].to_vec())
        .collect::<Vec<_>>();
    let mut trailing = raw.as_bytes().to_vec();
    trailing.push(0);
    cases.push(trailing);
    for (offset, value) in [
        (0, 0xff),
        (len_offset(), u8::try_from(MAX_INDEX_FIELDS + 1).unwrap()),
        (first_component_len_offset(), 0),
        (first_component_len_offset() + 1, 0),
    ] {
        let mut bytes = raw.as_bytes().to_vec();
        bytes[offset] = value;
        cases.push(bytes);
    }
    cases.push(vec![0; IndexKey::MAX_STORED_SIZE_USIZE + 1]);
    for bytes in cases {
        let raw = RawIndexStoreKey::from_persisted_bytes(bytes);
        assert_eq!(
            decode_index_key_frame(&raw).err(),
            IndexKey::try_from_raw(&raw).err()
        );
    }
    let mut bytes = raw.as_bytes().to_vec();
    let suffix = bytes.len() - key.primary_key.len();
    bytes[suffix] = 0xff;
    let raw = RawIndexStoreKey::from_persisted_bytes(bytes);
    let frame = decode_index_key_frame(&raw).unwrap();
    assert_eq!(
        frame.primary_key_value_and_bytes().err(),
        IndexKey::primary_key_value_and_bytes_from_raw(&raw).err()
    );
    assert!(frame.primary_key_value_and_bytes().is_err());
}
