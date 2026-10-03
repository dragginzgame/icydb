//! Current enum frames and accepted nesting have the same borrowed/owned boundary.

use super::*;
use crate::db::data::structural_field::{
    binary::{TAG_UNIT, push_binary_list_len, push_binary_map_len, push_binary_text},
    value_storage::{
        ValueStorageView, decode_structural_value_storage_bytes,
        validate_structural_value_storage_bytes,
    },
};

fn assert_readers_reject(bytes: &[u8]) {
    assert!(decode_canonical_value_storage_bytes(bytes).is_err());
    assert!(decode_structural_value_storage_bytes(bytes).is_err());
    assert!(ValueStorageView::from_raw_validated(bytes).is_err());
    assert!(validate_structural_value_storage_bytes(bytes).is_err());
}

#[test]
fn canonical_traversal_selects_scalars_and_enum_leaves_in_both_sibling_orders() {
    for state in [
        canonical_enum(None),
        canonical_enum(Some(CanonicalValue::List(vec![
            canonical_enum(Some(CanonicalValue::Nat64(5))),
            CanonicalValue::Null,
        ]))),
    ] {
        for state_first in [false, true] {
            let mut entries = vec![
                (
                    CanonicalValue::Text("rank".into()),
                    CanonicalValue::Nat64(9),
                ),
                (CanonicalValue::Text("state".into()), state.clone()),
            ];
            if state_first {
                entries.reverse();
            }
            let root = CanonicalValue::Map(entries);
            let bytes = encode_canonical_value_storage_bytes(&root).unwrap();
            assert_eq!(decode_structural_value_storage_bytes(&bytes).unwrap(), root);
            for _ in 0..2 {
                let view = ValueStorageView::from_raw_validated(&bytes).unwrap();
                for (name, expected) in [
                    (b"rank".as_slice(), CanonicalValue::Nat64(9)),
                    (b"state".as_slice(), state.clone()),
                ] {
                    let leaf = view.map_text_key_bytes(name).unwrap().unwrap();
                    assert_eq!(
                        decode_structural_value_storage_bytes(leaf.as_bytes()).unwrap(),
                        expected
                    );
                }
                assert!(view.map_text_key_bytes(b"missing").unwrap().is_none());
            }
        }
    }
}

#[test]
fn canonical_traversal_counts_each_list_map_and_enum_value_once() {
    for shape in 0..3 {
        let mut value = CanonicalValue::Unit;
        for _ in 0..MAX_ACCEPTED_RECURSIVE_DEPTH - 2 {
            value = match shape {
                0 => CanonicalValue::List(vec![value]),
                1 => CanonicalValue::Map(vec![(CanonicalValue::Text("leaf".into()), value)]),
                _ => canonical_enum(Some(value)),
            };
        }
        let root = CanonicalValue::Map(vec![(CanonicalValue::Text("leaf".into()), value.clone())]);
        let bytes = encode_canonical_value_storage_bytes(&root).unwrap();
        assert_eq!(decode_canonical_value_storage_bytes(&bytes).unwrap(), root);
        assert_eq!(decode_structural_value_storage_bytes(&bytes).unwrap(), root);
        let view = ValueStorageView::from_raw_validated(&bytes).unwrap();
        let leaf = view.map_text_key_bytes(b"leaf").unwrap().unwrap();
        assert_eq!(
            decode_structural_value_storage_bytes(leaf.as_bytes()).unwrap(),
            value
        );
        let excessive = CanonicalValue::List(vec![root]);
        assert!(encode_canonical_value_storage_bytes(&excessive).is_err());
        // Construct an over-limit current frame without the encoder's admission.
        let mut bytes = Vec::new();
        push_binary_list_len(&mut bytes, 1);
        bytes.extend_from_slice(
            &encode_canonical_value_storage_bytes(match &excessive {
                CanonicalValue::List(values) => &values[0],
                _ => unreachable!(),
            })
            .unwrap(),
        );
        assert_readers_reject(&bytes);
    }
}

#[test]
fn canonical_traversal_rejects_incomplete_enum_frames_and_excess_payloads() {
    for value in [
        canonical_enum(None),
        canonical_enum(Some(CanonicalValue::Nat64(5))),
    ] {
        let bytes = encode_canonical_value_storage_bytes(&value).unwrap();
        for end in 0..bytes.len() {
            assert_readers_reject(&bytes[..end]);
        }
        let mut trailing = bytes.clone();
        trailing.push(TAG_UNIT);
        assert_readers_reject(&trailing);
        for index in [1, 5, 9, 10] {
            let mut malformed = bytes.clone();
            if index == 9 {
                malformed[index] = 0xff;
            } else if index == 10 {
                malformed[10..14].copy_from_slice(&u32::MAX.to_be_bytes());
            } else {
                malformed[index..index + 4].fill(0);
            }
            assert_readers_reject(&malformed);
        }
        // An invalid enum sibling cannot be hidden by selecting a valid scalar.
        let mut map = Vec::new();
        push_binary_map_len(&mut map, 2);
        push_binary_text(&mut map, "rank");
        map.extend_from_slice(
            &encode_canonical_value_storage_bytes(&CanonicalValue::Nat64(9)).unwrap(),
        );
        push_binary_text(&mut map, "state");
        map.extend_from_slice(&bytes[..bytes.len() - 1]);
        assert_readers_reject(&map);
    }
}
