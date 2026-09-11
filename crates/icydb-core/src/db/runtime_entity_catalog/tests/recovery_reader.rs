//! Recovery borrows complete final images while returning owned witness rows.

use super::{DATA_STORE, ENTITY_PATH, test_db};
use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        commit::CommitRowOp,
        data::{DecodedDataStoreKey, RawRow},
        index::StructuralPrimaryRowReader,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        runtime_entity_catalog::CanonicalCommitReader,
    },
    error::ErrorClass,
    types::{EntityTag, Ulid},
};
use ic_memory::ic_stable_structures::Storable;
use std::ptr;

fn key(id: u128) -> DecodedDataStoreKey {
    DecodedDataStoreKey::new_primary_key_value(
        EntityTag::new(91),
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Ulid(Ulid::from_u128(id))),
    )
}

fn op(id: u128, after: Option<Vec<u8>>) -> CommitRowOp {
    CommitRowOp::new(ENTITY_PATH, key(id).to_raw().unwrap(), None, after, [0; 16])
}

#[test]
fn final_view_borrows_operation_keys_and_images() {
    let db = test_db();
    let ops = [op(1, Some(vec![1, 2, 3])), op(2, None)];
    let reader = CanonicalCommitReader::from_row_ops(&db, &ops).unwrap();
    for op in &ops {
        let (retained_key, retained_row) = reader.batch_final_rows.get_key_value(&op.key).unwrap();
        assert!(ptr::eq(*retained_key, &raw const op.key));
        match (retained_row, op.after.as_deref()) {
            (Some(retained), Some(source)) => assert!(ptr::eq(*retained, source)),
            (None, None) => {}
            _ => panic!("the final view must preserve operation presence"),
        }
    }
}

#[test]
fn final_rows_and_tombstones_override_canonical_storage_and_reads_are_owned() {
    let db = test_db();
    // This reader routes opaque bytes; accepted-row decoding remains with
    // commit preparation. Distinct payloads expose fallback mistakes directly.
    for id in 1..=3 {
        DATA_STORE.with_borrow_mut(|store| {
            store.insert_raw_for_test(
                key(id).to_raw().unwrap(),
                RawRow::try_new(vec![10]).unwrap(),
            );
        });
    }
    let ops = [op(1, Some(vec![20])), op(2, None)];
    let reader = CanonicalCommitReader::from_row_ops(&db, &ops).unwrap();
    assert!(reader.has_primary_row_override(&key(1)).unwrap());
    assert!(reader.has_primary_row_override(&key(2)).unwrap());
    assert!(!reader.has_primary_row_override(&key(3)).unwrap());

    let first = reader.read_primary_row(&key(1)).unwrap().unwrap();
    assert_eq!(first.as_bytes(), &[20]);
    assert_ne!(
        first.as_bytes().as_ptr(),
        ops[0].after.as_ref().unwrap().as_ptr()
    );
    let mut returned = first.into_bytes();
    returned[0] = 99;
    assert_eq!(
        reader
            .read_primary_row(&key(1))
            .unwrap()
            .unwrap()
            .as_bytes(),
        &[20]
    );
    assert!(reader.read_primary_row(&key(2)).unwrap().is_none());
    // Heap-only registration has no canonical journal authority. Missing
    // overrides must preserve that failure, not return a live row or absence.
    for id in [3, 4] {
        let error = reader.read_primary_row(&key(id)).unwrap_err();
        assert_eq!(error.class(), ErrorClass::Corruption);
    }
}

#[test]
fn duplicate_final_keys_reject_including_tombstones() {
    let db = test_db();
    for after in [None, Some(vec![1])] {
        let ops = [op(1, after), op(1, None)];
        let error = CanonicalCommitReader::from_row_ops(&db, &ops)
            .err()
            .expect("duplicate final rows must reject the complete reader");
        assert_eq!(error.class(), ErrorClass::Corruption);
    }
}

#[test]
fn final_view_enforces_existing_raw_row_limit_without_copying_payloads() {
    let db = test_db();
    let ops = [op(1, Some(vec![0; MAX_ROW_BYTES as usize]))];
    let reader = CanonicalCommitReader::from_row_ops(&db, &ops).unwrap();
    let retained = reader.batch_final_rows.get(&ops[0].key).unwrap().unwrap();
    assert!(ptr::eq(retained, ops[0].after.as_deref().unwrap()));

    let oversized = [op(1, Some(vec![0; MAX_ROW_BYTES as usize + 1]))];
    let error = CanonicalCommitReader::from_row_ops(&db, &oversized)
        .err()
        .expect("oversized final rows must reject at reader construction");
    assert_eq!(error.class(), ErrorClass::Unsupported);
}
