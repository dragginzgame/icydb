//! Shared images keep both semantic sides and all accepted-row validation.

use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        commit::prepare::{
            CommitRowImages, decode_commit_marker_rows_for_preflight,
            decode_commit_marker_structural_slots,
        },
        data::{
            CanonicalSlotReader, DecodedDataStoreKey, RawRow, StructuralRowContract,
            canonical_row_from_runtime_value_source_with_accepted_contract,
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedRowLayoutRuntimeContract,
            AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId,
            FieldStorageDecode, LeafCodec, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            ScalarCodec, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
            empty_accepted_enum_catalog_for_tests,
        },
    },
    error::ErrorClass,
    types::EntityTag,
    value::Value,
};
use std::{borrow::Cow, cell::Cell, ptr};

fn contract() -> StructuralRowContract {
    let fields = [
        ("id", AcceptedFieldKind::Nat64, ScalarCodec::Nat64),
        (
            "payload",
            AcceptedFieldKind::Text { max_len: Some(128) },
            ScalarCodec::Text,
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(slot, (name, kind, codec))| {
        PersistedFieldSnapshot::new_initial(
            FieldId::new(u32::try_from(slot).unwrap() + 1),
            name.to_string(),
            SchemaFieldSlot::new(u16::try_from(slot).unwrap()),
            kind,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(codec),
        )
    })
    .collect::<Vec<_>>();
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::CommitImages".to_string(),
        "CommitImages".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ));
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        descriptor.row_decode_contract(catalog),
    )
}

fn row(contract: &StructuralRowContract, payload: &str) -> RawRow {
    let values = [Value::Nat64(7), Value::Text(payload.to_string())];
    canonical_row_from_runtime_value_source_with_accepted_contract(contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row()
}

fn key(id: u64) -> DecodedDataStoreKey {
    DecodedDataStoreKey::new_primary_key_value(
        EntityTag::new(7),
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(id)),
    )
}

#[test]
fn identical_images_copy_and_validate_once_but_keep_both_sides() {
    let contract = contract();
    let row = row(&contract, "same");
    let images = CommitRowImages::from_bytes(Some(row.as_bytes()), Some(row.as_bytes())).unwrap();
    let (before, after) = images.as_refs();
    assert!(ptr::eq(before.unwrap(), after.unwrap()));
    let key = key(7);
    let validations = Cell::new(0);
    let decoded = images
        .try_map_ref(|row| {
            validations.set(validations.get() + 1);
            decode_commit_marker_structural_slots(&key, row, &contract)
        })
        .unwrap();
    assert_eq!(validations.get(), 1);
    let (before, after) = decoded.as_refs();
    assert!(ptr::eq(before.unwrap(), after.unwrap()));
    assert_eq!(
        before.unwrap().required_value_by_contract(1).unwrap(),
        Value::Text("same".into())
    );
    assert_eq!(
        after.unwrap().required_value_by_contract(0).unwrap(),
        Value::Nat64(7)
    );
}

#[test]
fn different_images_and_absent_sides_preserve_their_values() {
    let contract = contract();
    let before = row(&contract, "before");
    let after = row(&contract, "after");
    let key = key(7);
    for (old, new) in [
        (Some(before.as_bytes()), Some(after.as_bytes())),
        (Some(before.as_bytes()), None),
        (None, Some(after.as_bytes())),
        (None, None),
    ] {
        let images = CommitRowImages::from_bytes(old, new).unwrap();
        let decoded = decode_commit_marker_rows_for_preflight(&key, &images, &contract).unwrap();
        let (old_slots, new_slots) = decoded.as_refs();
        assert_eq!(old_slots.is_some(), old.is_some());
        assert_eq!(new_slots.is_some(), new.is_some());
        if let Some(slots) = old_slots {
            assert_eq!(
                slots.required_value_by_contract(1).unwrap(),
                Value::Text("before".into())
            );
        }
        if let Some(slots) = new_slots {
            assert_eq!(
                slots.required_value_by_contract(1).unwrap(),
                Value::Text("after".into())
            );
        }
    }
}

#[test]
fn shared_invalid_images_and_wrong_primary_keys_still_reject() {
    let contract = contract();
    let valid = row(&contract, "valid");
    for (old, new, key) in [
        (&[u8::MAX][..], &[u8::MAX][..], key(7)),
        (valid.as_bytes(), &[u8::MAX][..], key(7)),
        (valid.as_bytes(), valid.as_bytes(), key(8)),
    ] {
        let images = CommitRowImages::from_bytes(Some(old), Some(new)).unwrap();
        let error = decode_commit_marker_rows_for_preflight(&key, &images, &contract)
            .err()
            .expect("invalid images must reject the complete pair");
        assert_eq!(error.class(), ErrorClass::Corruption);
    }
}

#[test]
fn oversized_images_reject_before_shared_or_distinct_construction() {
    let oversized = vec![0; MAX_ROW_BYTES as usize + 1];
    for (before, after) in [
        (Some(oversized.as_slice()), Some(oversized.as_slice())),
        (Some(oversized.as_slice()), None),
        (None, Some(oversized.as_slice())),
    ] {
        let error = CommitRowImages::from_bytes(before, after)
            .err()
            .expect("oversized images must reject");
        assert_eq!(error.class(), ErrorClass::Unsupported);
    }
}

// Observe the maintained construction owner, not a second sizing implementation.
// The count is successful reader constructions, not instructions or heap usage.
fn prepared_image_work(
    contract: &StructuralRowContract,
    before: Option<&RawRow>,
    after: Option<&RawRow>,
) -> (usize, usize) {
    let images =
        CommitRowImages::from_bytes(before.map(RawRow::as_bytes), after.map(RawRow::as_bytes))
            .unwrap();
    let mut payload_bytes = 0;
    let mut readers = 0;
    let key = key(7);
    let decoded = images
        .try_map_ref(|image| {
            payload_bytes += image.as_bytes().len();
            readers += 1;
            decode_commit_marker_structural_slots(&key, image, contract)
        })
        .unwrap();
    let (old, new) = decoded.as_refs();
    assert_eq!(old.is_some(), before.is_some());
    assert_eq!(new.is_some(), after.is_some());
    (payload_bytes, readers)
}

#[test]
fn mixed_replay_images_obey_per_row_acquisition_and_preparation_bounds() {
    let contract = contract();
    let large = "x".repeat(120);
    let transitions = [
        (None, Some(row(&contract, &large))),
        (Some(row(&contract, "x")), Some(row(&contract, &large))),
        (Some(row(&contract, &large)), Some(row(&contract, "x"))),
        (Some(row(&contract, &large)), None),
        (Some(row(&contract, "same")), Some(row(&contract, "same"))),
    ];
    let row_len = |row: &Option<RawRow>| row.as_ref().map_or(0, |row| row.as_bytes().len());
    let original_before_bytes: usize = transitions.iter().map(|(old, _)| row_len(old)).sum();
    let final_bytes: usize = transitions.iter().map(|(_, new)| row_len(new)).sum();
    let acquisition_bound: usize = transitions
        .iter()
        .map(|(old, new)| row_len(old).max(row_len(new)))
        .sum();
    let predecessor_work: Vec<_> = transitions
        .iter()
        .map(|(old, new)| prepared_image_work(&contract, old.as_ref(), new.as_ref()))
        .collect();
    let mut largest_acquisition = 0;

    // Exhaust all image combinations, a superset of row-prefix publication.
    // This checks the image owner only; it does not simulate index or store state.
    for applied_mask in 0..(1usize << transitions.len()) {
        let mut acquired_before_bytes = 0;
        for (ordinal, (old, new)) in transitions.iter().enumerate() {
            let before = if applied_mask & (1 << ordinal) == 0 {
                old
            } else {
                new
            };
            acquired_before_bytes += row_len(before);
            let actual = prepared_image_work(&contract, before.as_ref(), new.as_ref());
            assert!(actual.0 <= predecessor_work[ordinal].0);
            assert!(actual.1 <= predecessor_work[ordinal].1);
        }
        assert!(acquired_before_bytes <= acquisition_bound);
        largest_acquisition = largest_acquisition.max(acquired_before_bytes);
    }

    assert_eq!(largest_acquisition, acquisition_bound);
    assert!(
        largest_acquisition > original_before_bytes.max(final_bytes),
        "the larger whole-batch endpoint is not a bound for mixed row states",
    );
}
