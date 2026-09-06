//! Nested projection borrows reader-owned roots and emits independent keys.

use super::*;
use crate::{
    db::{
        data::{
            CanonicalSlotReader, RawRow, StructuralSlotReader,
            canonical_row_from_runtime_value_source_with_accepted_contract,
        },
        relation::reverse_index::{
            AcceptedNestedRelationSource, relation_target_raw_keys_for_source_slots,
        },
        schema::{AcceptedCompositeCatalog, PersistedRelationPathStepSnapshot},
    },
    value::Value,
};
use std::borrow::Cow;

fn fixture() -> (
    StructuralRowContract,
    AcceptedRelationInfo,
    ReverseRelationSourceInfo,
) {
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".into(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            vec![],
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "targets".into(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Text { max_len: None }),
                value: Box::new(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64))),
            },
            vec![],
            true,
            SchemaInsertDefault::None,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        ),
    ];
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "Source".into(),
        "Source".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(fields.iter().map(|f| (f.id(), f.slot())).collect()),
        fields,
    ));
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    let contract = StructuralRowContract::from_accepted_decode_contract(
        "Source",
        descriptor.row_decode_contract(catalog.clone()),
    );
    let mut edge = relation(1, AcceptedFieldKind::Nat64);
    edge.source = AcceptedRelationSource::Nested(AcceptedNestedRelationSource {
        root_slot: 1,
        steps: vec![
            PersistedRelationPathStepSnapshot::OptionalSome,
            PersistedRelationPathStepSnapshot::MapValues,
            PersistedRelationPathStepSnapshot::ListItems,
        ],
        value_catalog: catalog,
    });
    (
        contract,
        edge,
        ReverseRelationSourceInfo {
            path: "Source".into(),
            entity_tag: EntityTag::new(9),
        },
    )
}

fn row(contract: &StructuralRowContract, value: &Value) -> RawRow {
    let id = Value::Nat64(7);
    canonical_row_from_runtime_value_source_with_accepted_contract(contract, |slot| {
        Ok(Cow::Borrowed(if slot == 0 { &id } else { value }))
    })
    .unwrap()
    .into_raw_row()
}

fn payload(items: usize, key_bytes: usize) -> Value {
    Value::Map(
        (0..items)
            .map(|i| {
                (
                    Value::Text(format!("{i:04}{}", "x".repeat(key_bytes))),
                    Value::List(vec![Value::Nat64((i % 2) as u64)]),
                )
            })
            .collect(),
    )
}

#[test]
fn nested_root_projection_preserves_reader_and_charges_duplicates_before_dedup() {
    let (contract, edge, source) = fixture();
    let value = payload(3, 1024);
    let raw = row(&contract, &value);
    let mut projection = RelationProjectionBudget::default();
    let mut batch = RelationCommitBudget::default();
    let keys = {
        let reader =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&raw, &contract)
                .unwrap();
        let root = reader.required_value_by_contract_cow(1).unwrap();
        let keys = relation_target_raw_keys_for_source_slots(
            &reader,
            &source,
            &edge,
            &mut projection,
            &mut batch,
        )
        .unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(projection.raw_references, 3);
        assert_eq!(projection.traversal_work, 5);
        assert_eq!(batch.raw_references, 3);
        assert_eq!(batch.traversal_work, 5);
        assert_eq!(root.as_ref(), &value);
        assert!(std::ptr::eq(
            root.as_ref(),
            reader.required_value_by_contract_cow(1).unwrap().as_ref()
        ));
        assert_eq!(
            relation_target_raw_keys_for_source_slots(
                &reader,
                &source,
                &edge,
                &mut projection,
                &mut batch
            )
            .unwrap(),
            keys
        );
        assert_eq!(projection.raw_references, 6);
        assert_eq!(batch.traversal_work, 10);
        keys
    };
    // The output does not borrow the reader or the raw source row.
    drop(raw);
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1]);
}

#[test]
fn nested_root_projection_preserves_null_empty_and_budget_rejection() {
    let (contract, edge, source) = fixture();
    for value in [Value::Null, Value::Map(vec![])] {
        let raw = row(&contract, &value);
        let reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &contract).unwrap();
        let mut projection = RelationProjectionBudget::default();
        let mut batch = RelationCommitBudget::default();
        assert!(
            relation_target_raw_keys_for_source_slots(
                &reader,
                &source,
                &edge,
                &mut projection,
                &mut batch
            )
            .unwrap()
            .is_empty()
        );
        assert_eq!(projection.raw_references, 0);
        assert_eq!(
            reader.required_value_by_contract_cow(1).unwrap().as_ref(),
            &value
        );
    }
    let value = payload(3, 64);
    let raw = row(&contract, &value);
    let reader =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, &contract).unwrap();
    for (traversal_work, raw_references) in [
        (MAX_NESTED_RELATION_IMAGE_TRAVERSAL_WORK, 0),
        (0, MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES - 2),
    ] {
        let mut projection = RelationProjectionBudget {
            traversal_work,
            raw_references,
        };
        assert!(
            relation_target_raw_keys_for_source_slots(
                &reader,
                &source,
                &edge,
                &mut projection,
                &mut RelationCommitBudget::default()
            )
            .is_err()
        );
        assert_eq!(
            reader.required_value_by_contract_cow(1).unwrap().as_ref(),
            &value
        );
    }
    let keys = relation_target_raw_keys_for_source_slots(
        &reader,
        &source,
        &edge,
        &mut RelationProjectionBudget::default(),
        &mut RelationCommitBudget::default(),
    )
    .unwrap();
    assert_eq!(keys.len(), 2);
}
