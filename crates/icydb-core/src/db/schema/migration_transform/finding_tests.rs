//! Evaluator failures must remain persistable without inventing field identities.

use super::*;
use crate::db::{
    data::{RawRow, SlotReader},
    key_taxonomy::PrimaryKeyComponent,
    schema::{
        AcceptedCompositeCatalog, AcceptedRowLayoutRuntimeContract, AcceptedSchemaRevision,
        AcceptedValueCatalogHandle, FieldStorageDecode, PersistedFieldSnapshot,
        PersistedSchemaMigrationFinding, PersistedSchemaSnapshot, SchemaInsertDefault,
        SchemaRowLayout, SchemaVersion, empty_accepted_enum_catalog_for_tests,
    },
};

fn contract(value_id: u32, max_len: u32) -> StructuralRowContract {
    let fields = [
        (1, "id", AcceptedFieldKind::Nat64),
        (
            value_id,
            "value",
            AcceptedFieldKind::Text {
                max_len: Some(max_len),
            },
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(slot, (id, name, kind))| {
        PersistedFieldSnapshot::new_initial(
            FieldId::new(id),
            name.into(),
            SchemaFieldSlot::new(u16::try_from(slot).unwrap()),
            kind.clone(),
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            kind.leaf_codec_for_storage(FieldStorageDecode::ByKind),
        )
    })
    .collect::<Vec<_>>();
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::MigrationFinding".into(),
        "MigrationFinding".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ));
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&snapshot).unwrap();
    StructuralRowContract::from_accepted_decode_contract(
        snapshot.entity_path(),
        descriptor.row_decode_contract(AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        )),
    )
}

fn preserving_program() -> CompiledMigrationEntityProgram {
    CompiledMigrationEntityProgram {
        store: TargetStoreIdentity::from_bytes([1; 32]),
        store_path: "tests::MigrationStore",
        entity: EntityTag::new(1),
        before_path: "tests::MigrationFinding".into(),
        candidate_path: "tests::MigrationFinding".into(),
        preserved_slots: vec![
            (SchemaFieldSlot::new(0), SchemaFieldSlot::new(0)),
            (SchemaFieldSlot::new(1), SchemaFieldSlot::new(1)),
        ],
        transforms: Vec::new(),
    }
}

fn persist(
    program: &CompiledMigrationEntityProgram,
    finding: MigrationTransformFinding,
) -> Result<PersistedSchemaMigrationFinding, InternalError> {
    PersistedSchemaMigrationFinding::try_new_transform(
        program.store(),
        program.entity(),
        vec![1],
        finding.source_field(),
        finding.target_field(),
        finding.reason(),
    )
}

#[test]
fn preserved_slot_failure_retains_both_accepted_field_identities() {
    let before_contract = contract(9, 64);
    let candidate_contract = contract(27, 64);
    let values = [Value::Nat64(7), Value::Text("valid".into())];
    let row =
        canonical_row_from_runtime_value_source_with_accepted_contract(&before_contract, |slot| {
            Ok(Cow::Borrowed(&values[slot]))
        })
        .unwrap()
        .into_raw_row();
    let reader =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &before_contract).unwrap();
    let field = reader.get_bytes(1).unwrap();
    let offset = field.as_ptr().addr() - row.as_bytes().as_ptr().addr();
    let mut corrupt = row.as_bytes().to_vec();
    corrupt[offset + field.len() - 1] = 0xff;
    let row = RawRow::try_new(corrupt).unwrap();
    // This directly qualifies the evaluator's preserved-slot error mapping.
    // Physical migration validation independently admits before rows eagerly.
    let before =
        StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &before_contract).unwrap();
    let program = preserving_program();
    let key = PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7));
    let finding = program
        .evaluate(&before, &candidate_contract, &key)
        .unwrap_err();
    let retained =
        persist(&program, finding).expect("preserved-slot failure must be a retainable finding");
    assert_eq!(retained.source_field(), Some(FieldId::new(9)));
    assert_eq!(retained.target_field(), Some(FieldId::new(27)));
    assert_eq!(
        retained.transform_reason(),
        Some(PersistedSchemaMigrationTransformReason::ValueContract)
    );
}

#[test]
fn row_contract_failures_are_retainable_without_field_attribution() {
    let before_contract = contract(9, 64);
    let values = [Value::Nat64(7), Value::Text("unchanged".into())];
    let row: RawRow =
        canonical_row_from_runtime_value_source_with_accepted_contract(&before_contract, |slot| {
            Ok(Cow::Borrowed(&values[slot]))
        })
        .unwrap()
        .into_raw_row();
    let before =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &before_contract)
            .unwrap();
    let program = preserving_program();
    // Candidate encoding and final PK agreement fail at the whole-row boundary.
    for (max_len, id) in [(1, 7), (64, 8)] {
        let candidate_contract = contract(27, max_len);
        let key = PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(id));
        let finding = program
            .evaluate(&before, &candidate_contract, &key)
            .unwrap_err();
        let retained =
            persist(&program, finding).expect("row contract failure must be a retainable finding");
        assert_eq!(retained.source_field(), None);
        assert_eq!(retained.target_field(), None);
        assert_eq!(
            retained.transform_reason(),
            Some(PersistedSchemaMigrationTransformReason::ValueContract)
        );
    }
    let candidate_contract = contract(27, 64);
    let key = PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7));
    assert!(program.evaluate(&before, &candidate_contract, &key).is_ok());
}
