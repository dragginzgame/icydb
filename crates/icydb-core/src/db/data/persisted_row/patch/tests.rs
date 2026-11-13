use super::*;
use crate::db::data::{CanonicalSlotReader, FieldSlot};
use crate::db::schema::{
    AcceptedCompositeCatalog, AcceptedConstraintKind, AcceptedFieldKind,
    AcceptedRowLayoutRuntimeContract, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
    AcceptedValueCatalogHandle, FieldId, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
    PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout,
    SchemaVersion, empty_accepted_enum_catalog_for_tests,
};
use crate::error::MutationDiagnosticContext;
use crate::types::Ulid;
use icydb_diagnostic_code::DiagnosticMutationOperation;

type ExistingRowResolver = fn(
    &StructuralRowContract,
    CommitSchemaFingerprint,
    &CompiledAcceptedRowConstraints,
    &StructuralSlotReader<'_>,
    &AcceptedMutationIntentPatch,
    AcceptedWriteContext,
    MutationDiagnosticContext,
) -> Result<ResolvedAcceptedMutationRow, InternalError>;

const EXISTING_ROW_RESOLVERS: [(DiagnosticMutationOperation, ExistingRowResolver); 2] = [
    (
        DiagnosticMutationOperation::Update,
        resolve_update_structural_patch_with_accepted_contract,
    ),
    (
        DiagnosticMutationOperation::Replace,
        resolve_existing_replace_structural_patch_with_accepted_contract,
    ),
];

fn replacement_owned_snapshot() -> AcceptedSchemaSnapshot {
    use crate::db::schema::SchemaFieldWritePolicy;

    let fields = [
        ("id", AcceptedFieldKind::Nat64, None, None),
        ("payload", AcceptedFieldKind::Nat64, None, None),
        (
            "token",
            AcceptedFieldKind::Ulid,
            Some(FieldInsertGeneration::Ulid),
            None,
        ),
        (
            "stamp",
            AcceptedFieldKind::Timestamp,
            Some(FieldInsertGeneration::Timestamp),
            None,
        ),
        (
            "created_at",
            AcceptedFieldKind::Timestamp,
            None,
            Some(FieldWriteManagement::CreatedAt),
        ),
        (
            "updated_at",
            AcceptedFieldKind::Timestamp,
            None,
            Some(FieldWriteManagement::UpdatedAt),
        ),
        ("optional", AcceptedFieldKind::Nat64, None, None),
    ]
    .into_iter()
    .enumerate()
    .map(|(slot, (name, kind, generation, management))| {
        let codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
        PersistedFieldSnapshot::new_initial_with_write_policy(
            FieldId::new(u32::try_from(slot).unwrap() + 1),
            name.into(),
            SchemaFieldSlot::new(u16::try_from(slot).unwrap()),
            kind,
            vec![],
            slot == 6,
            SchemaInsertDefault::None,
            SchemaFieldWritePolicy::from_model_policies(generation, management),
            FieldStorageDecode::ByKind,
            codec,
        )
    })
    .collect::<Vec<_>>();
    AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::ReplacementOwned".into(),
        "ReplacementOwned".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ))
    .unwrap()
}

fn replacement_owned_contract(
    accepted: &AcceptedSchemaSnapshot,
) -> (StructuralRowContract, CompiledAcceptedRowConstraints) {
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let constraints = CompiledAcceptedRowConstraints::compile(
        accepted,
        &catalog,
        [7; 16],
        &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
    )
    .unwrap();
    let layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(accepted).unwrap();
    let contract = StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        layout.row_decode_contract(catalog),
    );
    (contract, constraints)
}

fn replacement_owned_values() -> [Value; 7] {
    use crate::types::Timestamp;

    [
        Value::Nat64(1),
        Value::Nat64(10),
        Value::Ulid(Ulid::from_u128(7)),
        Value::Timestamp(Timestamp::from_millis(7)),
        Value::Timestamp(Timestamp::from_millis(7)),
        Value::Timestamp(Timestamp::from_millis(7)),
        Value::Null,
    ]
}

#[cfg(feature = "sql")]
#[test]
fn fixed_update_binding_preserves_payload_bytes_across_rows_and_noops() {
    use crate::types::Timestamp;
    use icydb_diagnostic_code::DiagnosticMutationOperation;

    let (contract, constraints) = replacement_owned_contract(&replacement_owned_snapshot());
    let payload = encode_canonical_value_for_accepted_field_contract(
        &contract
            .required_accepted_field_persistence_contract(1)
            .unwrap(),
        &Value::Nat64(77),
    )
    .unwrap();
    let null = encode_canonical_value_for_accepted_field_contract(
        &contract
            .required_accepted_field_persistence_contract(6)
            .unwrap(),
        &Value::Null,
    )
    .unwrap();
    let fixed = AcceptedFixedUpdatePatch::from_canonical_fields(vec![
        (FieldSlot::from_validated_index(1), payload.clone()),
        (FieldSlot::from_validated_index(6), null.clone()),
    ])
    .unwrap();
    let patch = fixed.to_update_intent(&contract).unwrap();
    for (id, before_value) in [(1, 10), (2, 20), (3, 77)] {
        let mut values = replacement_owned_values();
        values[0] = Value::Nat64(id);
        values[1] = Value::Nat64(before_value);
        let before =
            canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                Ok(Cow::Borrowed(&values[slot]))
            })
            .unwrap()
            .into_raw_row();
        let baseline =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&before, &contract)
                .unwrap();
        let after = resolve_update_structural_patch_with_accepted_contract(
            &contract,
            [7; 16],
            &constraints,
            &baseline,
            &patch,
            AcceptedWriteContext::new(Timestamp::from_millis(8)),
            MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                [7; 16],
                1,
                DiagnosticMutationOperation::Update,
                0,
            ),
        )
        .unwrap()
        .into_parts()
        .0
        .into_raw_row();
        let reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&after, &contract).unwrap();
        assert_eq!(reader.required_bytes(1).unwrap(), payload);
        assert_eq!(reader.required_bytes(6).unwrap(), null);
        assert_eq!(reader.required_cached_value(0).unwrap(), &Value::Nat64(id));
        let timestamp = if before_value == 77 { 7 } else { 8 };
        assert_eq!(
            reader.required_cached_value(5).unwrap(),
            &Value::Timestamp(Timestamp::from_millis(timestamp))
        );
        if before_value == 77 {
            assert_eq!(after.as_bytes(), before.as_bytes());
        }
    }
}

#[cfg(feature = "sql")]
#[test]
fn fixed_update_binding_rejects_malformed_wrong_contract_and_managed_payloads() {
    let (contract, _) = replacement_owned_contract(&replacement_owned_snapshot());
    let payload = encode_canonical_value_for_accepted_field_contract(
        &contract
            .required_accepted_field_persistence_contract(1)
            .unwrap(),
        &Value::Nat64(77),
    )
    .unwrap();
    for (slot, bytes) in [(1, Vec::new()), (1, vec![0xff]), (99, payload.clone())] {
        let expected =
            crate::db::data::decode_runtime_value_from_row_contract(&contract, slot, &bytes)
                .unwrap_err();
        let fixed = AcceptedFixedUpdatePatch::from_canonical_fields(vec![(
            FieldSlot::from_validated_index(slot),
            bytes,
        )])
        .unwrap();
        let error = fixed.to_update_intent(&contract).unwrap_err();
        assert_eq!(error.diagnostic(), expected.diagnostic());
        assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    }
    for slot in 2..=5 {
        let fixed = AcceptedFixedUpdatePatch::from_canonical_fields(vec![(
            FieldSlot::from_validated_index(slot),
            vec![0xff],
        )])
        .unwrap();
        let error = fixed.to_update_intent(&contract).unwrap_err();
        assert_eq!(
            error.diagnostic(),
            InternalError::executor_invariant().diagnostic()
        );
    }
    let fixed = AcceptedFixedUpdatePatch::from_canonical_fields(vec![(
        FieldSlot::from_validated_index(1),
        payload.clone(),
    )])
    .unwrap();
    let (other_contract, _) =
        replacement_owned_contract(&narrow_integer_snapshot(AcceptedFieldKind::Nat16, 2));
    let expected =
        crate::db::data::decode_runtime_value_from_row_contract(&other_contract, 1, &payload)
            .unwrap_err();
    let error = fixed.to_update_intent(&other_contract).unwrap_err();
    assert_eq!(error.diagnostic(), expected.diagnostic());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
}

#[test]
fn insert_generated_values_share_the_frozen_operation_timestamp() {
    use crate::types::Timestamp;
    use icydb_diagnostic_code::DiagnosticMutationOperation;

    let (contract, constraints) = replacement_owned_contract(&replacement_owned_snapshot());
    let mut previous = None;
    for (index, millis) in [1000, 1000, 1001].into_iter().enumerate() {
        let timestamp = Timestamp::from_millis(millis);
        let patch = AcceptedMutationIntentPatch::new()
            .set_authored(
                FieldSlot::from_validated_index(0),
                InputValue::nat64(u64::try_from(index).unwrap() + 1),
            )
            .set_authored(FieldSlot::from_validated_index(1), InputValue::nat64(10));
        let row = resolve_insert_structural_patch_with_accepted_contract(
            &contract,
            [7; 16],
            &constraints,
            &patch,
            AcceptedWriteContext::new(timestamp),
            MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                [7; 16],
                1,
                DiagnosticMutationOperation::Insert,
                u32::try_from(index).unwrap(),
            ),
            None,
        )
        .unwrap()
        .into_parts()
        .0
        .into_raw_row();
        let reader =
            StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .unwrap();
        let Value::Ulid(token) = reader.required_cached_value(2).unwrap() else {
            panic!("generated token must be a ULID");
        };
        assert_eq!(
            ulid::Ulid::from_bytes(token.to_bytes()).timestamp_ms(),
            u64::try_from(millis).unwrap()
        );
        if let Some(previous) = previous {
            assert!(previous < *token);
        }
        previous = Some(*token);
        for slot in 3..=5 {
            assert_eq!(
                reader.required_cached_value(slot).unwrap(),
                &Value::Timestamp(timestamp)
            );
        }
    }
}

#[test]
fn existing_mutations_preserve_generated_values_and_real_change_timestamps() {
    use crate::types::Timestamp;
    use icydb_diagnostic_code::ErrorCode;

    for (operation, resolve) in EXISTING_ROW_RESOLVERS {
        let (contract, constraints) = replacement_owned_contract(&replacement_owned_snapshot());
        let values = replacement_owned_values();
        let baseline =
            canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                Ok(Cow::Borrowed(&values[slot]))
            })
            .unwrap()
            .into_raw_row();
        let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
            &baseline, &contract,
        )
        .unwrap();
        let context = MutationDiagnosticContext::new(
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            [7; 16],
            1,
            operation,
            0,
        );
        let patch = |payload| {
            AcceptedMutationIntentPatch::new()
                .set_authored(FieldSlot::from_validated_index(0), InputValue::nat64(1))
                .set_authored(
                    FieldSlot::from_validated_index(1),
                    InputValue::text("ignored".into()),
                )
                .set_authored(
                    FieldSlot::from_validated_index(1),
                    InputValue::nat64(payload),
                )
        };
        for (payload, now) in [(10, 6), (10, 20), (11, 20)] {
            let (row, provenance) = resolve(
                &contract,
                [7; 16],
                &constraints,
                &reader,
                &patch(payload),
                AcceptedWriteContext::new(Timestamp::from_millis(now)),
                context,
            )
            .unwrap()
            .into_parts();
            let row = row.into_raw_row();
            let after = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                &row, &contract,
            )
            .unwrap();
            for slot in 2..=4 {
                assert_eq!(after.required_cached_value(slot).unwrap(), &values[slot]);
                assert_eq!(
                    provenance[slot],
                    Some(AcceptedFieldWriteProvenance::Preserved)
                );
            }
            assert_eq!(after.required_cached_value(6).unwrap(), &Value::Null);
            assert_eq!(
                after.required_cached_value(5).unwrap(),
                &Value::Timestamp(Timestamp::from_millis(if payload == 10 { 7 } else { now }))
            );
            if payload == 10 {
                assert_eq!(row.as_bytes(), baseline.as_bytes());
            }
        }
        let error = resolve(
            &contract,
            [7; 16],
            &constraints,
            &reader,
            &patch(11),
            AcceptedWriteContext::new(Timestamp::from_millis(6)),
            context,
        )
        .err()
        .expect("real changes must still reject managed time regression");
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::RUNTIME_BOUNDARY_MUTATION_MANAGED_TIMESTAMP_REGRESSION
        );
    }
}

#[test]
fn existing_noops_reject_inverted_stored_timestamps() {
    use crate::types::Timestamp;
    use icydb_diagnostic_code::ErrorCode;

    let (contract, constraints) = replacement_owned_contract(&replacement_owned_snapshot());
    let mut values = replacement_owned_values();
    values[4] = Value::Timestamp(Timestamp::from_millis(9));
    let baseline =
        canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
            Ok(Cow::Borrowed(&values[slot]))
        })
        .unwrap()
        .into_raw_row();
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&baseline, &contract)
            .unwrap();
    let patch = AcceptedMutationIntentPatch::new()
        .set_authored(FieldSlot::from_validated_index(0), InputValue::nat64(1))
        .set_authored(FieldSlot::from_validated_index(1), InputValue::nat64(10));
    for (operation, resolve) in EXISTING_ROW_RESOLVERS {
        let context = MutationDiagnosticContext::new(
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            [7; 16],
            1,
            operation,
            0,
        );
        let error = resolve(
            &contract,
            [7; 16],
            &constraints,
            &reader,
            &patch,
            AcceptedWriteContext::new(Timestamp::from_millis(20)),
            context,
        )
        .err()
        .expect("unchanged writes must reject inverted stored timestamps");
        assert_eq!(
            error.diagnostic().error_code(),
            ErrorCode::RUNTIME_BOUNDARY_MUTATION_MANAGED_TIMESTAMP_REGRESSION
        );
    }
}

#[test]
fn replacement_preserves_generated_historical_fill() {
    use crate::{
        db::schema::{
            PersistedFieldOrigin, RowLayoutVersion, SchemaFieldWritePolicy, SchemaHistoricalFill,
        },
        types::Timestamp,
    };
    use icydb_diagnostic_code::DiagnosticMutationOperation;

    let initial = replacement_owned_snapshot();
    let (old_contract, _) = replacement_owned_contract(&initial);
    let values = replacement_owned_values();
    let baseline =
        canonical_row_from_runtime_value_source_with_accepted_contract(&old_contract, |slot| {
            Ok(Cow::Borrowed(&values[slot]))
        })
        .unwrap()
        .into_raw_row();
    let fill = Value::Timestamp(Timestamp::from_millis(42));
    let payload = encode_canonical_value_for_accepted_field_contract(
        &old_contract
            .required_accepted_field_persistence_contract(3)
            .unwrap(),
        &fill,
    )
    .unwrap();
    let layout = RowLayoutVersion::INITIAL.checked_next().unwrap();
    let mut fields = initial.persisted_snapshot().fields().to_vec();
    fields.push(PersistedFieldSnapshot::new_with_write_policy_and_origin(
        FieldId::new(8),
        "historical_stamp".into(),
        SchemaFieldSlot::new(7),
        AcceptedFieldKind::Timestamp,
        vec![],
        false,
        layout,
        SchemaInsertDefault::None,
        SchemaHistoricalFill::SlotPayload(payload),
        SchemaFieldWritePolicy::from_model_policies(Some(FieldInsertGeneration::Timestamp), None),
        PersistedFieldOrigin::Generated,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Timestamp),
    ));
    let accepted = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        initial.entity_path().into(),
        "ReplacementOwned".into(),
        FieldId::new(1),
        SchemaRowLayout::new(
            layout,
            RowLayoutVersion::INITIAL,
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ))
    .unwrap();
    let (contract, constraints) = replacement_owned_contract(&accepted);
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&baseline, &contract)
            .unwrap();
    assert!(reader.get_bytes(7).is_none());
    let patch = AcceptedMutationIntentPatch::new()
        .set_authored(FieldSlot::from_validated_index(0), InputValue::nat64(1))
        .set_authored(FieldSlot::from_validated_index(1), InputValue::nat64(10));
    let (row, provenance) = resolve_existing_replace_structural_patch_with_accepted_contract(
        &contract,
        [7; 16],
        &constraints,
        &reader,
        &patch,
        AcceptedWriteContext::new(Timestamp::from_millis(20)),
        MutationDiagnosticContext::new(
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            [7; 16],
            1,
            DiagnosticMutationOperation::Replace,
            0,
        ),
    )
    .unwrap()
    .into_parts();
    let raw = row.into_raw_row();
    let after =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&raw, &contract)
            .unwrap();
    assert_eq!(after.required_cached_value(7).unwrap(), &fill);
    assert_eq!(
        provenance[7],
        Some(AcceptedFieldWriteProvenance::HistoricalFill)
    );
    assert_eq!(after.required_cached_value(5).unwrap(), &values[5]);
}

fn narrow_integer_snapshot(kind: AcceptedFieldKind, width: usize) -> AcceptedSchemaSnapshot {
    let codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
    let mut default = vec![0xff, 1, 7];
    default.resize(2 + width, 0);
    let fields = vec![
        PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            kind.clone(),
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            codec,
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "amount".to_string(),
            SchemaFieldSlot::new(1),
            kind,
            Vec::new(),
            true,
            SchemaInsertDefault::SlotPayload(default),
            FieldStorageDecode::ByKind,
            codec,
        ),
    ];
    AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::NarrowWrite".to_string(),
        "NarrowWrite".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(fields.iter().map(|f| (f.id(), f.slot())).collect()),
        fields,
    ))
    .unwrap()
}

#[test]
fn narrow_integer_writes_preserve_defaults_updates_and_primary_keys() {
    for (kind, width, initial, boundary, invalid) in [
        (
            AcceptedFieldKind::Int8,
            1,
            Value::Int64(7),
            Value::Int64(-128),
            Value::Int64(128),
        ),
        (
            AcceptedFieldKind::Int16,
            2,
            Value::Int64(7),
            Value::Int64(-32768),
            Value::Int64(32768),
        ),
        (
            AcceptedFieldKind::Int32,
            4,
            Value::Int64(7),
            Value::Int64(i64::from(i32::MIN)),
            Value::Int64(i64::from(i32::MAX) + 1),
        ),
        (
            AcceptedFieldKind::Nat8,
            1,
            Value::Nat64(7),
            Value::Nat64(255),
            Value::Nat64(256),
        ),
        (
            AcceptedFieldKind::Nat16,
            2,
            Value::Nat64(7),
            Value::Nat64(65535),
            Value::Nat64(65536),
        ),
        (
            AcceptedFieldKind::Nat32,
            4,
            Value::Nat64(7),
            Value::Nat64(u64::from(u32::MAX)),
            Value::Nat64(u64::from(u32::MAX) + 1),
        ),
    ] {
        assert_narrow_integer_write(kind, width, &initial, boundary, &invalid);
    }
}

fn assert_narrow_integer_write(
    kind: AcceptedFieldKind,
    width: usize,
    initial: &Value,
    boundary: Value,
    invalid: &Value,
) {
    use crate::db::key_taxonomy::PrimaryKeyComponent;
    use icydb_diagnostic_code::DiagnosticMutationOperation;

    let accepted = narrow_integer_snapshot(kind, width);
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let fingerprint = [7; 16];
    let constraints = CompiledAcceptedRowConstraints::compile(
        &accepted,
        &catalog,
        fingerprint,
        &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
    )
    .unwrap();
    let layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    let contract = StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        layout.row_decode_contract(catalog),
    );
    let input = |value: &Value| match value {
        Value::Int64(value) => InputValue::int64(*value),
        Value::Nat64(value) => InputValue::nat64(*value),
        Value::Null => InputValue::null(),
        _ => panic!("integer fixture"),
    };
    let patch = AcceptedMutationIntentPatch::new()
        .set_authored(FieldSlot::from_validated_index(0), input(initial));
    let write = AcceptedWriteContext::new(crate::types::Timestamp::from_millis(1));
    let row = resolve_insert_structural_patch_with_accepted_contract(
        &contract,
        fingerprint,
        &constraints,
        &patch,
        write,
        MutationDiagnosticContext::new(
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            fingerprint,
            1,
            DiagnosticMutationOperation::Insert,
            0,
        ),
        None,
    )
    .unwrap()
    .into_parts()
    .0
    .into_raw_row();
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
            .unwrap();
    assert_eq!(reader.required_cached_value(1).unwrap(), initial);
    assert_eq!(reader.required_bytes(1).unwrap().len(), 2 + width);
    let key = PrimaryKeyComponent::from_runtime_value(initial)
        .unwrap()
        .into();
    reader.validate_primary_key_value(&key).unwrap();
    let replaced = resolve_existing_replace_structural_patch_with_accepted_contract(
        &contract,
        fingerprint,
        &constraints,
        &reader,
        &patch,
        write,
        MutationDiagnosticContext::new(
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            fingerprint,
            1,
            DiagnosticMutationOperation::Replace,
            0,
        ),
    )
    .unwrap()
    .into_parts()
    .0
    .into_raw_row();
    assert_eq!(
        replaced.as_bytes(),
        row.as_bytes(),
        "replacement retains the same accepted default"
    );
    for value in [
        None,
        Some(initial.clone()),
        Some(boundary),
        Some(Value::Null),
        Some(invalid.clone()),
    ] {
        let patch = value
            .as_ref()
            .map_or_else(AcceptedMutationIntentPatch::new, |value| {
                AcceptedMutationIntentPatch::new()
                    .set_authored(FieldSlot::from_validated_index(1), input(value))
            });
        let value = value.unwrap_or_else(|| initial.clone());
        let result = resolve_update_structural_patch_with_accepted_contract(
            &contract,
            fingerprint,
            &constraints,
            &StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(&row, &contract)
                .unwrap(),
            &patch,
            write,
            MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                fingerprint,
                1,
                DiagnosticMutationOperation::Update,
                0,
            ),
        );
        if value == *invalid {
            assert!(result.is_err());
            continue;
        }
        let updated = result.unwrap().into_parts().0.into_raw_row();
        if value == *initial {
            assert_eq!(updated.as_bytes(), row.as_bytes());
        }
        let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
            &updated, &contract,
        )
        .unwrap();
        assert_eq!(reader.required_cached_value(1).unwrap(), &value);
        assert_eq!(
            reader.required_bytes(1).unwrap().len(),
            if value == Value::Null { 2 } else { 2 + width }
        );
        reader.validate_primary_key_value(&key).unwrap();
    }
}

#[test]
fn managed_timestamp_progression_uses_the_current_managed_write_time() {
    let existing_created_at = crate::types::Timestamp::from_millis(10);
    let existing_updated_at = crate::types::Timestamp::from_millis(20);

    assert!(managed_timestamp_values_regress(
        Some(existing_created_at),
        Some(existing_updated_at),
        crate::types::Timestamp::from_millis(19),
    ));
    assert!(!managed_timestamp_values_regress(
        Some(existing_created_at),
        Some(existing_updated_at),
        crate::types::Timestamp::from_millis(20),
    ));
    assert!(!managed_timestamp_values_regress(
        Some(existing_created_at),
        Some(existing_updated_at),
        crate::types::Timestamp::from_millis(30),
    ));
}

#[test]
fn accepted_not_null_pre_encoding_failure_preserves_constraint_identity() {
    let field = PersistedFieldSnapshot::new_initial(
        FieldId::new(1),
        "id".to_string(),
        SchemaFieldSlot::new(0),
        crate::db::schema::AcceptedFieldKind::Ulid,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Ulid),
    );
    let accepted = AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::User".to_string(),
        "User".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![field],
    ))
    .expect("test not-null snapshot should close");
    let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let fingerprint = [7; 16];
    let constraints = CompiledAcceptedRowConstraints::compile(
        &accepted,
        &value_catalog,
        fingerprint,
        &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
    )
    .expect("accepted not-null program should compile");
    let row_layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted row layout should build");
    let patch = AcceptedMutationIntentPatch::new().set_authored(
        crate::db::data::FieldSlot::from_validated_index(0),
        InputValue::null(),
    );
    let contract = StructuralRowContract::from_accepted_decode_contract(
        accepted.entity_path(),
        row_layout.row_decode_contract(value_catalog),
    );
    let Err(error) = resolve_insert_structural_patch_with_accepted_contract(
        &contract,
        fingerprint,
        &constraints,
        &patch,
        AcceptedWriteContext::new(crate::types::Timestamp::from_millis(1)),
        MutationDiagnosticContext::new(
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            fingerprint,
            1,
            icydb_diagnostic_code::DiagnosticMutationOperation::Insert,
            0,
        ),
        None,
    ) else {
        panic!("explicit null should violate accepted not-null");
    };
    let accepted_identity = accepted
        .persisted_snapshot()
        .constraints()
        .iter()
        .find(|constraint| {
            matches!(
                constraint.kind(),
                AcceptedConstraintKind::NotNull { field_id }
                    if *field_id == FieldId::new(1)
            )
        })
        .expect("accepted not-null identity should exist");

    let facts = error.diagnostic_facts();
    assert!(facts.contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintId,
        u64::from(accepted_identity.id().get()),
    )));
    assert!(facts.contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::NotNull.raw(),
    )));
    assert!(facts.contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
        icydb_diagnostic_code::DiagnosticMutationOperation::Insert.raw(),
    )));
}
