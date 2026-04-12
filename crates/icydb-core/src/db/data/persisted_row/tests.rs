use super::{
    CachedSlotValue, CompleteSerializedPatchWriter, FieldSlot, ScalarSlotValueRef, ScalarValueRef,
    SerializedFieldUpdate, SerializedUpdatePatch, SlotBufferWriter, SlotReader, SlotWriter,
    UpdatePatch, apply_serialized_update_patch_to_raw_row, apply_update_patch_to_raw_row,
    canonical_row_from_complete_serialized_update_patch, decode_persisted_custom_many_slot_payload,
    decode_persisted_custom_slot_payload, decode_persisted_non_null_slot_payload,
    decode_persisted_option_slot_payload, decode_persisted_slot_payload,
    decode_slot_value_by_contract, decode_slot_value_from_bytes,
    encode_persisted_custom_many_slot_payload, encode_persisted_custom_slot_payload,
    encode_scalar_slot_value, encode_slot_payload_from_parts, encode_slot_value_from_value,
    materialize_entity_from_serialized_update_patch,
    serialize_entity_slots_as_complete_serialized_patch, serialize_update_patch_fields,
    with_structural_read_metrics,
};
use crate::{
    db::{
        codec::serialize_row_payload,
        data::{RawRow, StructuralSlotReader, decode_structural_value_storage_bytes},
    },
    error::InternalError,
    model::{
        EntityModel,
        field::{EnumVariantModel, FieldKind, FieldModel, FieldStorageDecode},
    },
    serialize::serialize,
    testing::SIMPLE_ENTITY_TAG,
    traits::{EntitySchema, FieldValue},
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Principal,
        Subaccount, Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};

crate::test_canister! {
    ident = PersistedRowPatchBridgeCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = PersistedRowPatchBridgeStore,
    canister = PersistedRowPatchBridgeCanister,
}

///
/// PersistedRowPatchBridgeEntity
///
/// PersistedRowPatchBridgeEntity
///
/// PersistedRowPatchBridgeEntity is the smallest derive-owned entity used
/// to validate the typed-entity -> serialized-patch bridge.
/// It lets the persisted-row tests exercise the same dense slot writer the
/// save/update path now uses.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct PersistedRowPatchBridgeEntity {
    id: crate::types::Ulid,
    name: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct PersistedRowProfileValue {
    bio: String,
}

impl FieldValue for PersistedRowProfileValue {
    fn kind() -> crate::traits::FieldValueKind {
        crate::traits::FieldValueKind::Structured { queryable: false }
    }

    fn to_value(&self) -> Value {
        Value::from_map(vec![(
            Value::Text("bio".to_string()),
            Value::Text(self.bio.clone()),
        )])
        .expect("profile test value should encode as canonical map")
    }

    fn from_value(value: &Value) -> Option<Self> {
        let Value::Map(entries) = value else {
            return None;
        };
        let normalized = Value::normalize_map_entries(entries.clone()).ok()?;
        let bio = normalized
            .iter()
            .find_map(|(entry_key, entry_value)| match entry_key {
                Value::Text(entry_key) if entry_key == "bio" => match entry_value {
                    Value::Text(bio) => Some(bio.clone()),
                    _ => None,
                },
                _ => None,
            })?;

        if normalized.len() != 1 {
            return None;
        }

        Some(Self { bio })
    }
}

crate::test_entity_schema! {
    ident = PersistedRowPatchBridgeEntity,
    id = crate::types::Ulid,
    id_field = id,
    entity_name = "PersistedRowPatchBridgeEntity",
    entity_tag = SIMPLE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
    ],
    indexes = [],
    store = PersistedRowPatchBridgeStore,
    canister = PersistedRowPatchBridgeCanister,
}

static STATE_VARIANTS: &[EnumVariantModel] = &[EnumVariantModel::new(
    "Loaded",
    Some(&FieldKind::Uint),
    FieldStorageDecode::ByKind,
)];
static FIELD_MODELS: [FieldModel; 2] = [
    FieldModel::generated("name", FieldKind::Text),
    FieldModel::generated_with_storage_decode(
        "payload",
        FieldKind::Text,
        FieldStorageDecode::Value,
    ),
];
static LIST_FIELD_MODELS: [FieldModel; 1] = [FieldModel::generated(
    "tags",
    FieldKind::List(&FieldKind::Text),
)];
static MAP_FIELD_MODELS: [FieldModel; 1] = [FieldModel::generated(
    "props",
    FieldKind::Map {
        key: &FieldKind::Text,
        value: &FieldKind::Uint,
    },
)];
static ENUM_FIELD_MODELS: [FieldModel; 1] = [FieldModel::generated(
    "state",
    FieldKind::Enum {
        path: "tests::State",
        variants: STATE_VARIANTS,
    },
)];
static ACCOUNT_FIELD_MODELS: [FieldModel; 1] = [FieldModel::generated("owner", FieldKind::Account)];
static OPTIONAL_ACCOUNT_FIELD_MODELS: [FieldModel; 1] =
    [FieldModel::generated_with_storage_decode_and_nullability(
        "from",
        FieldKind::Account,
        FieldStorageDecode::ByKind,
        true,
    )];
static REQUIRED_STRUCTURED_FIELD_MODELS: [FieldModel; 1] = [FieldModel::generated(
    "profile",
    FieldKind::Structured { queryable: false },
)];
static OPTIONAL_STRUCTURED_FIELD_MODELS: [FieldModel; 1] =
    [FieldModel::generated_with_storage_decode_and_nullability(
        "profile",
        FieldKind::Structured { queryable: false },
        FieldStorageDecode::ByKind,
        true,
    )];
static VALUE_STORAGE_STRUCTURED_FIELD_MODELS: [FieldModel; 1] =
    [FieldModel::generated_with_storage_decode(
        "manifest",
        FieldKind::Structured { queryable: false },
        FieldStorageDecode::Value,
    )];
static STRUCTURED_MAP_VALUE_KIND: FieldKind = FieldKind::Structured { queryable: false };
static STRUCTURED_MAP_VALUE_STORAGE_FIELD_MODELS: [FieldModel; 1] =
    [FieldModel::generated_with_storage_decode(
        "projects",
        FieldKind::Map {
            key: &FieldKind::Principal,
            value: &STRUCTURED_MAP_VALUE_KIND,
        },
        FieldStorageDecode::Value,
    )];
static INDEX_MODELS: [&crate::model::index::IndexModel; 0] = [];
static TEST_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowFieldCodecEntity",
    "persisted_row_field_codec_entity",
    &FIELD_MODELS[0],
    0,
    &FIELD_MODELS,
    &INDEX_MODELS,
);
static LIST_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowListFieldCodecEntity",
    "persisted_row_list_field_codec_entity",
    &LIST_FIELD_MODELS[0],
    0,
    &LIST_FIELD_MODELS,
    &INDEX_MODELS,
);
static MAP_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowMapFieldCodecEntity",
    "persisted_row_map_field_codec_entity",
    &MAP_FIELD_MODELS[0],
    0,
    &MAP_FIELD_MODELS,
    &INDEX_MODELS,
);
static ENUM_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowEnumFieldCodecEntity",
    "persisted_row_enum_field_codec_entity",
    &ENUM_FIELD_MODELS[0],
    0,
    &ENUM_FIELD_MODELS,
    &INDEX_MODELS,
);
static ACCOUNT_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowAccountFieldCodecEntity",
    "persisted_row_account_field_codec_entity",
    &ACCOUNT_FIELD_MODELS[0],
    0,
    &ACCOUNT_FIELD_MODELS,
    &INDEX_MODELS,
);
static OPTIONAL_ACCOUNT_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowOptionalAccountFieldCodecEntity",
    "persisted_row_optional_account_field_codec_entity",
    &OPTIONAL_ACCOUNT_FIELD_MODELS[0],
    0,
    &OPTIONAL_ACCOUNT_FIELD_MODELS,
    &INDEX_MODELS,
);
static REQUIRED_STRUCTURED_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowRequiredStructuredFieldCodecEntity",
    "persisted_row_required_structured_field_codec_entity",
    &REQUIRED_STRUCTURED_FIELD_MODELS[0],
    0,
    &REQUIRED_STRUCTURED_FIELD_MODELS,
    &INDEX_MODELS,
);
static OPTIONAL_STRUCTURED_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowOptionalStructuredFieldCodecEntity",
    "persisted_row_optional_structured_field_codec_entity",
    &OPTIONAL_STRUCTURED_FIELD_MODELS[0],
    0,
    &OPTIONAL_STRUCTURED_FIELD_MODELS,
    &INDEX_MODELS,
);
static VALUE_STORAGE_STRUCTURED_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowValueStorageStructuredFieldCodecEntity",
    "persisted_row_value_storage_structured_field_codec_entity",
    &VALUE_STORAGE_STRUCTURED_FIELD_MODELS[0],
    0,
    &VALUE_STORAGE_STRUCTURED_FIELD_MODELS,
    &INDEX_MODELS,
);
static STRUCTURED_MAP_VALUE_STORAGE_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowStructuredMapValueStorageEntity",
    "persisted_row_structured_map_value_storage_entity",
    &STRUCTURED_MAP_VALUE_STORAGE_FIELD_MODELS[0],
    0,
    &STRUCTURED_MAP_VALUE_STORAGE_FIELD_MODELS,
    &INDEX_MODELS,
);

fn representative_value_storage_cases() -> Vec<Value> {
    let nested = Value::from_map(vec![
        (
            Value::Text("blob".to_string()),
            Value::Blob(vec![0x10, 0x20, 0x30]),
        ),
        (
            Value::Text("i128".to_string()),
            Value::Int128(Int128::from(-123i128)),
        ),
        (
            Value::Text("u128".to_string()),
            Value::Uint128(Nat128::from(456u128)),
        ),
        (
            Value::Text("enum".to_string()),
            Value::Enum(
                ValueEnum::new("Loaded", Some("tests::PersistedRowManifest"))
                    .with_payload(Value::Blob(vec![0xAA, 0xBB])),
            ),
        ),
    ])
    .expect("nested value storage case should normalize");

    vec![
        Value::Account(Account::dummy(7)),
        Value::Blob(vec![1u8, 2u8, 3u8]),
        Value::Bool(true),
        Value::Date(Date::new(2024, 1, 2)),
        Value::Decimal(Decimal::new(123, 2)),
        Value::Duration(Duration::from_secs(1)),
        Value::Enum(
            ValueEnum::new("Ready", Some("tests::PersistedRowState")).with_payload(nested.clone()),
        ),
        Value::Float32(Float32::try_new(1.25).expect("float32 sample should be finite")),
        Value::Float64(Float64::try_new(2.5).expect("float64 sample should be finite")),
        Value::Int(-7),
        Value::Int128(Int128::from(123i128)),
        Value::IntBig(Int::from(99i32)),
        Value::List(vec![
            Value::Blob(vec![0xCC, 0xDD]),
            Value::Text("nested".to_string()),
            nested.clone(),
        ]),
        nested,
        Value::Null,
        Value::Principal(Principal::dummy(9)),
        Value::Subaccount(Subaccount::new([7u8; 32])),
        Value::Text("example".to_string()),
        Value::Timestamp(Timestamp::from_secs(1)),
        Value::Uint(7),
        Value::Uint128(Nat128::from(9u128)),
        Value::UintBig(Nat::from(11u64)),
        Value::Ulid(Ulid::from_u128(42)),
        Value::Unit,
    ]
}

fn representative_structured_value_storage_cases() -> Vec<Value> {
    let nested_map = Value::from_map(vec![
        (
            Value::Text("account".to_string()),
            Value::Account(Account::dummy(7)),
        ),
        (
            Value::Text("blob".to_string()),
            Value::Blob(vec![1u8, 2u8, 3u8]),
        ),
        (Value::Text("bool".to_string()), Value::Bool(true)),
        (
            Value::Text("date".to_string()),
            Value::Date(Date::new(2024, 1, 2)),
        ),
        (
            Value::Text("decimal".to_string()),
            Value::Decimal(Decimal::new(123, 2)),
        ),
        (
            Value::Text("duration".to_string()),
            Value::Duration(Duration::from_secs(1)),
        ),
        (
            Value::Text("enum".to_string()),
            Value::Enum(
                ValueEnum::new("Loaded", Some("tests::PersistedRowManifest"))
                    .with_payload(Value::Blob(vec![0xAA, 0xBB])),
            ),
        ),
        (
            Value::Text("f32".to_string()),
            Value::Float32(Float32::try_new(1.25).expect("float32 sample should be finite")),
        ),
        (
            Value::Text("f64".to_string()),
            Value::Float64(Float64::try_new(2.5).expect("float64 sample should be finite")),
        ),
        (Value::Text("i64".to_string()), Value::Int(-7)),
        (
            Value::Text("i128".to_string()),
            Value::Int128(Int128::from(123i128)),
        ),
        (
            Value::Text("ibig".to_string()),
            Value::IntBig(Int::from(99i32)),
        ),
        (Value::Text("null".to_string()), Value::Null),
        (
            Value::Text("principal".to_string()),
            Value::Principal(Principal::dummy(9)),
        ),
        (
            Value::Text("subaccount".to_string()),
            Value::Subaccount(Subaccount::new([7u8; 32])),
        ),
        (
            Value::Text("text".to_string()),
            Value::Text("example".to_string()),
        ),
        (
            Value::Text("timestamp".to_string()),
            Value::Timestamp(Timestamp::from_secs(1)),
        ),
        (Value::Text("u64".to_string()), Value::Uint(7)),
        (
            Value::Text("u128".to_string()),
            Value::Uint128(Nat128::from(9u128)),
        ),
        (
            Value::Text("ubig".to_string()),
            Value::UintBig(Nat::from(11u64)),
        ),
        (
            Value::Text("ulid".to_string()),
            Value::Ulid(Ulid::from_u128(42)),
        ),
        (Value::Text("unit".to_string()), Value::Unit),
    ])
    .expect("structured value-storage map should normalize");

    vec![
        nested_map.clone(),
        Value::List(vec![
            Value::Blob(vec![0xCC, 0xDD]),
            Value::Text("nested".to_string()),
            nested_map,
        ]),
    ]
}

fn encode_slot_payload_allowing_missing_for_tests(
    model: &'static EntityModel,
    slots: &[Option<&[u8]>],
) -> Result<Vec<u8>, InternalError> {
    if slots.len() != model.fields().len() {
        return Err(InternalError::persisted_row_encode_failed(format!(
            "noncanonical slot payload test helper expected {} slots for entity '{}', found {}",
            model.fields().len(),
            model.path(),
            slots.len()
        )));
    }
    let mut payload_bytes = Vec::new();
    let mut slot_table = Vec::with_capacity(slots.len());

    for slot_payload in slots {
        match slot_payload {
            Some(bytes) => {
                let start = u32::try_from(payload_bytes.len()).map_err(|_| {
                    InternalError::persisted_row_encode_failed(
                        "slot payload start exceeds u32 range",
                    )
                })?;
                let len = u32::try_from(bytes.len()).map_err(|_| {
                    InternalError::persisted_row_encode_failed(
                        "slot payload length exceeds u32 range",
                    )
                })?;
                payload_bytes.extend_from_slice(bytes);
                slot_table.push((start, len));
            }
            None => slot_table.push((0_u32, 0_u32)),
        }
    }

    encode_slot_payload_from_parts(slots.len(), slot_table.as_slice(), payload_bytes.as_slice())
}

#[test]
fn decode_slot_value_from_bytes_decodes_scalar_slots_through_one_owner() {
    let payload = encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let value =
        decode_slot_value_from_bytes(&TEST_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(value, Value::Text("Ada".to_string()));
}

#[test]
fn decode_slot_value_from_bytes_reports_scalar_prefix_bytes() {
    let err = decode_slot_value_from_bytes(&TEST_MODEL, 0, &[0x00, 1])
        .expect_err("invalid scalar slot prefix should fail closed");

    assert!(
        err.message
            .contains("expected slot envelope prefix byte 0xFF, found 0x00"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn decode_slot_value_from_bytes_respects_value_storage_decode_contract() {
    let payload = crate::serialize::serialize(&Value::Text("Ada".to_string()))
        .expect("encode value-storage payload");

    let value =
        decode_slot_value_from_bytes(&TEST_MODEL, 1, payload.as_slice()).expect("decode slot");

    assert_eq!(value, Value::Text("Ada".to_string()));
}

#[test]
fn encode_slot_value_from_value_roundtrips_scalar_slots() {
    let payload = encode_slot_value_from_value(&TEST_MODEL, 0, &Value::Text("Ada".to_string()))
        .expect("encode slot");
    let decoded =
        decode_slot_value_from_bytes(&TEST_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(decoded, Value::Text("Ada".to_string()));
}

#[test]
fn encode_slot_value_from_value_roundtrips_value_storage_slots() {
    let payload = encode_slot_value_from_value(&TEST_MODEL, 1, &Value::Text("Ada".to_string()))
        .expect("encode slot");
    let decoded =
        decode_slot_value_from_bytes(&TEST_MODEL, 1, payload.as_slice()).expect("decode slot");

    assert_eq!(decoded, Value::Text("Ada".to_string()));
}

#[test]
fn encode_slot_value_from_value_roundtrips_structured_value_storage_slots_for_all_cases() {
    for value in representative_structured_value_storage_cases() {
        let payload = encode_slot_value_from_value(&VALUE_STORAGE_STRUCTURED_MODEL, 0, &value)
            .unwrap_or_else(|err| {
                panic!("structured value-storage slot should encode for value {value:?}: {err:?}")
            });
        let decoded = decode_slot_value_from_bytes(
            &VALUE_STORAGE_STRUCTURED_MODEL,
            0,
            payload.as_slice(),
        )
        .unwrap_or_else(|err| {
            panic!(
                "structured value-storage slot should decode for value {value:?} with payload {payload:?}: {err:?}"
            )
        });

        assert_eq!(decoded, value);
    }
}

#[test]
fn encode_slot_value_from_value_roundtrips_list_by_kind_slots() {
    let payload = encode_slot_value_from_value(
        &LIST_MODEL,
        0,
        &Value::List(vec![Value::Text("alpha".to_string())]),
    )
    .expect("encode list slot");
    let decoded =
        decode_slot_value_from_bytes(&LIST_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(decoded, Value::List(vec![Value::Text("alpha".to_string())]),);
}

#[test]
fn encode_slot_value_from_value_roundtrips_map_by_kind_slots() {
    let payload = encode_slot_value_from_value(
        &MAP_MODEL,
        0,
        &Value::Map(vec![(Value::Text("alpha".to_string()), Value::Uint(7))]),
    )
    .expect("encode map slot");
    let decoded =
        decode_slot_value_from_bytes(&MAP_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(
        decoded,
        Value::Map(vec![(Value::Text("alpha".to_string()), Value::Uint(7))]),
    );
}

#[test]
fn encode_slot_value_from_value_accepts_value_storage_maps_with_structured_values() {
    let principal = Principal::dummy(7);
    let project = Value::from_map(vec![
        (Value::Text("pid".to_string()), Value::Principal(principal)),
        (
            Value::Text("status".to_string()),
            Value::Enum(ValueEnum::new(
                "Saved",
                Some("design::app::user::customise::project::ProjectStatus"),
            )),
        ),
    ])
    .expect("project value should normalize into a canonical map");
    let projects = Value::from_map(vec![(Value::Principal(principal), project)])
        .expect("outer map should normalize into a canonical map");

    let payload = encode_slot_value_from_value(&STRUCTURED_MAP_VALUE_STORAGE_MODEL, 0, &projects)
        .expect("encode structured map slot");
    let decoded =
        decode_slot_value_from_bytes(&STRUCTURED_MAP_VALUE_STORAGE_MODEL, 0, payload.as_slice())
            .expect("decode structured map slot");

    assert_eq!(decoded, projects);
}

#[test]
fn structured_value_storage_cases_decode_through_direct_value_storage_boundary() {
    for value in representative_value_storage_cases() {
        let payload = serialize(&value).unwrap_or_else(|err| {
            panic!("structured value-storage payload should serialize for value {value:?}: {err:?}")
        });
        let decoded = decode_structural_value_storage_bytes(payload.as_slice()).unwrap_or_else(
            |err| {
                panic!(
                    "structured value-storage payload should decode for value {value:?} with payload {payload:?}: {err:?}"
                )
            },
        );

        assert_eq!(decoded, value);
    }
}

#[test]
fn encode_slot_value_from_value_roundtrips_enum_by_kind_slots() {
    let payload = encode_slot_value_from_value(
        &ENUM_MODEL,
        0,
        &Value::Enum(ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7))),
    )
    .expect("encode enum slot");
    let decoded =
        decode_slot_value_from_bytes(&ENUM_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(
        decoded,
        Value::Enum(ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7,))),
    );
}

#[test]
fn encode_slot_value_from_value_roundtrips_leaf_by_kind_wrapper_slots() {
    let account = Account::from_parts(Principal::dummy(7), Some(Subaccount::from([7_u8; 32])));
    let payload = encode_slot_value_from_value(&ACCOUNT_MODEL, 0, &Value::Account(account))
        .expect("encode account slot");
    let decoded =
        decode_slot_value_from_bytes(&ACCOUNT_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(decoded, Value::Account(account));
}

#[test]
fn custom_slot_payload_roundtrips_structured_field_value() {
    let profile = PersistedRowProfileValue {
        bio: "Ada".to_string(),
    };
    let payload = encode_persisted_custom_slot_payload(&profile, "profile")
        .expect("encode custom structured payload");
    let decoded = decode_persisted_custom_slot_payload::<PersistedRowProfileValue>(
        payload.as_slice(),
        "profile",
    )
    .expect("decode custom structured payload");

    assert_eq!(decoded, profile);
    assert_eq!(
        decode_persisted_slot_payload::<Value>(payload.as_slice(), "profile")
            .expect("decode raw value payload"),
        profile.to_value(),
    );
}

#[test]
fn custom_many_slot_payload_roundtrips_structured_value_lists() {
    let profiles = vec![
        PersistedRowProfileValue {
            bio: "Ada".to_string(),
        },
        PersistedRowProfileValue {
            bio: "Grace".to_string(),
        },
    ];
    let payload = encode_persisted_custom_many_slot_payload(profiles.as_slice(), "profiles")
        .expect("encode custom structured list payload");
    let decoded = decode_persisted_custom_many_slot_payload::<PersistedRowProfileValue>(
        payload.as_slice(),
        "profiles",
    )
    .expect("decode custom structured list payload");

    assert_eq!(decoded, profiles);
}

#[test]
fn decode_persisted_non_null_slot_payload_rejects_null_for_required_structured_fields() {
    let err =
        decode_persisted_non_null_slot_payload::<PersistedRowProfileValue>(&[0xF6], "profile")
            .expect_err("required structured payload must reject null");

    assert!(
        err.message
            .contains("unexpected null for non-nullable field"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn decode_persisted_option_slot_payload_treats_cbor_null_as_none() {
    let decoded =
        decode_persisted_option_slot_payload::<PersistedRowProfileValue>(&[0xF6], "profile")
            .expect("optional structured payload should decode");

    assert_eq!(decoded, None);
}

#[test]
fn encode_slot_value_from_value_rejects_null_for_required_structured_slots() {
    let err = encode_slot_value_from_value(&REQUIRED_STRUCTURED_MODEL, 0, &Value::Null)
        .expect_err("required structured slot must reject null");

    assert!(
        err.message.contains("required field cannot store null"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn encode_slot_value_from_value_allows_null_for_optional_structured_slots() {
    let payload = encode_slot_value_from_value(&OPTIONAL_STRUCTURED_MODEL, 0, &Value::Null)
        .expect("optional structured slot should allow null");
    let decoded = decode_slot_value_from_bytes(&OPTIONAL_STRUCTURED_MODEL, 0, payload.as_slice())
        .expect("optional structured slot should decode");

    assert_eq!(decoded, Value::Null);
}

#[test]
fn decode_slot_value_from_bytes_allows_null_for_optional_account_slots() {
    let payload = encode_slot_value_from_value(&OPTIONAL_ACCOUNT_MODEL, 0, &Value::Null)
        .expect("optional account slot should allow null");
    let decoded = decode_slot_value_from_bytes(&OPTIONAL_ACCOUNT_MODEL, 0, payload.as_slice())
        .expect("optional account slot should decode");

    assert_eq!(decoded, Value::Null);
}

#[test]
fn structural_slot_reader_accepts_null_for_optional_account_slots() {
    let mut writer = SlotBufferWriter::for_model(&OPTIONAL_ACCOUNT_MODEL);
    let payload = encode_slot_value_from_value(&OPTIONAL_ACCOUNT_MODEL, 0, &Value::Null)
        .expect("optional account slot should allow null");
    writer
        .write_slot(0, Some(payload.as_slice()))
        .expect("write optional account slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");

    let mut reader = StructuralSlotReader::from_raw_row(&raw_row, &OPTIONAL_ACCOUNT_MODEL)
        .expect("row-open validation should accept null optional account slots");

    assert_eq!(reader.get_value(0).expect("decode slot"), Some(Value::Null));
}

#[test]
fn encode_slot_value_from_value_rejects_unknown_enum_payload_variants() {
    let err = encode_slot_value_from_value(
        &ENUM_MODEL,
        0,
        &Value::Enum(ValueEnum::new("Unknown", Some("tests::State")).with_payload(Value::Uint(7))),
    )
    .expect_err("unknown enum payload should fail closed");

    assert!(err.message.contains("unknown enum variant"));
}

#[test]
fn structural_slot_reader_and_direct_decode_share_the_same_field_codec_boundary() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");

    let direct_slots =
        StructuralSlotReader::from_raw_row_lazy(&raw_row, &TEST_MODEL).expect("decode row");
    let mut cached_slots =
        StructuralSlotReader::from_raw_row_lazy(&raw_row, &TEST_MODEL).expect("decode row");

    let direct_name = decode_slot_value_by_contract(&direct_slots, 0).expect("decode name");
    let direct_payload = decode_slot_value_by_contract(&direct_slots, 1).expect("decode payload");
    let cached_name = cached_slots.get_value(0).expect("cached name");
    let cached_payload = cached_slots.get_value(1).expect("cached payload");

    assert_eq!(direct_name, cached_name);
    assert_eq!(direct_payload, cached_payload);
}

#[test]
fn structural_slot_reader_validates_declared_slots_but_defers_non_scalar_materialization() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");

    let mut reader = StructuralSlotReader::from_raw_row_lazy(&raw_row, &TEST_MODEL)
        .expect("row-open structural envelope decode should succeed");

    match &reader.cached_values[0] {
        CachedSlotValue::Scalar { materialized, .. } => {
            assert!(
                materialized.get().is_none(),
                "scalar slot should stay untouched until first semantic access",
            );
        }
        other @ CachedSlotValue::Deferred { .. } => {
            panic!("expected scalar cache for slot 0, found {other:?}")
        }
    }
    match &reader.cached_values[1] {
        CachedSlotValue::Deferred { materialized } => {
            assert!(
                materialized.get().is_none(),
                "non-scalar slot should stay untouched until first semantic access",
            );
        }
        other @ CachedSlotValue::Scalar { .. } => {
            panic!("expected deferred cache for slot 1, found {other:?}")
        }
    }

    assert_eq!(
        reader.get_value(1).expect("decode deferred slot"),
        Some(Value::Text("payload".to_string()))
    );

    assert_eq!(
        reader
            .get_value(0)
            .expect("materialize deferred scalar slot"),
        Some(Value::Text("Ada".to_string()))
    );

    match &reader.cached_values[0] {
        CachedSlotValue::Scalar { materialized, .. } => {
            assert_eq!(
                materialized.get(),
                Some(&Value::Text("Ada".to_string())),
                "scalar slot should materialize on first semantic access",
            );
        }
        other @ CachedSlotValue::Deferred { .. } => {
            panic!("expected scalar cache for slot 0, found {other:?}")
        }
    }

    match &reader.cached_values[1] {
        CachedSlotValue::Deferred { materialized } => {
            assert_eq!(
                materialized.get(),
                Some(&Value::Text("payload".to_string())),
                "non-scalar slot should materialize on first semantic access",
            );
        }
        other @ CachedSlotValue::Scalar { .. } => {
            panic!("expected deferred cache for slot 1, found {other:?}")
        }
    }
}

#[test]
fn structural_slot_reader_metrics_report_zero_non_scalar_materializations_for_scalar_only_access() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");

    let (_scalar_read, metrics) = with_structural_read_metrics(|| {
        let reader = StructuralSlotReader::from_raw_row_lazy(&raw_row, &TEST_MODEL)
            .expect("row-open structural envelope decode should succeed");

        matches!(
            reader
                .get_scalar(0)
                .expect("scalar fast path should succeed"),
            Some(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        )
    });

    assert_eq!(metrics.rows_opened, 1);
    assert_eq!(metrics.declared_slots_validated, 1);
    assert_eq!(metrics.validated_non_scalar_slots, 0);
    assert_eq!(
        metrics.materialized_non_scalar_slots, 0,
        "scalar-only access should not materialize the unused value-storage slot",
    );
    assert_eq!(metrics.rows_without_lazy_non_scalar_materializations, 1);
}

#[test]
fn structural_slot_reader_metrics_report_one_non_scalar_materialization_on_first_semantic_access() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");

    let (_value, metrics) = with_structural_read_metrics(|| {
        let mut reader = StructuralSlotReader::from_raw_row_lazy(&raw_row, &TEST_MODEL)
            .expect("row-open structural envelope decode should succeed");

        reader
            .get_value(1)
            .expect("deferred slot should materialize")
    });

    assert_eq!(metrics.rows_opened, 1);
    assert_eq!(metrics.declared_slots_validated, 1);
    assert_eq!(metrics.validated_non_scalar_slots, 1);
    assert_eq!(
        metrics.materialized_non_scalar_slots, 1,
        "first semantic access should materialize the value-storage slot exactly once",
    );
    assert_eq!(metrics.rows_without_lazy_non_scalar_materializations, 0);
}

#[test]
fn structural_slot_reader_rejects_malformed_unused_value_storage_slot_on_first_access() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(&[0xFF]))
        .expect("write malformed value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");

    let mut reader = StructuralSlotReader::from_raw_row_lazy(&raw_row, &TEST_MODEL)
        .expect("row-open structural envelope decode should succeed");
    let err = reader
        .get_value(1)
        .expect_err("malformed unused value-storage slot must fail on first semantic access");

    assert!(
        err.message.contains("field 'payload'"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn apply_update_patch_to_raw_row_updates_only_targeted_slots() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");
    let patch = UpdatePatch::new().set(
        FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
        Value::Text("Grace".to_string()),
    );

    let patched =
        apply_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &patch).expect("apply patch");
    let mut reader = StructuralSlotReader::from_raw_row(&patched, &TEST_MODEL).expect("decode row");

    assert_eq!(
        reader.get_value(0).expect("decode slot"),
        Some(Value::Text("Grace".to_string()))
    );
    assert_eq!(
        reader.get_value(1).expect("decode slot"),
        Some(Value::Text("payload".to_string()))
    );
}

#[test]
fn serialize_update_patch_fields_encodes_canonical_slot_payloads() {
    let patch = UpdatePatch::new()
        .set(
            FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
            Value::Text("Grace".to_string()),
        )
        .set(
            FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
            Value::Text("payload".to_string()),
        );

    let serialized = serialize_update_patch_fields(&TEST_MODEL, &patch).expect("serialize patch");

    assert_eq!(serialized.entries().len(), 2);
    assert_eq!(
        decode_slot_value_from_bytes(
            &TEST_MODEL,
            serialized.entries()[0].slot().index(),
            serialized.entries()[0].payload(),
        )
        .expect("decode slot payload"),
        Value::Text("Grace".to_string())
    );
    assert_eq!(
        decode_slot_value_from_bytes(
            &TEST_MODEL,
            serialized.entries()[1].slot().index(),
            serialized.entries()[1].payload(),
        )
        .expect("decode slot payload"),
        Value::Text("payload".to_string())
    );
}

#[test]
fn serialized_patch_writer_rejects_clear_slots() {
    let mut writer = CompleteSerializedPatchWriter::for_model(&TEST_MODEL);

    let err = writer
        .write_slot(0, None)
        .expect_err("0.65 patch staging must reject missing-slot clears");

    assert!(
        err.message
            .contains("serialized patch writer cannot clear slot 0"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message.contains(TEST_MODEL.path()),
        "unexpected error: {err:?}"
    );
}

#[test]
fn slot_buffer_writer_rejects_clear_slots() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);

    let err = writer
        .write_slot(0, None)
        .expect_err("canonical row staging must reject missing-slot clears");

    assert!(
        err.message
            .contains("slot buffer writer cannot clear slot 0"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message.contains(TEST_MODEL.path()),
        "unexpected error: {err:?}"
    );
}

#[test]
fn apply_update_patch_to_raw_row_uses_last_write_wins() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");
    let slot = FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot");
    let patch = UpdatePatch::new()
        .set(slot, Value::Text("Grace".to_string()))
        .set(slot, Value::Text("Lin".to_string()));

    let patched =
        apply_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &patch).expect("apply patch");
    let mut reader = StructuralSlotReader::from_raw_row(&patched, &TEST_MODEL).expect("decode row");

    assert_eq!(
        reader.get_value(0).expect("decode slot"),
        Some(Value::Text("Lin".to_string()))
    );
}

#[test]
fn apply_update_patch_to_raw_row_rejects_noncanonical_missing_slot_baseline() {
    let empty_slots = vec![None::<&[u8]>; TEST_MODEL.fields().len()];
    let raw_row = RawRow::try_new(
        serialize_row_payload(
            encode_slot_payload_allowing_missing_for_tests(&TEST_MODEL, empty_slots.as_slice())
                .expect("encode malformed slot payload"),
        )
        .expect("serialize row payload"),
    )
    .expect("build raw row");
    let patch = UpdatePatch::new().set(
        FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
        Value::Text("payload".to_string()),
    );

    let err = apply_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &patch)
        .expect_err("noncanonical rows with missing slots must fail closed");

    assert_eq!(err.message, "row decode: missing slot payload: slot=0");
}

#[test]
fn apply_serialized_update_patch_to_raw_row_rejects_noncanonical_scalar_baseline() {
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    let malformed_slots = [Some([0xF6].as_slice()), Some(payload.as_slice())];
    let raw_row = RawRow::try_new(
        serialize_row_payload(
            encode_slot_payload_allowing_missing_for_tests(&TEST_MODEL, &malformed_slots)
                .expect("encode malformed slot payload"),
        )
        .expect("serialize row payload"),
    )
    .expect("build raw row");
    let patch = UpdatePatch::new().set(
        FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
        Value::Text("patched".to_string()),
    );
    let serialized = serialize_update_patch_fields(&TEST_MODEL, &patch).expect("serialize patch");

    let err = apply_serialized_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &serialized)
        .expect_err("noncanonical scalar baseline must fail closed");

    assert!(
        err.message.contains("field 'name'"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message
            .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn apply_serialized_update_patch_to_raw_row_rejects_noncanonical_scalar_patch_payload() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");
    let serialized = SerializedUpdatePatch::new(vec![SerializedFieldUpdate::new(
        FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
        vec![0xF6],
    )]);

    let err = apply_serialized_update_patch_to_raw_row(&TEST_MODEL, &raw_row, &serialized)
        .expect_err("noncanonical serialized patch payload must fail closed");

    assert!(
        err.message.contains("field 'name'"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message
            .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn structural_slot_reader_rejects_slot_count_mismatch() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload =
        crate::serialize::serialize(&Value::Text("payload".to_string())).expect("encode payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write payload slot");
    let mut payload = writer.finish().expect("finish slot payload");
    payload[..2].copy_from_slice(&1_u16.to_be_bytes());
    let raw_row = RawRow::try_new(serialize_row_payload(payload).expect("serialize row payload"))
        .expect("build raw row");

    let err = StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL)
        .err()
        .expect("slot-count drift must fail closed");

    assert_eq!(
        err.message,
        "row decode: slot count mismatch: expected 2, found 1"
    );
}

#[test]
fn structural_slot_reader_rejects_slot_span_exceeds_payload_length() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload =
        crate::serialize::serialize(&Value::Text("payload".to_string())).expect("encode payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write payload slot");
    let mut payload = writer.finish().expect("finish slot payload");

    // Corrupt the second slot span so the payload table points past the
    // available data section.
    payload[14..18].copy_from_slice(&u32::MAX.to_be_bytes());
    let raw_row = RawRow::try_new(serialize_row_payload(payload).expect("serialize row payload"))
        .expect("build raw row");

    let err = StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL)
        .err()
        .expect("slot span drift must fail closed");

    assert_eq!(err.message, "row decode: slot span exceeds payload length");
}

#[test]
fn apply_serialized_update_patch_to_raw_row_replays_preencoded_slots() {
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    writer
        .write_scalar(0, ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")))
        .expect("write scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize row payload"),
    )
    .expect("build raw row");
    let patch = UpdatePatch::new().set(
        FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
        Value::Text("Grace".to_string()),
    );
    let serialized = serialize_update_patch_fields(&TEST_MODEL, &patch).expect("serialize patch");

    let patched = raw_row
        .apply_serialized_update_patch(&TEST_MODEL, &serialized)
        .expect("apply serialized patch");
    let mut reader = StructuralSlotReader::from_raw_row(&patched, &TEST_MODEL).expect("decode row");

    assert_eq!(
        reader.get_value(0).expect("decode slot"),
        Some(Value::Text("Grace".to_string()))
    );
}

#[test]
fn serialize_entity_slots_as_complete_serialized_patch_replays_full_typed_after_image() {
    let old_entity = PersistedRowPatchBridgeEntity {
        id: crate::types::Ulid::from_u128(7),
        name: "Ada".to_string(),
    };
    let new_entity = PersistedRowPatchBridgeEntity {
        id: crate::types::Ulid::from_u128(7),
        name: "Grace".to_string(),
    };
    let raw_row = RawRow::from_entity(&old_entity).expect("encode old row");
    let old_decoded = raw_row
        .try_decode::<PersistedRowPatchBridgeEntity>()
        .expect("decode old entity");
    let serialized = serialize_entity_slots_as_complete_serialized_patch(&new_entity)
        .expect("serialize complete entity slot image");
    let direct = RawRow::from_complete_serialized_update_patch(
        PersistedRowPatchBridgeEntity::MODEL,
        &serialized,
    )
    .expect("direct row emission should succeed");

    let patched = raw_row
        .apply_serialized_update_patch(PersistedRowPatchBridgeEntity::MODEL, &serialized)
        .expect("apply serialized patch");
    let decoded = patched
        .try_decode::<PersistedRowPatchBridgeEntity>()
        .expect("decode patched entity");

    assert_eq!(
        direct, patched,
        "fresh row emission and replayed full-image patch must converge on identical bytes",
    );
    assert_eq!(old_decoded, old_entity);
    assert_eq!(decoded, new_entity);
}

#[test]
fn materialize_entity_from_serialized_update_patch_rejects_missing_required_field() {
    let patch = UpdatePatch::new().set(
        FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
        Value::Text("Ada".to_string()),
    );
    let serialized = serialize_update_patch_fields(PersistedRowPatchBridgeEntity::MODEL, &patch)
        .expect("serialize sparse patch");

    let err = materialize_entity_from_serialized_update_patch::<PersistedRowPatchBridgeEntity>(
        &serialized,
    )
    .expect_err("sparse typed bridge must fail closed when a required slot is absent");

    assert_eq!(err.message, "row decode: missing required field 'id'");
}

#[test]
fn materialize_entity_from_serialized_update_patch_rejects_noncanonical_scalar_payload() {
    let patch = UpdatePatch::new()
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            Value::Ulid(crate::types::Ulid::from_u128(7)),
        )
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            Value::Text("Ada".to_string()),
        );
    let valid = serialize_update_patch_fields(PersistedRowPatchBridgeEntity::MODEL, &patch)
        .expect("serialize valid patch");
    let serialized = SerializedUpdatePatch::new(vec![
        SerializedFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            vec![0xF6],
        ),
        SerializedFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            valid.entries()[1].payload().to_vec(),
        ),
    ]);

    let err = materialize_entity_from_serialized_update_patch::<PersistedRowPatchBridgeEntity>(
        &serialized,
    )
    .expect_err("typed sparse patch bridge must reject malformed scalar payloads");

    assert!(
        err.message.contains("field 'id'"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message
            .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn canonical_row_from_complete_serialized_update_patch_rejects_noncanonical_scalar_payload() {
    let patch = UpdatePatch::new()
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            Value::Ulid(crate::types::Ulid::from_u128(7)),
        )
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            Value::Text("Ada".to_string()),
        );
    let valid = serialize_update_patch_fields(PersistedRowPatchBridgeEntity::MODEL, &patch)
        .expect("serialize valid patch");
    let serialized = SerializedUpdatePatch::new(vec![
        SerializedFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            vec![0xF6],
        ),
        SerializedFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            valid.entries()[1].payload().to_vec(),
        ),
    ]);

    let err = canonical_row_from_complete_serialized_update_patch(
        PersistedRowPatchBridgeEntity::MODEL,
        &serialized,
    )
    .expect_err("complete serialized patch row emission must reject malformed scalar payloads");

    assert!(
        err.message.contains("field 'id'"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message
            .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn canonical_row_from_complete_serialized_update_patch_rejects_incomplete_slot_image() {
    let patch = UpdatePatch::new().set(
        FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
        Value::Text("Ada".to_string()),
    );
    let serialized = serialize_update_patch_fields(PersistedRowPatchBridgeEntity::MODEL, &patch)
        .expect("serialize sparse patch");

    let err = canonical_row_from_complete_serialized_update_patch(
        PersistedRowPatchBridgeEntity::MODEL,
        &serialized,
    )
    .expect_err("complete serialized patch row emission must reject missing declared slots");

    assert!(
        err.message.contains("serialized patch did not emit slot 0"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn canonical_row_from_raw_row_replays_canonical_full_image_bytes() {
    let entity = PersistedRowPatchBridgeEntity {
        id: crate::types::Ulid::from_u128(11),
        name: "Ada".to_string(),
    };
    let raw_row = RawRow::from_entity(&entity).expect("encode canonical row");
    let canonical =
        super::canonical_row_from_raw_row(PersistedRowPatchBridgeEntity::MODEL, &raw_row)
            .expect("canonical re-emission should succeed");

    assert_eq!(
        canonical.as_bytes(),
        raw_row.as_bytes(),
        "canonical raw-row rebuild must preserve already canonical row bytes",
    );
}

#[test]
fn canonical_row_from_raw_row_rejects_noncanonical_scalar_payload() {
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    let mut writer = SlotBufferWriter::for_model(&TEST_MODEL);
    writer
        .write_slot(0, Some(&[0xF6]))
        .expect("write malformed scalar slot");
    writer
        .write_slot(1, Some(payload.as_slice()))
        .expect("write value-storage slot");
    let raw_row = RawRow::try_new(
        serialize_row_payload(writer.finish().expect("finish slot payload"))
            .expect("serialize malformed row"),
    )
    .expect("build malformed raw row");

    let err = super::canonical_row_from_raw_row(&TEST_MODEL, &raw_row)
        .expect_err("canonical raw-row rebuild must reject malformed scalar payloads");

    assert!(
        err.message.contains("field 'name'"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message
            .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn raw_row_from_complete_serialized_update_patch_rejects_noncanonical_scalar_payload() {
    let payload = crate::serialize::serialize(&Value::Text("payload".to_string()))
        .expect("encode value-storage payload");
    let serialized = SerializedUpdatePatch::new(vec![
        SerializedFieldUpdate::new(
            FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
            vec![0xF6],
        ),
        SerializedFieldUpdate::new(
            FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
            payload,
        ),
    ]);

    let err = RawRow::from_complete_serialized_update_patch(&TEST_MODEL, &serialized)
        .expect_err("fresh row emission must reject noncanonical serialized patch payloads");

    assert!(
        err.message.contains("field 'name'"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message
            .contains("expected slot envelope prefix byte 0xFF, found 0xF6"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn raw_row_from_complete_serialized_update_patch_rejects_incomplete_slot_image() {
    let serialized = SerializedUpdatePatch::new(vec![SerializedFieldUpdate::new(
        FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
        crate::serialize::serialize(&Value::Text("payload".to_string()))
            .expect("encode value-storage payload"),
    )]);

    let err = RawRow::from_complete_serialized_update_patch(&TEST_MODEL, &serialized)
        .expect_err("fresh row emission must reject missing declared slots");

    assert!(
        err.message.contains("serialized patch did not emit slot 0"),
        "unexpected error: {err:?}"
    );
}
