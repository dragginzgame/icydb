use super::{
    SlotReader, SlotWriter, StructuralPatch,
    apply_serialized_structural_patch_to_raw_row_with_accepted_contract,
    canonical_row_from_complete_serialized_structural_patch,
    canonical_row_from_raw_row_with_structural_contract, decode_persisted_slot_payload_by_kind,
    decode_persisted_structured_many_slot_payload, decode_persisted_structured_slot_payload,
    decode_sparse_required_slot_with_contract, encode_persisted_slot_payload_by_kind,
    encode_persisted_structured_many_slot_payload, encode_persisted_structured_slot_payload,
    materialize_entity_from_serialized_structural_patch,
    materialize_entity_from_serialized_structural_patch_with_accepted_contract,
    serialize_complete_structural_patch_fields_with_accepted_contract,
    serialize_entity_slots_as_complete_serialized_patch,
    serialize_structural_patch_fields_with_accepted_contract, with_structural_read_metrics,
};
use super::{
    codec::{ScalarSlotValueRef, ScalarValueRef, encode_scalar_slot_value},
    contract::{
        decode_slot_into_runtime_value, encode_runtime_value_into_slot,
        encode_slot_payload_from_parts,
    },
    reader::{CachedSlotValue, StructuralSlotReader},
    types::{FieldSlot, SerializedStructuralFieldUpdate, SerializedStructuralPatch},
    writer::CompleteSerializedPatchWriter,
};
use crate::{
    db::{
        codec::serialize_row_payload,
        data::{
            CanonicalRow, RawRow, StructuralRowContract, decode_structural_row_payload,
            decode_structural_value_storage_bytes, encode_structural_value_storage_bytes,
            structural_field::{
                encode_list_field_items, encode_map_field_entries,
                encode_value_storage_list_item_slices, encode_value_storage_map_entry_slices,
            },
        },
        predicate::{ComparePredicate, Predicate, PredicateProgram},
        schema::{
            AcceptedRowDecodeContract, AcceptedRowLayoutRuntimeDescriptor, AcceptedSchemaSnapshot,
            PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldDefault,
            compiled_schema_proposal_for_model,
        },
    },
    error::{ErrorClass, InternalError},
    model::{
        EntityModel,
        field::{EnumVariantModel, FieldKind, FieldModel, FieldStorageDecode, RelationStrength},
    },
    testing::SIMPLE_ENTITY_TAG,
    traits::{
        EntitySchema, FieldTypeMeta, PersistedByKindCodec, PersistedFieldSlotCodec,
        PersistedStructuredFieldCodec, RuntimeValueDecode, RuntimeValueEncode,
    },
    types::{
        Account, Blob, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128,
        Principal, Subaccount, Timestamp, Ulid, Unit,
    },
    value::StorageKey,
    value::{Value, ValueEnum},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

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
///
/// PersistedRowPatchBridgeEntity is the smallest derive-owned entity used
/// to validate the typed-entity -> serialized-patch bridge.
/// It lets the persisted-row tests exercise the same dense slot writer the
/// save/update path now uses.
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct PersistedRowPatchBridgeEntity {
    id: crate::types::Ulid,
    name: String,
}

///
/// PersistedRowTypedMetaEntity
///
/// PersistedRowTypedMetaEntity proves that the metadata-free `PersistedRow`
/// derive can reuse a typed field's own slot codec directly.
/// It intentionally uses a schema-like wrapper instead of the runtime-only
/// `Value` union so persisted fields stay statically contracted.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct PersistedRowTypedMetaEntity {
    id: crate::types::Ulid,
    payload: PersistedRowProfileValue,
}

impl Default for PersistedRowTypedMetaEntity {
    fn default() -> Self {
        Self {
            id: crate::types::Ulid::from_u128(0),
            payload: PersistedRowProfileValue::default(),
        }
    }
}

///
/// PersistedRowManyTypedMetaEntity
///
/// PersistedRowManyTypedMetaEntity proves that the metadata-free
/// `PersistedRow` derive can persist containers of typed schema-like fields
/// without allowing `Vec<Value>` as a persisted escape hatch.
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct PersistedRowManyTypedMetaEntity {
    id: crate::types::Ulid,
    payloads: Vec<PersistedRowProfileValue>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct PersistedRowProfileValue {
    bio: String,
}

#[derive(Clone, Debug, PartialEq)]
struct DirectPersistedProfileValue {
    bio: String,
}

impl PersistedStructuredFieldCodec for DirectPersistedProfileValue {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        let bio = self.bio.as_bytes();
        let len = u16::try_from(bio.len())
            .map_err(|_| InternalError::persisted_row_encode_failed("bio payload too large"))?;
        let mut out = Vec::with_capacity(2 + bio.len());
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(bio);
        Ok(out)
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        if bytes.len() < 2 {
            return Err(InternalError::persisted_row_decode_failed(
                "direct structured payload missing length prefix",
            ));
        }

        let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        let bio_bytes = &bytes[2..];
        if bio_bytes.len() != len {
            return Err(InternalError::persisted_row_decode_failed(format!(
                "direct structured payload length prefix mismatch: declared={len} actual={}",
                bio_bytes.len()
            )));
        }

        let bio = std::str::from_utf8(bio_bytes)
            .map_err(InternalError::persisted_row_decode_failed)?
            .to_owned();

        Ok(Self { bio })
    }
}

impl PersistedStructuredFieldCodec for PersistedRowProfileValue {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        let bio_key = crate::db::encode_generated_structural_text_payload_bytes("bio");
        let bio_value = String::encode_persisted_structured_payload(&self.bio)?;
        let entries = [(bio_key.as_slice(), bio_value.as_slice())];

        Ok(crate::db::encode_generated_structural_map_payload_bytes(
            entries.as_slice(),
        ))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        let entries = crate::db::decode_generated_structural_map_payload_bytes(bytes)?;
        if entries.len() != 1 {
            return Err(InternalError::persisted_row_decode_failed(format!(
                "structured profile payload field count mismatch: expected 1, got {}",
                entries.len(),
            )));
        }

        let (entry_key, entry_value) = entries[0];
        let entry_name = crate::db::decode_generated_structural_text_payload_bytes(entry_key)?;
        if entry_name != "bio" {
            return Err(InternalError::persisted_row_decode_failed(format!(
                "structured profile payload contains unknown field `{entry_name}`",
            )));
        }

        Ok(Self {
            bio: String::decode_persisted_structured_payload(entry_value)?,
        })
    }
}

impl FieldTypeMeta for PersistedRowProfileValue {
    const KIND: FieldKind = FieldKind::Structured { queryable: false };
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
    const NESTED_FIELDS: &'static [FieldModel] = &[FieldModel::generated(
        "bio",
        FieldKind::Text { max_len: None },
    )];
}

impl RuntimeValueEncode for PersistedRowProfileValue {
    fn to_value(&self) -> Value {
        Value::from_map(vec![(
            Value::Text("bio".to_string()),
            Value::Text(self.bio.clone()),
        )])
        .expect("typed persisted-row profile value should normalize")
    }
}

impl RuntimeValueDecode for PersistedRowProfileValue {
    fn from_value(value: &Value) -> Option<Self> {
        let Value::Map(entries) = value else {
            return None;
        };

        entries.iter().find_map(|(key, value)| match (key, value) {
            (Value::Text(key), Value::Text(value)) if key == "bio" => {
                Some(Self { bio: value.clone() })
            }
            _ => None,
        })
    }
}

impl PersistedFieldSlotCodec for PersistedRowProfileValue {
    fn encode_persisted_slot(&self, field_name: &'static str) -> Result<Vec<u8>, InternalError> {
        crate::db::encode_persisted_slot_payload_by_meta(self, field_name)
    }

    fn decode_persisted_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        crate::db::decode_persisted_slot_payload_by_meta(bytes, field_name)
    }

    fn encode_persisted_option_slot(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        crate::db::encode_persisted_option_slot_payload_by_meta(value, field_name)
    }

    fn decode_persisted_option_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        crate::db::decode_persisted_option_slot_payload_by_meta(bytes, field_name)
    }
}

impl PersistedByKindCodec for PersistedRowProfileValue {
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        match kind {
            FieldKind::Structured { queryable } => {
                if queryable {
                    return Err(InternalError::persisted_row_field_encode_failed(
                        field_name,
                        "structured by-kind queryability mismatch",
                    ));
                }

                self.encode_persisted_structured_payload()
            }
            other => Err(InternalError::persisted_row_field_encode_failed(
                field_name,
                format!("field kind {other:?} does not accept structured profile payload"),
            )),
        }
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        match kind {
            FieldKind::Structured { queryable } => {
                if queryable {
                    return Err(InternalError::persisted_row_field_decode_failed(
                        field_name,
                        "structured by-kind queryability mismatch",
                    ));
                }

                Self::decode_persisted_structured_payload(bytes).map(Some)
            }
            other => Err(InternalError::persisted_row_field_decode_failed(
                field_name,
                format!("field kind {other:?} does not accept structured profile payload"),
            )),
        }
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
        ("name", FieldKind::Text { max_len: None }),
    ],
    indexes = [],
    store = PersistedRowPatchBridgeStore,
    canister = PersistedRowPatchBridgeCanister,
}

crate::test_entity_schema! {
    ident = PersistedRowTypedMetaEntity,
    id = crate::types::Ulid,
    id_field = id,
    entity_name = "PersistedRowTypedMetaEntity",
    entity_tag = SIMPLE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "payload",
            FieldKind::Structured { queryable: false },
            FieldStorageDecode::Value
        ),
    ],
    indexes = [],
    store = PersistedRowPatchBridgeStore,
    canister = PersistedRowPatchBridgeCanister,
}

crate::test_entity_schema! {
    ident = PersistedRowManyTypedMetaEntity,
    id = crate::types::Ulid,
    id_field = id,
    entity_name = "PersistedRowManyTypedMetaEntity",
    entity_tag = SIMPLE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "payloads",
            FieldKind::List(&FieldKind::Structured { queryable: false }),
            FieldStorageDecode::Value
        ),
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
    FieldModel::generated("name", FieldKind::Text { max_len: None }),
    FieldModel::generated_with_storage_decode(
        "payload",
        FieldKind::Text { max_len: None },
        FieldStorageDecode::Value,
    ),
];
static ADDITIVE_NULLABLE_FIELD_MODELS: [FieldModel; 3] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("name", FieldKind::Text { max_len: None }),
    FieldModel::generated_with_storage_decode_and_nullability(
        "nickname",
        FieldKind::Text { max_len: None },
        FieldStorageDecode::ByKind,
        true,
    ),
];
static ADDITIVE_PREFIX_FIELD_MODELS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("name", FieldKind::Text { max_len: None }),
];
static ADDITIVE_REQUIRED_FIELD_MODELS: [FieldModel; 3] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("name", FieldKind::Text { max_len: None }),
    FieldModel::generated("score", FieldKind::Uint),
];
static LIST_FIELD_MODELS: [FieldModel; 1] = [FieldModel::generated(
    "tags",
    FieldKind::List(&FieldKind::Text { max_len: None }),
)];
static MAP_FIELD_MODELS: [FieldModel; 1] = [FieldModel::generated(
    "props",
    FieldKind::Map {
        key: &FieldKind::Text { max_len: None },
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
static OPTIONAL_DECIMAL_FIELD_MODELS: [FieldModel; 1] =
    [FieldModel::generated_with_storage_decode_and_nullability(
        "attribute_score_normalized",
        FieldKind::Decimal { scale: 3 },
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
static ADDITIVE_NULLABLE_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowAdditiveNullableEntity",
    "persisted_row_additive_nullable_entity",
    &ADDITIVE_NULLABLE_FIELD_MODELS[0],
    0,
    &ADDITIVE_NULLABLE_FIELD_MODELS,
    &INDEX_MODELS,
);
static ADDITIVE_PREFIX_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowAdditiveNullableEntity",
    "persisted_row_additive_nullable_entity",
    &ADDITIVE_PREFIX_FIELD_MODELS[0],
    0,
    &ADDITIVE_PREFIX_FIELD_MODELS,
    &INDEX_MODELS,
);
static ADDITIVE_REQUIRED_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowAdditiveRequiredEntity",
    "persisted_row_additive_required_entity",
    &ADDITIVE_REQUIRED_FIELD_MODELS[0],
    0,
    &ADDITIVE_REQUIRED_FIELD_MODELS,
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
static OPTIONAL_DECIMAL_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowOptionalDecimalFieldCodecEntity",
    "persisted_row_optional_decimal_field_codec_entity",
    &OPTIONAL_DECIMAL_FIELD_MODELS[0],
    0,
    &OPTIONAL_DECIMAL_FIELD_MODELS,
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
static RELATION_PK_KEY_KIND: FieldKind = FieldKind::Ulid;
static RELATION_PK_FIELD_MODELS: [FieldModel; 1] = [FieldModel::generated(
    "token_id",
    FieldKind::Relation {
        target_path: "tests::PersistedRowRelationPkTargetEntity",
        target_entity_name: "PersistedRowRelationPkTargetEntity",
        target_entity_tag: crate::types::EntityTag::new(71),
        target_store_path: "tests::PersistedRowRelationPkTargetStore",
        key_kind: &RELATION_PK_KEY_KIND,
        strength: RelationStrength::Weak,
    },
)];
static RELATION_PK_MODEL: EntityModel = EntityModel::generated(
    "tests::PersistedRowRelationPkEntity",
    "persisted_row_relation_pk_entity",
    &RELATION_PK_FIELD_MODELS[0],
    0,
    &RELATION_PK_FIELD_MODELS,
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

// Encode one persisted `FieldStorageDecode::Value` fixture payload through the
// owner-local structural value-storage boundary.
fn encode_value_storage_payload(value: &Value) -> Vec<u8> {
    encode_structural_value_storage_bytes(value)
        .expect("value-storage payload fixture should encode")
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

// Build one dense canonical slot container for tests without routing through a
// dedicated writer type now that production no longer needs that staging path.
fn encode_dense_slot_payload_for_tests(
    model: &'static EntityModel,
    slots: &[&[u8]],
) -> Result<Vec<u8>, InternalError> {
    let dense_slots = slots.iter().copied().map(Some).collect::<Vec<_>>();

    encode_slot_payload_allowing_missing_for_tests(model, dense_slots.as_slice())
}

// Build one raw row fixture from already-encoded dense slot payload bytes.
fn raw_row_from_dense_slot_payloads_for_tests(
    model: &'static EntityModel,
    slots: &[&[u8]],
) -> RawRow {
    let slot_payload =
        encode_dense_slot_payload_for_tests(model, slots).expect("encode dense slot payload");

    RawRow::try_new(serialize_row_payload(slot_payload).expect("serialize row payload"))
        .expect("build raw row")
}

// Build one structural row contract from the accepted schema path so tests can
// exercise row decode against saved-schema field contracts instead of the
// generated-only fallback.
fn accepted_row_decode_contract_for_model(
    model: &'static EntityModel,
) -> AcceptedRowDecodeContract {
    let snapshot = compiled_schema_proposal_for_model(model).initial_persisted_schema_snapshot();
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot).expect("accepted schema fixture should validate");
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("accepted runtime descriptor should build");

    descriptor.row_decode_contract()
}

// Build one structural row contract from the accepted schema path so tests can
// exercise row decode against saved-schema field contracts instead of the
// generated-only fallback.
fn accepted_row_contract_for_model(model: &'static EntityModel) -> StructuralRowContract {
    StructuralRowContract::from_generated_model_with_accepted_decode_contract_for_test(
        model,
        accepted_row_decode_contract_for_model(model),
    )
}

// Serialize one structural patch through the accepted-schema fixture contract.
// Tests use this helper instead of the removed generated serializer so write
// boundary coverage exercises the same authority production save paths use.
fn serialize_structural_patch_fields_for_accepted_test_model(
    model: &'static EntityModel,
    patch: &StructuralPatch,
) -> Result<SerializedStructuralPatch, InternalError> {
    serialize_structural_patch_fields_with_accepted_contract(
        model.path(),
        accepted_row_decode_contract_for_model(model),
        patch,
    )
}

// Replay one serialized structural patch through the accepted-schema fixture
// contract. This keeps patch replay tests on the accepted after-image path
// rather than preserving the old generated-row replay helper.
fn apply_serialized_structural_patch_to_raw_row_for_accepted_test_model(
    model: &'static EntityModel,
    raw_row: &RawRow,
    patch: &SerializedStructuralPatch,
) -> Result<CanonicalRow, InternalError> {
    apply_serialized_structural_patch_to_raw_row_with_accepted_contract(
        model.path(),
        accepted_row_decode_contract_for_model(model),
        raw_row,
        patch,
    )
}

// Rebuild one raw row through the accepted-schema fixture contract. This is
// the test counterpart to accepted before-image canonicalization in save.
fn canonical_row_from_raw_row_for_accepted_test_model(
    model: &'static EntityModel,
    raw_row: &RawRow,
) -> Result<CanonicalRow, InternalError> {
    canonical_row_from_raw_row_with_structural_contract(
        raw_row,
        accepted_row_contract_for_model(model),
    )
}

// Build one accepted row contract for the additive required-field fixture with
// an explicit schema-owned default payload on the appended score slot.
fn accepted_defaulted_required_score_row_decode_contract_for_tests(
    score_payload: Vec<u8>,
) -> AcceptedRowDecodeContract {
    let proposal = compiled_schema_proposal_for_model(&ADDITIVE_REQUIRED_MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let mut fields = expected.fields().to_vec();
    let score = &expected.fields()[2];
    fields[2] = PersistedFieldSnapshot::new_with_write_policy(
        score.id(),
        score.name().to_string(),
        score.slot(),
        score.kind().clone(),
        score.nested_leaves().to_vec(),
        score.nullable(),
        SchemaFieldDefault::SlotPayload(score_payload),
        score.write_policy(),
        score.storage_decode(),
        score.leaf_codec(),
    );
    let defaulted_snapshot = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_id(),
        expected.row_layout().clone(),
        fields,
    );
    let accepted = AcceptedSchemaSnapshot::try_new(defaulted_snapshot)
        .expect("accepted defaulted schema fixture should validate");
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
        .expect("accepted defaulted runtime descriptor should build");

    descriptor.row_decode_contract()
}

// Build one accepted row contract for the additive required-field fixture with
// an explicit schema-owned default payload on the appended score slot.
fn accepted_defaulted_required_score_row_contract_for_tests(
    score_payload: Vec<u8>,
) -> StructuralRowContract {
    StructuralRowContract::from_generated_model_with_accepted_decode_contract_for_test(
        &ADDITIVE_REQUIRED_MODEL,
        accepted_defaulted_required_score_row_decode_contract_for_tests(score_payload),
    )
}

// Build an old two-slot row fixture used to test accepted additive contracts.
// The fixture simulates a row written before the third field was appended.
fn old_two_slot_additive_raw_row_for_tests(id: Ulid) -> RawRow {
    let id_payload = encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Ulid(id)));
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let slot_payload = encode_slot_payload_from_parts(
        2,
        &[
            (
                0_u32,
                u32::try_from(id_payload.len()).expect("id payload length should fit"),
            ),
            (
                u32::try_from(id_payload.len()).expect("name payload start should fit"),
                u32::try_from(name_payload.len()).expect("name payload length should fit"),
            ),
        ],
        &[id_payload.as_slice(), name_payload.as_slice()].concat(),
    )
    .expect("old two-slot payload should encode");

    RawRow::try_new(serialize_row_payload(slot_payload).expect("old two-slot row should serialize"))
        .expect("old two-slot row should be accepted as raw bytes")
}

// Build a malformed old two-slot row whose existing second slot span points
// beyond the payload table. Accepted append-only compatibility must not mask
// corruption in physical slots that are actually present.
fn malformed_old_two_slot_additive_raw_row_for_tests(id: Ulid) -> RawRow {
    let id_payload = encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Ulid(id)));
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let slot_payload = encode_slot_payload_from_parts(
        2,
        &[
            (
                0_u32,
                u32::try_from(id_payload.len()).expect("id payload length should fit"),
            ),
            (
                u32::try_from(id_payload.len()).expect("name payload start should fit"),
                u32::MAX,
            ),
        ],
        &[id_payload.as_slice(), name_payload.as_slice()].concat(),
    )
    .expect("malformed old two-slot payload should encode");

    RawRow::try_new(serialize_row_payload(slot_payload).expect("malformed row should serialize"))
        .expect("malformed old two-slot row should stay bounded raw bytes")
}

fn assert_direct_persisted_structured_roundtrip<T>(value: T)
where
    T: Clone + std::fmt::Debug + PartialEq + PersistedStructuredFieldCodec,
{
    let encoded = value
        .encode_persisted_structured_payload()
        .expect("direct structured payload should encode");
    let decoded = T::decode_persisted_structured_payload(encoded.as_slice())
        .expect("direct structured payload should decode");
    let reencoded = decoded
        .encode_persisted_structured_payload()
        .expect("decoded structured payload should re-encode canonically");

    assert_eq!(decoded, value);
    assert_eq!(reencoded, encoded);
}

fn assert_direct_persisted_by_kind_roundtrip<T>(value: T, kind: FieldKind)
where
    T: Clone + std::fmt::Debug + PartialEq + PersistedByKindCodec,
{
    let encoded = value
        .encode_persisted_slot_payload_by_kind(kind, "sample")
        .expect("direct by-kind payload should encode");
    let decoded =
        T::decode_persisted_option_slot_payload_by_kind(encoded.as_slice(), kind, "sample")
            .expect("direct by-kind payload should decode");
    let reencoded = decoded
        .as_ref()
        .expect("roundtrip should keep direct by-kind value present")
        .encode_persisted_slot_payload_by_kind(kind, "sample")
        .expect("decoded by-kind payload should re-encode canonically");

    assert_eq!(decoded, Some(value));
    assert_eq!(reencoded, encoded);
}

fn assert_direct_persisted_by_kind_rejects_truncated_payload<T>(value: T, kind: FieldKind)
where
    T: Clone + std::fmt::Debug + PartialEq + PersistedByKindCodec,
{
    let encoded = value
        .encode_persisted_slot_payload_by_kind(kind, "sample")
        .expect("direct by-kind payload should encode");
    let truncated = encoded[..encoded.len().saturating_sub(1)].to_vec();
    let err = T::decode_persisted_option_slot_payload_by_kind(truncated.as_slice(), kind, "sample")
        .expect_err("truncated by-kind payload must fail closed");

    assert_eq!(err.class(), crate::error::ErrorClass::Corruption);
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DirectByKindLeaf(u64);

impl PersistedByKindCodec for DirectByKindLeaf {
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        self.0
            .encode_persisted_slot_payload_by_kind(kind, field_name)
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        u64::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)
            .map(|value| value.map(Self))
    }
}

#[test]
fn direct_persisted_structured_scalar_codecs_cover_reachable_leaf_family() {
    assert_direct_persisted_structured_roundtrip(true);
    assert_direct_persisted_structured_roundtrip(String::from("Ada"));
    assert_direct_persisted_structured_roundtrip(Blob::from(vec![0xAB, 0xCD]));
    assert_direct_persisted_structured_roundtrip(Account::new(
        Principal::anonymous(),
        Some(Subaccount::from_array([7u8; 32])),
    ));
    assert_direct_persisted_structured_roundtrip(Decimal::new(12_345, 2));
    assert_direct_persisted_structured_roundtrip(Float32::from(12));
    assert_direct_persisted_structured_roundtrip(Float64::from(34));
    assert_direct_persisted_structured_roundtrip(Principal::anonymous());
    assert_direct_persisted_structured_roundtrip(Subaccount::from_array([9u8; 32]));
    assert_direct_persisted_structured_roundtrip(Timestamp::from_millis(1_234_567));
    assert_direct_persisted_structured_roundtrip(Date::from_days_since_epoch(321));
    assert_direct_persisted_structured_roundtrip(Duration::from_millis(9_876));
    assert_direct_persisted_structured_roundtrip(Ulid::from_parts(77, 3));
    assert_direct_persisted_structured_roundtrip(Int128::from(-123_i128));
    assert_direct_persisted_structured_roundtrip(Nat128::from(456_u128));
    assert_direct_persisted_structured_roundtrip(Int::from(-789_i32));
    assert_direct_persisted_structured_roundtrip(Nat::from(987_u64));
    assert_direct_persisted_structured_roundtrip(Unit);
    assert_direct_persisted_structured_roundtrip(-5_i8);
    assert_direct_persisted_structured_roundtrip(-6_i16);
    assert_direct_persisted_structured_roundtrip(-7_i32);
    assert_direct_persisted_structured_roundtrip(-8_i64);
    assert_direct_persisted_structured_roundtrip(5_u8);
    assert_direct_persisted_structured_roundtrip(6_u16);
    assert_direct_persisted_structured_roundtrip(7_u32);
    assert_direct_persisted_structured_roundtrip(8_u64);
}

#[test]
fn direct_persisted_by_kind_leaf_codecs_cover_tier_one_family() {
    assert_direct_persisted_by_kind_roundtrip(true, FieldKind::Bool);
    assert_direct_persisted_by_kind_roundtrip(
        String::from("Ada"),
        FieldKind::Text { max_len: None },
    );
    assert_direct_persisted_by_kind_roundtrip(
        Blob::from(vec![0xAB, 0xCD]),
        FieldKind::Blob { max_len: None },
    );
    assert_direct_persisted_by_kind_roundtrip(
        Float32::try_new(1.25).expect("finite float32"),
        FieldKind::Float32,
    );
    assert_direct_persisted_by_kind_roundtrip(
        Float64::try_new(2.5).expect("finite float64"),
        FieldKind::Float64,
    );
    assert_direct_persisted_by_kind_roundtrip(-5_i8, FieldKind::Int);
    assert_direct_persisted_by_kind_roundtrip(-6_i16, FieldKind::Int);
    assert_direct_persisted_by_kind_roundtrip(-7_i32, FieldKind::Int);
    assert_direct_persisted_by_kind_roundtrip(-8_i64, FieldKind::Int);
    assert_direct_persisted_by_kind_roundtrip(5_u8, FieldKind::Uint);
    assert_direct_persisted_by_kind_roundtrip(6_u16, FieldKind::Uint);
    assert_direct_persisted_by_kind_roundtrip(7_u32, FieldKind::Uint);
    assert_direct_persisted_by_kind_roundtrip(8_u64, FieldKind::Uint);
    assert_direct_persisted_by_kind_roundtrip(
        Timestamp::from_millis(1_234_567),
        FieldKind::Timestamp,
    );
    assert_direct_persisted_by_kind_roundtrip(Principal::anonymous(), FieldKind::Principal);
    assert_direct_persisted_by_kind_roundtrip(
        Subaccount::from_array([9u8; 32]),
        FieldKind::Subaccount,
    );
    assert_direct_persisted_by_kind_roundtrip(Ulid::from_parts(77, 3), FieldKind::Ulid);
    assert_direct_persisted_by_kind_roundtrip(Unit, FieldKind::Unit);
}

#[test]
fn direct_persisted_by_kind_leaf_codecs_cover_tier_two_family() {
    assert_direct_persisted_by_kind_roundtrip(
        Account::new(
            Principal::anonymous(),
            Some(Subaccount::from_array([7u8; 32])),
        ),
        FieldKind::Account,
    );
    assert_direct_persisted_by_kind_roundtrip(
        Date::new_checked(2025, 10, 19).expect("valid date"),
        FieldKind::Date,
    );
    assert_direct_persisted_by_kind_roundtrip(
        Decimal::from_i128_with_scale(12_345, 2),
        FieldKind::Decimal { scale: 2 },
    );
    assert_direct_persisted_by_kind_roundtrip(Duration::from_secs(5), FieldKind::Duration);
    assert_direct_persisted_by_kind_roundtrip(Int128::from(-123_i128), FieldKind::Int128);
    assert_direct_persisted_by_kind_roundtrip(Nat128::from(456_u128), FieldKind::Uint128);
    assert_direct_persisted_by_kind_roundtrip(
        Int::from(candid::Int::from(-789_i32)),
        FieldKind::IntBig,
    );
    assert_direct_persisted_by_kind_roundtrip(
        Nat::from(candid::Nat::from(987_u64)),
        FieldKind::UintBig,
    );
}

#[test]
fn direct_persisted_by_kind_wrapper_codecs_recurse_without_runtime_value_bridge() {
    assert_direct_persisted_by_kind_roundtrip(
        vec![DirectByKindLeaf(3), DirectByKindLeaf(5)],
        FieldKind::List(&FieldKind::Uint),
    );
    assert_direct_persisted_by_kind_roundtrip(
        BTreeSet::from([DirectByKindLeaf(7), DirectByKindLeaf(9)]),
        FieldKind::Set(&FieldKind::Uint),
    );
    assert_direct_persisted_by_kind_roundtrip(
        BTreeMap::from([
            (DirectByKindLeaf(11), DirectByKindLeaf(13)),
            (DirectByKindLeaf(17), DirectByKindLeaf(19)),
        ]),
        FieldKind::Map {
            key: &FieldKind::Uint,
            value: &FieldKind::Uint,
        },
    );
}

#[test]
fn malformed_by_kind_set_payload_rejects_duplicate_logical_items() {
    let kind = FieldKind::Set(&FieldKind::Uint);
    let item = DirectByKindLeaf(7)
        .encode_persisted_slot_payload_by_kind(FieldKind::Uint, "sample")
        .expect("by-kind set item should encode");
    let items = [item.as_slice(), item.as_slice()];
    let payload = encode_list_field_items(items.as_slice(), kind, "sample").expect("set frame");
    let err = BTreeSet::<DirectByKindLeaf>::decode_persisted_option_slot_payload_by_kind(
        payload.as_slice(),
        kind,
        "sample",
    )
    .expect_err("by-kind set payload must reject duplicate framed items");

    assert_eq!(err.class(), crate::error::ErrorClass::Corruption);
    assert!(
        err.message()
            .contains("by-kind set payload contains duplicate items"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn malformed_by_kind_map_payload_rejects_duplicate_logical_key() {
    let kind = FieldKind::Map {
        key: &FieldKind::Uint,
        value: &FieldKind::Uint,
    };
    let key = DirectByKindLeaf(7)
        .encode_persisted_slot_payload_by_kind(FieldKind::Uint, "sample")
        .expect("by-kind map key should encode");
    let first_value = DirectByKindLeaf(11)
        .encode_persisted_slot_payload_by_kind(FieldKind::Uint, "sample")
        .expect("first by-kind map value should encode");
    let second_value = DirectByKindLeaf(13)
        .encode_persisted_slot_payload_by_kind(FieldKind::Uint, "sample")
        .expect("second by-kind map value should encode");
    let entries = [
        (key.as_slice(), first_value.as_slice()),
        (key.as_slice(), second_value.as_slice()),
    ];
    let payload = encode_map_field_entries(entries.as_slice(), kind, "sample").expect("map frame");
    let err =
        BTreeMap::<DirectByKindLeaf, DirectByKindLeaf>::decode_persisted_option_slot_payload_by_kind(
            payload.as_slice(),
            kind,
            "sample",
        )
        .expect_err("by-kind map payload must reject duplicate framed keys");

    assert_eq!(err.class(), crate::error::ErrorClass::Corruption);
    assert!(
        err.message()
            .contains("by-kind map payload contains duplicate or unordered keys"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn malformed_structured_set_payload_rejects_duplicate_logical_items() {
    let item = 7_u64
        .encode_persisted_structured_payload()
        .expect("structured set item should encode");
    let items = [item.as_slice(), item.as_slice()];
    let payload = encode_value_storage_list_item_slices(items.as_slice());
    let err = BTreeSet::<u64>::decode_persisted_structured_payload(payload.as_slice())
        .expect_err("structured set payload must reject duplicate framed items");

    assert_eq!(err.class(), crate::error::ErrorClass::Corruption);
    assert!(
        err.message()
            .contains("value payload does not match BTreeSet<u64>"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn malformed_structured_map_payload_rejects_duplicate_or_unordered_logical_keys() {
    let key = 7_u64
        .encode_persisted_structured_payload()
        .expect("structured map key should encode");
    let first_value = 11_u64
        .encode_persisted_structured_payload()
        .expect("first structured map value should encode");
    let second_value = 13_u64
        .encode_persisted_structured_payload()
        .expect("second structured map value should encode");
    let entries = [
        (key.as_slice(), first_value.as_slice()),
        (key.as_slice(), second_value.as_slice()),
    ];
    let payload = encode_value_storage_map_entry_slices(entries.as_slice());
    let err = BTreeMap::<u64, u64>::decode_persisted_structured_payload(payload.as_slice())
        .expect_err("structured map payload must reject duplicate framed keys");

    assert_eq!(err.class(), crate::error::ErrorClass::Corruption);
    assert!(
        err.message()
            .contains("value payload does not match BTreeMap<u64, u64>"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn direct_persisted_by_kind_leaf_codecs_reject_truncated_payloads() {
    assert_direct_persisted_by_kind_rejects_truncated_payload(
        Account::new(
            Principal::anonymous(),
            Some(Subaccount::from_array([7u8; 32])),
        ),
        FieldKind::Account,
    );
    assert_direct_persisted_by_kind_rejects_truncated_payload(
        Decimal::from_i128_with_scale(12_345, 2),
        FieldKind::Decimal { scale: 2 },
    );
    assert_direct_persisted_by_kind_rejects_truncated_payload(
        Timestamp::from_millis(1_234_567),
        FieldKind::Timestamp,
    );
    assert_direct_persisted_by_kind_rejects_truncated_payload(
        Principal::anonymous(),
        FieldKind::Principal,
    );
    assert_direct_persisted_by_kind_rejects_truncated_payload(
        Subaccount::from_array([9u8; 32]),
        FieldKind::Subaccount,
    );
    assert_direct_persisted_by_kind_rejects_truncated_payload(
        Ulid::from_parts(77, 3),
        FieldKind::Ulid,
    );
    assert_direct_persisted_by_kind_rejects_truncated_payload(
        Float32::try_new(1.25).expect("finite float32"),
        FieldKind::Float32,
    );
    assert_direct_persisted_by_kind_rejects_truncated_payload(
        Float64::try_new(2.5).expect("finite float64"),
        FieldKind::Float64,
    );
}

#[test]
fn direct_persisted_structured_float_codecs_reject_nonfinite_payloads() {
    let nan32 = f32::NAN.to_bits().to_be_bytes();
    let nan64 = f64::NAN.to_bits().to_be_bytes();

    let err32 = Float32::decode_persisted_structured_payload(&nan32)
        .expect_err("float32 structured codec must reject nonfinite payloads");
    let err64 = Float64::decode_persisted_structured_payload(&nan64)
        .expect_err("float64 structured codec must reject nonfinite payloads");

    assert_eq!(err32.class(), crate::error::ErrorClass::Corruption);
    assert_eq!(err64.class(), crate::error::ErrorClass::Corruption);
}

#[test]
fn decode_slot_into_runtime_value_decodes_scalar_slots_through_one_owner() {
    let payload = encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let value =
        decode_slot_into_runtime_value(&TEST_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(value, Value::Text("Ada".to_string()));
}

#[test]
fn decode_slot_into_runtime_value_reports_scalar_prefix_bytes() {
    let err = decode_slot_into_runtime_value(&TEST_MODEL, 0, &[0x00, 1])
        .expect_err("invalid scalar slot prefix should fail closed");

    assert!(
        err.message
            .contains("expected slot envelope prefix byte 0xFF, found 0x00"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn decode_slot_into_runtime_value_respects_value_storage_decode_contract() {
    let payload = encode_value_storage_payload(&Value::Text("Ada".to_string()));

    let value =
        decode_slot_into_runtime_value(&TEST_MODEL, 1, payload.as_slice()).expect("decode slot");

    assert_eq!(value, Value::Text("Ada".to_string()));
}

#[test]
fn encode_runtime_value_into_slot_roundtrips_scalar_slots() {
    let payload = encode_runtime_value_into_slot(&TEST_MODEL, 0, &Value::Text("Ada".to_string()))
        .expect("encode slot");
    let decoded =
        decode_slot_into_runtime_value(&TEST_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(decoded, Value::Text("Ada".to_string()));
}

#[test]
fn encode_runtime_value_into_slot_roundtrips_value_storage_slots() {
    let payload = encode_runtime_value_into_slot(&TEST_MODEL, 1, &Value::Text("Ada".to_string()))
        .expect("encode slot");
    let decoded =
        decode_slot_into_runtime_value(&TEST_MODEL, 1, payload.as_slice()).expect("decode slot");

    assert_eq!(decoded, Value::Text("Ada".to_string()));
}

#[test]
fn encode_runtime_value_into_slot_roundtrips_structured_value_storage_slots_for_all_cases() {
    for value in representative_structured_value_storage_cases() {
        let payload = encode_runtime_value_into_slot(&VALUE_STORAGE_STRUCTURED_MODEL, 0, &value)
            .unwrap_or_else(|err| {
                panic!("structured value-storage slot should encode for value {value:?}: {err:?}")
            });
        let decoded = decode_slot_into_runtime_value(
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
fn encode_runtime_value_into_slot_roundtrips_list_by_kind_slots() {
    let payload = encode_runtime_value_into_slot(
        &LIST_MODEL,
        0,
        &Value::List(vec![Value::Text("alpha".to_string())]),
    )
    .expect("encode list slot");
    let decoded =
        decode_slot_into_runtime_value(&LIST_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(decoded, Value::List(vec![Value::Text("alpha".to_string())]),);
}

#[test]
fn encode_runtime_value_into_slot_roundtrips_map_by_kind_slots() {
    let payload = encode_runtime_value_into_slot(
        &MAP_MODEL,
        0,
        &Value::Map(vec![(Value::Text("alpha".to_string()), Value::Uint(7))]),
    )
    .expect("encode map slot");
    let decoded =
        decode_slot_into_runtime_value(&MAP_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(
        decoded,
        Value::Map(vec![(Value::Text("alpha".to_string()), Value::Uint(7))]),
    );
}

#[test]
fn encode_runtime_value_into_slot_accepts_value_storage_maps_with_structured_values() {
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

    let payload = encode_runtime_value_into_slot(&STRUCTURED_MAP_VALUE_STORAGE_MODEL, 0, &projects)
        .expect("encode structured map slot");
    let decoded =
        decode_slot_into_runtime_value(&STRUCTURED_MAP_VALUE_STORAGE_MODEL, 0, payload.as_slice())
            .expect("decode structured map slot");

    assert_eq!(decoded, projects);
}

#[test]
fn structured_value_storage_cases_decode_through_direct_value_storage_boundary() {
    for value in representative_value_storage_cases() {
        let payload = encode_value_storage_payload(&value);
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
fn encode_runtime_value_into_slot_roundtrips_enum_by_kind_slots() {
    let payload = encode_runtime_value_into_slot(
        &ENUM_MODEL,
        0,
        &Value::Enum(ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7))),
    )
    .expect("encode enum slot");
    let decoded =
        decode_slot_into_runtime_value(&ENUM_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(
        decoded,
        Value::Enum(ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7,))),
    );
}

#[test]
fn encode_runtime_value_into_slot_roundtrips_leaf_by_kind_wrapper_slots() {
    let account = Account::from_parts(Principal::dummy(7), Some(Subaccount::from([7_u8; 32])));
    let payload = encode_runtime_value_into_slot(&ACCOUNT_MODEL, 0, &Value::Account(account))
        .expect("encode account slot");
    let decoded =
        decode_slot_into_runtime_value(&ACCOUNT_MODEL, 0, payload.as_slice()).expect("decode slot");

    assert_eq!(decoded, Value::Account(account));
}

#[test]
fn structured_slot_payload_roundtrips_generated_structured_payload() {
    let profile = PersistedRowProfileValue {
        bio: "Ada".to_string(),
    };
    let payload = encode_persisted_structured_slot_payload(&profile, "profile")
        .expect("encode structured field payload");
    let decoded = decode_persisted_structured_slot_payload::<PersistedRowProfileValue>(
        payload.as_slice(),
        "profile",
    )
    .expect("decode structured field payload");

    assert_eq!(decoded, profile);
}

#[test]
fn structured_slot_payload_roundtrips_direct_structured_codec_without_runtime_value_bridge() {
    let profile = DirectPersistedProfileValue {
        bio: "Ada".to_string(),
    };
    let payload = encode_persisted_structured_slot_payload(&profile, "profile")
        .expect("encode direct structured payload");
    let decoded = decode_persisted_structured_slot_payload::<DirectPersistedProfileValue>(
        payload.as_slice(),
        "profile",
    )
    .expect("decode direct structured payload");

    assert_eq!(decoded, profile);
    assert_eq!(payload, vec![0, 3, b'A', b'd', b'a']);
}

#[test]
fn structured_many_slot_payload_roundtrips_structured_value_lists() {
    let profiles = vec![
        PersistedRowProfileValue {
            bio: "Ada".to_string(),
        },
        PersistedRowProfileValue {
            bio: "Grace".to_string(),
        },
    ];
    let payload = encode_persisted_structured_many_slot_payload(profiles.as_slice(), "profiles")
        .expect("encode structured list payload");
    let decoded = decode_persisted_structured_many_slot_payload::<PersistedRowProfileValue>(
        payload.as_slice(),
        "profiles",
    )
    .expect("decode structured list payload");

    assert_eq!(decoded, profiles);
}

#[test]
fn decode_persisted_slot_payload_by_kind_rejects_malformed_structured_null_payload() {
    let err = decode_persisted_slot_payload_by_kind::<PersistedRowProfileValue>(
        &[0xF6],
        FieldKind::Structured { queryable: false },
        "profile",
    )
    .expect_err("structured payload must reject malformed null bytes");

    assert!(
        err.message.contains("structural binary: unknown tag 0xF6"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn encode_runtime_value_into_slot_rejects_null_for_required_structured_slots() {
    let err = encode_runtime_value_into_slot(&REQUIRED_STRUCTURED_MODEL, 0, &Value::Null)
        .expect_err("required structured slot must reject null");

    assert!(
        err.message.contains("required field cannot store null"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn encode_runtime_value_into_slot_allows_null_for_optional_structured_slots() {
    let payload = encode_runtime_value_into_slot(&OPTIONAL_STRUCTURED_MODEL, 0, &Value::Null)
        .expect("optional structured slot should allow null");
    let decoded = decode_slot_into_runtime_value(&OPTIONAL_STRUCTURED_MODEL, 0, payload.as_slice())
        .expect("optional structured slot should decode");

    assert_eq!(decoded, Value::Null);
}

#[test]
fn encode_runtime_value_into_slot_allows_null_for_optional_decimal_slots() {
    let payload = encode_runtime_value_into_slot(&OPTIONAL_DECIMAL_MODEL, 0, &Value::Null)
        .expect("optional decimal slot should allow null");
    let decoded = decode_slot_into_runtime_value(&OPTIONAL_DECIMAL_MODEL, 0, payload.as_slice())
        .expect("optional decimal slot should decode");

    assert_eq!(decoded, Value::Null);
}

#[test]
fn encode_runtime_value_into_slot_normalizes_decimal_to_declared_scale() {
    let payload = encode_runtime_value_into_slot(
        &OPTIONAL_DECIMAL_MODEL,
        0,
        &Value::Decimal(Decimal::from_i128_with_scale(140, 0)),
    )
    .expect("decimal slot should normalize to field scale");
    let decoded = decode_slot_into_runtime_value(&OPTIONAL_DECIMAL_MODEL, 0, payload.as_slice())
        .expect("normalized decimal slot should decode");
    let Value::Decimal(decimal) = decoded else {
        panic!("normalized decimal slot should decode as Decimal");
    };

    assert_eq!(decimal, Decimal::from_i128_with_scale(140_000, 3));
    assert_eq!(decimal.scale(), 3);
}

#[test]
fn option_decimal_by_kind_codec_preserves_none_as_null() {
    let value: Option<Decimal> = None;
    let payload = value
        .encode_persisted_slot_payload_by_kind(
            FieldKind::Decimal { scale: 3 },
            "attribute_score_normalized",
        )
        .expect("optional decimal none should encode");
    let decoded = Option::<Decimal>::decode_persisted_option_slot_payload_by_kind(
        payload.as_slice(),
        FieldKind::Decimal { scale: 3 },
        "attribute_score_normalized",
    )
    .expect("optional decimal none should decode");

    assert_eq!(decoded, Some(None));
}

#[test]
fn option_by_kind_codec_preserves_valid_non_null_value() {
    let value = Some(Decimal::new(12_345, 3));
    let payload = value
        .encode_persisted_slot_payload_by_kind(
            FieldKind::Decimal { scale: 3 },
            "attribute_score_normalized",
        )
        .expect("optional decimal value should encode");
    let decoded = Option::<Decimal>::decode_persisted_option_slot_payload_by_kind(
        payload.as_slice(),
        FieldKind::Decimal { scale: 3 },
        "attribute_score_normalized",
    )
    .expect("optional decimal value should decode");

    assert_eq!(decoded, Some(value));
}

#[test]
fn option_storage_key_backed_by_kind_malformed_non_null_error_is_stable() {
    let malformed = encode_structural_value_storage_bytes(&Value::Text("not a uint".to_string()))
        .expect("malformed storage-key fixture should still be structural bytes");
    let err = Option::<u64>::decode_persisted_option_slot_payload_by_kind(
        malformed.as_slice(),
        FieldKind::Uint,
        "sample",
    )
    .expect_err("malformed non-null storage-key option payload must fail");

    assert_eq!(err.class(), crate::error::ErrorClass::Corruption);
    assert_eq!(
        err.message(),
        "row decode failed for field 'sample': structural binary: expected u64 integer payload",
    );
}

#[test]
fn option_by_kind_leaf_malformed_non_null_error_is_stable() {
    let malformed = encode_structural_value_storage_bytes(&Value::Text("not a float".to_string()))
        .expect("malformed by-kind fixture should still be structural bytes");
    let err = Option::<Float64>::decode_persisted_option_slot_payload_by_kind(
        malformed.as_slice(),
        FieldKind::Float64,
        "sample",
    )
    .expect_err("malformed non-null by-kind option payload must fail");

    assert_eq!(err.class(), crate::error::ErrorClass::Corruption);
    assert_eq!(
        err.message(),
        "row decode failed for field 'sample': structural binary: expected f64 float payload",
    );
}

#[test]
fn decode_slot_into_runtime_value_allows_null_for_optional_account_slots() {
    let payload = encode_runtime_value_into_slot(&OPTIONAL_ACCOUNT_MODEL, 0, &Value::Null)
        .expect("optional account slot should allow null");
    let decoded = decode_slot_into_runtime_value(&OPTIONAL_ACCOUNT_MODEL, 0, payload.as_slice())
        .expect("optional account slot should decode");

    assert_eq!(decoded, Value::Null);
}

#[test]
fn structural_slot_reader_accepts_null_for_optional_account_slots() {
    let payload = encode_runtime_value_into_slot(&OPTIONAL_ACCOUNT_MODEL, 0, &Value::Null)
        .expect("optional account slot should allow null");
    let raw_row =
        raw_row_from_dense_slot_payloads_for_tests(&OPTIONAL_ACCOUNT_MODEL, &[payload.as_slice()]);

    let mut reader = StructuralSlotReader::from_raw_row(&raw_row, &OPTIONAL_ACCOUNT_MODEL)
        .expect("row-open validation should accept null optional account slots");

    assert_eq!(reader.get_value(0).expect("decode slot"), Some(Value::Null));
}

#[test]
fn encode_runtime_value_into_slot_rejects_unknown_enum_payload_variants() {
    let err = encode_runtime_value_into_slot(
        &ENUM_MODEL,
        0,
        &Value::Enum(ValueEnum::new("Unknown", Some("tests::State")).with_payload(Value::Uint(7))),
    )
    .expect_err("unknown enum payload should fail closed");

    assert!(err.message.contains("unknown enum variant"));
}

#[test]
fn structural_slot_reader_and_direct_decode_share_the_same_field_codec_boundary() {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );

    let direct_slots = StructuralSlotReader::from_raw_row_with_contract(
        &raw_row,
        StructuralRowContract::from_generated_model_for_test(&TEST_MODEL),
    )
    .expect("decode row");
    let mut cached_slots = StructuralSlotReader::from_raw_row_with_contract(
        &raw_row,
        StructuralRowContract::from_generated_model_for_test(&TEST_MODEL),
    )
    .expect("decode row");

    let direct_name = direct_slots
        .get_bytes(0)
        .map(|bytes| decode_slot_into_runtime_value(&TEST_MODEL, 0, bytes))
        .transpose()
        .expect("decode name");
    let direct_payload = direct_slots
        .get_bytes(1)
        .map(|bytes| decode_slot_into_runtime_value(&TEST_MODEL, 1, bytes))
        .transpose()
        .expect("decode payload");
    let cached_name = cached_slots.get_value(0).expect("cached name");
    let cached_payload = cached_slots.get_value(1).expect("cached payload");

    assert_eq!(direct_name, cached_name);
    assert_eq!(direct_payload, cached_payload);
}

#[test]
fn accepted_row_contract_reads_missing_trailing_nullable_slots_as_null() {
    let id = Ulid::from_u128(147);
    let raw_row = old_two_slot_additive_raw_row_for_tests(id);
    let contract = accepted_row_contract_for_model(&ADDITIVE_NULLABLE_MODEL);

    let mut reader =
        StructuralSlotReader::from_raw_row_with_validated_contract(&raw_row, contract.clone())
            .expect("accepted row contract should allow missing nullable append-only slot");
    let dense =
        super::decode_dense_raw_row_with_contract(&raw_row, contract.clone(), StorageKey::Ulid(id))
            .expect("dense direct decode should synthesize null for missing nullable slot");
    let sparse = super::decode_sparse_raw_row_with_contract(
        &raw_row,
        contract.clone(),
        StorageKey::Ulid(id),
        &[2],
    )
    .expect("sparse direct decode should synthesize null for missing nullable slot");
    let compact = super::decode_sparse_indexed_raw_row_with_contract(
        &raw_row,
        contract.clone(),
        StorageKey::Ulid(id),
        &[2],
    )
    .expect("compact sparse direct decode should synthesize null for missing nullable slot");
    let required =
        decode_sparse_required_slot_with_contract(&raw_row, contract, StorageKey::Ulid(id), 2)
            .expect(
                "required sparse direct decode should synthesize null for missing nullable slot",
            );

    assert_eq!(
        reader.get_value(2).expect("reader missing slot"),
        Some(Value::Null)
    );
    assert!(matches!(
        reader.get_scalar(2).expect("reader scalar missing slot"),
        Some(ScalarSlotValueRef::Null),
    ));
    assert_eq!(
        dense,
        vec![
            Some(Value::Ulid(id)),
            Some(Value::Text("Ada".to_string())),
            Some(Value::Null),
        ],
    );
    assert_eq!(sparse, vec![None, None, Some(Value::Null)]);
    assert_eq!(compact, vec![Some(Value::Null)]);
    assert_eq!(required, Some(Value::Null));
}

#[test]
fn accepted_row_contract_reemits_canonical_rows_with_accepted_slot_count() {
    let id = Ulid::from_u128(148);
    let raw_row = old_two_slot_additive_raw_row_for_tests(id);
    let accepted_decode_contract = accepted_row_decode_contract_for_model(&ADDITIVE_NULLABLE_MODEL);
    let contract =
        StructuralRowContract::from_generated_model_with_accepted_decode_contract_for_test(
            &ADDITIVE_NULLABLE_MODEL,
            accepted_decode_contract.clone(),
        );
    let canonical_from_reader =
        canonical_row_from_raw_row_with_structural_contract(&raw_row, contract.clone())
            .expect("accepted structural contract should re-emit the current slot count");
    let canonical_from_patch = apply_serialized_structural_patch_to_raw_row_with_accepted_contract(
        ADDITIVE_NULLABLE_MODEL.path(),
        accepted_decode_contract,
        &raw_row,
        &SerializedStructuralPatch::default(),
    )
    .expect("accepted patch replay should re-emit the current slot count");

    for canonical in [canonical_from_reader, canonical_from_patch] {
        let row_payload =
            decode_structural_row_payload(canonical.as_raw_row()).expect("decode row payload");
        let slot_count = u16::from_be_bytes(
            row_payload.as_ref()[0..2]
                .try_into()
                .expect("slot count prefix should be present"),
        );
        let mut reader = StructuralSlotReader::from_raw_row_with_validated_contract(
            canonical.as_raw_row(),
            contract.clone(),
        )
        .expect("canonical accepted row should reopen through accepted contract");

        assert_eq!(slot_count, 3);
        assert_eq!(
            reader.get_value(2).expect("accepted appended slot"),
            Some(Value::Null),
        );
    }
}

#[test]
fn accepted_row_contract_reemits_defaulted_rows_with_accepted_default() {
    let id = Ulid::from_u128(149);
    let score_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Uint(99)));
    let raw_row = old_two_slot_additive_raw_row_for_tests(id);
    let accepted_decode_contract =
        accepted_defaulted_required_score_row_decode_contract_for_tests(score_payload);
    let contract =
        StructuralRowContract::from_generated_model_with_accepted_decode_contract_for_test(
            &ADDITIVE_REQUIRED_MODEL,
            accepted_decode_contract.clone(),
        );
    let canonical_from_reader =
        canonical_row_from_raw_row_with_structural_contract(&raw_row, contract.clone())
            .expect("accepted structural contract should re-emit defaulted slot count");
    let canonical_from_patch = apply_serialized_structural_patch_to_raw_row_with_accepted_contract(
        ADDITIVE_REQUIRED_MODEL.path(),
        accepted_decode_contract,
        &raw_row,
        &SerializedStructuralPatch::default(),
    )
    .expect("accepted patch replay should re-emit defaulted slot count");

    for canonical in [canonical_from_reader, canonical_from_patch] {
        let row_payload =
            decode_structural_row_payload(canonical.as_raw_row()).expect("decode row payload");
        let slot_count = u16::from_be_bytes(
            row_payload.as_ref()[0..2]
                .try_into()
                .expect("slot count prefix should be present"),
        );
        let mut reader = StructuralSlotReader::from_raw_row_with_validated_contract(
            canonical.as_raw_row(),
            contract.clone(),
        )
        .expect("canonical accepted row should reopen through accepted contract");

        assert_eq!(slot_count, 3);
        assert_eq!(
            reader.get_value(2).expect("accepted defaulted slot"),
            Some(Value::Uint(99)),
        );
    }
}

#[test]
fn accepted_row_contract_validates_primary_key_with_accepted_contract() {
    let id = Ulid::from_u128(149);
    let raw_row = old_two_slot_additive_raw_row_for_tests(id);
    let contract = accepted_row_contract_for_model(&ADDITIVE_NULLABLE_MODEL);

    let decoded =
        super::decode_dense_raw_row_with_contract(&raw_row, contract.clone(), StorageKey::Ulid(id))
            .expect("accepted primary-key contract should validate expected row key");
    let err = super::decode_dense_raw_row_with_contract(
        &raw_row,
        contract,
        StorageKey::Ulid(Ulid::from_u128(150)),
    )
    .expect_err("accepted primary-key contract should reject mismatched row key");

    assert_eq!(decoded.first(), Some(&Some(Value::Ulid(id))));
    assert!(
        err.message.contains("row key mismatch"),
        "accepted primary-key mismatch should preserve persisted-row mismatch taxonomy: {err:?}",
    );
}

#[test]
fn accepted_row_contract_preserves_malformed_present_slot_corruption_taxonomy() {
    let id = Ulid::from_u128(151);
    let raw_row = malformed_old_two_slot_additive_raw_row_for_tests(id);
    let contract = accepted_row_contract_for_model(&ADDITIVE_NULLABLE_MODEL);

    let Err(lazy_err) =
        StructuralSlotReader::from_raw_row_with_validated_contract(&raw_row, contract.clone())
    else {
        panic!("accepted row contract must reject malformed present slot bytes");
    };
    let dense_err =
        super::decode_dense_raw_row_with_contract(&raw_row, contract, StorageKey::Ulid(id))
            .expect_err("accepted dense decode must reject malformed present slot bytes");

    assert_eq!(lazy_err.class, ErrorClass::Corruption);
    assert_eq!(dense_err.class, ErrorClass::Corruption);
    assert_eq!(
        lazy_err.message,
        "row decode: slot span exceeds payload length"
    );
    assert_eq!(
        dense_err.message,
        "row decode: slot span exceeds payload length"
    );
}

#[test]
fn accepted_row_contract_rejects_missing_trailing_required_slots() {
    let id = Ulid::from_u128(148);
    let raw_row = old_two_slot_additive_raw_row_for_tests(id);
    let contract = accepted_row_contract_for_model(&ADDITIVE_REQUIRED_MODEL);

    let Err(err) = StructuralSlotReader::from_raw_row_with_contract(&raw_row, contract) else {
        panic!("accepted row contract should reject a missing required appended slot");
    };

    assert!(
        err.message.contains("declared field missing")
            || err.message.contains("missing declared field"),
        "required missing-slot rejection should stay on persisted-row missing-field taxonomy: {err:?}",
    );
}

#[test]
fn accepted_row_contract_reads_missing_trailing_defaulted_slots_as_default() {
    let id = Ulid::from_u128(46);
    let score_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Uint(99)));
    let contract = accepted_defaulted_required_score_row_contract_for_tests(score_payload);
    let raw_row = old_two_slot_additive_raw_row_for_tests(id);

    let mut reader = StructuralSlotReader::from_raw_row_with_contract(&raw_row, contract)
        .expect("accepted row contract should allow missing defaulted append-only slot");

    assert_eq!(
        reader.get_value(2).expect("score slot should materialize"),
        Some(Value::Uint(99)),
    );
}

#[test]
fn accepted_row_contract_rejects_malformed_missing_default_payload() {
    let id = Ulid::from_u128(47);
    let contract = accepted_defaulted_required_score_row_contract_for_tests(vec![0xFE]);
    let raw_row = old_two_slot_additive_raw_row_for_tests(id);

    let Err(err) = StructuralSlotReader::from_raw_row_with_contract(&raw_row, contract) else {
        panic!("malformed schema default payload should reject old row materialization");
    };

    assert_eq!(err.class, ErrorClass::Corruption);
    assert!(
        err.message.contains("field 'score'"),
        "default payload decode error should name the defaulted field: {err:?}",
    );
}

#[test]
fn structural_slot_reader_validates_declared_slots_but_defers_non_scalar_materialization() {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );

    let mut reader = StructuralSlotReader::from_raw_row_with_contract(
        &raw_row,
        StructuralRowContract::from_generated_model_for_test(&TEST_MODEL),
    )
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
fn structural_slot_reader_direct_projection_decodes_value_storage_scalar_without_cache_materialization()
 {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );

    let reader = StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL)
        .expect("row-open structural envelope decode should succeed");

    assert_eq!(
        reader
            .required_direct_projection_value(1)
            .expect("direct projection should decode value-storage scalar"),
        Value::Text("payload".to_string())
    );

    match &reader.cached_values[1] {
        CachedSlotValue::Deferred { materialized } => {
            assert!(
                materialized.get().is_none(),
                "direct scalar projection should not populate deferred value cache",
            );
        }
        other @ CachedSlotValue::Scalar { .. } => {
            panic!("expected deferred cache for slot 1, found {other:?}")
        }
    }
}

#[test]
fn structural_slot_reader_direct_projection_preserves_value_storage_mismatch_fallback() {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Int(42));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );

    let reader = StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL)
        .expect("row-open structural envelope decode should succeed");

    assert_eq!(
        reader
            .required_direct_projection_value(1)
            .expect("mismatched value-storage scalar should use canonical fallback"),
        Value::Int(42)
    );

    match &reader.cached_values[1] {
        CachedSlotValue::Deferred { materialized } => {
            assert_eq!(
                materialized.get(),
                Some(&Value::Int(42)),
                "fallback path should preserve the existing materialized cache behavior",
            );
        }
        other @ CachedSlotValue::Scalar { .. } => {
            panic!("expected deferred cache for slot 1, found {other:?}")
        }
    }
}

#[test]
fn structural_slot_reader_predicate_compares_value_storage_scalar_without_cache_materialization() {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );
    let predicate = Predicate::Compare(ComparePredicate::eq(
        "payload".to_string(),
        Value::Text("payload".to_string()),
    ));
    let program = PredicateProgram::compile_for_model_only(&TEST_MODEL, &predicate);

    let reader = StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL)
        .expect("row-open structural envelope decode should succeed");

    assert!(
        program
            .eval_with_structural_slot_reader(&reader)
            .expect("value-storage scalar predicate should evaluate"),
    );

    match &reader.cached_values[1] {
        CachedSlotValue::Deferred { materialized } => {
            assert!(
                materialized.get().is_none(),
                "value-storage scalar predicate should not populate deferred value cache",
            );
        }
        other @ CachedSlotValue::Scalar { .. } => {
            panic!("expected deferred cache for slot 1, found {other:?}")
        }
    }
}

#[test]
fn structural_slot_reader_metrics_report_zero_non_scalar_materializations_for_scalar_only_access() {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );

    let (_scalar_read, metrics) = with_structural_read_metrics(|| {
        let reader = StructuralSlotReader::from_raw_row_with_contract(
            &raw_row,
            StructuralRowContract::from_generated_model_for_test(&TEST_MODEL),
        )
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
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );

    let (_value, metrics) = with_structural_read_metrics(|| {
        let mut reader = StructuralSlotReader::from_raw_row_with_contract(
            &raw_row,
            StructuralRowContract::from_generated_model_for_test(&TEST_MODEL),
        )
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
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), &[0xFF]],
    );

    let mut reader = StructuralSlotReader::from_raw_row_with_contract(
        &raw_row,
        StructuralRowContract::from_generated_model_for_test(&TEST_MODEL),
    )
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
fn apply_structural_patch_to_raw_row_updates_only_targeted_slots() {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );
    let patch = StructuralPatch::new().set(
        FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
        Value::Text("Grace".to_string()),
    );

    let serialized = serialize_structural_patch_fields_for_accepted_test_model(&TEST_MODEL, &patch)
        .expect("serialize patch");
    let patched = apply_serialized_structural_patch_to_raw_row_for_accepted_test_model(
        &TEST_MODEL,
        &raw_row,
        &serialized,
    )
    .expect("apply patch");
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
fn serialize_structural_patch_fields_encodes_canonical_slot_payloads() {
    let patch = StructuralPatch::new()
        .set(
            FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
            Value::Text("Grace".to_string()),
        )
        .set(
            FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
            Value::Text("payload".to_string()),
        );

    let serialized = serialize_structural_patch_fields_for_accepted_test_model(&TEST_MODEL, &patch)
        .expect("serialize patch");

    assert_eq!(serialized.entries().len(), 2);
    assert_eq!(
        decode_slot_into_runtime_value(
            &TEST_MODEL,
            serialized.entries()[0].slot().index(),
            serialized.entries()[0].payload(),
        )
        .expect("decode slot payload"),
        Value::Text("Grace".to_string())
    );
    assert_eq!(
        decode_slot_into_runtime_value(
            &TEST_MODEL,
            serialized.entries()[1].slot().index(),
            serialized.entries()[1].payload(),
        )
        .expect("decode slot payload"),
        Value::Text("payload".to_string())
    );
}

#[test]
fn serialize_structural_patch_fields_with_accepted_contract_rejects_unaccepted_slots() {
    let accepted_decode_contract = accepted_row_decode_contract_for_model(&ADDITIVE_PREFIX_MODEL);
    let patch = StructuralPatch::new().set(
        FieldSlot::from_index(&ADDITIVE_NULLABLE_MODEL, 2).expect("resolve generated slot"),
        Value::Text("Ada".to_string()),
    );

    let err = serialize_structural_patch_fields_with_accepted_contract(
        ADDITIVE_NULLABLE_MODEL.path(),
        accepted_decode_contract,
        &patch,
    )
    .expect_err("accepted serializer must reject slots outside accepted layout");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
}

#[test]
fn serialize_structural_patch_fields_with_accepted_contract_normalizes_decimal_scale() {
    let accepted_decode_contract = accepted_row_decode_contract_for_model(&OPTIONAL_DECIMAL_MODEL);
    let patch = StructuralPatch::new().set(
        FieldSlot::from_index(&OPTIONAL_DECIMAL_MODEL, 0).expect("resolve decimal slot"),
        Value::Decimal(Decimal::from_i128_with_scale(140, 0)),
    );

    let serialized = serialize_structural_patch_fields_with_accepted_contract(
        OPTIONAL_DECIMAL_MODEL.path(),
        accepted_decode_contract,
        &patch,
    )
    .expect("accepted decimal patch should serialize");
    let decoded = decode_slot_into_runtime_value(
        &OPTIONAL_DECIMAL_MODEL,
        serialized.entries()[0].slot().index(),
        serialized.entries()[0].payload(),
    )
    .expect("accepted decimal patch payload should decode");
    let Value::Decimal(decimal) = decoded else {
        panic!("accepted decimal patch payload should decode as Decimal");
    };

    assert_eq!(decimal, Decimal::from_i128_with_scale(140_000, 3));
    assert_eq!(decimal.scale(), 3);
}

#[test]
fn serialize_complete_structural_patch_with_accepted_contract_fills_missing_database_defaults() {
    let score_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Uint(99)));
    let accepted_decode_contract =
        accepted_defaulted_required_score_row_decode_contract_for_tests(score_payload);
    let id = Ulid::from_u128(149);
    let patch = StructuralPatch::new()
        .set(
            FieldSlot::from_index(&ADDITIVE_REQUIRED_MODEL, 0).expect("resolve id slot"),
            Value::Ulid(id),
        )
        .set(
            FieldSlot::from_index(&ADDITIVE_REQUIRED_MODEL, 1).expect("resolve name slot"),
            Value::Text("Ada".to_string()),
        );

    let serialized = serialize_complete_structural_patch_fields_with_accepted_contract(
        ADDITIVE_REQUIRED_MODEL.path(),
        accepted_decode_contract,
        &patch,
    )
    .expect("complete accepted structural patch should fill missing defaulted slots");

    assert_eq!(serialized.entries().len(), 3);
    assert_eq!(
        decode_slot_into_runtime_value(
            &ADDITIVE_REQUIRED_MODEL,
            serialized.entries()[2].slot().index(),
            serialized.entries()[2].payload(),
        )
        .expect("default payload should decode"),
        Value::Uint(99),
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
fn apply_structural_patch_to_raw_row_uses_last_write_wins() {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );
    let slot = FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot");
    let patch = StructuralPatch::new()
        .set(slot, Value::Text("Grace".to_string()))
        .set(slot, Value::Text("Lin".to_string()));

    let serialized = serialize_structural_patch_fields_for_accepted_test_model(&TEST_MODEL, &patch)
        .expect("serialize patch");
    let patched = apply_serialized_structural_patch_to_raw_row_for_accepted_test_model(
        &TEST_MODEL,
        &raw_row,
        &serialized,
    )
    .expect("apply patch");
    let mut reader = StructuralSlotReader::from_raw_row(&patched, &TEST_MODEL).expect("decode row");

    assert_eq!(
        reader.get_value(0).expect("decode slot"),
        Some(Value::Text("Lin".to_string()))
    );
}

#[test]
fn apply_structural_patch_to_raw_row_rejects_noncanonical_missing_slot_baseline() {
    let empty_slots = vec![None::<&[u8]>; TEST_MODEL.fields().len()];
    let raw_row = RawRow::try_new(
        serialize_row_payload(
            encode_slot_payload_allowing_missing_for_tests(&TEST_MODEL, empty_slots.as_slice())
                .expect("encode malformed slot payload"),
        )
        .expect("serialize row payload"),
    )
    .expect("build raw row");
    let patch = StructuralPatch::new().set(
        FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
        Value::Text("payload".to_string()),
    );

    let serialized = serialize_structural_patch_fields_for_accepted_test_model(&TEST_MODEL, &patch)
        .expect("serialize patch");
    let err = apply_serialized_structural_patch_to_raw_row_for_accepted_test_model(
        &TEST_MODEL,
        &raw_row,
        &serialized,
    )
    .expect_err("noncanonical rows with missing slots must fail closed");

    assert_eq!(err.message, "row decode: missing slot payload: slot=0");
}

#[test]
fn apply_serialized_structural_patch_to_raw_row_rejects_noncanonical_scalar_baseline() {
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let malformed_slots = [Some([0xF6].as_slice()), Some(payload.as_slice())];
    let raw_row = RawRow::try_new(
        serialize_row_payload(
            encode_slot_payload_allowing_missing_for_tests(&TEST_MODEL, &malformed_slots)
                .expect("encode malformed slot payload"),
        )
        .expect("serialize row payload"),
    )
    .expect("build raw row");
    let patch = StructuralPatch::new().set(
        FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
        Value::Text("patched".to_string()),
    );
    let serialized = serialize_structural_patch_fields_for_accepted_test_model(&TEST_MODEL, &patch)
        .expect("serialize patch");

    let err = apply_serialized_structural_patch_to_raw_row_for_accepted_test_model(
        &TEST_MODEL,
        &raw_row,
        &serialized,
    )
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
fn apply_serialized_structural_patch_to_raw_row_rejects_noncanonical_scalar_patch_payload() {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );
    let serialized = SerializedStructuralPatch::new(vec![SerializedStructuralFieldUpdate::new(
        FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
        vec![0xF6],
    )]);

    let err = apply_serialized_structural_patch_to_raw_row_for_accepted_test_model(
        &TEST_MODEL,
        &raw_row,
        &serialized,
    )
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
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let mut slot_payload = encode_dense_slot_payload_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    )
    .expect("finish slot payload");
    slot_payload[..2].copy_from_slice(&1_u16.to_be_bytes());
    let raw_row =
        RawRow::try_new(serialize_row_payload(slot_payload).expect("serialize row payload"))
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
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let mut slot_payload = encode_dense_slot_payload_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    )
    .expect("finish slot payload");

    // Corrupt the second slot span so the payload table points past the
    // available data section.
    slot_payload[14..18].copy_from_slice(&u32::MAX.to_be_bytes());
    let raw_row =
        RawRow::try_new(serialize_row_payload(slot_payload).expect("serialize row payload"))
            .expect("build raw row");

    let err = StructuralSlotReader::from_raw_row(&raw_row, &TEST_MODEL)
        .err()
        .expect("slot span drift must fail closed");

    assert_eq!(err.message, "row decode: slot span exceeds payload length");
}

#[test]
fn dense_row_decode_materializes_relation_primary_key_from_authoritative_storage_key() {
    let token_id = Ulid::from_u128(91);
    let token_id_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Ulid(token_id)));
    let raw_row = RawRow::try_new(
        serialize_row_payload(
            encode_slot_payload_from_parts(
                1,
                &[(
                    0_u32,
                    u32::try_from(token_id_payload.len())
                        .expect("relation primary-key slot length should fit in u32"),
                )],
                token_id_payload.as_slice(),
            )
            .expect("encode slot payload"),
        )
        .expect("serialize row payload"),
    )
    .expect("build raw row");

    let decoded = super::decode_dense_raw_row_with_contract(
        &raw_row,
        StructuralRowContract::from_generated_model_for_test(&RELATION_PK_MODEL),
        StorageKey::Ulid(token_id),
    )
    .expect("relation primary-key row decode should succeed");

    assert_eq!(decoded, vec![Some(Value::Ulid(token_id))]);
}

#[test]
fn sparse_required_slot_decode_materializes_relation_primary_key_from_authoritative_storage_key() {
    let token_id = Ulid::from_u128(92);
    let token_id_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Ulid(token_id)));
    let raw_row = RawRow::try_new(
        serialize_row_payload(
            encode_slot_payload_from_parts(
                1,
                &[(
                    0_u32,
                    u32::try_from(token_id_payload.len())
                        .expect("relation primary-key slot length should fit in u32"),
                )],
                token_id_payload.as_slice(),
            )
            .expect("encode slot payload"),
        )
        .expect("serialize row payload"),
    )
    .expect("build raw row");

    let decoded = decode_sparse_required_slot_with_contract(
        &raw_row,
        StructuralRowContract::from_generated_model_for_test(&RELATION_PK_MODEL),
        StorageKey::Ulid(token_id),
        0,
    )
    .expect("relation primary-key sparse required-slot decode should succeed");

    assert_eq!(decoded, Some(Value::Ulid(token_id)));
}

#[test]
fn apply_serialized_structural_patch_to_raw_row_replays_preencoded_slots() {
    let name_payload =
        encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Text("Ada")));
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row = raw_row_from_dense_slot_payloads_for_tests(
        &TEST_MODEL,
        &[name_payload.as_slice(), payload.as_slice()],
    );
    let patch = StructuralPatch::new().set(
        FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
        Value::Text("Grace".to_string()),
    );
    let serialized = serialize_structural_patch_fields_for_accepted_test_model(&TEST_MODEL, &patch)
        .expect("serialize patch");
    let patched = apply_serialized_structural_patch_to_raw_row_for_accepted_test_model(
        &TEST_MODEL,
        &raw_row,
        &serialized,
    )
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
    let raw_row = CanonicalRow::from_entity(&old_entity)
        .expect("encode old row")
        .into_raw_row();
    let old_decoded = raw_row
        .try_decode::<PersistedRowPatchBridgeEntity>()
        .expect("decode old entity");
    let serialized = serialize_entity_slots_as_complete_serialized_patch(&new_entity)
        .expect("serialize complete entity slot image");
    let direct = RawRow::from_complete_serialized_structural_patch(
        PersistedRowPatchBridgeEntity::MODEL,
        &serialized,
    )
    .expect("direct row emission should succeed");

    let patched = apply_serialized_structural_patch_to_raw_row_for_accepted_test_model(
        PersistedRowPatchBridgeEntity::MODEL,
        &raw_row,
        &serialized,
    )
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
fn persisted_row_typed_meta_field_uses_field_slot_contract() {
    let entity = PersistedRowTypedMetaEntity {
        id: crate::types::Ulid::from_u128(81),
        payload: PersistedRowProfileValue {
            bio: "meta".to_string(),
        },
    };
    let expected_payload =
        crate::db::encode_persisted_slot_payload_by_meta(&entity.payload, "payload")
            .expect("typed payload bytes should encode through field metadata");
    let raw_row = CanonicalRow::from_entity(&entity)
        .expect("derived entity should encode")
        .into_raw_row();
    let reader = StructuralSlotReader::from_raw_row(&raw_row, PersistedRowTypedMetaEntity::MODEL)
        .expect("raw row should decode structurally");

    assert_eq!(
        reader.get_bytes(1),
        Some(expected_payload.as_slice()),
        "derived typed metadata field should emit bytes from the field type's slot contract",
    );
}

#[test]
fn typed_meta_field_decodes_matching_field_slot_payload() {
    let id = crate::types::Ulid::from_u128(82);
    let id_payload = encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Ulid(id)));
    let payload_value = PersistedRowProfileValue {
        bio: "field-meta".to_string(),
    };
    let payload_bytes = crate::db::encode_persisted_slot_payload_by_meta(&payload_value, "payload")
        .expect("matching typed field bytes should encode");
    let payload = encode_slot_payload_from_parts(
        2,
        &[
            (
                0_u32,
                u32::try_from(id_payload.len()).expect("id slot length should fit in u32"),
            ),
            (
                u32::try_from(id_payload.len()).expect("id slot start should fit in u32"),
                u32::try_from(payload_bytes.len()).expect("payload slot length should fit in u32"),
            ),
        ],
        &[id_payload.as_slice(), payload_bytes.as_slice()].concat(),
    )
    .expect("test row payload should encode");
    let raw_row =
        RawRow::try_new(serialize_row_payload(payload).expect("test row bytes should serialize"))
            .expect("test row should encode");
    let decoded = raw_row
        .try_decode::<PersistedRowTypedMetaEntity>()
        .expect("derived typed metadata field should decode matching field payload");

    assert_eq!(
        decoded,
        PersistedRowTypedMetaEntity {
            id,
            payload: payload_value,
        }
    );
}

#[test]
fn persisted_row_many_typed_meta_uses_container_slot_contract() {
    let entity = PersistedRowManyTypedMetaEntity {
        id: crate::types::Ulid::from_u128(83),
        payloads: vec![
            PersistedRowProfileValue {
                bio: "alpha".to_string(),
            },
            PersistedRowProfileValue {
                bio: "beta".to_string(),
            },
        ],
    };
    let expected_payload = encode_persisted_slot_payload_by_kind(
        &entity.payloads,
        FieldKind::List(&PersistedRowProfileValue::KIND),
        "payloads",
    )
    .expect("typed payload list bytes should encode through static item metadata");
    let raw_row = CanonicalRow::from_entity(&entity)
        .expect("derived entity should encode")
        .into_raw_row();
    let reader =
        StructuralSlotReader::from_raw_row(&raw_row, PersistedRowManyTypedMetaEntity::MODEL)
            .expect("raw row should decode structurally");

    assert_eq!(
        reader.get_bytes(1),
        Some(expected_payload.as_slice()),
        "derived typed container field should emit bytes from the container slot contract",
    );
}

#[test]
fn many_typed_meta_decodes_matching_container_slot_payload() {
    let id = crate::types::Ulid::from_u128(84);
    let id_payload = encode_scalar_slot_value(ScalarSlotValueRef::Value(ScalarValueRef::Ulid(id)));
    let payload_values = vec![
        PersistedRowProfileValue {
            bio: "left".to_string(),
        },
        PersistedRowProfileValue {
            bio: "right".to_string(),
        },
    ];
    let payload_bytes = encode_persisted_slot_payload_by_kind(
        &payload_values,
        FieldKind::List(&PersistedRowProfileValue::KIND),
        "payloads",
    )
    .expect("matching typed container bytes should encode");
    let payload = encode_slot_payload_from_parts(
        2,
        &[
            (
                0_u32,
                u32::try_from(id_payload.len()).expect("id slot length should fit in u32"),
            ),
            (
                u32::try_from(id_payload.len()).expect("id slot start should fit in u32"),
                u32::try_from(payload_bytes.len()).expect("payload slot length should fit in u32"),
            ),
        ],
        &[id_payload.as_slice(), payload_bytes.as_slice()].concat(),
    )
    .expect("test row payload should encode");
    let raw_row =
        RawRow::try_new(serialize_row_payload(payload).expect("test row bytes should serialize"))
            .expect("test row should encode");
    let decoded = raw_row
        .try_decode::<PersistedRowManyTypedMetaEntity>()
        .expect("derived many typed field should decode matching container payload");

    assert_eq!(
        decoded,
        PersistedRowManyTypedMetaEntity {
            id,
            payloads: payload_values,
        }
    );
}

#[test]
fn materialize_entity_from_serialized_structural_patch_rejects_missing_required_field() {
    let patch = StructuralPatch::new().set(
        FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
        Value::Text("Ada".to_string()),
    );
    let serialized = serialize_structural_patch_fields_for_accepted_test_model(
        PersistedRowPatchBridgeEntity::MODEL,
        &patch,
    )
    .expect("serialize sparse patch");

    let err = materialize_entity_from_serialized_structural_patch::<PersistedRowPatchBridgeEntity>(
        &serialized,
    )
    .expect_err("sparse typed bridge must fail closed when a required slot is absent");

    assert_eq!(err.message, "row decode: missing required field 'id'");
}

#[test]
fn materialize_entity_from_serialized_structural_patch_with_accepted_contract_matches_generated_bridge()
 {
    let patch = StructuralPatch::new()
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            Value::Ulid(crate::types::Ulid::from_u128(7)),
        )
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            Value::Text("Ada".to_string()),
        );
    let serialized = serialize_structural_patch_fields_for_accepted_test_model(
        PersistedRowPatchBridgeEntity::MODEL,
        &patch,
    )
    .expect("serialize accepted patch");

    let generated = materialize_entity_from_serialized_structural_patch::<
        PersistedRowPatchBridgeEntity,
    >(&serialized)
    .expect("generated bridge should materialize the accepted patch");
    let accepted = materialize_entity_from_serialized_structural_patch_with_accepted_contract::<
        PersistedRowPatchBridgeEntity,
    >(
        &serialized,
        accepted_row_decode_contract_for_model(PersistedRowPatchBridgeEntity::MODEL),
    )
    .expect("accepted bridge should materialize the accepted patch");

    assert_eq!(accepted, generated);
}

#[test]
fn materialize_entity_from_serialized_structural_patch_rejects_noncanonical_scalar_payload() {
    let patch = StructuralPatch::new()
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            Value::Ulid(crate::types::Ulid::from_u128(7)),
        )
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            Value::Text("Ada".to_string()),
        );
    let valid = serialize_structural_patch_fields_for_accepted_test_model(
        PersistedRowPatchBridgeEntity::MODEL,
        &patch,
    )
    .expect("serialize valid patch");
    let serialized = SerializedStructuralPatch::new(vec![
        SerializedStructuralFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            vec![0xF6],
        ),
        SerializedStructuralFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            valid.entries()[1].payload().to_vec(),
        ),
    ]);

    let err = materialize_entity_from_serialized_structural_patch::<PersistedRowPatchBridgeEntity>(
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
fn canonical_row_from_complete_serialized_structural_patch_rejects_noncanonical_scalar_payload() {
    let patch = StructuralPatch::new()
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            Value::Ulid(crate::types::Ulid::from_u128(7)),
        )
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            Value::Text("Ada".to_string()),
        );
    let valid = serialize_structural_patch_fields_for_accepted_test_model(
        PersistedRowPatchBridgeEntity::MODEL,
        &patch,
    )
    .expect("serialize valid patch");
    let serialized = SerializedStructuralPatch::new(vec![
        SerializedStructuralFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            vec![0xF6],
        ),
        SerializedStructuralFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            valid.entries()[1].payload().to_vec(),
        ),
    ]);

    let err = canonical_row_from_complete_serialized_structural_patch(
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
fn canonical_row_from_complete_serialized_structural_patch_rejects_incomplete_slot_image() {
    let patch = StructuralPatch::new().set(
        FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
        Value::Text("Ada".to_string()),
    );
    let serialized = serialize_structural_patch_fields_for_accepted_test_model(
        PersistedRowPatchBridgeEntity::MODEL,
        &patch,
    )
    .expect("serialize sparse patch");

    let err = canonical_row_from_complete_serialized_structural_patch(
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
fn materialize_entity_from_serialized_structural_patch_duplicate_slot_prefers_last_payload() {
    let first_name_patch = StructuralPatch::new().set(
        FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
        Value::Text("Ada".to_string()),
    );
    let final_patch = StructuralPatch::new()
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            Value::Ulid(crate::types::Ulid::from_u128(7)),
        )
        .set(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            Value::Text("Grace".to_string()),
        );
    let first_name_serialized = serialize_structural_patch_fields_for_accepted_test_model(
        PersistedRowPatchBridgeEntity::MODEL,
        &first_name_patch,
    )
    .expect("serialize first-name patch");
    let final_serialized = serialize_structural_patch_fields_for_accepted_test_model(
        PersistedRowPatchBridgeEntity::MODEL,
        &final_patch,
    )
    .expect("serialize final patch");
    let serialized = SerializedStructuralPatch::new(vec![
        SerializedStructuralFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 0).expect("resolve slot"),
            final_serialized.entries()[0].payload().to_vec(),
        ),
        SerializedStructuralFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            first_name_serialized.entries()[0].payload().to_vec(),
        ),
        SerializedStructuralFieldUpdate::new(
            FieldSlot::from_index(PersistedRowPatchBridgeEntity::MODEL, 1).expect("resolve slot"),
            final_serialized.entries()[1].payload().to_vec(),
        ),
    ]);

    let entity =
        materialize_entity_from_serialized_structural_patch::<PersistedRowPatchBridgeEntity>(
            &serialized,
        )
        .expect("duplicate sparse patch slot should keep the last payload");

    assert_eq!(
        entity,
        PersistedRowPatchBridgeEntity {
            id: crate::types::Ulid::from_u128(7),
            name: "Grace".to_string(),
        },
    );
}

#[test]
fn canonical_row_from_raw_row_replays_canonical_full_image_bytes() {
    let entity = PersistedRowPatchBridgeEntity {
        id: crate::types::Ulid::from_u128(11),
        name: "Ada".to_string(),
    };
    let raw_row = CanonicalRow::from_entity(&entity)
        .expect("encode canonical row")
        .into_raw_row();
    let canonical = canonical_row_from_raw_row_for_accepted_test_model(
        PersistedRowPatchBridgeEntity::MODEL,
        &raw_row,
    )
    .expect("canonical re-emission should succeed");

    assert_eq!(
        canonical.as_bytes(),
        raw_row.as_bytes(),
        "canonical raw-row rebuild must preserve already canonical row bytes",
    );
}

#[test]
fn canonical_row_from_raw_row_rejects_noncanonical_scalar_payload() {
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let raw_row =
        raw_row_from_dense_slot_payloads_for_tests(&TEST_MODEL, &[&[0xF6], payload.as_slice()]);

    let err = canonical_row_from_raw_row_for_accepted_test_model(&TEST_MODEL, &raw_row)
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
fn raw_row_from_complete_serialized_structural_patch_rejects_noncanonical_scalar_payload() {
    let payload = encode_value_storage_payload(&Value::Text("payload".to_string()));
    let serialized = SerializedStructuralPatch::new(vec![
        SerializedStructuralFieldUpdate::new(
            FieldSlot::from_index(&TEST_MODEL, 0).expect("resolve slot"),
            vec![0xF6],
        ),
        SerializedStructuralFieldUpdate::new(
            FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
            payload,
        ),
    ]);

    let err = RawRow::from_complete_serialized_structural_patch(&TEST_MODEL, &serialized)
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
fn raw_row_from_complete_serialized_structural_patch_rejects_incomplete_slot_image() {
    let serialized = SerializedStructuralPatch::new(vec![SerializedStructuralFieldUpdate::new(
        FieldSlot::from_index(&TEST_MODEL, 1).expect("resolve slot"),
        encode_value_storage_payload(&Value::Text("payload".to_string())),
    )]);

    let err = RawRow::from_complete_serialized_structural_patch(&TEST_MODEL, &serialized)
        .expect_err("fresh row emission must reject missing declared slots");

    assert!(
        err.message.contains("serialized patch did not emit slot 0"),
        "unexpected error: {err:?}"
    );
}
