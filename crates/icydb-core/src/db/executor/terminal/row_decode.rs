//! Module: executor::terminal::row_decode
//! Responsibility: structural scalar row decode from persisted bytes into kernel rows.
//! Does not own: typed response reconstruction or access-path iteration policy.
//! Boundary: scalar runtime row production consumes this structural decode contract.

#[cfg(any(test, feature = "structural-read-metrics"))]
use crate::db::executor::projection::record_sql_projection_full_row_decode_materialization;
#[cfg(test)]
use crate::model::field::EnumVariantModel;
#[cfg(test)]
use crate::types::Ulid;
use crate::{
    db::{
        data::{
            DataRow, RawRow, StorageKey, StructuralRowContract, StructuralSlotReader,
            decode_dense_raw_row_with_contract, decode_sparse_indexed_raw_row_with_contract,
            decode_sparse_raw_row_with_contract, decode_sparse_required_slot_with_contract,
        },
        executor::terminal::{RetainedSlotLayout, RetainedSlotRow, page::KernelRow},
    },
    error::InternalError,
    model::entity::EntityModel,
    value::Value,
};

///
/// RowLayout
///
/// RowLayout is the structural scalar row-decode plan built once at the typed
/// boundary.
/// It captures stable field ordering so row production no longer needs typed
/// entity materialization.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) struct RowLayout {
    contract: StructuralRowContract,
    field_count: usize,
    primary_key_slot: usize,
}

impl RowLayout {
    /// Build one structural row layout from model metadata.
    #[must_use]
    pub(in crate::db::executor) const fn from_model(model: &'static EntityModel) -> Self {
        let contract = StructuralRowContract::from_model(model);

        Self {
            contract,
            field_count: contract.field_count(),
            primary_key_slot: contract.primary_key_slot(),
        }
    }

    /// Borrow the frozen field-count authority carried by this layout.
    #[must_use]
    pub(in crate::db::executor) const fn field_count(self) -> usize {
        self.field_count
    }

    /// Borrow the frozen primary-key slot authority carried by this layout.
    #[must_use]
    pub(in crate::db::executor) const fn primary_key_slot(self) -> usize {
        self.primary_key_slot
    }

    /// Borrow one authoritative field name by structural slot index.
    #[must_use]
    pub(in crate::db::executor) fn field_name(self, slot: usize) -> Option<&'static str> {
        self.contract.fields().get(slot).map(|field| field.name)
    }

    /// Open one raw row through the authority-owned structural decode contract.
    pub(in crate::db::executor) fn open_raw_row(
        self,
        row: &RawRow,
    ) -> Result<StructuralSlotReader<'_>, InternalError> {
        StructuralSlotReader::from_raw_row_with_contract(row, self.contract)
    }
}

///
/// RowDecoder
///
/// RowDecoder is the named structural decode contract for scalar row
/// production.
/// The scalar runtime owns this decoder and feeds it raw persisted rows plus a
/// precomputed `RowLayout`, keeping typed entity reconstruction out of the hot
/// execution loop.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) struct RowDecoder {
    decode: fn(&RowLayout, DataRow) -> Result<KernelRow, InternalError>,
    #[cfg(test)]
    decode_slots: RowDecodeSlotsFn,
}

#[cfg(test)]
type RowDecodeSlotsFn = fn(
    &RowLayout,
    StorageKey,
    &RawRow,
    Option<&[usize]>,
) -> Result<Vec<Option<Value>>, InternalError>;

impl RowDecoder {
    /// Build the canonical structural row decoder used by scalar execution.
    #[must_use]
    pub(in crate::db::executor) const fn structural() -> Self {
        Self {
            decode: decode_kernel_row_structural,
            #[cfg(test)]
            decode_slots: decode_structural_slots,
        }
    }

    /// Decode one persisted row into one structural kernel row.
    pub(in crate::db::executor) fn decode(
        self,
        layout: &RowLayout,
        data_row: DataRow,
    ) -> Result<KernelRow, InternalError> {
        (self.decode)(layout, data_row)
    }

    /// Decode one persisted row into slot-indexed structural values without
    /// constructing one full kernel-row envelope.
    #[cfg(test)]
    pub(in crate::db::executor) fn decode_slots(
        self,
        layout: &RowLayout,
        expected_key: StorageKey,
        row: &RawRow,
        required_slots: Option<&[usize]>,
    ) -> Result<Vec<Option<Value>>, InternalError> {
        (self.decode_slots)(layout, expected_key, row, required_slots)
    }

    /// Decode one retained structural slot value without constructing one
    /// full kernel-row envelope or returning the surrounding slot vector.
    pub(in crate::db::executor) fn decode_required_slot_value(
        layout: &RowLayout,
        expected_key: StorageKey,
        row: &RawRow,
        required_slot: usize,
    ) -> Result<Option<Value>, InternalError> {
        decode_sparse_required_slot_with_contract(row, layout.contract, expected_key, required_slot)
    }

    /// Decode one retained structural slot-row without materializing a dense
    /// field-count-sized slot vector.
    pub(in crate::db::executor) fn decode_retained_slots(
        layout: &RowLayout,
        expected_key: StorageKey,
        row: &RawRow,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<RetainedSlotRow, InternalError> {
        // Phase 1: let dense retained-slot callers reuse the dedicated direct
        // full-row decode path instead of constructing the sparse reader first.
        if required_slots_match_full_layout(layout, retained_slot_layout.required_slots()) {
            return Ok(RetainedSlotRow::from_dense_slots(
                decode_dense_raw_row_with_contract(row, layout.contract, expected_key)?,
            ));
        }

        // Phase 2: reuse the canonical row-open validation boundary once, then
        // retain only the caller-declared slot/value pairs in compact layout
        // order for the retained-row wrapper.
        Ok(RetainedSlotRow::from_indexed_values(
            retained_slot_layout,
            decode_sparse_indexed_raw_row_with_contract(
                row,
                layout.contract,
                expected_key,
                retained_slot_layout.required_slots(),
            )?,
        ))
    }

    /// Decode one compact retained-slot value buffer without constructing one
    /// retained-row wrapper or field-count-sized slot image.
    pub(in crate::db::executor) fn decode_indexed_slot_values(
        layout: &RowLayout,
        expected_key: StorageKey,
        row: &RawRow,
        retained_slot_layout: &RetainedSlotLayout,
    ) -> Result<Vec<Option<Value>>, InternalError> {
        // Phase 1: let dense callers stay on the dedicated direct full-row
        // decode path so compact retained layouts do not regress all-slot reads.
        if required_slots_match_full_layout(layout, retained_slot_layout.required_slots()) {
            #[cfg(any(test, feature = "structural-read-metrics"))]
            record_sql_projection_full_row_decode_materialization();

            return decode_dense_raw_row_with_contract(row, layout.contract, expected_key);
        }

        // Phase 2: reuse the canonical row-open validation boundary once, then
        // materialize only the caller-declared retained slots into compact
        // layout order.
        decode_sparse_indexed_raw_row_with_contract(
            row,
            layout.contract,
            expected_key,
            retained_slot_layout.required_slots(),
        )
    }
}

// Decode one persisted data row into one structural kernel row using the
// precomputed slot layout and structural field decoders only.
fn decode_kernel_row_structural(
    layout: &RowLayout,
    data_row: DataRow,
) -> Result<KernelRow, InternalError> {
    let slots = decode_structural_slots(layout, data_row.0.storage_key(), &data_row.1, None)?;

    Ok(KernelRow::new(data_row, slots))
}

// Decode one persisted row directly into slot-indexed structural values while
// still validating the primary-key slot against storage identity.
fn decode_structural_slots(
    layout: &RowLayout,
    expected_key: StorageKey,
    row: &RawRow,
    required_slots: Option<&[usize]>,
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: route dense full-slot callers straight to the dedicated dense
    // decode path so they do not pay per-row sparse reader construction.
    if required_slots
        .is_none_or(|required_slots| required_slots_match_full_layout(layout, required_slots))
    {
        #[cfg(any(test, feature = "structural-read-metrics"))]
        record_sql_projection_full_row_decode_materialization();

        return decode_dense_raw_row_with_contract(row, layout.contract, expected_key);
    }

    // Phase 2: sparse callers decode only the slots their compiled plan will
    // actually touch without building the general row-reader cache.
    decode_sparse_raw_row_with_contract(
        row,
        layout.contract,
        expected_key,
        required_slots.expect("dense full-slot callers return earlier"),
    )
}

// Detect the dense retained-slot case up front so full-row and full-slot SQL
// paths can stay on the straight-line dense decode before compact retained-row
// conversion instead of paying the sparse per-slot decode machinery.
fn required_slots_match_full_layout(layout: &RowLayout, required_slots: &[usize]) -> bool {
    required_slots.len() == layout.field_count
        && required_slots
            .iter()
            .copied()
            .enumerate()
            .all(|(expected_slot, slot)| slot == expected_slot)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::data::{decode_structural_field_by_kind_bytes, with_structural_read_metrics},
        error::{ErrorClass, ErrorOrigin},
        model::field::{FieldKind, FieldStorageDecode},
        serialize::serialize,
        traits::EntitySchema,
        types::{Blob, Text},
        value::{Value, ValueEnum},
    };
    use icydb_derive::{FieldProjection, PersistedRow};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    crate::test_canister! {
        ident = RowDecodeCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = RowDecodeStore,
        canister = RowDecodeCanister,
    }

    #[derive(
        Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
    )]
    struct RowDecodeEntity {
        id: Ulid,
        title: Text,
        tags: Vec<Text>,
        portrait: Blob,
    }

    crate::test_entity_schema! {
        ident = RowDecodeEntity,
        id = Ulid,
        id_field = id,
        entity_name = "RowDecodeEntity",
        entity_tag = crate::testing::PROBE_ENTITY_TAG,
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("title", FieldKind::Text),
            ("tags", FieldKind::List(&FieldKind::Text)),
            ("portrait", FieldKind::Blob),
        ],
        indexes = [],
        store = RowDecodeStore,
        canister = RowDecodeCanister,
    }

    fn decode_test_row(entity: &RowDecodeEntity) -> KernelRow {
        let key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(entity.id)
            .expect("test key construction should succeed");
        let row = RawRow::from_entity(entity).expect("test row serialization should succeed");

        RowDecoder::structural()
            .decode(&RowLayout::from_model(RowDecodeEntity::MODEL), (key, row))
            .expect("structural row decode should succeed")
    }

    fn decode_test_row_with_metrics(
        entity: &RowDecodeEntity,
    ) -> (KernelRow, crate::db::data::StructuralReadMetrics) {
        with_structural_read_metrics(|| decode_test_row(entity))
    }

    fn decode_required_test_slots_with_metrics(
        entity: &RowDecodeEntity,
        required_slots: &[usize],
    ) -> (Vec<Option<Value>>, crate::db::data::StructuralReadMetrics) {
        let key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(entity.id)
            .expect("test key construction should succeed");
        let row = RawRow::from_entity(entity).expect("test row serialization should succeed");

        with_structural_read_metrics(|| {
            RowDecoder::structural()
                .decode_slots(
                    &RowLayout::from_model(RowDecodeEntity::MODEL),
                    key.storage_key(),
                    &row,
                    Some(required_slots),
                )
                .expect("selective slot decode should succeed")
        })
    }

    fn to_cbor_bytes<T: Serialize>(value: &T) -> Vec<u8> {
        serde_cbor::to_vec(value).expect("test fixture should serialize into CBOR bytes")
    }

    #[test]
    fn structural_row_decoder_materializes_slot_values_without_entity_decode() {
        let entity = RowDecodeEntity {
            id: Ulid::from_u128(7),
            title: "alpha".to_string(),
            tags: vec!["one".to_string(), "two".to_string()],
            portrait: Blob::from(vec![0x10, 0x20, 0x30]),
        };
        let row = decode_test_row(&entity);

        assert_eq!(row.slot(0), Some(Value::Ulid(entity.id)));
        assert_eq!(row.slot(1), Some(Value::Text(entity.title)));
        assert_eq!(
            row.slot(2),
            Some(Value::List(vec![
                Value::Text("one".to_string()),
                Value::Text("two".to_string()),
            ])),
        );
        assert_eq!(row.slot(3), Some(Value::Blob(vec![0x10, 0x20, 0x30])));
    }

    #[test]
    fn structural_row_decoder_metrics_report_full_non_scalar_materialization() {
        let entity = RowDecodeEntity {
            id: Ulid::from_u128(17),
            title: "alpha".to_string(),
            tags: vec!["one".to_string(), "two".to_string()],
            portrait: Blob::from(vec![0x10, 0x20, 0x30]),
        };
        let (_row, metrics) = decode_test_row_with_metrics(&entity);

        assert_eq!(metrics.rows_opened, 0);
        assert_eq!(metrics.declared_slots_validated, 0);
        assert_eq!(
            metrics.validated_non_scalar_slots, 0,
            "dense full-row decode now bypasses the sparse reader metrics path",
        );
        assert_eq!(
            metrics.materialized_non_scalar_slots, 0,
            "dense full-row decode now bypasses the sparse reader metrics path",
        );
        assert_eq!(
            metrics.rows_without_lazy_non_scalar_materializations, 0,
            "dense full-row decode no longer routes through reader-probe aggregation",
        );
    }

    #[test]
    fn selective_slot_decode_can_skip_unused_non_scalar_materialization() {
        let entity = RowDecodeEntity {
            id: Ulid::from_u128(23),
            title: "alpha".to_string(),
            tags: vec!["one".to_string(), "two".to_string()],
            portrait: Blob::from(vec![0x10, 0x20, 0x30]),
        };
        let (slots, metrics) = decode_required_test_slots_with_metrics(&entity, &[0, 1]);

        assert_eq!(slots[0], Some(Value::Ulid(entity.id)));
        assert_eq!(slots[1], Some(Value::Text(entity.title)));
        assert_eq!(slots[2], None);
        assert_eq!(slots[3], None);
        assert_eq!(metrics.rows_opened, 1);
        assert_eq!(metrics.declared_slots_validated, 2);
        assert_eq!(
            metrics.validated_non_scalar_slots, 0,
            "selective slot decode should not validate untouched non-scalar fields",
        );
        assert_eq!(
            metrics.materialized_non_scalar_slots, 0,
            "selective slot decode should leave untouched non-scalar fields lazy",
        );
        assert_eq!(metrics.rows_without_lazy_non_scalar_materializations, 1);
    }

    #[test]
    fn structural_row_decoder_rejects_raw_cbor_scalar_slot_payloads() {
        let entity = RowDecodeEntity {
            id: Ulid::from_u128(8),
            title: "alpha".to_string(),
            tags: vec!["one".to_string(), "two".to_string()],
            portrait: Blob::from(vec![0x10, 0x20, 0x30]),
        };
        let key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(entity.id)
            .expect("test key construction should succeed");
        let id_bytes = crate::db::data::encode_persisted_scalar_slot_payload(&entity.id, "id")
            .expect("id payload should encode");
        let raw_title = serialize(&entity.title).expect("raw scalar title should encode");
        let tags_bytes = crate::db::data::encode_persisted_slot_payload(&entity.tags, "tags")
            .expect("tags payload should encode");
        let portrait_bytes =
            crate::db::data::encode_persisted_scalar_slot_payload(&entity.portrait, "portrait")
                .expect("portrait payload should encode");
        let slot_payloads = [
            id_bytes.as_slice(),
            raw_title.as_slice(),
            tags_bytes.as_slice(),
            portrait_bytes.as_slice(),
        ];
        let mut payload = Vec::new();
        let mut offset = 0_u32;

        payload.extend_from_slice(&4_u16.to_be_bytes());
        for bytes in slot_payloads {
            let len = u32::try_from(bytes.len()).expect("slot length should fit u32");
            payload.extend_from_slice(&offset.to_be_bytes());
            payload.extend_from_slice(&len.to_be_bytes());
            offset = offset.saturating_add(len);
        }
        for bytes in slot_payloads {
            payload.extend_from_slice(bytes);
        }
        let row = RawRow::try_new(
            crate::db::codec::serialize_row_payload(payload).expect("serialize row payload"),
        )
        .expect("build raw row");

        let Err(err) = RowDecoder::structural()
            .decode(&RowLayout::from_model(RowDecodeEntity::MODEL), (key, row))
        else {
            panic!("raw CBOR scalar slot payloads must fail closed");
        };

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Serialize);
        assert!(
            err.message.contains("field 'title'"),
            "unexpected error: {err:?}"
        );
        assert!(
            err.message
                .contains("expected slot envelope prefix byte 0xFF"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn structural_row_decoder_rejects_primary_key_mismatch() {
        let entity = RowDecodeEntity {
            id: Ulid::from_u128(9),
            title: "alpha".to_string(),
            tags: vec![],
            portrait: Blob::default(),
        };
        let wrong_key = crate::db::data::DataKey::try_new::<RowDecodeEntity>(Ulid::from_u128(10))
            .expect("wrong test key construction should succeed");
        let row = RawRow::from_entity(&entity).expect("test row serialization should succeed");
        let Err(err) = RowDecoder::structural().decode(
            &RowLayout::from_model(RowDecodeEntity::MODEL),
            (wrong_key, row),
        ) else {
            panic!("key mismatch must fail closed")
        };

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
    }

    #[test]
    fn structural_row_decoder_returns_null_for_structured_field_kind() {
        let decoded = decode_structural_field_by_kind_bytes(
            &to_cbor_bytes(&vec!["x".to_string(), "y".to_string()]),
            FieldKind::Structured { queryable: false },
        )
        .expect("structured field decode should succeed");

        assert_eq!(decoded, Value::Null);
    }

    #[test]
    fn structural_row_decoder_preserves_enum_payload_shape_best_effort() {
        static ENUM_VARIANTS: &[EnumVariantModel] = &[EnumVariantModel::new(
            "Loaded",
            Some(&FieldKind::Uint),
            FieldStorageDecode::ByKind,
        )];

        let decoded = decode_structural_field_by_kind_bytes(
            &to_cbor_bytes(&serde_cbor::Value::Map(BTreeMap::from([(
                serde_cbor::Value::Text("Loaded".to_string()),
                serde_cbor::Value::Integer(7),
            )]))),
            FieldKind::Enum {
                path: "tests::State",
                variants: ENUM_VARIANTS,
            },
        )
        .expect("enum payload decode should succeed");

        assert_eq!(
            decoded,
            Value::Enum(
                ValueEnum::new("Loaded", Some("tests::State")).with_payload(Value::Uint(7)),
            ),
        );
    }

    #[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize)]
    struct RowDecodeValueEntity {
        id: Ulid,
        status: Value,
    }

    impl Default for RowDecodeValueEntity {
        fn default() -> Self {
            Self {
                id: Ulid::from_u128(0),
                status: Value::Null,
            }
        }
    }

    crate::test_entity_schema! {
        ident = RowDecodeValueEntity,
        id = Ulid,
        id_field = id,
        entity_name = "RowDecodeValueEntity",
        entity_tag = crate::testing::PROBE_ENTITY_TAG,
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            (
                "status",
                FieldKind::Enum {
                    path: "tests::Status",
                    variants: &[],
                },
                crate::model::field::FieldStorageDecode::Value
            ),
        ],
        indexes = [],
        store = RowDecodeStore,
        canister = RowDecodeCanister,
    }

    #[test]
    fn structural_row_decoder_respects_value_storage_decode_contract() {
        let entity = RowDecodeValueEntity {
            id: Ulid::from_u128(77),
            status: Value::Enum(
                ValueEnum::new("Paid", Some("tests::Status")).with_payload(Value::Uint(7)),
            ),
        };
        let key = crate::db::data::DataKey::try_new::<RowDecodeValueEntity>(entity.id)
            .expect("test key construction should succeed");
        let row = RawRow::from_entity(&entity).expect("test row serialization should succeed");
        let decoded = RowDecoder::structural()
            .decode(
                &RowLayout::from_model(RowDecodeValueEntity::MODEL),
                (key, row),
            )
            .expect("structural row decode should succeed");

        assert_eq!(decoded.slot(1), Some(entity.status));
    }
}
