use super::*;

use crate::{
    db::{
        data::{CanonicalSlotReader, ScalarSlotValueRef, SlotReader},
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexState, IndexStore,
            RawIndexStoreKey,
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            AcceptedFieldKind, AcceptedSchemaMutationError, FieldId, MutationPlan,
            PersistedFieldSnapshot, PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaFieldDefault, SchemaFieldSlot, SchemaMutationDelta, SchemaMutationRequest,
            SchemaRowLayout, SchemaVersion, classify_schema_mutation_delta,
            schema_mutation_request_for_snapshots,
        },
    },
    error::InternalError,
    model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
    types::EntityTag,
    value::Value,
};
use std::borrow::Cow;

struct RebuildSlotReader {
    values: Vec<Option<Value>>,
}

impl SlotReader for RebuildSlotReader {
    fn has(&self, slot: usize) -> bool {
        self.values.get(slot).is_some_and(Option::is_some)
    }

    fn get_bytes(&self, _slot: usize) -> Option<&[u8]> {
        panic!("rebuild key test reader should not decode raw bytes")
    }

    fn get_scalar(&self, _slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        panic!("rebuild key test reader should not route through scalar fast paths")
    }

    fn get_value(&mut self, _slot: usize) -> Result<Option<Value>, InternalError> {
        panic!("rebuild key test reader should not route through generated get_value")
    }
}

impl CanonicalSlotReader for RebuildSlotReader {
    fn field_name(&self, _slot: usize) -> Result<&str, InternalError> {
        Ok("test")
    }

    fn field_leaf_codec(&self, _slot: usize) -> Result<LeafCodec, InternalError> {
        panic!("rebuild key test reader should not decode through field contracts")
    }

    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError> {
        self.values
            .get(slot)
            .and_then(Option::as_ref)
            .cloned()
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing("test"))
    }

    fn required_value_by_contract_cow(&self, slot: usize) -> Result<Cow<'_, Value>, InternalError> {
        self.values
            .get(slot)
            .and_then(Option::as_ref)
            .map(Cow::Borrowed)
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing("test"))
    }
}

fn nullable_text_field(name: &str, id: u32, slot: u16) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new(
        FieldId::new(id),
        name.to_string(),
        SchemaFieldSlot::new(slot),
        AcceptedFieldKind::Text { max_len: None },
        Vec::new(),
        true,
        SchemaFieldDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    )
}

fn non_unique_name_index() -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        1,
        "by_name".to_string(),
        "test::mutation::by_name".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["name".to_string()],
            AcceptedFieldKind::Text { max_len: None },
            false,
        )]),
        Some("name IS NOT NULL".to_string()),
    )
}

fn name_key_path() -> PersistedIndexFieldPathSnapshot {
    PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["name".to_string()],
        AcceptedFieldKind::Text { max_len: None },
        false,
    )
}

fn expression_name_index() -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        2,
        "by_lower_name".to_string(),
        "test::mutation::by_lower_name".to_string(),
        false,
        PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
            Box::new(PersistedIndexExpressionSnapshot::new(
                PersistedIndexExpressionOp::Lower,
                name_key_path(),
                AcceptedFieldKind::Text { max_len: None },
                AcceptedFieldKind::Text { max_len: None },
                "expr:v1:LOWER(name)".to_string(),
            )),
        )]),
        Some("LOWER(name) IS NOT NULL".to_string()),
    )
}

fn base_snapshot() -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "test::MutationEntity".to_string(),
        "MutationEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "name".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    )
}

fn append_fields_snapshot(
    snapshot: &PersistedSchemaSnapshot,
    fields: &[PersistedFieldSnapshot],
) -> PersistedSchemaSnapshot {
    let mut next_fields = snapshot.fields().to_vec();
    next_fields.extend_from_slice(fields);

    let mut next_layout_entries = snapshot.row_layout().field_to_slot().to_vec();
    next_layout_entries.extend(fields.iter().map(|field| (field.id(), field.slot())));

    PersistedSchemaSnapshot::new(
        SchemaVersion::new(snapshot.version().get() + 1),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.first_primary_key_field_id(),
        SchemaRowLayout::new(
            SchemaVersion::new(snapshot.row_layout().version().get() + 1),
            next_layout_entries,
        ),
        next_fields,
    )
}

fn snapshot_with_indexes(
    snapshot: &PersistedSchemaSnapshot,
    indexes: Vec<PersistedIndexSnapshot>,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::new(snapshot.version().get() + 1),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.first_primary_key_field_id(),
        SchemaRowLayout::new(
            SchemaVersion::new(snapshot.row_layout().version().get() + 1),
            snapshot.row_layout().field_to_slot().to_vec(),
        ),
        snapshot.fields().to_vec(),
        indexes,
    )
}

mod planning;
mod user_index_domain;
