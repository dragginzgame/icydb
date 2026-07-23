use super::*;

use crate::{
    db::{
        data::{CanonicalSlotReader, ScalarSlotValueRef, SlotReader, StructuralRowContract},
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexState, IndexStore,
            RawIndexStoreKey,
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            AcceptedCheckExprV1, AcceptedConstraintCatalog, AcceptedConstraintKind,
            AcceptedFieldKind, AcceptedSchemaFingerprint, AcceptedSchemaMutationError,
            ConstraintActivationKind, ConstraintIdAllocator, ConstraintOrigin, FieldId,
            MutationPlan, PersistedFieldOrigin, PersistedFieldSnapshot, PersistedIndexExpressionOp,
            PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId, RowLayoutVersion,
            SchemaDdlSecondaryIndexFieldPathIntent, SchemaDdlSecondaryIndexKeyCandidateError,
            SchemaDdlSecondaryIndexKeyIntent, SchemaFieldSlot, SchemaFieldWritePolicy,
            SchemaHistoricalFill, SchemaIndexId, SchemaInsertDefault, SchemaMutationDelta,
            SchemaMutationRequest, SchemaRowLayout, SchemaVersion,
            build_sql_ddl_secondary_index_candidate, classify_schema_mutation_delta,
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
    PersistedFieldSnapshot::new_initial(
        FieldId::new(id),
        name.to_string(),
        SchemaFieldSlot::new(slot),
        AcceptedFieldKind::Text { max_len: None },
        Vec::new(),
        true,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    )
}

fn non_unique_name_index() -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).expect("test index identity should be non-zero"),
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
        SchemaIndexId::new(2).expect("test index identity should be non-zero"),
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
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "name".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
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
    let constraint_catalog = fields
        .iter()
        .try_fold(snapshot.constraint_catalog().clone(), |catalog, field| {
            catalog.with_added_not_null(field)
        })
        .expect("test appended-field constraint catalog should build");

    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        SchemaVersion::new(snapshot.version().get() + 1),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            snapshot
                .row_layout()
                .current_version()
                .checked_next()
                .expect("test layout version should advance"),
            snapshot.row_layout().history_floor(),
            next_layout_entries,
        ),
        next_fields,
        snapshot.indexes().to_vec(),
    )
    .with_constraint_catalog(constraint_catalog)
    .with_relations(snapshot.relations().to_vec())
}

fn snapshot_with_indexes(
    snapshot: &PersistedSchemaSnapshot,
    indexes: Vec<PersistedIndexSnapshot>,
) -> PersistedSchemaSnapshot {
    let mut constraint_catalog = snapshot.constraint_catalog().clone();
    for removed in snapshot.indexes().iter().filter(|existing| {
        existing.unique()
            && !indexes
                .iter()
                .any(|candidate| candidate.schema_id() == existing.schema_id())
    }) {
        constraint_catalog = constraint_catalog
            .with_removed_unique(removed.schema_id())
            .expect("test removed-index constraint catalog should close");
    }
    for added in indexes.iter().filter(|candidate| {
        candidate.unique()
            && !snapshot
                .indexes()
                .iter()
                .any(|existing| existing.schema_id() == candidate.schema_id())
    }) {
        constraint_catalog = constraint_catalog
            .with_added_unique(added)
            .expect("test added-index constraint catalog should close");
    }

    PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::new(snapshot.version().get() + 1),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.first_primary_key_field_id(),
        snapshot.row_layout().clone(),
        snapshot.fields().to_vec(),
        indexes,
    )
    .with_constraint_catalog(constraint_catalog)
    .with_relations(snapshot.relations().to_vec())
}

mod planning;
mod user_index_domain;
