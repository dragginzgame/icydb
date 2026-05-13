use super::*;

use crate::{
    db::{
        data::{
            CanonicalSlotReader, ScalarSlotValueRef, SlotReader, StorageKey,
            StructuralFieldDecodeContract,
        },
        index::{
            IndexId, IndexKey, IndexKeyKind, IndexState, IndexStore, RawIndexEntry, RawIndexKey,
        },
        schema::{
            AcceptedSchemaMutationError, FieldId, MutationCompatibility, MutationPlan,
            PersistedFieldKind, PersistedFieldSnapshot, PersistedIndexExpressionOp,
            PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedSchemaSnapshot, RebuildRequirement, SchemaFieldDefault, SchemaFieldSlot,
            SchemaMutation, SchemaMutationDelta, SchemaMutationRequest, SchemaRebuildAction,
            SchemaRowLayout, SchemaVersion, classify_schema_mutation_delta,
            mutation::{MutationPublicationBlocker, MutationPublicationStatus},
            schema_mutation_request_for_snapshots,
        },
    },
    error::InternalError,
    model::field::FieldModel,
    model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
    testing::test_memory,
    types::EntityTag,
    value::Value,
};
use std::{borrow::Cow, collections::BTreeMap};

struct RebuildSlotReader {
    values: Vec<Option<Value>>,
}

#[derive(Default)]
struct RecordingStagedStoreWriter {
    writes: Vec<(String, RawIndexKey, RawIndexEntry)>,
}

#[derive(Default)]
struct RecordingStagedStoreRollbackWriter {
    actions: Vec<(String, RawIndexKey, Option<RawIndexEntry>)>,
}

#[derive(Default)]
struct RecordingStagedStoreReadView {
    entries: BTreeMap<(String, RawIndexKey), RawIndexEntry>,
}

#[derive(Default)]
struct RecordingRuntimeInvalidationSink {
    invalidations: Vec<(
        String,
        super::SchemaMutationRuntimeEpoch,
        super::SchemaMutationRuntimeEpoch,
    )>,
}

#[derive(Default)]
struct RecordingAcceptedSnapshotPublicationSink {
    publications: Vec<(
        String,
        PersistedSchemaSnapshot,
        super::SchemaMutationRuntimeEpoch,
        super::SchemaMutationRuntimeEpoch,
    )>,
}

impl RecordingStagedStoreReadView {
    fn insert(&mut self, store: &str, key: RawIndexKey, entry: RawIndexEntry) {
        self.entries.insert((store.to_string(), key), entry);
    }
}

impl super::SchemaFieldPathIndexStagedStoreReadView for RecordingStagedStoreReadView {
    fn read_staged_entry(&self, store: &str, key: &RawIndexKey) -> Option<RawIndexEntry> {
        self.entries.get(&(store.to_string(), key.clone())).cloned()
    }
}

impl super::SchemaFieldPathIndexStagedStoreWriter for RecordingStagedStoreWriter {
    fn write_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        self.writes
            .push((store.to_string(), key.clone(), entry.clone()));
    }
}

impl super::SchemaFieldPathIndexStagedStoreRollbackWriter for RecordingStagedStoreRollbackWriter {
    fn restore_staged_entry(&mut self, store: &str, key: &RawIndexKey, entry: &RawIndexEntry) {
        self.actions
            .push((store.to_string(), key.clone(), Some(entry.clone())));
    }

    fn remove_staged_entry(&mut self, store: &str, key: &RawIndexKey) {
        self.actions.push((store.to_string(), key.clone(), None));
    }
}

impl super::SchemaMutationRuntimeInvalidationSink for RecordingRuntimeInvalidationSink {
    fn invalidate_runtime_schema(
        &mut self,
        store: &str,
        before: &super::SchemaMutationRuntimeEpoch,
        after: &super::SchemaMutationRuntimeEpoch,
    ) {
        self.invalidations
            .push((store.to_string(), before.clone(), after.clone()));
    }
}

impl super::SchemaMutationAcceptedSnapshotPublicationSink
    for RecordingAcceptedSnapshotPublicationSink
{
    fn publish_accepted_schema(
        &mut self,
        store: &str,
        accepted_after: &PersistedSchemaSnapshot,
        before: &super::SchemaMutationRuntimeEpoch,
        after: &super::SchemaMutationRuntimeEpoch,
    ) {
        self.publications.push((
            store.to_string(),
            accepted_after.clone(),
            before.clone(),
            after.clone(),
        ));
    }
}

impl SlotReader for RebuildSlotReader {
    fn generated_compatible_field_model(&self, _slot: usize) -> Result<&FieldModel, InternalError> {
        panic!("rebuild key test reader should not reopen generated field models")
    }

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
    fn field_decode_contract(
        &self,
        _slot: usize,
    ) -> Result<StructuralFieldDecodeContract, InternalError> {
        panic!("rebuild key test reader should not decode through field contracts")
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
        PersistedFieldKind::Text { max_len: None },
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
            PersistedFieldKind::Text { max_len: None },
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
        PersistedFieldKind::Text { max_len: None },
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
                PersistedFieldKind::Text { max_len: None },
                PersistedFieldKind::Text { max_len: None },
                "expr:v1:LOWER(name)".to_string(),
            )),
        )]),
        Some("LOWER(name) IS NOT NULL".to_string()),
    )
}

fn accepted_name_field_path_target() -> super::SchemaFieldPathIndexRebuildTarget {
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    target
}

fn staged_name_index_store() -> super::SchemaFieldPathIndexStagedStore {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        accepted_name_field_path_target(),
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Nat(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");

    super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
        .expect("valid staged rebuild should write into an in-memory staged store buffer")
}

fn extra_staged_name_index_entry() -> super::SchemaFieldPathIndexStagedEntry {
    let extra = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Margaret".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        accepted_name_field_path_target(),
        [super::SchemaFieldPathIndexRebuildRow::new(
            StorageKey::Nat(3),
            &extra,
        )],
    )
    .expect("extra field-path rebuild row should stage into a raw index entry");

    staged.entries()[0].clone()
}

fn initialized_index_store(memory_id: u8) -> IndexStore {
    let mut store = IndexStore::init(test_memory(memory_id));
    store.clear();
    store
}

fn validated_isolated_name_index_store(
    memory_id: u8,
) -> super::SchemaFieldPathIndexIsolatedIndexStoreValidation {
    let buffer = staged_name_index_store();
    let mut index_store = initialized_index_store(memory_id);
    let mut writer =
        super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(buffer.store(), &mut index_store);
    let batch = buffer.write_batch(&writer);
    let _ = batch.write_to(&mut writer);

    writer
        .validate_batch(&batch)
        .expect("isolated IndexStore should validate against staged batch")
}

fn field_path_index_runner_context() -> (
    PersistedSchemaSnapshot,
    PersistedSchemaSnapshot,
    super::SchemaMutationExecutionPlan,
) {
    let before = base_snapshot();
    let after = snapshot_with_indexes(&before, vec![non_unique_name_index()]);
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();

    (before, after, plan.execution_plan())
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
                PersistedFieldKind::Ulid,
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
                PersistedFieldKind::Text { max_len: None },
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
        snapshot.primary_key_field_id(),
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
        snapshot.primary_key_field_id(),
        SchemaRowLayout::new(
            SchemaVersion::new(snapshot.row_layout().version().get() + 1),
            snapshot.row_layout().field_to_slot().to_vec(),
        ),
        snapshot.fields().to_vec(),
        indexes,
    )
}

mod field_path_runner;
mod field_path_staging;
mod field_path_store;
mod planning;
