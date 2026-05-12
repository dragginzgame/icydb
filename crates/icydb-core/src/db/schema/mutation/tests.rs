use crate::{
    db::{
        data::{
            CanonicalSlotReader, ScalarSlotValueRef, SlotReader, StorageKey,
            StructuralFieldDecodeContract,
        },
        index::{IndexId, IndexKey, IndexState, IndexStore, RawIndexEntry, RawIndexKey},
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
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
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
            StorageKey::Uint(3),
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

#[test]
fn append_only_field_mutation_plan_is_no_rebuild() {
    let field = nullable_text_field("nickname", 3, 2);
    let plan = MutationPlan::append_only_fields(&[field]);

    assert_eq!(
        plan.compatibility(),
        MutationCompatibility::MetadataOnlySafe
    );
    assert_eq!(
        plan.rebuild_requirement(),
        RebuildRequirement::NoRebuildRequired
    );
    assert_eq!(plan.added_field_count(), 1);
    assert_eq!(
        plan.mutations(),
        &[SchemaMutation::AddNullableField {
            field_id: FieldId::new(3),
            name: "nickname".to_string(),
            slot: SchemaFieldSlot::new(2),
        }]
    );
}

#[test]
fn mutation_plan_fingerprint_is_deterministic_and_semantic() {
    let nickname = nullable_text_field("nickname", 3, 2);
    let handle = nullable_text_field("handle", 3, 2);
    let first = MutationPlan::append_only_fields(std::slice::from_ref(&nickname));
    let second = MutationPlan::append_only_fields(&[nickname]);
    let changed = MutationPlan::append_only_fields(&[handle]);

    assert_eq!(first.fingerprint(), second.fingerprint());
    assert_ne!(first.fingerprint(), changed.fingerprint());
}

#[test]
fn index_mutation_plans_are_rebuild_gated() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let expression =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("accepted expression index should lower")
            .lower_to_plan();
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();

    for plan in [&field_path, &expression, &drop] {
        assert_eq!(plan.compatibility(), MutationCompatibility::RequiresRebuild);
        assert_eq!(
            plan.rebuild_requirement(),
            RebuildRequirement::IndexRebuildRequired
        );
        assert_eq!(
            plan.publication_status(),
            MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
                MutationCompatibility::RequiresRebuild,
            )),
        );
    }
}

#[test]
fn rebuild_plan_derives_physical_index_actions() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let expression =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("accepted expression index should lower")
            .lower_to_plan();
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();

    let field_path_rebuild = field_path.rebuild_plan();
    let [SchemaRebuildAction::BuildFieldPathIndex { target }] = field_path_rebuild.actions() else {
        panic!("field-path index addition should derive one field-path rebuild target");
    };
    assert_eq!(target.ordinal(), 1);
    assert_eq!(target.name(), "by_name");
    assert_eq!(target.store(), "test::mutation::by_name");
    assert!(!target.unique());
    assert_eq!(target.predicate_sql(), Some("name IS NOT NULL"));
    let [key_path] = target.key_paths() else {
        panic!("field-path rebuild target should carry one accepted key path");
    };
    assert_eq!(key_path.field_id(), FieldId::new(2));
    assert_eq!(key_path.slot(), SchemaFieldSlot::new(1));
    assert_eq!(key_path.path(), &["name".to_string()]);
    assert_eq!(key_path.kind(), &PersistedFieldKind::Text { max_len: None });
    assert!(!key_path.nullable());
    let expression_rebuild = expression.rebuild_plan();
    let [SchemaRebuildAction::BuildExpressionIndex { target }] = expression_rebuild.actions()
    else {
        panic!("expression index addition should derive one expression rebuild target");
    };
    assert_eq!(target.ordinal(), 2);
    assert_eq!(target.name(), "by_lower_name");
    assert_eq!(target.store(), "test::mutation::by_lower_name");
    assert!(!target.unique());
    assert_eq!(target.predicate_sql(), Some("LOWER(name) IS NOT NULL"));
    let [super::SchemaExpressionIndexRebuildKey::Expression(expression)] = target.key_items()
    else {
        panic!("expression rebuild target should carry one expression key");
    };
    assert_eq!(expression.op(), PersistedIndexExpressionOp::Lower);
    assert_eq!(expression.canonical_text(), "expr:v1:LOWER(name)");
    assert_eq!(
        expression.input_kind(),
        &PersistedFieldKind::Text { max_len: None }
    );
    assert_eq!(
        expression.output_kind(),
        &PersistedFieldKind::Text { max_len: None }
    );
    assert_eq!(expression.source().field_id(), FieldId::new(2));
    assert_eq!(expression.source().slot(), SchemaFieldSlot::new(1));
    let drop_rebuild = drop.rebuild_plan();
    let [SchemaRebuildAction::DropSecondaryIndex { target }] = drop_rebuild.actions() else {
        panic!("secondary index drop should derive one cleanup target");
    };
    assert_eq!(target.ordinal(), 1);
    assert_eq!(target.name(), "by_name");
    assert_eq!(target.store(), "test::mutation::by_name");
    assert!(!target.unique());
    assert_eq!(target.predicate_sql(), Some("name IS NOT NULL"));
}

#[test]
fn execution_plan_keeps_metadata_only_mutations_publishable_without_steps() {
    let field = nullable_text_field("nickname", 3, 2);
    let plan = MutationPlan::append_only_fields(&[field]);
    let execution = plan.execution_plan();

    assert_eq!(
        execution.readiness(),
        super::SchemaMutationExecutionReadiness::PublishableNow,
    );
    assert!(execution.steps().is_empty());
    assert!(execution.runner_capabilities().is_empty());
    assert_eq!(
        execution.execution_gate(),
        super::SchemaMutationExecutionGate::ReadyToPublish,
    );
    assert_eq!(
        execution.admit_runner_capabilities(&[]),
        super::SchemaMutationExecutionAdmission::PublishableNow,
    );
    assert_eq!(
        plan.publication_status(),
        MutationPublicationStatus::Publishable,
    );
}

#[test]
fn execution_plan_schedules_index_work_before_validation_and_invalidation() {
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();
    let execution = drop.execution_plan();

    assert_eq!(
        execution.readiness(),
        super::SchemaMutationExecutionReadiness::RequiresPhysicalRunner(
            RebuildRequirement::IndexRebuildRequired,
        ),
    );
    assert_eq!(
        execution.execution_gate(),
        super::SchemaMutationExecutionGate::AwaitingPhysicalWork {
            requirement: RebuildRequirement::IndexRebuildRequired,
            step_count: 3,
        },
    );
    let [
        super::SchemaMutationExecutionStep::DropSecondaryIndex { target },
        super::SchemaMutationExecutionStep::ValidatePhysicalWork,
        super::SchemaMutationExecutionStep::InvalidateRuntimeState,
    ] = execution.steps()
    else {
        panic!("drop execution should schedule cleanup, validation, and invalidation");
    };
    assert_eq!(target.name(), "by_name");
    assert_eq!(target.store(), "test::mutation::by_name");
}

#[test]
fn execution_plan_reports_runner_capabilities_without_duplicates() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let expression =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("accepted expression index should lower")
            .lower_to_plan();
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();

    assert_eq!(
        field_path.execution_plan().runner_capabilities(),
        vec![
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        expression.execution_plan().runner_capabilities(),
        vec![
            super::SchemaMutationRunnerCapability::BuildExpressionIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        drop.execution_plan().runner_capabilities(),
        vec![
            super::SchemaMutationRunnerCapability::DropSecondaryIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
}

#[test]
fn execution_admission_fails_closed_on_missing_runner_capabilities() {
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();
    let execution = drop.execution_plan();

    assert_eq!(
        execution.admit_runner_capabilities(&[]),
        super::SchemaMutationExecutionAdmission::MissingRunnerCapabilities {
            missing: vec![
                super::SchemaMutationRunnerCapability::DropSecondaryIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
    assert_eq!(
        execution.admit_runner_capabilities(&[
            super::SchemaMutationRunnerCapability::DropSecondaryIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
        ]),
        super::SchemaMutationExecutionAdmission::MissingRunnerCapabilities {
            missing: vec![super::SchemaMutationRunnerCapability::InvalidateRuntimeState],
        },
    );
    assert_eq!(
        execution.admit_runner_capabilities(&[
            super::SchemaMutationRunnerCapability::DropSecondaryIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ]),
        super::SchemaMutationExecutionAdmission::RunnerReady {
            required: vec![
                super::SchemaMutationRunnerCapability::DropSecondaryIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
}

#[test]
fn runner_contract_preflight_deduplicates_capabilities_and_preserves_gate() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let execution = field_path.execution_plan();
    let runner = super::SchemaMutationRunnerContract::new(&[
        super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
        super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
        super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
        super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
    ]);

    assert_eq!(
        runner.capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        runner.preflight(&execution),
        super::SchemaMutationRunnerPreflight::Ready {
            step_count: 3,
            required: vec![
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
    assert_eq!(
        execution.execution_gate(),
        super::SchemaMutationExecutionGate::AwaitingPhysicalWork {
            requirement: RebuildRequirement::IndexRebuildRequired,
            step_count: 3,
        },
    );
}

#[test]
fn runner_contract_preflight_keeps_no_work_and_rejections_non_executable() {
    let metadata_only = MutationPlan::append_only_fields(&[nullable_text_field("nickname", 3, 2)]);
    let rewrite = SchemaMutationRequest::Incompatible.lower_to_plan();
    let unsupported = SchemaMutationRequest::AlterNullability {
        field_id: FieldId::new(2),
    }
    .lower_to_plan();
    let runner = super::SchemaMutationRunnerContract::new(&[
        super::SchemaMutationRunnerCapability::RewriteAllRows,
    ]);

    assert_eq!(
        runner.preflight(&metadata_only.execution_plan()),
        super::SchemaMutationRunnerPreflight::NoPhysicalWork,
    );
    assert_eq!(
        runner.preflight(&rewrite.execution_plan()),
        super::SchemaMutationRunnerPreflight::Rejected {
            requirement: RebuildRequirement::FullDataRewriteRequired,
        },
    );
    assert_eq!(
        runner.preflight(&unsupported.execution_plan()),
        super::SchemaMutationRunnerPreflight::Rejected {
            requirement: RebuildRequirement::Unsupported,
        },
    );
}

#[test]
fn runner_outcome_reports_no_work_and_ready_physical_work() {
    let metadata_only = MutationPlan::append_only_fields(&[nullable_text_field("nickname", 3, 2)]);
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let runner = super::SchemaMutationRunnerContract::new(&[
        super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
        super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
        super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
    ]);

    let super::SchemaMutationRunnerOutcome::NoPhysicalWork(no_work) =
        runner.outcome(&metadata_only.execution_plan())
    else {
        panic!("metadata-only mutation should not require physical work");
    };
    assert_eq!(no_work.step_count(), 0);
    assert!(no_work.required_capabilities().is_empty());
    assert_eq!(
        no_work.completed_phases(),
        &[super::SchemaMutationRunnerPhase::Preflight],
    );
    assert!(no_work.has_completed_phase(super::SchemaMutationRunnerPhase::Preflight));
    assert_eq!(no_work.store_visibility(), None);
    assert_eq!(no_work.rows_scanned(), 0);
    assert_eq!(no_work.rows_skipped(), 0);
    assert_eq!(no_work.index_keys_written(), 0);
    assert!(!no_work.physical_work_allows_publication());

    let super::SchemaMutationRunnerOutcome::ReadyForPhysicalWork(ready) =
        runner.outcome(&field_path.execution_plan())
    else {
        panic!("field-path index mutation should be ready for staged physical work");
    };
    assert_eq!(ready.step_count(), 3);
    assert_eq!(
        ready.required_capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        ready.completed_phases(),
        &[super::SchemaMutationRunnerPhase::Preflight],
    );
    assert_eq!(
        ready.store_visibility(),
        Some(super::SchemaMutationStoreVisibility::StagedOnly),
    );
    assert_eq!(ready.rows_scanned(), 0);
    assert_eq!(ready.rows_skipped(), 0);
    assert_eq!(ready.index_keys_written(), 0);
    assert!(!ready.physical_work_allows_publication());
}

#[test]
fn runner_outcome_classifies_missing_capabilities_and_unsupported_requirements() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();
    let no_runner = super::SchemaMutationRunnerContract::new(&[]);

    let super::SchemaMutationRunnerOutcome::Rejected(missing) =
        no_runner.outcome(&field_path.execution_plan())
    else {
        panic!("missing runner capabilities should reject before physical work");
    };
    assert_eq!(missing.phase(), super::SchemaMutationRunnerPhase::Preflight);
    assert_eq!(
        missing.kind(),
        super::SchemaMutationRunnerRejectionKind::MissingCapabilities,
    );
    assert_eq!(
        missing.requirement(),
        Some(RebuildRequirement::IndexRebuildRequired),
    );
    assert_eq!(
        missing.missing_capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );

    let super::SchemaMutationRunnerOutcome::Rejected(unsupported) =
        no_runner.outcome(&incompatible.execution_plan())
    else {
        panic!("full rewrite should remain rejected by runner outcome");
    };
    assert_eq!(
        unsupported.kind(),
        super::SchemaMutationRunnerRejectionKind::UnsupportedRequirement,
    );
    assert_eq!(
        unsupported.requirement(),
        Some(RebuildRequirement::FullDataRewriteRequired),
    );
    assert!(unsupported.missing_capabilities().is_empty());
}

#[test]
fn runner_input_binds_accepted_snapshots_to_execution_plan() {
    let before = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let after = append_fields_snapshot(&before, std::slice::from_ref(&added));
    let plan = MutationPlan::append_only_fields(&[added]);
    let input = super::SchemaMutationRunnerInput::new(&before, &after, plan.execution_plan())
        .expect("same-entity accepted snapshots should build runner input");
    let runner = super::SchemaMutationRunnerContract::new(&[]);

    assert_eq!(input.accepted_before().entity_path(), before.entity_path());
    assert_eq!(
        input.accepted_after().fields().len(),
        before.fields().len() + 1,
    );
    assert_eq!(
        input.execution_plan().readiness(),
        super::SchemaMutationExecutionReadiness::PublishableNow,
    );
    assert!(matches!(
        input.outcome(&runner),
        super::SchemaMutationRunnerOutcome::NoPhysicalWork(_),
    ));
}

#[test]
fn runner_input_rejects_cross_entity_snapshot_pairs() {
    let before = base_snapshot();
    let wrong_entity = PersistedSchemaSnapshot::new(
        before.version(),
        "test::OtherEntity".to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_id(),
        before.row_layout().clone(),
        before.fields().to_vec(),
    );
    let wrong_name = PersistedSchemaSnapshot::new(
        before.version(),
        before.entity_path().to_string(),
        "OtherEntity".to_string(),
        before.primary_key_field_id(),
        before.row_layout().clone(),
        before.fields().to_vec(),
    );
    let wrong_pk = PersistedSchemaSnapshot::new(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        FieldId::new(99),
        before.row_layout().clone(),
        before.fields().to_vec(),
    );

    assert_eq!(
        super::SchemaMutationRunnerInput::new(
            &before,
            &wrong_entity,
            MutationPlan::exact_match().execution_plan(),
        ),
        Err(super::SchemaMutationRunnerInputError::EntityPath),
    );
    assert_eq!(
        super::SchemaMutationRunnerInput::new(
            &before,
            &wrong_name,
            MutationPlan::exact_match().execution_plan(),
        ),
        Err(super::SchemaMutationRunnerInputError::EntityName),
    );
    assert_eq!(
        super::SchemaMutationRunnerInput::new(
            &before,
            &wrong_pk,
            MutationPlan::exact_match().execution_plan(),
        ),
        Err(super::SchemaMutationRunnerInputError::PrimaryKeyField),
    );
}

#[test]
fn noop_runner_accepts_metadata_only_input_and_rejects_physical_work() {
    let before = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let metadata_after = append_fields_snapshot(&before, std::slice::from_ref(&added));
    let metadata_input = super::SchemaMutationRunnerInput::new(
        &before,
        &metadata_after,
        MutationPlan::append_only_fields(&[added]).execution_plan(),
    )
    .expect("metadata-only same-entity input should build");
    let index_after = snapshot_with_indexes(&before, vec![non_unique_name_index()]);
    let index_plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let index_input =
        super::SchemaMutationRunnerInput::new(&before, &index_after, index_plan.execution_plan())
            .expect("index same-entity input should build");
    let runner = super::SchemaMutationNoopRunner::new();

    assert!(matches!(
        runner.run(&metadata_input),
        super::SchemaMutationRunnerOutcome::NoPhysicalWork(_),
    ));

    let super::SchemaMutationRunnerOutcome::Rejected(rejection) = runner.run(&index_input) else {
        panic!("no-op runner must reject physical index work");
    };
    assert_eq!(
        rejection.kind(),
        super::SchemaMutationRunnerRejectionKind::MissingCapabilities,
    );
    assert_eq!(
        rejection.requirement(),
        Some(RebuildRequirement::IndexRebuildRequired),
    );
    assert_eq!(
        rejection.missing_capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
}

#[test]
fn runtime_epoch_identity_tracks_accepted_snapshot_changes() {
    let before = base_snapshot();
    let repeated_before = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let after = append_fields_snapshot(&before, &[added]);

    let before_epoch = super::SchemaMutationRuntimeEpoch::from_snapshot(&before)
        .expect("base snapshot should hash into runtime epoch");
    let repeated_epoch = super::SchemaMutationRuntimeEpoch::from_snapshot(&repeated_before)
        .expect("same snapshot should hash into runtime epoch");
    let after_epoch = super::SchemaMutationRuntimeEpoch::from_snapshot(&after)
        .expect("changed snapshot should hash into runtime epoch");

    assert_eq!(before_epoch, repeated_epoch);
    assert_ne!(before_epoch, after_epoch);
    assert_eq!(before_epoch.entity_path(), before.entity_path());
    assert_eq!(after_epoch.schema_version(), after.version());
    assert_ne!(
        before_epoch.snapshot_fingerprint(),
        after_epoch.snapshot_fingerprint(),
    );
}

#[test]
fn publication_identity_keeps_staged_epoch_invisible_until_published() {
    let before = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let after = append_fields_snapshot(&before, std::slice::from_ref(&added));
    let input = super::SchemaMutationRunnerInput::new(
        &before,
        &after,
        MutationPlan::append_only_fields(&[added]).execution_plan(),
    )
    .expect("same-entity metadata input should build");

    let staged = super::SchemaMutationPublicationIdentity::from_input(
        &input,
        super::SchemaMutationStoreVisibility::StagedOnly,
    )
    .expect("staged publication identity should derive from snapshots");
    let published = super::SchemaMutationPublicationIdentity::from_input(
        &input,
        super::SchemaMutationStoreVisibility::Published,
    )
    .expect("published publication identity should derive from snapshots");

    assert!(staged.changes_epoch());
    assert_eq!(
        staged.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(staged.visible_epoch(), staged.before_epoch());
    assert_eq!(staged.published_epoch(), None);
    assert_eq!(
        published.store_visibility(),
        super::SchemaMutationStoreVisibility::Published,
    );
    assert_eq!(published.visible_epoch(), published.after_epoch());
    assert_eq!(published.published_epoch(), Some(published.after_epoch()));
}

#[test]
fn field_path_rebuild_key_materializes_from_accepted_target_slots() {
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let slots = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let storage_key = crate::db::data::StorageKey::Uint(42);

    let key = IndexKey::new_from_slots_with_field_path_rebuild_target(
        EntityTag::new(7),
        storage_key,
        &target,
        &slots,
    )
    .expect("accepted field-path target should build index key")
    .expect("text key component should be indexable");

    assert_eq!(key.index_id(), &IndexId::new(EntityTag::new(7), 1));
    assert_eq!(key.component_count(), 1);
    assert_eq!(
        key.primary_storage_key()
            .expect("index key should carry primary storage key"),
        storage_key,
    );
}

#[test]
fn field_path_rebuild_stages_sorted_entries_without_publication() {
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let skipped = RebuildSlotReader {
        values: vec![None, Some(Value::Null)],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };

    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(3), &skipped),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");

    assert_eq!(staged.target().name(), "by_name");
    assert_eq!(staged.source_rows(), 3);
    assert_eq!(staged.skipped_rows(), 1);
    assert_eq!(staged.entries().len(), 2);
    assert_eq!(
        staged.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert!(
        staged
            .entries()
            .windows(2)
            .all(|pair| pair[0].key() <= pair[1].key())
    );
    let staged_members = staged
        .entries()
        .iter()
        .map(|entry| {
            entry
                .entry()
                .try_decode()
                .expect("staged entry should decode")
                .iter_ids()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        staged_members,
        vec![vec![StorageKey::Uint(1)], vec![StorageKey::Uint(2)]],
    );

    let validation = staged
        .validate()
        .expect("fresh staged rebuild output should validate");
    assert_eq!(validation.entry_count(), 2);
    assert_eq!(validation.source_rows(), 3);
    assert_eq!(validation.skipped_rows(), 1);
    assert_eq!(
        validation.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
}

#[test]
fn field_path_rebuild_validation_fails_closed_for_mutated_staged_state() {
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");

    let mut duplicate = staged.clone();
    duplicate.entries[1] = duplicate.entries[0].clone();
    assert_eq!(
        duplicate.validate(),
        Err(super::SchemaFieldPathIndexStagedValidationError::UnsortedOrDuplicateEntries),
    );
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target")
            .lower_to_plan();
    let rejection = duplicate
        .validated_runner_report(&plan.execution_plan())
        .expect_err("invalid staged state should reject runner reporting");
    assert_eq!(
        rejection.phase(),
        super::SchemaMutationRunnerPhase::ValidatePhysicalState,
    );
    assert_eq!(
        rejection.kind(),
        super::SchemaMutationRunnerRejectionKind::ValidationFailed,
    );
    assert_eq!(
        rejection.requirement(),
        Some(RebuildRequirement::IndexRebuildRequired),
    );

    let mut mismatched_count = staged.clone();
    mismatched_count.skipped_rows = 1;
    assert_eq!(
        mismatched_count.validate(),
        Err(super::SchemaFieldPathIndexStagedValidationError::EntryCountMismatch),
    );

    let mut published = staged;
    published.store_visibility = super::SchemaMutationStoreVisibility::Published;
    assert_eq!(
        published.validate(),
        Err(super::SchemaFieldPathIndexStagedValidationError::PublishedVisibility),
    );
}

#[test]
fn field_path_rebuild_validation_reports_runner_diagnostics_without_publication() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let skipped = RebuildSlotReader {
        values: vec![None, Some(Value::Null)],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(3), &skipped),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");

    let report = staged
        .validated_runner_report(&plan.execution_plan())
        .expect("valid staged rebuild output should produce runner diagnostics");

    assert_eq!(report.step_count(), 3);
    assert_eq!(
        report.required_capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        report.completed_phases(),
        &[
            super::SchemaMutationRunnerPhase::Preflight,
            super::SchemaMutationRunnerPhase::StageStores,
            super::SchemaMutationRunnerPhase::BuildPhysicalState,
            super::SchemaMutationRunnerPhase::ValidatePhysicalState,
        ],
    );
    assert_eq!(
        report.store_visibility(),
        Some(super::SchemaMutationStoreVisibility::StagedOnly),
    );
    assert_eq!(report.rows_scanned(), 3);
    assert_eq!(report.rows_skipped(), 1);
    assert_eq!(report.index_keys_written(), 2);
    assert!(report.has_completed_phase(super::SchemaMutationRunnerPhase::ValidatePhysicalState));
    assert!(!report.has_completed_phase(super::SchemaMutationRunnerPhase::InvalidateRuntimeState));
    assert!(!report.physical_work_allows_publication());
}

#[test]
fn field_path_rebuild_writes_validated_entries_to_staged_store_buffer() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");

    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");

    assert_eq!(buffer.store(), "test::mutation::by_name");
    assert_eq!(buffer.entries(), staged.entries());
    assert_eq!(buffer.validation().entry_count(), 2);
    assert_eq!(
        buffer.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(buffer.report().rows_scanned(), 2);
    assert_eq!(buffer.report().index_keys_written(), 2);
    assert!(!buffer.physical_work_allows_publication());

    let discard = buffer.discard();
    assert_eq!(discard.store(), "test::mutation::by_name");
    assert_eq!(discard.discarded_entries(), 2);
    assert_eq!(
        discard.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
}

#[test]
fn field_path_rebuild_writer_reports_staged_write_intents_without_physical_mutation() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");
    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");
    let mut writer = RecordingStagedStoreWriter::default();

    let report = buffer.write_to(&mut writer);

    assert_eq!(report.store(), "test::mutation::by_name");
    assert_eq!(report.intended_entries(), 2);
    assert_eq!(
        report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(report.runner_report().rows_scanned(), 2);
    assert_eq!(report.runner_report().index_keys_written(), 2);
    assert!(!report.runner_report().physical_work_allows_publication());
    assert_eq!(writer.writes.len(), 2);
    for ((store, key, entry), staged_entry) in writer.writes.iter().zip(buffer.entries()) {
        assert_eq!(store, "test::mutation::by_name");
        assert_eq!(key, staged_entry.key());
        assert_eq!(entry, staged_entry.entry());
    }
    assert!(!buffer.physical_work_allows_publication());
}

#[test]
fn field_path_rebuild_write_batch_snapshots_physical_rollback_without_publication() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");
    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let mut read_view = RecordingStagedStoreReadView::default();
    read_view.insert(
        buffer.store(),
        buffer.entries()[0].key().clone(),
        previous_entry.clone(),
    );

    let batch = buffer.write_batch(&read_view);

    assert_eq!(batch.store(), "test::mutation::by_name");
    assert_eq!(batch.entries(), buffer.entries());
    assert_eq!(batch.rollback_snapshots().len(), 2);
    assert_eq!(
        batch.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(batch.runner_report().index_keys_written(), 2);
    assert_eq!(batch.rollback_snapshots()[0].store(), buffer.store());
    assert_eq!(
        batch.rollback_snapshots()[0].key(),
        buffer.entries()[0].key(),
    );
    assert_eq!(
        batch.rollback_snapshots()[0].previous_entry(),
        Some(&previous_entry),
    );
    assert_eq!(batch.rollback_snapshots()[1].store(), buffer.store());
    assert_eq!(
        batch.rollback_snapshots()[1].key(),
        buffer.entries()[1].key(),
    );
    assert_eq!(batch.rollback_snapshots()[1].previous_entry(), None);

    let mut writer = RecordingStagedStoreWriter::default();
    let report = batch.write_to(&mut writer);

    assert_eq!(report.intended_entries(), 2);
    assert_eq!(
        report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert!(!report.runner_report().physical_work_allows_publication());
    assert_eq!(writer.writes.len(), 2);
}

#[test]
fn field_path_rebuild_write_batch_derives_reverse_rollback_plan() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");
    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let mut read_view = RecordingStagedStoreReadView::default();
    read_view.insert(
        buffer.store(),
        buffer.entries()[0].key().clone(),
        previous_entry.clone(),
    );
    let batch = buffer.write_batch(&read_view);

    let rollback_plan = batch.rollback_plan();

    assert_eq!(rollback_plan.store(), "test::mutation::by_name");
    assert_eq!(rollback_plan.actions().len(), 2);
    assert_eq!(
        rollback_plan.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(rollback_plan.runner_report().index_keys_written(), 2);
    assert_eq!(rollback_plan.actions()[0].store(), buffer.store());
    assert_eq!(rollback_plan.actions()[0].key(), buffer.entries()[1].key());
    assert_eq!(rollback_plan.actions()[0].restore_entry(), None);
    assert_eq!(rollback_plan.actions()[1].store(), buffer.store());
    assert_eq!(rollback_plan.actions()[1].key(), buffer.entries()[0].key());
    assert_eq!(
        rollback_plan.actions()[1].restore_entry(),
        Some(&previous_entry),
    );
    assert!(
        !rollback_plan
            .runner_report()
            .physical_work_allows_publication()
    );
}

#[test]
fn field_path_rebuild_rollback_plan_reports_mocked_restore_and_remove_actions() {
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
        panic!("field-path index request should preserve rebuild target");
    };
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
        "test::mutation::entity",
        EntityTag::new(7),
        target,
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
    )
    .expect("field-path rebuild rows should stage into raw index entries");
    let buffer =
        super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
            .expect("valid staged rebuild should write into an in-memory staged store buffer");
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let mut read_view = RecordingStagedStoreReadView::default();
    read_view.insert(
        buffer.store(),
        buffer.entries()[0].key().clone(),
        previous_entry.clone(),
    );
    let rollback_plan = buffer.write_batch(&read_view).rollback_plan();
    let mut writer = RecordingStagedStoreRollbackWriter::default();

    let report = rollback_plan.rollback_to(&mut writer);

    assert_eq!(report.store(), "test::mutation::by_name");
    assert_eq!(report.actions_applied(), 2);
    assert_eq!(report.restored_entries(), 1);
    assert_eq!(report.removed_entries(), 1);
    assert_eq!(
        report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert!(!report.runner_report().physical_work_allows_publication());
    assert_eq!(writer.actions.len(), 2);
    assert_eq!(writer.actions[0].0, buffer.store());
    assert_eq!(writer.actions[0].1, *buffer.entries()[1].key());
    assert_eq!(writer.actions[0].2, None);
    assert_eq!(writer.actions[1].0, buffer.store());
    assert_eq!(writer.actions[1].1, *buffer.entries()[0].key());
    assert_eq!(writer.actions[1].2, Some(previous_entry));
}

#[test]
fn field_path_rebuild_isolated_index_store_writer_writes_and_rolls_back() {
    let buffer = staged_name_index_store();
    let mut index_store = initialized_index_store(239);
    let mut writer =
        super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(buffer.store(), &mut index_store);

    let batch = buffer.write_batch(&writer);
    let write_report = batch.write_to(&mut writer);
    let validation = writer
        .validate_batch(&batch)
        .expect("isolated IndexStore should validate against staged batch");

    assert_eq!(writer.store(), buffer.store());
    assert_eq!(
        writer.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly
    );
    assert_eq!(writer.index_state(), IndexState::Building);
    assert_eq!(writer.len(), 2);
    assert_eq!(writer.generation(), writer.generation_before() + 2);
    assert_eq!(write_report.intended_entries(), 2);
    assert_eq!(
        write_report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert!(
        !write_report
            .runner_report()
            .physical_work_allows_publication()
    );
    assert_eq!(
        writer.get(buffer.entries()[0].key()),
        Some(buffer.entries()[0].entry().clone()),
    );
    assert_eq!(
        writer.get(buffer.entries()[1].key()),
        Some(buffer.entries()[1].entry().clone()),
    );
    assert_eq!(validation.store(), buffer.store());
    assert_eq!(validation.entry_count(), 2);
    assert_eq!(validation.index_state(), IndexState::Building);
    assert_eq!(validation.generation_before(), writer.generation_before());
    assert_eq!(validation.generation_after(), writer.generation());
    assert_eq!(
        validation.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    let publication_readiness = validation.publication_readiness();
    assert_eq!(
        publication_readiness.blockers(),
        &[
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::StoreStillStaged,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::RuntimeStateNotInvalidated,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::SnapshotNotPublished,
        ],
    );
    assert!(!publication_readiness.allows_publication());

    let rollback_report = batch.rollback_plan().rollback_to(&mut writer);

    assert_eq!(rollback_report.actions_applied(), 2);
    assert_eq!(rollback_report.removed_entries(), 2);
    assert_eq!(rollback_report.restored_entries(), 0);
    assert_eq!(writer.len(), 0);
    assert_eq!(writer.generation(), writer.generation_before() + 4);
    assert_eq!(writer.index_state(), IndexState::Building);
    assert!(
        !rollback_report
            .runner_report()
            .physical_work_allows_publication()
    );
}

#[test]
fn field_path_rebuild_isolated_index_store_validation_fails_closed() {
    let buffer = staged_name_index_store();
    let extra_entry = extra_staged_name_index_entry();
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let batch = buffer.write_batch(&super::SchemaFieldPathIndexStagedStoreOverlay::new(
        buffer.store(),
    ));

    let mut wrong_store = initialized_index_store(238);
    let writer =
        super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new("wrong::store", &mut wrong_store);
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::StoreMismatch),
    );

    let mut published_store = initialized_index_store(237);
    let mut writer = super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(
        buffer.store(),
        &mut published_store,
    );
    writer.store_visibility = super::SchemaMutationStoreVisibility::Published;
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::PublishedVisibility),
    );

    let mut ready_store = initialized_index_store(236);
    let writer =
        super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(buffer.store(), &mut ready_store);
    writer.index_store.mark_ready();
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::StoreNotBuilding),
    );

    let mut partial_store = initialized_index_store(235);
    let writer = super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(
        buffer.store(),
        &mut partial_store,
    );
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryCountMismatch),
    );

    let mut missing_store = initialized_index_store(234);
    let writer = super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(
        buffer.store(),
        &mut missing_store,
    );
    writer.index_store.insert(
        buffer.entries()[0].key().clone(),
        buffer.entries()[0].entry().clone(),
    );
    writer
        .index_store
        .insert(extra_entry.key().clone(), extra_entry.entry().clone());
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::MissingEntry),
    );

    let mut mismatch_store = initialized_index_store(233);
    let mut writer = super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(
        buffer.store(),
        &mut mismatch_store,
    );
    let _ = batch.write_to(&mut writer);
    writer
        .index_store
        .insert(buffer.entries()[0].key().clone(), previous_entry);
    assert_eq!(
        writer.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexIsolatedIndexStoreValidationError::EntryMismatch),
    );
}

#[test]
fn field_path_rebuild_runtime_invalidation_records_epoch_handoff_without_publication() {
    let buffer = staged_name_index_store();
    let mut index_store = initialized_index_store(232);
    let mut writer =
        super::SchemaFieldPathIndexIsolatedIndexStoreWriter::new(buffer.store(), &mut index_store);
    let batch = buffer.write_batch(&writer);
    let _ = batch.write_to(&mut writer);
    let validation = writer
        .validate_batch(&batch)
        .expect("isolated IndexStore should validate before invalidation");
    let before = base_snapshot();
    let after = snapshot_with_indexes(&before, vec![non_unique_name_index()]);
    let plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, plan.execution_plan())
        .expect("same-entity accepted snapshots should build runner input");

    let invalidation_plan =
        super::SchemaFieldPathIndexRuntimeInvalidationPlan::from_isolated_index_store_validation(
            &validation,
            &input,
        )
        .expect("validated staged store should bind runtime invalidation epochs");
    let mut sink = RecordingRuntimeInvalidationSink::default();
    let report = invalidation_plan.invalidate_runtime_state(&mut sink);

    assert_eq!(invalidation_plan.store(), buffer.store());
    assert_eq!(invalidation_plan.entry_count(), 2);
    assert!(invalidation_plan.requires_invalidation());
    assert_eq!(
        invalidation_plan.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(sink.invalidations.len(), 1);
    assert_eq!(sink.invalidations[0].0, buffer.store());
    assert_eq!(
        &sink.invalidations[0].1,
        invalidation_plan.publication_identity().before_epoch(),
    );
    assert_eq!(
        &sink.invalidations[0].2,
        invalidation_plan.publication_identity().after_epoch(),
    );
    assert_eq!(report.store(), buffer.store());
    assert_eq!(report.entry_count(), 2);
    assert_eq!(report.invalidated_epochs(), 1);
    assert_eq!(
        report.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(
        report.publication_identity().visible_epoch(),
        invalidation_plan.publication_identity().before_epoch(),
    );
    assert!(
        report
            .runner_report()
            .has_completed_phase(super::SchemaMutationRunnerPhase::InvalidateRuntimeState),
    );
    assert!(
        !report
            .runner_report()
            .has_completed_phase(super::SchemaMutationRunnerPhase::PublishSnapshot),
    );
    assert!(!report.runner_report().physical_work_allows_publication());
    let readiness = report.publication_readiness();
    assert_eq!(
        readiness.blockers(),
        &[
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::StoreStillStaged,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::SnapshotNotPublished,
        ],
    );
    assert!(!readiness.allows_publication());
}

#[test]
fn field_path_rebuild_snapshot_publication_handoff_reports_publishable_runner_state() {
    let validation = validated_isolated_name_index_store(231);
    let (before, after, execution_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, execution_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let invalidation_plan =
        super::SchemaFieldPathIndexRuntimeInvalidationPlan::from_isolated_index_store_validation(
            &validation,
            &input,
        )
        .expect("validated staged store should bind runtime invalidation epochs");
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let invalidation_report = invalidation_plan.invalidate_runtime_state(&mut invalidation_sink);

    let publication_plan =
        super::SchemaFieldPathIndexSnapshotPublicationPlan::from_runtime_invalidation_report(
            &invalidation_report,
            &input,
        )
        .expect("runtime invalidation should allow snapshot publication planning");
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();
    let publication_report = publication_plan.publish_snapshot(&mut publication_sink);

    assert_eq!(publication_plan.store(), validation.store());
    assert_eq!(publication_plan.entry_count(), validation.entry_count());
    assert_eq!(publication_plan.accepted_after(), &after);
    assert_eq!(publication_sink.publications.len(), 1);
    assert_eq!(publication_sink.publications[0].0, validation.store());
    assert_eq!(publication_sink.publications[0].1, after);
    assert_eq!(
        &publication_sink.publications[0].2,
        publication_plan.publication_identity().before_epoch(),
    );
    assert_eq!(
        &publication_sink.publications[0].3,
        publication_plan.publication_identity().after_epoch(),
    );
    assert_eq!(
        publication_report.store_visibility(),
        super::SchemaMutationStoreVisibility::Published,
    );
    assert_eq!(
        publication_report.publication_identity().visible_epoch(),
        publication_plan.publication_identity().after_epoch(),
    );
    assert_eq!(
        publication_report.publication_identity().published_epoch(),
        Some(publication_plan.publication_identity().after_epoch()),
    );
    assert_eq!(
        publication_report.accepted_after(),
        publication_plan.accepted_after()
    );
    assert!(
        publication_report
            .runner_report()
            .has_completed_phase(super::SchemaMutationRunnerPhase::PublishSnapshot),
    );
    assert!(
        publication_report
            .runner_report()
            .physical_work_allows_publication()
    );
    let readiness = publication_report.publication_readiness();
    assert!(readiness.blockers().is_empty());
    assert!(readiness.allows_publication());
}

#[test]
fn field_path_runner_orchestrates_staging_to_publication_handoff() {
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let (before, after, execution_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, execution_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(234);
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();

    let report = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        [
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
        ],
        &mut index_store,
        &mut invalidation_sink,
        &mut publication_sink,
    )
    .expect("accepted field-path execution plan should complete the runner handoff");

    assert_eq!(report.store(), "test::mutation::by_name");
    assert_eq!(report.write_report().store(), report.store());
    assert_eq!(report.write_report().intended_entries(), 2);
    assert_eq!(report.validation().store(), report.store());
    assert_eq!(report.validation().entry_count(), 2);
    assert_eq!(report.validation().index_state(), IndexState::Building);
    assert_eq!(
        report.validation().store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(report.invalidation_report().invalidated_epochs(), 1);
    assert_eq!(
        report.publication_report().store_visibility(),
        super::SchemaMutationStoreVisibility::Published,
    );
    assert_eq!(report.published_store_report().store(), report.store());
    assert_eq!(report.published_store_report().entry_count(), 2);
    assert_eq!(
        report.published_store_report().index_state(),
        IndexState::Ready,
    );
    assert_eq!(
        report.published_store_report().store_visibility(),
        super::SchemaMutationStoreVisibility::Published,
    );
    assert_eq!(index_store.len(), 2);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert_eq!(invalidation_sink.invalidations.len(), 1);
    assert_eq!(invalidation_sink.invalidations[0].0, report.store());
    assert_eq!(publication_sink.publications.len(), 1);
    assert_eq!(publication_sink.publications[0].0, report.store());
    assert_eq!(publication_sink.publications[0].1, after);
    assert_eq!(report.runner_report().rows_scanned(), 2);
    assert_eq!(report.runner_report().rows_skipped(), 0);
    assert_eq!(report.runner_report().index_keys_written(), 2);
    assert!(
        report
            .runner_report()
            .has_completed_phase(super::SchemaMutationRunnerPhase::PublishSnapshot),
    );
    assert!(report.runner_report().physical_work_allows_publication());
    assert!(report.publication_readiness().allows_publication());
}

#[test]
fn field_path_runner_rejects_target_mismatch_before_physical_work() {
    let mismatched_index = PersistedIndexSnapshot::new(
        9,
        "by_alias".to_string(),
        "test::mutation::by_alias".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        Some("name IS NOT NULL".to_string()),
    );
    let request =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&mismatched_index)
            .expect("mismatched field-path index should lower to a rebuild target");
    let SchemaMutationRequest::AddNonUniqueFieldPathIndex {
        target: mismatched_target,
    } = request
    else {
        panic!("field-path request should carry a rebuild target");
    };
    let (before, after, execution_plan) = field_path_index_runner_context();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, execution_plan)
        .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(236);
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();

    let result = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        mismatched_target,
        std::iter::empty(),
        &mut index_store,
        &mut invalidation_sink,
        &mut publication_sink,
    );

    assert_eq!(
        result,
        Err(super::SchemaFieldPathIndexRunnerError::TargetMismatch),
    );
    assert_eq!(index_store.len(), 0);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert!(invalidation_sink.invalidations.is_empty());
    assert!(publication_sink.publications.is_empty());
}

#[test]
fn field_path_runner_rejects_non_field_path_execution_plan_before_physical_work() {
    let before = base_snapshot();
    let after = snapshot_with_indexes(&before, vec![expression_name_index()]);
    let expression_plan =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("accepted expression index should lower")
            .lower_to_plan();
    let input =
        super::SchemaMutationRunnerInput::new(&before, &after, expression_plan.execution_plan())
            .expect("same-entity accepted snapshots should build runner input");
    let mut index_store = initialized_index_store(235);
    let mut invalidation_sink = RecordingRuntimeInvalidationSink::default();
    let mut publication_sink = RecordingAcceptedSnapshotPublicationSink::default();

    let result = super::SchemaFieldPathIndexRunner::run(
        &input,
        EntityTag::new(7),
        accepted_name_field_path_target(),
        std::iter::empty(),
        &mut index_store,
        &mut invalidation_sink,
        &mut publication_sink,
    );

    assert_eq!(
        result,
        Err(super::SchemaFieldPathIndexRunnerError::UnsupportedExecutionPlan),
    );
    assert_eq!(index_store.len(), 0);
    assert_eq!(index_store.state(), IndexState::Ready);
    assert!(invalidation_sink.invalidations.is_empty());
    assert!(publication_sink.publications.is_empty());
}

#[test]
fn field_path_rebuild_staged_overlay_writes_and_rolls_back_without_index_store() {
    let buffer = staged_name_index_store();
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let mut overlay = super::SchemaFieldPathIndexStagedStoreOverlay::from_entries(
        buffer.store(),
        [(buffer.entries()[0].key().clone(), previous_entry.clone())],
    );

    let batch = buffer.write_batch(&overlay);
    let write_report = batch.write_to(&mut overlay);
    let overlay_validation = overlay
        .validate_batch(&batch)
        .expect("overlay should validate against the staged write batch");

    assert_eq!(overlay.store(), buffer.store());
    assert_eq!(overlay.len(), 2);
    assert_eq!(
        overlay.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(write_report.intended_entries(), 2);
    assert_eq!(
        overlay.get(buffer.entries()[0].key()),
        Some(buffer.entries()[0].entry()),
    );
    assert_eq!(
        overlay.get(buffer.entries()[1].key()),
        Some(buffer.entries()[1].entry()),
    );
    assert_eq!(overlay_validation.store(), buffer.store());
    assert_eq!(overlay_validation.entry_count(), 2);
    assert_eq!(
        overlay_validation.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(overlay_validation.runner_report().index_keys_written(), 2);
    assert!(
        !overlay_validation
            .runner_report()
            .physical_work_allows_publication()
    );
    let publication_readiness = overlay_validation.publication_readiness();
    assert_eq!(publication_readiness.store(), buffer.store());
    assert_eq!(publication_readiness.entry_count(), 2);
    assert_eq!(
        publication_readiness.blockers(),
        &[
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::StoreStillStaged,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::RuntimeStateNotInvalidated,
            super::SchemaFieldPathIndexStagedStorePublicationBlocker::SnapshotNotPublished,
        ],
    );
    assert!(!publication_readiness.allows_publication());
    assert!(
        !publication_readiness
            .runner_report()
            .physical_work_allows_publication()
    );

    let rollback_report = batch.rollback_plan().rollback_to(&mut overlay);

    assert_eq!(rollback_report.actions_applied(), 2);
    assert_eq!(rollback_report.restored_entries(), 1);
    assert_eq!(rollback_report.removed_entries(), 1);
    assert_eq!(overlay.len(), 1);
    assert_eq!(
        overlay.get(buffer.entries()[0].key()),
        Some(&previous_entry)
    );
    assert_eq!(overlay.get(buffer.entries()[1].key()), None);
    assert!(
        !rollback_report
            .runner_report()
            .physical_work_allows_publication()
    );
}

#[test]
fn field_path_rebuild_staged_overlay_validation_fails_closed() {
    let buffer = staged_name_index_store();
    let extra_entry = extra_staged_name_index_entry();
    let previous_entry =
        RawIndexEntry::try_from_keys([StorageKey::Uint(99)]).expect("previous entry should encode");
    let batch = buffer.write_batch(&super::SchemaFieldPathIndexStagedStoreOverlay::new(
        buffer.store(),
    ));

    let wrong_store = super::SchemaFieldPathIndexStagedStoreOverlay::new("wrong::store");
    assert_eq!(
        wrong_store.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::StoreMismatch),
    );

    let mut published_overlay = super::SchemaFieldPathIndexStagedStoreOverlay::new(buffer.store());
    published_overlay.store_visibility = super::SchemaMutationStoreVisibility::Published;
    assert_eq!(
        published_overlay.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::PublishedVisibility),
    );

    let partial_overlay = super::SchemaFieldPathIndexStagedStoreOverlay::from_entries(
        buffer.store(),
        [(
            buffer.entries()[0].key().clone(),
            buffer.entries()[0].entry().clone(),
        )],
    );
    assert_eq!(
        partial_overlay.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::EntryCountMismatch),
    );

    let missing_entry_overlay = super::SchemaFieldPathIndexStagedStoreOverlay::from_entries(
        buffer.store(),
        [
            (
                buffer.entries()[0].key().clone(),
                buffer.entries()[0].entry().clone(),
            ),
            (extra_entry.key().clone(), extra_entry.entry().clone()),
        ],
    );
    assert_eq!(
        missing_entry_overlay.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::MissingEntry),
    );

    let mismatched_entry_overlay = super::SchemaFieldPathIndexStagedStoreOverlay::from_entries(
        buffer.store(),
        [
            (buffer.entries()[0].key().clone(), previous_entry),
            (
                buffer.entries()[1].key().clone(),
                buffer.entries()[1].entry().clone(),
            ),
        ],
    );
    assert_eq!(
        mismatched_entry_overlay.validate_batch(&batch),
        Err(super::SchemaFieldPathIndexStagedStoreOverlayValidationError::EntryMismatch),
    );
}

#[test]
fn execution_plan_keeps_full_rewrite_and_unsupported_non_executable() {
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();
    let rewrite_execution = incompatible.execution_plan();

    assert_eq!(
        rewrite_execution.readiness(),
        super::SchemaMutationExecutionReadiness::Unsupported(
            RebuildRequirement::FullDataRewriteRequired,
        ),
    );
    assert_eq!(
        rewrite_execution.execution_gate(),
        super::SchemaMutationExecutionGate::Rejected {
            requirement: RebuildRequirement::FullDataRewriteRequired,
        },
    );
    assert_eq!(
        rewrite_execution.steps(),
        &[super::SchemaMutationExecutionStep::RewriteAllRows],
    );
    assert_eq!(
        rewrite_execution.runner_capabilities(),
        vec![super::SchemaMutationRunnerCapability::RewriteAllRows],
    );
    assert_eq!(
        rewrite_execution
            .admit_runner_capabilities(&[super::SchemaMutationRunnerCapability::RewriteAllRows,]),
        super::SchemaMutationExecutionAdmission::Rejected {
            requirement: RebuildRequirement::FullDataRewriteRequired,
        },
    );

    let nullability = SchemaMutationRequest::AlterNullability {
        field_id: FieldId::new(2),
    }
    .lower_to_plan();
    let unsupported_execution = nullability.execution_plan();

    assert_eq!(
        unsupported_execution.readiness(),
        super::SchemaMutationExecutionReadiness::Unsupported(RebuildRequirement::Unsupported),
    );
    assert_eq!(
        unsupported_execution.execution_gate(),
        super::SchemaMutationExecutionGate::Rejected {
            requirement: RebuildRequirement::Unsupported,
        },
    );
    assert_eq!(
        unsupported_execution.steps(),
        &[super::SchemaMutationExecutionStep::Unsupported {
            reason: "alter nullability requires data proof or rewrite",
        }],
    );
    assert!(unsupported_execution.runner_capabilities().is_empty());
}

#[test]
fn field_path_index_request_lowering_fails_closed_for_unsupported_indexes() {
    let unique = PersistedIndexSnapshot::new(
        1,
        "unique_name".to_string(),
        "test::mutation::unique_name".to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    );
    let explicit_items = PersistedIndexSnapshot::new(
        2,
        "items_name".to_string(),
        "test::mutation::items_name".to_string(),
        false,
        PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::FieldPath(
            name_key_path(),
        )]),
        None,
    );
    let empty = PersistedIndexSnapshot::new(
        3,
        "empty_name".to_string(),
        "test::mutation::empty_name".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(Vec::new()),
        None,
    );

    assert_eq!(
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&unique),
        Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&explicit_items),
        Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&empty),
        Err(AcceptedSchemaMutationError::EmptyIndexKey),
    );
}

#[test]
fn expression_index_request_lowering_fails_closed_for_unsupported_indexes() {
    let unique = PersistedIndexSnapshot::new(
        1,
        "unique_lower_name".to_string(),
        "test::mutation::unique_lower_name".to_string(),
        true,
        expression_name_index().key().clone(),
        None,
    );
    let field_path_only = non_unique_name_index();
    let items_without_expression = PersistedIndexSnapshot::new(
        2,
        "items_name".to_string(),
        "test::mutation::items_name".to_string(),
        false,
        PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::FieldPath(
            name_key_path(),
        )]),
        None,
    );
    let empty = PersistedIndexSnapshot::new(
        3,
        "empty_expression".to_string(),
        "test::mutation::empty_expression".to_string(),
        false,
        PersistedIndexKeySnapshot::Items(Vec::new()),
        None,
    );

    assert_eq!(
        SchemaMutationRequest::from_accepted_expression_index(&unique),
        Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_expression_index(&field_path_only),
        Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_expression_index(&items_without_expression),
        Err(AcceptedSchemaMutationError::ExpressionIndexRequiresExpressionKey),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_expression_index(&empty),
        Err(AcceptedSchemaMutationError::EmptyIndexKey),
    );
}

#[test]
fn secondary_index_drop_request_lowering_fails_closed_for_unique_indexes() {
    let unique = PersistedIndexSnapshot::new(
        1,
        "unique_name".to_string(),
        "test::mutation::unique_name".to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    );

    assert_eq!(
        SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(&unique),
        Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
    );
}

#[test]
fn rebuild_plan_keeps_unsupported_and_full_rewrite_shapes_explicit() {
    let nullability = SchemaMutationRequest::AlterNullability {
        field_id: FieldId::new(2),
    }
    .lower_to_plan();
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

    assert_eq!(
        nullability.rebuild_plan().actions(),
        &[SchemaRebuildAction::Unsupported {
            reason: "alter nullability requires data proof or rewrite",
        }],
    );
    assert_eq!(
        incompatible.rebuild_plan().actions(),
        &[SchemaRebuildAction::RewriteAllRows],
    );
}

#[test]
fn unsupported_mutation_plans_fail_closed() {
    let alteration = SchemaMutationRequest::AlterNullability {
        field_id: FieldId::new(2),
    }
    .lower_to_plan();
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

    assert_eq!(
        alteration.compatibility(),
        MutationCompatibility::UnsupportedPreOne
    );
    assert_eq!(
        alteration.rebuild_requirement(),
        RebuildRequirement::Unsupported
    );
    assert_eq!(
        alteration.publication_status(),
        MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
            MutationCompatibility::UnsupportedPreOne,
        )),
    );
    assert_eq!(
        incompatible.compatibility(),
        MutationCompatibility::Incompatible
    );
    assert_eq!(
        incompatible.rebuild_requirement(),
        RebuildRequirement::FullDataRewriteRequired
    );
}

#[test]
fn publication_gate_allows_only_metadata_safe_no_rebuild_plans() {
    let field = nullable_text_field("nickname", 3, 2);
    let append_only = MutationPlan::append_only_fields(&[field]);
    let metadata_safe_but_rebuild_required = MutationPlan {
        mutations: Vec::new(),
        compatibility: MutationCompatibility::MetadataOnlySafe,
        rebuild: RebuildRequirement::IndexRebuildRequired,
    };
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

    assert_eq!(
        append_only.publication_status(),
        MutationPublicationStatus::Publishable
    );
    assert_eq!(
        metadata_safe_but_rebuild_required.publication_status(),
        MutationPublicationStatus::Blocked(MutationPublicationBlocker::RebuildRequired(
            RebuildRequirement::IndexRebuildRequired,
        )),
    );
    assert_eq!(
        incompatible.publication_status(),
        MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
            MutationCompatibility::Incompatible,
        )),
    );
}

#[test]
fn publication_preflight_requires_runner_readiness_before_physical_work() {
    let field = nullable_text_field("nickname", 3, 2);
    let append_only = MutationPlan::append_only_fields(&[field]);
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let no_runner = super::SchemaMutationRunnerContract::new(&[]);
    let index_runner = super::SchemaMutationRunnerContract::new(&[
        super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
        super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
        super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
    ]);
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

    assert_eq!(
        append_only.publication_preflight(&no_runner),
        super::MutationPublicationPreflight::PublishableNow,
    );
    assert_eq!(
        field_path.publication_preflight(&no_runner),
        super::MutationPublicationPreflight::MissingRunnerCapabilities {
            missing: vec![
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
    assert_eq!(
        field_path.publication_preflight(&index_runner),
        super::MutationPublicationPreflight::PhysicalWorkReady {
            step_count: 3,
            required: vec![
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
    assert_eq!(
        incompatible.publication_preflight(&index_runner),
        super::MutationPublicationPreflight::Rejected {
            requirement: RebuildRequirement::FullDataRewriteRequired,
        },
    );
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

#[test]
fn snapshot_delta_classifier_names_append_only_fields() {
    let stored = base_snapshot();
    let mut fields = stored.fields().to_vec();
    fields.push(nullable_text_field("nickname", 3, 2));
    let generated = PersistedSchemaSnapshot::new(
        stored.version(),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.primary_key_field_id(),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ],
        ),
        fields,
    );

    let SchemaMutationDelta::AppendOnlyFields(added_fields) =
        classify_schema_mutation_delta(&stored, &generated)
    else {
        panic!("append-only snapshot change should classify as appended fields");
    };

    assert_eq!(added_fields.len(), 1);
    assert_eq!(added_fields[0].name(), "nickname");
}

#[test]
fn snapshot_delta_request_lowers_append_only_fields_to_mutation_plan() {
    let stored = base_snapshot();
    let mut fields = stored.fields().to_vec();
    fields.push(nullable_text_field("nickname", 3, 2));
    let generated = PersistedSchemaSnapshot::new(
        stored.version(),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.primary_key_field_id(),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ],
        ),
        fields,
    );

    let SchemaMutationRequest::AppendOnlyFields(added_fields) =
        schema_mutation_request_for_snapshots(&stored, &generated)
    else {
        panic!("append-only snapshot change should lower into append-only request");
    };

    let plan = SchemaMutationRequest::AppendOnlyFields(added_fields).lower_to_plan();
    assert_eq!(plan.added_field_count(), 1);
    assert_eq!(
        plan.publication_status(),
        MutationPublicationStatus::Publishable
    );
}

#[test]
fn snapshot_delta_classifier_rejects_non_prefix_field_changes() {
    let stored = base_snapshot();
    let mut generated_fields = stored.fields().to_vec();
    generated_fields[1] = nullable_text_field("renamed", 2, 1);
    let generated = PersistedSchemaSnapshot::new(
        stored.version(),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.primary_key_field_id(),
        stored.row_layout().clone(),
        generated_fields,
    );

    assert_eq!(
        classify_schema_mutation_delta(&stored, &generated),
        SchemaMutationDelta::Incompatible
    );
}
