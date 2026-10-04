//! Corrupt source rows must be classified once, then leave both derived domains.

use crate::{
    db::{
        DbSession, DynamicStructuralPatch, DynamicWriteCell, RequestExecutionRoot,
        commit::publish_accepted_schema_candidate,
        data::{DataStore, DecodedDataStoreKey, RawRow},
        index::IndexStore,
        integrity::{
            DeepIntegrityPageStatus, DerivedInspectionLimits, IntegrityCheckRequest,
            IntegrityCheckResult, IntegrityEntityIdentity, IntegrityFinding, IntegrityFindingKind,
            IntegrityJobOwner, IntegrityJobReceipt, IntegritySubmissionKey,
            IntegrityTerminalOutcome, IntegrityVerifierFamily, PhysicalUnitCheckpoint,
            execute_index_integrity_page, execute_reverse_integrity_page,
        },
        journal::JournalTailStore,
        registry::{
            StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
            StoreRuntimeStorageCapabilities,
        },
        schema::{
            AcceptedConstraintCatalog, AcceptedFieldKind, AcceptedInspectionPlan,
            AcceptedSchemaRevision, CandidateSchemaRevision, FieldId, FieldStorageDecode,
            PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot,
            PersistedIndexSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot,
            RelationId, SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault, SchemaRowLayout,
            SchemaStore, SchemaVersion, accepted_schema_candidate_with_field_bindings_for_tests,
        },
    },
    testing::test_memory,
    traits::{CanisterKind, Path},
    types::EntityTag,
    value::InputValue,
};
use ic_memory::ic_stable_structures::Storable;
use std::{borrow::Cow, cell::RefCell, collections::BTreeMap};

const STORE: &str = "tests::DerivedIntegrityStore";
const ENTITY: &str = "tests::DerivedIntegrityRow";
const TAG: EntityTag = EntityTag::new(41);

struct TestCanister;

impl CanisterKind for TestCanister {
    const COMMIT_STABLE_KEY: &'static str = "icydb.derived_integrity.commit.v1";
    const STARTUP_STABLE_KEY: &'static str = "icydb.derived_integrity.startup.v1";
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.derived_integrity.progress.v1";

    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(140)
    }
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(141)
    }
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(142)
    }
}

impl Path for TestCanister {
    const PATH: &'static str = "tests::DerivedIntegrityCanister";
}

thread_local! {
    static DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(test_memory(143)));
    static INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(test_memory(144)));
    static SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(test_memory(145)));
    static JOURNAL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(test_memory(146)));
    static REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(STORE, &DATA, &INDEX, &SCHEMA, &JOURNAL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(143, "icydb.derived_integrity.data.v1"),
                StoreAllocationIdentity::new(144, "icydb.derived_integrity.index.v1"),
                StoreAllocationIdentity::new(145, "icydb.derived_integrity.schema.v1"),
                StoreAllocationIdentity::new(146, "icydb.derived_integrity.journal.v1")),
            StoreRuntimeStorageCapabilities::journaled()).unwrap();
        registry
    };
}

fn candidate() -> CandidateSchemaRevision {
    let kind = AcceptedFieldKind::Nat64;
    let field = PersistedFieldSnapshot::new_initial(
        FieldId::new(1),
        "id".into(),
        SchemaFieldSlot::new(0),
        kind.clone(),
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        kind.leaf_codec_for_storage(FieldStorageDecode::ByKind),
    );
    let index = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).unwrap(),
        1,
        "by_id".into(),
        STORE.into(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(1),
            SchemaFieldSlot::new(0),
            vec!["id".into()],
            kind,
            false,
        )]),
        None,
    );
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        ENTITY.into(),
        "DerivedIntegrityRow".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![field],
        vec![index],
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new_direct(
        RelationId::new(1).unwrap(),
        "self".into(),
        ENTITY.into(),
        vec![FieldId::new(1)],
    )]);
    let constraints = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .unwrap();
    let snapshot = snapshot.with_constraint_catalog(constraints);
    accepted_schema_candidate_with_field_bindings_for_tests(
        STORE,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(TAG, snapshot)]),
        BTreeMap::from([(
            (
                TAG,
                icydb_schema::FieldSourceKey::try_new("tests::DerivedIntegrityRow.id").unwrap(),
            ),
            FieldId::new(1),
        )]),
    )
}

fn fixture(corrupt_id: u64, row_len: usize) -> (DbSession<TestCanister>, AcceptedInspectionPlan) {
    let session =
        DbSession::<TestCanister>::new(&REGISTRY, &RequestExecutionRoot::__new_runtime_root());
    session
        .db
        .drive_startup_recovery_page_with_failure_authority()
        .unwrap_or_else(|failure| {
            panic!(
                "recovery {:?}: {:?}",
                failure.authority(),
                failure.error().diagnostic()
            )
        });
    let candidate = candidate();
    let store = session.db.store_handle(STORE).unwrap();
    publish_accepted_schema_candidate(STORE, store, AcceptedSchemaRevision::NONE, &candidate)
        .unwrap();
    session
        .execute_trusted_dynamic_insert_batch(
            "DerivedIntegrityRow",
            (1..=2)
                .map(|id| {
                    DynamicStructuralPatch::new(vec![(
                        "id".into(),
                        DynamicWriteCell::Value(InputValue::nat64(id)),
                    )])
                })
                .collect(),
        )
        .unwrap();
    assert!((0..8).any(|_| session.db.drive_startup_recovery_page().unwrap()));
    // Preserve the accepted witnesses while corrupting only their source bytes.
    if row_len != 0 {
        let key = DecodedDataStoreKey::new(
            TAG,
            &crate::db::key_taxonomy::PrimaryKeyComponent::Nat64(corrupt_id).into(),
        )
        .to_raw()
        .unwrap();
        store.with_data_mut(|data| {
            data.insert_raw_for_test(key, RawRow::from_bytes(Cow::Owned(vec![0; row_len])))
        });
    }
    let selection = store
        .with_schema(|schema| schema.current_accepted_catalog_selection(TAG, ENTITY, STORE))
        .unwrap()
        .unwrap();
    let plan = AcceptedInspectionPlan::compile(
        &session.db,
        selection.identity(),
        selection.snapshot(),
        selection.value_catalog_handle().clone(),
    )
    .unwrap();
    (session, plan)
}

fn page(
    session: &DbSession<TestCanister>,
    plan: &AcceptedInspectionPlan,
    reverse: bool,
    checkpoint: PhysicalUnitCheckpoint,
    limits: DerivedInspectionLimits,
) -> (PhysicalUnitCheckpoint, bool, Vec<IntegrityFinding>) {
    let result = if reverse {
        execute_reverse_integrity_page(&session.db, plan, 0, checkpoint, limits).unwrap()
    } else {
        execute_index_integrity_page(&session.db, plan, 0, checkpoint, limits).unwrap()
    };
    (
        result.checkpoint().clone(),
        result.exhausted(),
        result.findings().to_vec(),
    )
}

fn assert_source_progress(reverse: bool) {
    let (session, plan) = fixture(1, crate::db::codec::MAX_ROW_BYTES as usize * 2);
    let first = page(
        &session,
        &plan,
        reverse,
        PhysicalUnitCheckpoint::BeforeFirst,
        DerivedInspectionLimits::standard(),
    );
    assert!(matches!(&first.0, PhysicalUnitCheckpoint::After { .. }));
    assert!(!first.1);
    assert_eq!(first.2.len(), 1);
    assert_eq!(
        first.2[0].kind(),
        if reverse {
            IntegrityFindingKind::DivergentReverseRelationEntry
        } else {
            IntegrityFindingKind::DivergentIndexEntry
        }
    );
    let next = page(
        &session,
        &plan,
        reverse,
        first.0.clone(),
        DerivedInspectionLimits::standard(),
    );
    assert!(next.1);
    assert_ne!(&next.0, &first.0);
    assert!(next.2.is_empty());
}

#[test]
fn oversized_source_advances_index_domain_and_keeps_clean_neighbors() {
    assert_source_progress(false);
}

#[test]
fn oversized_source_advances_reverse_domain_and_keeps_clean_neighbors() {
    assert_source_progress(true);
}

#[test]
fn oversized_later_entry_yields_then_progresses_on_its_own_page() {
    let (session, plan) = fixture(2, crate::db::codec::MAX_ROW_BYTES as usize * 2);
    for reverse in [false, true] {
        let first = page(
            &session,
            &plan,
            reverse,
            PhysicalUnitCheckpoint::BeforeFirst,
            DerivedInspectionLimits::standard(),
        );
        assert!(!first.1);
        assert!(first.2.is_empty());
        let next = page(
            &session,
            &plan,
            reverse,
            first.0.clone(),
            DerivedInspectionLimits::standard(),
        );
        assert!(next.1);
        assert_ne!(&next.0, &first.0);
        assert_eq!(next.2.len(), 1);
    }
}

fn inspect_to_completion(
    session: &DbSession<TestCanister>,
    plan: &AcceptedInspectionPlan,
) -> (IntegrityTerminalOutcome, Vec<IntegrityFindingKind>) {
    let owner = IntegrityJobOwner::new("derived-integrity-owner").unwrap();
    let start = IntegrityCheckRequest::DeepStart {
        entity: IntegrityEntityIdentity::from_accepted_identity(plan.identity_ref()),
        submission_key: IntegritySubmissionKey::new("source-inspection").unwrap(),
    };
    let IntegrityCheckResult::Deep(mut receipt) = session
        .execute_admin_integrity(start, owner.clone())
        .unwrap()
    else {
        panic!("expected Deep receipt")
    };
    let mut kinds = Vec::new();
    for _ in 0..20 {
        let request =
            IntegrityCheckRequest::deep_continue(receipt.job_id(), receipt.page_sequence());
        let next = session
            .execute_admin_integrity(request.clone(), owner.clone())
            .unwrap();
        assert_eq!(
            session
                .execute_admin_integrity(request, owner.clone())
                .unwrap(),
            next
        );
        let IntegrityCheckResult::Deep(IntegrityJobReceipt::Page(result)) = next else {
            panic!("expected page")
        };
        kinds.extend(result.findings().iter().map(IntegrityFinding::kind));
        if let DeepIntegrityPageStatus::Terminal(outcome) = result.status() {
            return (outcome.clone(), kinds);
        }
        receipt = IntegrityJobReceipt::Page(result);
    }
    panic!("Deep must complete instead of refreshing an unchanged checkpoint");
}

#[test]
fn public_deep_inspection_of_oversized_source_completes_with_replayable_findings() {
    let (session, plan) = fixture(1, crate::db::codec::MAX_ROW_BYTES as usize * 2);
    let (outcome, kinds) = inspect_to_completion(&session, &plan);
    assert_eq!(outcome, IntegrityTerminalOutcome::DeepCompleteWithFindings);
    for kind in [
        IntegrityFindingKind::OversizedRow,
        IntegrityFindingKind::DivergentIndexEntry,
        IntegrityFindingKind::DivergentReverseRelationEntry,
    ] {
        assert!(kinds.contains(&kind), "{kinds:?}");
    }
}

#[test]
fn public_deep_inspection_of_clean_sources_completes_clean_with_replayable_receipts() {
    let (session, plan) = fixture(0, 0);
    let (outcome, kinds) = inspect_to_completion(&session, &plan);
    assert_eq!(outcome, IntegrityTerminalOutcome::DeepCompleteClean);
    assert!(kinds.is_empty());
}

#[test]
fn unique_atom_resume_after_oversized_source_preserves_the_next_entry() {
    let (session, plan) = fixture(1, crate::db::codec::MAX_ROW_BYTES as usize * 2);
    let first = page(
        &session,
        &plan,
        false,
        PhysicalUnitCheckpoint::BeforeFirst,
        DerivedInspectionLimits::standard(),
    );
    let PhysicalUnitCheckpoint::After { physical_key } = &first.0 else {
        panic!("expected completed corrupt entry")
    };
    let next = page(
        &session,
        &plan,
        false,
        PhysicalUnitCheckpoint::Within {
            physical_key: physical_key.clone(),
            verifier_family: IntegrityVerifierFamily::IndexEntry,
            ordinal: 0,
        },
        DerivedInspectionLimits::standard(),
    );
    assert!(next.1);
    assert_ne!(next.0, first.0);
    assert!(matches!(next.0, PhysicalUnitCheckpoint::After { .. }));
    assert!(next.2.is_empty());
}
