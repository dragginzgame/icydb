use super::*;
use crate::{
    db::{
        commit::{
            CommitRowOp, commit_memory_handle, current_commit_memory_allocation,
            database_control_proof_identity, observe_commit_control,
            retained_commit_marker_measurement_for_tests, select_commit_memory_allocation,
        },
        data::{DataStore, DecodedDataStoreKey},
        database_format::initialize_current_database_control_for_tests,
        index::IndexStore,
        integrity::DatabaseIncarnationId,
        journal::{FoldWatermark, JournalTailStore},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        registry::{
            StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
            StoreRuntimeStorageCapabilities,
        },
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, FieldId, FieldInsertGeneration,
            FieldStorageDecode, PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
            SchemaFieldWritePolicy, SchemaInsertDefault, SchemaRowLayout, SchemaStore,
            SchemaVersion, accepted_schema_candidate_for_tests,
        },
    },
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
    traits::Path,
    types::EntityTag,
};
use std::collections::BTreeMap;

struct SchedulerOverlayTestCanister;

impl Path for SchedulerOverlayTestCanister {
    const PATH: &'static str = "executor::mutation::tests::SchedulerOverlayTestCanister";
}

impl CanisterKind for SchedulerOverlayTestCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(1)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.scheduler_overlay.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(3)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.scheduler_overlay.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(2)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.scheduler_overlay.integrity.progress.v1";
}

thread_local! {
    static TEST_REGISTRY: StoreRegistry = StoreRegistry::new();
    static FIRST_IDENTITY_DATA: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(240)).expect("fixture storage should initialize"));
    static FIRST_IDENTITY_INDEX: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(241)).expect("fixture storage should initialize"));
    static FIRST_IDENTITY_SCHEMA: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(242)).expect("fixture storage should initialize"));
    static FIRST_IDENTITY_JOURNAL: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(243)).expect("fixture storage should initialize"));
    static SECOND_IDENTITY_DATA: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(244)).expect("fixture storage should initialize"));
    static SECOND_IDENTITY_INDEX: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(245)).expect("fixture storage should initialize"));
    static SECOND_IDENTITY_SCHEMA: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(246)).expect("fixture storage should initialize"));
    static SECOND_IDENTITY_JOURNAL: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(247)).expect("fixture storage should initialize"));
}

const IDENTITY_INCARNATION: DatabaseIncarnationId = DatabaseIncarnationId::for_tests(0x71);

#[test]
fn stale_index_preflight_rejects_before_marker_publication() {
    select_commit_memory_allocation(230, SchedulerOverlayTestCanister::COMMIT_STABLE_KEY);
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    let marker = CommitMarker::from_parts([0x71; 16], Vec::new()).unwrap();
    let before_control = observe_commit_control().unwrap();
    let before_proof = database_control_proof_identity().unwrap();

    // Either touched store can invalidate the complete operation. In
    // particular, an unchanged first guard must not permit publication.
    for changed_index in [&FIRST_IDENTITY_INDEX, &SECOND_IDENTITY_INDEX] {
        let guards = [
            IndexStoreGenerationGuard::capture(&FIRST_IDENTITY_INDEX),
            IndexStoreGenerationGuard::capture(&SECOND_IDENTITY_INDEX),
        ];
        changed_index.with_borrow_mut(|store| {
            store
                .reset_journaled_live_projection(
                    0,
                    FoldWatermark::initial(),
                    &std::collections::BTreeSet::new(),
                )
                .unwrap();
        });
        let error = begin_commit_window_payload::<SchedulerOverlayTestCanister>(&marker, &guards)
            .expect_err("stale preflight must fail before creating recovery work");
        assert_eq!(error.class(), ErrorClass::InvariantViolation);
        assert_eq!(error.origin(), ErrorOrigin::Executor);
        assert_eq!(
            error.diagnostic().error_code(),
            icydb_diagnostic_code::ErrorCode::RUNTIME_INVARIANT_VIOLATION,
        );
        assert_eq!(observe_commit_control().unwrap(), before_control);
        assert_eq!(database_control_proof_identity().unwrap(), before_proof);
        assert_eq!(
            retained_commit_marker_measurement_for_tests().unwrap(),
            None
        );
    }

    // Fresh guards still publish normally and leave marker retirement to
    // the existing commit owner; the rejected attempts need no recovery.
    let guards = [
        IndexStoreGenerationGuard::capture(&FIRST_IDENTITY_INDEX),
        IndexStoreGenerationGuard::capture(&SECOND_IDENTITY_INDEX),
    ];
    let guard = begin_commit_window_payload::<SchedulerOverlayTestCanister>(&marker, &guards)
        .expect("refreshed preflight should publish without a recovery pass");
    assert!(
        retained_commit_marker_measurement_for_tests()
            .unwrap()
            .is_some()
    );
    finish_commit(guard, |_| Ok(())).unwrap();
    assert_eq!(
        retained_commit_marker_measurement_for_tests().unwrap(),
        None
    );
}

#[test]
fn canonical_commit_work_bound_tracks_exact_prepared_fanout() {
    assert_eq!(MAX_MUTATION_PROGRESS_BATCH_ROWS_AT_MAX_INDEX_FANOUT, 240,);
    let final_zero_index_units =
        next_mutation_commit_work_units((4_096 - 1) * MUTATION_COMMIT_WORK_UNITS_PER_ROW, 0)
            .expect("the final zero-index row should be admitted");
    assert_eq!(final_zero_index_units, MAX_MUTATION_COMMIT_WORK_UNITS);
    let zero_index_error = next_mutation_commit_work_units(final_zero_index_units, 0)
        .expect_err("the next zero-index row must exceed the fixed work bound");
    assert_eq!(
        zero_index_error.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualCount,
                16_388,
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::Limit, 16_384),
        ],
    );

    let mut max_fanout_units = 0;
    for _ in 0..MAX_MUTATION_PROGRESS_BATCH_ROWS_AT_MAX_INDEX_FANOUT {
        max_fanout_units = next_mutation_commit_work_units(max_fanout_units, 64)
            .expect("240 maximum-fanout inserts should be admitted");
    }
    assert_eq!(max_fanout_units, 16_320);
    let max_fanout_error = next_mutation_commit_work_units(max_fanout_units, 64)
        .expect_err("the 241st maximum-fanout insert must be rejected");
    assert!(matches!(
        max_fanout_error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchCommitWorkExceeded,
        })
    ));
    assert_eq!(
        max_fanout_error.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualCount,
                16_388,
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::Limit, 16_384),
        ],
    );

    let mut identity_boundary_units =
        fixed_mutation_commit_work_units(1, false).expect("one Identity range should fit");
    for _ in 0..4_095 {
        identity_boundary_units = next_mutation_commit_work_units(identity_boundary_units, 0)
            .expect("4,095 zero-index rows plus one Identity range should fit");
    }
    assert_eq!(identity_boundary_units, MAX_MUTATION_COMMIT_WORK_UNITS);
    assert!(next_mutation_commit_work_units(identity_boundary_units, 0).is_err());
}

#[test]
fn individual_gate_keeps_priority_over_cumulative_pressure() {
    let gate_two_called = std::cell::Cell::new(false);
    let result =
        ensure_mutation_commit_work_admitted(MAX_MUTATION_COMMIT_WORK_UNITS + 1).and_then(|()| {
            gate_two_called.set(true);
            crate::db::commit::admit_backlog(
                crate::db::commit::ExactBacklogMeasurement::new(1, 1, 1),
                crate::db::commit::ExactBacklogMeasurement::new(1, 1, 1),
                crate::db::commit::BacklogLimits::new(1, 1, 1),
            )
        });
    let error = result.expect_err("individual admission must reject before dormant Gate 2");
    assert!(!gate_two_called.get());
    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchCommitWorkExceeded,
        })
    ));
}

fn identity_candidate(
    store_path: &str,
    entity_tag: EntityTag,
) -> crate::db::schema::CandidateSchemaRevision {
    let field_id = FieldId::new(1);
    let kind = AcceptedFieldKind::Nat64;
    let leaf_codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        format!("tests::Identity{}", entity_tag.value()),
        format!("Identity{}", entity_tag.value()),
        field_id,
        SchemaRowLayout::initial(vec![(field_id, SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial_with_write_policy(
            field_id,
            "id".to_string(),
            SchemaFieldSlot::new(0),
            kind,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            SchemaFieldWritePolicy::from_model_policies(
                Some(FieldInsertGeneration::Identity),
                None,
            ),
            FieldStorageDecode::ByKind,
            leaf_codec,
        )],
    );
    accepted_schema_candidate_for_tests(
        store_path,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(entity_tag, snapshot)]),
    )
}

fn identity_store_handles() -> (StoreHandle, StoreHandle) {
    (
        StoreHandle::new_journaled(
            &FIRST_IDENTITY_DATA,
            &FIRST_IDENTITY_INDEX,
            &FIRST_IDENTITY_SCHEMA,
            &FIRST_IDENTITY_JOURNAL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(240, "icydb.test.identity_set.first.data.v1"),
                StoreAllocationIdentity::new(241, "icydb.test.identity_set.first.index.v1"),
                StoreAllocationIdentity::new(242, "icydb.test.identity_set.first.schema.v1"),
                StoreAllocationIdentity::new(243, "icydb.test.identity_set.first.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ),
        StoreHandle::new_journaled(
            &SECOND_IDENTITY_DATA,
            &SECOND_IDENTITY_INDEX,
            &SECOND_IDENTITY_SCHEMA,
            &SECOND_IDENTITY_JOURNAL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(244, "icydb.test.identity_set.second.data.v1"),
                StoreAllocationIdentity::new(245, "icydb.test.identity_set.second.index.v1"),
                StoreAllocationIdentity::new(246, "icydb.test.identity_set.second.schema.v1"),
                StoreAllocationIdentity::new(247, "icydb.test.identity_set.second.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ),
    )
}

fn test_key(value: u64) -> DecodedDataStoreKey {
    DecodedDataStoreKey::new(
        EntityTag::new(41),
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(value)),
    )
}

fn test_row_op(key: &DecodedDataStoreKey, after: Option<Vec<u8>>) -> CanonicalRowCommitOp {
    CommitRowOp::new(
        "tests::SelfRelation",
        key.to_raw().expect("test key should encode"),
        None,
        after.map(|bytes| {
            crate::db::data::canonical_row_from_stored_raw_row(RawRow::try_new(bytes).unwrap())
        }),
        [7; 16],
    )
}

#[test]
fn journal_row_put_moves_owned_bytes_and_preserves_the_apply_copy() {
    let key = test_key(17);
    for length in [1, 4_096, crate::db::codec::MAX_ROW_BYTES as usize] {
        let bytes = vec![0xA7; length];
        let original_pointer = bytes.as_ptr();
        let expected = bytes.clone();
        let row_op = test_row_op(&key, Some(bytes));
        let prepared_apply_row = row_op.after.as_ref().unwrap().clone();
        let record = journal_record_for_row_op(row_op).expect("owned put should become a record");
        let JournalRecord::RowPut {
            entity_path,
            primary_key,
            row_bytes,
            schema_fingerprint,
        } = record
        else {
            panic!("put must retain its record family");
        };
        assert_eq!(row_bytes.as_ptr(), original_pointer);
        assert_eq!(row_bytes, expected);
        assert_eq!(primary_key, key.to_raw().unwrap());
        assert_eq!(entity_path, "tests::SelfRelation");
        assert_eq!(schema_fingerprint, [7; 16]);
        assert_ne!(
            prepared_apply_row.as_raw_row().as_bytes().as_ptr(),
            original_pointer
        );
        assert_eq!(prepared_apply_row.as_raw_row().as_bytes(), expected);
    }
}

#[test]
fn journal_row_conversion_preserves_delete_and_record_admission() {
    let key = test_key(18);
    let record = journal_record_for_row_op(test_row_op(&key, None))
        .expect("delete should retain its journal record");
    let JournalRecord::RowDelete {
        entity_path,
        primary_key,
        schema_fingerprint,
    } = record
    else {
        panic!("delete must retain its record family");
    };
    assert_eq!(entity_path, "tests::SelfRelation");
    assert_eq!(primary_key, key.to_raw().unwrap());
    assert_eq!(schema_fingerprint, [7; 16]);
    let mut invalid = test_row_op(&key, Some(vec![1]));
    invalid.entity_path = std::rc::Rc::from("");
    let error = journal_record_for_row_op(invalid).expect_err("empty entity path must refuse");
    assert_eq!(error.class(), ErrorClass::Corruption);
}

#[test]
fn scheduler_overlay_seeds_later_final_after_images_before_preflight() {
    let db: Db<SchedulerOverlayTestCanister> = Db::new(
        &TEST_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    let first = test_key(1);
    let later = test_key(2);
    let row_ops = vec![
        test_row_op(&first, Some(vec![1])),
        test_row_op(&later, Some(vec![2])),
    ];

    let overlay = PreflightStoreOverlay::from_row_ops(&db, row_ops.as_slice())
        .expect("complete batch overlay should build");
    assert!(overlay.has_primary_row_override(&later).unwrap());
    assert!(overlay.contains_primary_row(&later).unwrap());
    assert!(!overlay.has_primary_row_override(&test_key(4)).unwrap());
    let visible = overlay
        .read_primary_row(&later)
        .expect("later batch target lookup should succeed")
        .expect("later batch target must be visible before row-order preflight");

    assert_eq!(visible.as_bytes(), &[2]);
}

#[test]
fn scheduler_overlay_seeds_delete_absence_before_preflight() {
    let db: Db<SchedulerOverlayTestCanister> = Db::new(
        &TEST_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    let deleted = test_key(3);
    let row_ops = vec![test_row_op(&deleted, None)];

    let overlay = PreflightStoreOverlay::from_row_ops(&db, row_ops.as_slice())
        .expect("delete overlay should build");
    assert!(overlay.has_primary_row_override(&deleted).unwrap());
    assert!(!overlay.contains_primary_row(&deleted).unwrap());
    assert!(!overlay.has_primary_row_override(&test_key(4)).unwrap());

    assert!(
        overlay
            .read_primary_row(&deleted)
            .expect("delete target lookup should succeed")
            .is_none(),
        "the complete batch must mask deleted rows before storage-backed proofs",
    );
}

#[test]
fn stale_multi_owner_preflight_is_read_only_for_the_complete_owner_set() {
    let (first, second) = identity_store_handles();
    let first_entity = EntityTag::new(51);
    let second_entity = EntityTag::new(52);
    for (store_path, handle, entity_tag) in [
        ("tests::FirstIdentityStore", first, first_entity),
        ("tests::SecondIdentityStore", second, second_entity),
    ] {
        handle
            .with_schema_mut(|store| {
                *store = SchemaStore::init_journaled(test_memory(if entity_tag == first_entity {
                    242
                } else {
                    246
                }))
                .expect("fixture storage should initialize");
                store.publish_accepted_schema_candidate(
                    IDENTITY_INCARNATION,
                    AcceptedSchemaRevision::NONE,
                    &identity_candidate(store_path, entity_tag),
                )
            })
            .expect("the Identity owner should publish with explicit zero state");
    }

    let first_owner = crate::db::schema::IdentityStateOwner::try_new(
        IDENTITY_INCARNATION,
        first_entity,
        FieldId::new(1),
    )
    .expect("the first owner should admit");
    let second_owner = crate::db::schema::IdentityStateOwner::try_new(
        IDENTITY_INCARNATION,
        second_entity,
        FieldId::new(1),
    )
    .expect("the second owner should admit");
    let second_committed = IdentityRangeAdvance::try_new(second_owner, 0, 1, 1)
        .expect("the committed second-owner range should admit");
    second
        .with_schema_mut(|store| {
            store.apply_identity_range_advance(
                second_committed,
                IdentityAdvanceId::try_new([0x11; 16], [0x21; 16], 1, 0)
                    .expect("the committed advance identity should admit"),
            )
        })
        .expect("the competing second-owner range should materialize");

    let prepared = [
        PreparedIdentityRangeApply {
            store_path: "tests::FirstIdentityStore",
            handle: first,
            range: IdentityRangeAdvance::try_new(first_owner, 0, 1, 1)
                .expect("the first pending range should admit"),
            advance_id: IdentityAdvanceId::try_new([0x12; 16], [0x22; 16], 2, 0)
                .expect("the first pending advance identity should admit"),
        },
        PreparedIdentityRangeApply {
            store_path: "tests::SecondIdentityStore",
            handle: second,
            range: IdentityRangeAdvance::try_new(second_owner, 0, 1, 1)
                .expect("the stale second-owner range should remain structurally valid"),
            advance_id: IdentityAdvanceId::try_new([0x12; 16], [0x23; 16], 3, 0)
                .expect("the second pending advance identity should admit"),
        },
    ];

    let error = crate::db::query::preparation::with_preparation_work(|work| {
        preflight_identity_range_applies(prepared.as_slice(), work)
    })
    .expect_err("one stale owner must reject the complete set before publication");
    assert_eq!(error.class(), ErrorClass::Conflict);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
    for (handle, entity_tag, expected_high_water) in
        [(first, first_entity, 0), (second, second_entity, 1)]
    {
        let cursor = handle
            .with_schema(|store| {
                store.identity_statement_cursor(
                    IDENTITY_INCARNATION,
                    entity_tag,
                    FieldId::new(1),
                    &AcceptedFieldKind::Nat64,
                )
            })
            .expect("set preflight must leave every owner unchanged");
        assert_eq!(cursor.expected_high_water(), expected_high_water);
    }
}
