use super::*;
use crate::{
    db::{
        RequestExecutionRoot,
        commit::{
            ExactBacklogMeasurement, commit_memory_handle, current_commit_memory_allocation,
            current_database_backlog, register_runtime_journal_tails_for_backlog,
            select_commit_memory_allocation,
        },
        data::{DataStore, DecodedDataStoreKey},
        index::IndexStore,
        journal::{
            DatabaseCommitSequence, FoldWatermark, JournalBatch, JournalRecord, JournalSequence,
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        registry::{StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::SchemaStore,
    },
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
    traits::Path,
    types::EntityTag,
};
use ic_storage::Memory;
use std::cell::RefCell;

const FRESH_PATH: &str = "convergence_tests::Fresh";
const LIFE_A_PATH: &str = "convergence_tests::LifeA";
const LIFE_B_PATH: &str = "convergence_tests::LifeB";

struct ConvergenceCanister;

impl Path for ConvergenceCanister {
    const PATH: &'static str = "convergence_tests::Canister";
}

impl CanisterKind for ConvergenceCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(100)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.convergence.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(101)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.convergence.startup.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(102)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.convergence.integrity.v1";
}

const fn allocations(
    first_memory_id: u64,
    data_key: &'static str,
    index_key: &'static str,
    schema_key: &'static str,
    journal_key: &'static str,
) -> StoreAllocationIdentities {
    StoreAllocationIdentities::new_journaled(
        StoreAllocationIdentity::new(first_memory_id, data_key),
        StoreAllocationIdentity::new(first_memory_id + 1, index_key),
        StoreAllocationIdentity::new(first_memory_id + 2, schema_key),
        StoreAllocationIdentity::new(first_memory_id + 3, journal_key),
    )
}

thread_local! {
    static FRESH_DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(
        store_memory_owned(103, "icydb.test.convergence.fresh.data.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static FRESH_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(
        store_memory_owned(104, "icydb.test.convergence.fresh.index.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static FRESH_SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(
        store_memory_owned(105, "icydb.test.convergence.fresh.schema.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static FRESH_JOURNAL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(
        store_memory_owned(106, "icydb.test.convergence.fresh.journal.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static FRESH_REGISTRY: StoreRegistry = registry(
        FRESH_PATH,
        &FRESH_DATA,
        &FRESH_INDEX,
        &FRESH_SCHEMA,
        &FRESH_JOURNAL,
        allocations(
            103,
            "icydb.test.convergence.fresh.data.v1",
            "icydb.test.convergence.fresh.index.v1",
            "icydb.test.convergence.fresh.schema.v1",
            "icydb.test.convergence.fresh.journal.v1",
        ),
    );

    static LIFE_A_DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(
        store_memory_owned(115, "icydb.test.convergence.lifea.data.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static LIFE_A_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(
        store_memory_owned(116, "icydb.test.convergence.lifea.index.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static LIFE_A_SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(
        store_memory_owned(117, "icydb.test.convergence.lifea.schema.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static LIFE_A_JOURNAL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(
        store_memory_owned(118, "icydb.test.convergence.lifea.journal.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static LIFE_B_DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(
        store_memory_owned(119, "icydb.test.convergence.lifeb.data.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static LIFE_B_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(
        store_memory_owned(120, "icydb.test.convergence.lifeb.index.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static LIFE_B_SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(
        store_memory_owned(121, "icydb.test.convergence.lifeb.schema.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static LIFE_B_JOURNAL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(
        store_memory_owned(122, "icydb.test.convergence.lifeb.journal.v1").unwrap()
    ).expect("fixture storage should initialize"));
    static LIFE_A_REGISTRY: StoreRegistry = registry(
        LIFE_A_PATH,
        &LIFE_A_DATA,
        &LIFE_A_INDEX,
        &LIFE_A_SCHEMA,
        &LIFE_A_JOURNAL,
        allocations(
            115,
            "icydb.test.convergence.lifea.data.v1",
            "icydb.test.convergence.lifea.index.v1",
            "icydb.test.convergence.lifea.schema.v1",
            "icydb.test.convergence.lifea.journal.v1",
        ),
    );
    static LIFE_B_REGISTRY: StoreRegistry = registry(
        LIFE_B_PATH,
        &LIFE_B_DATA,
        &LIFE_B_INDEX,
        &LIFE_B_SCHEMA,
        &LIFE_B_JOURNAL,
        allocations(
            119,
            "icydb.test.convergence.lifeb.data.v1",
            "icydb.test.convergence.lifeb.index.v1",
            "icydb.test.convergence.lifeb.schema.v1",
            "icydb.test.convergence.lifeb.journal.v1",
        ),
    );
    static LIFE_AB_REGISTRY: StoreRegistry = {
        let mut registry = registry(
            LIFE_A_PATH,
            &LIFE_A_DATA,
            &LIFE_A_INDEX,
            &LIFE_A_SCHEMA,
            &LIFE_A_JOURNAL,
            allocations(
                115,
                "icydb.test.convergence.lifea.data.v1",
                "icydb.test.convergence.lifea.index.v1",
                "icydb.test.convergence.lifea.schema.v1",
                "icydb.test.convergence.lifea.journal.v1",
            ),
        );
        registry.register_journaled_store(
            LIFE_B_PATH,
            &LIFE_B_DATA,
            &LIFE_B_INDEX,
            &LIFE_B_SCHEMA,
            &LIFE_B_JOURNAL,
            allocations(
                119,
                "icydb.test.convergence.lifeb.data.v1",
                "icydb.test.convergence.lifeb.index.v1",
                "icydb.test.convergence.lifeb.schema.v1",
                "icydb.test.convergence.lifeb.journal.v1",
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ).unwrap();
        registry
    };
    static LIFE_EMPTY_REGISTRY: StoreRegistry = StoreRegistry::new();
}

fn registry(
    path: &'static str,
    data: &'static std::thread::LocalKey<RefCell<DataStore>>,
    index: &'static std::thread::LocalKey<RefCell<IndexStore>>,
    schema: &'static std::thread::LocalKey<RefCell<SchemaStore>>,
    journal: &'static std::thread::LocalKey<RefCell<JournalTailStore>>,
    allocations: StoreAllocationIdentities,
) -> StoreRegistry {
    let mut registry = StoreRegistry::new();
    registry
        .register_journaled_store(
            path,
            data,
            index,
            schema,
            journal,
            allocations,
            StoreRuntimeStorageCapabilities::journaled(),
        )
        .unwrap();
    registry
}

fn database(
    registry: &'static std::thread::LocalKey<StoreRegistry>,
) -> (RequestExecutionRoot, Db<ConvergenceCanister>) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = Db::new(registry, root.scope());
    (root, db)
}

fn maximum_registry() -> Vec<PersistedStoreAllocation> {
    (0..MAX_PERSISTED_STORE_ALLOCATIONS)
        .map(|ordinal| {
            let first = 1 + u64::try_from(CANISTER_CONTROL_ALLOCATION_COUNT + ordinal * 4).unwrap();
            let key = |role: &str| -> &'static str {
                Box::leak(
                    format!("icydb.test.convergence.max.s{ordinal}.{role}.v1").into_boxed_str(),
                )
            };
            PersistedStoreAllocation::active(allocations(
                first,
                key("data"),
                key("index"),
                key("schema"),
                key("journal"),
            ))
            .unwrap()
        })
        .collect()
}

#[test]
fn deployment_store_bound_is_canonical_and_rejects_seventeenth_store() {
    assert_eq!(MAX_PERSISTED_STORE_ALLOCATIONS, 16);

    let mut registry = maximum_registry();
    canonicalize_store_registry(&mut registry).unwrap();
    validate_persisted_allocation_set::<ConvergenceCanister>(&registry).unwrap();
    let memory = std::rc::Rc::new(test_memory(127));
    super::super::write_current_boot_record(&memory).unwrap();
    let replacement = prepare_commit_control_replacement(
        memory.clone(),
        crate::db::DatabaseIncarnationId::for_tests(0x71),
        [0x72; 32],
        0,
        &registry,
    )
    .unwrap();
    apply_prepared_commit_control_replacement(replacement);
    let PersistedCommitControlObservation::Current {
        registry: reopened, ..
    } = inspect_persisted_commit_control(memory).unwrap()
    else {
        panic!("maximum registry must remain current");
    };
    assert_eq!(reopened, registry);

    registry.push(
        PersistedStoreAllocation::active(allocations(
            167,
            "icydb.test.convergence.overflow.data.v1",
            "icydb.test.convergence.overflow.index.v1",
            "icydb.test.convergence.overflow.schema.v1",
            "icydb.test.convergence.overflow.journal.v1",
        ))
        .unwrap(),
    );
    assert!(
        prepare_commit_control_replacement(
            std::rc::Rc::new(test_memory(126)),
            crate::db::DatabaseIncarnationId::for_tests(0x73),
            [0x74; 32],
            0,
            &registry,
        )
        .is_err(),
        "a seventeenth current-format store must fail closed",
    );
    registry.pop();

    registry.push(registry[0].clone());
    assert!(canonicalize_store_registry(&mut registry).is_err());
    let control_collision = vec![
        PersistedStoreAllocation::active(allocations(
            ConvergenceCanister::commit_memory_id().expect("test allocation"),
            "icydb.test.convergence.collision.data.v1",
            "icydb.test.convergence.collision.index.v1",
            "icydb.test.convergence.collision.schema.v1",
            "icydb.test.convergence.collision.journal.v1",
        ))
        .unwrap(),
    ];
    assert!(validate_persisted_allocation_set::<ConvergenceCanister>(&control_collision).is_err());
}

#[test]
fn fresh_root_admission_does_not_initialize_journal_memory() {
    let (_root, fresh) = database(&FRESH_REGISTRY);
    let proposals = generated_store_proposals::<ConvergenceCanister>(&fresh).unwrap();
    for _ in 0..2 {
        require_fresh_proposal_roots(&proposals).unwrap();
        for proposal in &proposals {
            for allocation in required_allocations(proposal.allocations).unwrap() {
                assert_eq!(allocation_pages(allocation).unwrap(), 0);
            }
        }
    }
    // A physical journal header is not virgin, even with no batches.
    FRESH_JOURNAL.with_borrow(|tail| {
        assert!(!(tail.has_stored_batch()));
    });
    assert!(require_fresh_proposal_roots(&proposals).is_err());
}

#[test]
fn rejected_fresh_control_preparation_preserves_retryable_roots() {
    let control = std::rc::Rc::new(test_memory(123));
    let (_root, fresh) = database(&FRESH_REGISTRY);
    let proposals = generated_store_proposals::<ConvergenceCanister>(&fresh).unwrap();
    require_fresh_proposal_roots(&proposals).unwrap();
    // The existing control encoder rejects a zero cursor key after tail
    // handles are collected. Rejection must not initialize those handles.
    let result = initialize_controls(
        &control,
        generate_database_incarnation_id().unwrap(),
        [0; 32],
        0,
        proposals
            .iter()
            .map(|proposal| proposal.persisted.clone())
            .collect(),
        proposals.iter().map(|proposal| proposal.handle),
    );
    assert!(result.is_err());
    assert_eq!(control.size(), 0);
    require_fresh_proposal_roots(&proposals).unwrap();
    ensure_current_convergence_format(&fresh, &control, true, [0x17; 32]).unwrap();
    super::super::validate_current_boot_record(control.as_ref()).unwrap();
    FRESH_JOURNAL.with_borrow(|tail| {
        assert!(tail.validate_current_tail_authority().unwrap().is_empty());
        tail.validate_current_entity_mutation_revisions(&[])
            .unwrap();
    });
}

#[test]
fn fresh_initialization_publishes_exact_current_controls() {
    let fresh_control = std::rc::Rc::new(test_memory(123));
    let (_root, fresh) = database(&FRESH_REGISTRY);
    ensure_current_convergence_format(&fresh, &fresh_control, true, [0x17; 32]).unwrap();
    let PersistedCommitControlObservation::Current {
        database_commit_sequence,
        registry,
        ..
    } = inspect_persisted_commit_control(fresh_control).unwrap()
    else {
        panic!("fresh initialization must publish current control");
    };
    assert_eq!(database_commit_sequence, 0);
    assert_eq!(registry.len(), 1);
    assert_eq!(registry[0].state(), PersistedStoreAllocationState::Active);
    FRESH_JOURNAL.with_borrow(|tail| {
        assert!(tail.validate_current_tail_authority().unwrap().is_empty());
        tail.validate_current_entity_mutation_revisions(&[])
            .expect("fresh journal must retain the exact empty entity authority");
    });
}

#[test]
fn backlog_measurement_follows_active_and_retired_tail_authority() {
    select_commit_memory_allocation(100, ConvergenceCanister::COMMIT_STABLE_KEY);
    let control = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    let (_root, active_database) = database(&FRESH_REGISTRY);
    ensure_current_convergence_format(&active_database, &control, true, [0x17; 32]).unwrap();
    assert_eq!(
        current_database_backlog().unwrap(),
        ExactBacklogMeasurement::EMPTY
    );

    // An active persisted allocation needs its exact registered owner.
    LIFE_EMPTY_REGISTRY.with(register_runtime_journal_tails_for_backlog);
    let error = current_database_backlog().unwrap_err();
    assert_eq!(error.class, ErrorClass::InvariantViolation);
    assert_eq!(error.origin, ErrorOrigin::Store);
    FRESH_REGISTRY.with(register_runtime_journal_tails_for_backlog);
    let key = DecodedDataStoreKey::new(
        EntityTag::new(7),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(1)),
    )
    .to_raw()
    .unwrap();
    let batch = JournalBatch::new_with_database_commit_sequence(
        [0x71; 16],
        [0x72; 16],
        JournalSequence::new(1),
        DatabaseCommitSequence::new(1),
        vec![
            JournalRecord::row_put("convergence_tests::Backlog", key, vec![0], [0x75; 16]).unwrap(),
        ],
    )
    .unwrap();
    FRESH_JOURNAL.with_borrow_mut(|tail| tail.append_batch(&batch).unwrap());
    let observed = current_database_backlog().unwrap();
    assert_eq!(observed.batch_count(), 1);
    assert_eq!(observed.record_count(), 1);
    assert_eq!(
        observed.encoded_batch_bytes(),
        crate::db::journal::encode_journal_batch(&batch)
            .unwrap()
            .len() as u64
    );
    FRESH_JOURNAL.with_borrow_mut(|tail| {
        let retirement = tail
            .prepare_batch_retirement(&batch, FoldWatermark::new(JournalSequence::new(1), 1))
            .unwrap();
        tail.apply_prepared_batch_retirement(retirement);
        // Simulate restart without the generated owner: release the actual
        // journal lease before the dormant-allocation reader opens it.
        *tail =
            JournalTailStore::init(test_memory(254)).expect("fixture storage should initialize");
    });
    let (_root, empty_database) = database(&LIFE_EMPTY_REGISTRY);
    ensure_current_convergence_format(&empty_database, &control, false, [0x17; 32]).unwrap();
    let PersistedCommitControlObservation::Current { registry, .. } =
        inspect_persisted_commit_control(control).unwrap()
    else {
        panic!("retirement must retain current control authority");
    };
    assert_eq!(registry.len(), 1);
    assert_eq!(registry[0].state(), PersistedStoreAllocationState::Dormant);
    assert_eq!(
        current_database_backlog().unwrap(),
        ExactBacklogMeasurement::EMPTY
    );

    // A retired state is no substitute for checking physical authority.
    let unexpected = JournalBatch::new_with_database_commit_sequence(
        [0x73; 16],
        [0x74; 16],
        JournalSequence::new(2),
        DatabaseCommitSequence::new(2),
        Vec::new(),
    )
    .unwrap();
    let mut tail = JournalTailStore::init(
        store_memory_owned(106, "icydb.test.convergence.fresh.journal.v1").unwrap(),
    )
    .expect("fixture storage should initialize");
    tail.append_batch(&unexpected).unwrap();
    drop(tail);
    let error = current_database_backlog().unwrap_err();
    assert_eq!(error.class, ErrorClass::Corruption);
    assert_eq!(error.origin, ErrorOrigin::Store);
    let mut tail = JournalTailStore::init(
        store_memory_owned(106, "icydb.test.convergence.fresh.journal.v1").unwrap(),
    )
    .expect("fixture storage should initialize");
    let retirement = tail
        .prepare_batch_retirement(&unexpected, FoldWatermark::new(JournalSequence::new(2), 2))
        .unwrap();
    tail.apply_prepared_batch_retirement(retirement);
    drop(tail);
    assert_eq!(
        current_database_backlog().unwrap(),
        ExactBacklogMeasurement::EMPTY
    );
}

#[test]
fn unsupported_registry_is_observed_until_boot_reset_without_control_writes() {
    crate::testing::run_isolated(|| {
        use crate::db::database_format::{
            DatabaseFormatObservation, ensure_database_format_admitted,
            forget_database_format_admission_for_tests, observe_database_format,
            require_cursor_key_admitted,
        };

        select_commit_memory_allocation(
            ConvergenceCanister::commit_memory_id().unwrap(),
            ConvergenceCanister::COMMIT_STABLE_KEY,
        );
        let allocation = current_commit_memory_allocation().unwrap();
        let control = commit_memory_handle(allocation).unwrap();
        let (_root, database) = database(&LIFE_A_REGISTRY);
        ensure_current_convergence_format(&database, &control, true, [0x17; 32]).unwrap();
        let PersistedCommitControlObservation::Current {
            incarnation,
            cursor_authentication_key,
            database_commit_sequence,
            registry,
            ..
        } = inspect_persisted_commit_control(control.clone()).unwrap()
        else {
            panic!("fresh controls must be current");
        };
        let debt = JournalBatch::new_with_database_commit_sequence(
            [0x31; 16],
            [0x32; 16],
            JournalSequence::new(1),
            DatabaseCommitSequence::new(1),
            Vec::new(),
        )
        .unwrap();
        LIFE_A_JOURNAL.with_borrow_mut(|tail| tail.append_batch(&debt).unwrap());
        let dormant = registry
            .into_iter()
            .map(PersistedStoreAllocation::dormant)
            .collect::<Vec<_>>();
        let replacement = prepare_commit_control_replacement(
            control.clone(),
            incarnation,
            cursor_authentication_key,
            database_commit_sequence,
            &dormant,
        )
        .unwrap();
        apply_prepared_commit_control_replacement(replacement);
        let byte_len = usize::try_from(
            control
                .size()
                .checked_mul(super::super::WASM_PAGE_BYTES)
                .unwrap(),
        )
        .unwrap();
        let mut before = vec![0; byte_len];
        control.read(0, &mut before);

        for _ in 0..2 {
            let error = ensure_database_format_admitted(&database).unwrap_err();
            assert_eq!(error.class(), ErrorClass::Unsupported);
            assert_eq!(error.origin(), ErrorOrigin::Store);
            assert!(error.detail().is_none());
            let observed = observe_database_format(&LIFE_A_REGISTRY).unwrap_err();
            assert_eq!(observed.diagnostic(), error.diagnostic());
            assert!(observed.diagnostic_facts().is_empty());
            assert!(require_cursor_key_admitted().is_err());
        }
        let mut after = vec![0; before.len()];
        control.read(0, &mut after);
        assert_eq!(after, before);

        // One refused control allocation cannot poison another selection.
        select_commit_memory_allocation(250, "icydb.test.refusal.other.v1");
        assert_eq!(
            observe_database_format(&LIFE_EMPTY_REGISTRY).unwrap(),
            DatabaseFormatObservation::Uninitialized
        );
        select_commit_memory_allocation(allocation.memory_id, allocation.stable_key);
        assert!(observe_database_format(&LIFE_A_REGISTRY).is_err());

        // Upgrade drops the volatile refusal, leaving the old controls intact.
        forget_database_format_admission_for_tests();
        assert_eq!(
            observe_database_format(&LIFE_A_REGISTRY).unwrap(),
            DatabaseFormatObservation::Current
        );
        control.read(0, &mut after);
        assert_eq!(after, before);
    });
}

#[test]
fn pending_application_blocks_topology_before_any_registry_publication() {
    use crate::db::schema::{
        SchemaApplicationRecord, SchemaApplicationRecordOp, SchemaChangeActivation,
        SchemaChangeJob, SchemaChangeOutcome, SchemaChangeReceipt,
        apply_schema_application_record_op, derive_schema_change_job_id,
    };
    use icydb_schema::{
        ExpectedAcceptedHead, ExpectedSchemaFingerprint, SchemaProposalDigest, SchemaSubmissionKey,
        TargetDatabaseIdentity, TargetStoreIdentity,
    };
    crate::testing::run_isolated(|| {
        select_commit_memory_allocation(100, ConvergenceCanister::COMMIT_STABLE_KEY);
        let control = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
        let (_root, active) = database(&LIFE_A_REGISTRY);
        ensure_current_convergence_format(&active, &control, true, [0x17; 32]).unwrap();
        let key = SchemaSubmissionKey::try_new("pending-registry").unwrap();
        let identity = TargetDatabaseIdentity::from_bytes([3; 32]);
        let digest = SchemaProposalDigest::from_bytes([4; 32]);
        let head = ExpectedAcceptedHead::Exact {
            revision: 2,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([5; 32]),
        };
        let receipt = SchemaChangeReceipt::new(
            identity,
            key.clone(),
            digest,
            ExpectedAcceptedHead::Empty,
            SchemaChangeOutcome::Pending {
                job: SchemaChangeJob::new(
                    derive_schema_change_job_id(
                        identity,
                        &key,
                        digest,
                        &ExpectedAcceptedHead::Empty,
                    )
                    .unwrap(),
                ),
                candidate_head: head.clone(),
            },
        )
        .unwrap();
        let record = SchemaApplicationRecord::new(
            receipt,
            vec![
                SchemaChangeActivation::new(TargetStoreIdentity::from_bytes([6; 32]), 1, 1)
                    .unwrap(),
            ],
        )
        .unwrap();
        apply_schema_application_record_op(&SchemaApplicationRecordOp::insert(&record).unwrap())
            .unwrap();
        let mut before =
            vec![0; usize::try_from(control.size() * super::super::WASM_PAGE_BYTES).unwrap()];
        control.read(0, &mut before);
        // An added quartet is physically virgin; rejection must not initialise it.
        let (_root, added) = database(&LIFE_AB_REGISTRY);
        assert!(ensure_current_convergence_format(&added, &control, false, [0x17; 32]).is_err());
        let mut after = vec![0; before.len()];
        assert_eq!(
            control.size() * super::super::WASM_PAGE_BYTES,
            before.len() as u64
        );
        control.read(0, &mut after);
        assert_eq!(after, before);
        assert_eq!(
            allocation_pages(
                added
                    .store_handle(LIFE_B_PATH)
                    .unwrap()
                    .journal_allocation()
                    .unwrap()
            )
            .unwrap(),
            0
        );
        // Restoring the original declaration can resume the pending job.
        ensure_current_convergence_format(&active, &control, false, [0x17; 32]).unwrap();
        let terminal = SchemaApplicationRecord::new(
            SchemaChangeReceipt::new(
                identity,
                key,
                digest,
                ExpectedAcceptedHead::Empty,
                SchemaChangeOutcome::Aborted {
                    accepted_head: head,
                },
            )
            .unwrap(),
            Vec::new(),
        )
        .unwrap();
        apply_schema_application_record_op(
            &SchemaApplicationRecordOp::replace(&record, &terminal).unwrap(),
        )
        .unwrap();
        ensure_current_convergence_format(&added, &control, false, [0x17; 32]).unwrap();
    });
}

#[test]
fn current_registry_rejects_debt_and_restores_exact_dormant_quartets() {
    let control = std::rc::Rc::new(test_memory(126));
    let (_root, life_a) = database(&LIFE_A_REGISTRY);
    ensure_current_convergence_format(&life_a, &control, true, [0x17; 32]).unwrap();
    let retained = JournalBatch::new_with_database_commit_sequence(
        [0x71; 16],
        [0x72; 16],
        JournalSequence::new(1),
        DatabaseCommitSequence::new(1),
        Vec::new(),
    )
    .unwrap();
    LIFE_A_JOURNAL.with_borrow_mut(|tail| tail.append_batch(&retained).unwrap());
    ensure_current_convergence_format(&life_a, &control, false, [0x17; 32])
        .expect("current startup must admit a bounded nonempty tail");

    // Simulate removal of the generated store on restart: release its tree
    // before the dormant-allocation reader opens that physical lease.
    let release_a = || {
        LIFE_A_JOURNAL.with_borrow_mut(|tail| {
            *tail = JournalTailStore::init(test_memory(254))
                .expect("fixture storage should initialize");
        });
    };
    release_a();
    let (_root, empty) = database(&LIFE_EMPTY_REGISTRY);
    assert!(ensure_current_convergence_format(&empty, &control, false, [0x17; 32]).is_err());
    LIFE_A_JOURNAL.with_borrow_mut(|tail| {
        *tail = JournalTailStore::init(
            store_memory_owned(118, "icydb.test.convergence.lifea.journal.v1").unwrap(),
        )
        .expect("fixture storage should initialize");
        let retirement = tail
            .prepare_batch_retirement(&retained, FoldWatermark::new(JournalSequence::new(1), 1))
            .unwrap();
        tail.apply_prepared_batch_retirement(retirement);
    });
    release_a();
    ensure_current_convergence_format(&empty, &control, false, [0x17; 32]).unwrap();

    // A dormant registry row must still validate its physical tail on retry.
    let unexpected = JournalBatch::new_with_database_commit_sequence(
        [0x73; 16],
        [0x74; 16],
        JournalSequence::new(2),
        DatabaseCommitSequence::new(2),
        Vec::new(),
    )
    .unwrap();
    let mut tail = JournalTailStore::init(
        store_memory_owned(118, "icydb.test.convergence.lifea.journal.v1").unwrap(),
    )
    .expect("fixture storage should initialize");
    tail.append_batch(&unexpected).unwrap();
    drop(tail);
    assert!(ensure_current_convergence_format(&empty, &control, false, [0x17; 32]).is_err());
    let mut tail = JournalTailStore::init(
        store_memory_owned(118, "icydb.test.convergence.lifea.journal.v1").unwrap(),
    )
    .expect("fixture storage should initialize");
    let retirement = tail
        .prepare_batch_retirement(&unexpected, FoldWatermark::new(JournalSequence::new(2), 2))
        .unwrap();
    tail.apply_prepared_batch_retirement(retirement);
    drop(tail);
    ensure_current_convergence_format(&empty, &control, false, [0x17; 32]).unwrap();

    let (_root, life_b) = database(&LIFE_B_REGISTRY);
    ensure_current_convergence_format(&life_b, &control, false, [0x17; 32]).unwrap();
    let PersistedCommitControlObservation::Current { registry, .. } =
        inspect_persisted_commit_control(control.clone()).unwrap()
    else {
        panic!("lifecycle reconciliation must remain current");
    };
    assert_eq!(registry.len(), 2);
    assert_eq!(registry[0].state(), PersistedStoreAllocationState::Dormant);
    assert_eq!(registry[1].state(), PersistedStoreAllocationState::Active);

    let (_root, combined_database) = database(&LIFE_AB_REGISTRY);
    LIFE_A_JOURNAL.with_borrow_mut(|tail| {
        *tail = JournalTailStore::init(
            store_memory_owned(118, "icydb.test.convergence.lifea.journal.v1").unwrap(),
        )
        .unwrap();
    });
    ensure_current_convergence_format(&combined_database, &control, false, [0x17; 32]).unwrap();
    let PersistedCommitControlObservation::Current {
        registry: unchanged,
        ..
    } = inspect_persisted_commit_control(control).unwrap()
    else {
        panic!("restoration must retain current authority");
    };
    assert_eq!(unchanged.len(), registry.len());
    assert!(
        unchanged
            .iter()
            .all(|entry| entry.state() == PersistedStoreAllocationState::Active)
    );
    for (restored, prior) in unchanged.iter().zip(&registry) {
        assert_eq!(restored.roles(), prior.roles());
    }
}
