//! Real store pages must remain bounded and fair under source restarts.

use crate::{
    db::{
        DataStore, IndexStore, JournalTailStore, StoreAllocationIdentities,
        StoreAllocationIdentity, StoreRegistry, StoreRuntimeStorageCapabilities,
        commit::{
            commit_memory_handle, current_commit_memory_allocation, database_incarnation_id,
            select_commit_memory_allocation,
        },
        database_format::initialize_current_database_control_for_tests,
        journal::{FoldWatermark, JournalSequence},
        schema::{
            SchemaStore, cardinality_build::CardinalityBuildAuthority,
            cardinality_generation::CardinalityGenerationState,
        },
        startup::{GeneratedStartupDriverStep, driver::drive_cardinality_page},
    },
    testing::test_memory,
};
use std::{cell::RefCell, thread::LocalKey};

thread_local! {
    static A_DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(test_memory(180)).expect("fixture storage should initialize"));
    static A_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(test_memory(181)).expect("fixture storage should initialize"));
    static A_SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(test_memory(182)).expect("fixture storage should initialize"));
    static A_TAIL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(test_memory(183)).expect("fixture storage should initialize"));
    static Z_DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(test_memory(184)).expect("fixture storage should initialize"));
    static Z_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(test_memory(185)).expect("fixture storage should initialize"));
    static Z_SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(test_memory(186)).expect("fixture storage should initialize"));
    static Z_TAIL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(test_memory(187)).expect("fixture storage should initialize"));
    static STORES: StoreRegistry = registry();
    static OTHER_STORES: StoreRegistry = registry();
}

fn allocations(first: u8, name: &str) -> StoreAllocationIdentities {
    // Permanent static fixture keys; registry paths below select these two groups.
    let keys = match name {
        "a" => [
            "test.fair.a.data.v1",
            "test.fair.a.index.v1",
            "test.fair.a.schema.v1",
            "test.fair.a.journal.v1",
        ],
        "z" => [
            "test.fair.z.data.v1",
            "test.fair.z.index.v1",
            "test.fair.z.schema.v1",
            "test.fair.z.journal.v1",
        ],
        _ => panic!("unknown fixture store"),
    };
    StoreAllocationIdentities::new_journaled(
        StoreAllocationIdentity::new(first.into(), keys[0]),
        StoreAllocationIdentity::new((first + 1).into(), keys[1]),
        StoreAllocationIdentity::new((first + 2).into(), keys[2]),
        StoreAllocationIdentity::new((first + 3).into(), keys[3]),
    )
}

fn registry() -> StoreRegistry {
    let mut registry = StoreRegistry::new();
    // Reverse registration order exercises the existing sorted-store contract.
    registry
        .register_journaled_store(
            "z",
            &Z_DATA,
            &Z_INDEX,
            &Z_SCHEMA,
            &Z_TAIL,
            allocations(184, "z"),
            StoreRuntimeStorageCapabilities::journaled(),
        )
        .unwrap();
    registry
        .register_journaled_store(
            "a",
            &A_DATA,
            &A_INDEX,
            &A_SCHEMA,
            &A_TAIL,
            allocations(180, "a"),
            StoreRuntimeStorageCapabilities::journaled(),
        )
        .unwrap();
    registry
}

fn initialize() {
    select_commit_memory_allocation(230, "test.fair.commit.v1");
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    assert_eq!(
        drive_cardinality_page(&STORES).unwrap(),
        GeneratedStartupDriverStep::Recovering
    );
    assert!(
        Z_SCHEMA
            .with_borrow(|schema| schema.cardinality_generation_header().unwrap())
            .is_none()
    );
}

fn restart_first(epoch: u64) {
    // Model the canonical fold-watermark change that invalidates a build source.
    let watermark = FoldWatermark::new(JournalSequence::new(epoch), epoch);
    A_TAIL.with_borrow_mut(|tail| tail.persist_fold_watermark(watermark).unwrap());
    A_SCHEMA.with_borrow_mut(|schema| {
        let header = schema.cardinality_generation_header().unwrap().unwrap();
        let current = CardinalityBuildAuthority::derive(
            schema,
            database_incarnation_id().unwrap(),
            allocations(180, "a"),
            watermark,
        )
        .unwrap();
        schema
            .restart_cardinality_generation(header, current.source())
            .unwrap();
    });
}

fn later_is_ready() -> bool {
    Z_SCHEMA.with_borrow(|schema| {
        schema
            .cardinality_generation_header()
            .unwrap()
            .is_some_and(|header| header.state() == CardinalityGenerationState::Ready)
    })
}

#[test]
fn repeated_first_store_restarts_cannot_starve_a_later_store() {
    initialize();
    let mut ready = false;
    for epoch in 1..=4 {
        restart_first(epoch);
        let before = A_SCHEMA.with_borrow(|schema| schema.cardinality_generation_header().unwrap());
        assert_eq!(
            drive_cardinality_page(&STORES).unwrap(),
            GeneratedStartupDriverStep::Recovering
        );
        if later_is_ready() {
            // Exactly the later store got a page; the first build did not advance.
            assert_eq!(
                A_SCHEMA.with_borrow(|schema| schema.cardinality_generation_header().unwrap()),
                before
            );
            ready = true;
            break;
        }
    }
    assert!(
        ready,
        "the later store must get its own bounded maintenance turn"
    );
}

#[test]
fn registry_instances_are_isolated_and_tls_handle_aliases_share_a_turn() {
    initialize();
    restart_first(1);
    assert_eq!(
        drive_cardinality_page(&OTHER_STORES).unwrap(),
        GeneratedStartupDriverStep::Recovering
    );
    assert!(
        !later_is_ready(),
        "another registry starts at its own first store"
    );
    let alias: &'static LocalKey<StoreRegistry> = Box::leak(Box::new(STORES));
    assert_eq!(
        drive_cardinality_page(alias).unwrap(),
        GeneratedStartupDriverStep::Recovering
    );
    assert!(
        later_is_ready(),
        "a second handle to the original TLS registry retains its turn"
    );
}
