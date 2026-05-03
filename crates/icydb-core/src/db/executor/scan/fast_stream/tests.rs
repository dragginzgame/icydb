//! Module: db::executor::scan::fast_stream::tests
//! Covers fast-stream scan behavior and continuation handling.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        Db,
        access::{AccessPath, AccessPlan},
        data::DataStore,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, Context, ExecutableAccess,
            ExecutionOptimization, stream::access::TraversalRuntime,
        },
        index::IndexStore,
        registry::StoreRegistry,
        schema::SchemaStore,
    },
    model::field::FieldKind,
    testing::test_memory,
    traits::{EntityKind, Path},
    types::Ulid,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::cell::RefCell;

use crate::db::executor::scan::fast_stream::execute_structural_fast_stream_request;

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct FastStreamInvariantEntity {
    id: Ulid,
}

crate::test_canister! {
    ident = FastStreamInvariantCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = FastStreamInvariantStore,
    canister = FastStreamInvariantCanister,
}

crate::test_entity_schema! {
    ident = FastStreamInvariantEntity,
    id = Ulid,
    id_field = id,
    entity_name = "FastStreamInvariantEntity",
    entity_tag = crate::testing::FAST_STREAM_INVARIANT_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid)],
    indexes = [],
    store = FastStreamInvariantStore,
    canister = FastStreamInvariantCanister,
}

thread_local! {
    static FAST_STREAM_INVARIANT_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(170)));
    static FAST_STREAM_INVARIANT_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(171)));
    static FAST_STREAM_INVARIANT_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init(test_memory(172)));
    static FAST_STREAM_INVARIANT_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(
            FastStreamInvariantStore::PATH,
            &FAST_STREAM_INVARIANT_DATA_STORE,
            &FAST_STREAM_INVARIANT_INDEX_STORE,
            &FAST_STREAM_INVARIANT_SCHEMA_STORE,
        )
        .expect("fast-stream invariant test store registration should succeed");
        reg
    };
}

static FAST_STREAM_INVARIANT_DB: Db<FastStreamInvariantCanister> =
    Db::new(&FAST_STREAM_INVARIANT_REGISTRY);

#[test]
fn fast_stream_allows_missing_exact_key_count_hint() {
    let ctx = Context::<FastStreamInvariantEntity>::new(&FAST_STREAM_INVARIANT_DB);
    let id1 = Ulid::from_u128(1);
    let id2 = Ulid::from_u128(2);
    let access =
        AccessPlan::Union(vec![AccessPlan::by_key(id1), AccessPlan::by_key(id2)]).into_value_plan();
    let access = ExecutableAccess::from_executable_plan(
        access.executable_contract(),
        AccessStreamBindings {
            index_prefix_specs: &[],
            index_range_specs: &[],
            continuation: AccessScanContinuationInput::new(None, Direction::Asc),
        },
        None,
        None,
    );
    let runtime = TraversalRuntime::new(
        ctx.structural_store().expect("test store should resolve"),
        FastStreamInvariantEntity::ENTITY_TAG,
    );

    let mut fast = execute_structural_fast_stream_request(
        &runtime,
        &access.plan,
        access.bindings,
        access.physical_fetch_hint,
        access.index_predicate_execution,
        ExecutionOptimization::PrimaryKey,
    )
    .expect("fast-path execution should allow streams without exact count hints");

    assert_eq!(
        fast.rows_scanned, None,
        "missing exact-count hints should defer scan accounting to the consumer"
    );
    assert!(
        fast.ordered_key_stream
            .next_key()
            .expect("first fast-stream key should decode")
            .is_some(),
        "fast stream should still expose its keys when exact count is unknown"
    );
}

#[test]
fn fast_stream_defers_unbounded_primary_scan_candidate_counting() {
    let ctx = Context::<FastStreamInvariantEntity>::new(&FAST_STREAM_INVARIANT_DB);
    let access = AccessPlan::path(AccessPath::<crate::value::Value>::FullScan).into_value_plan();
    let access = ExecutableAccess::from_executable_plan(
        access.executable_contract(),
        AccessStreamBindings {
            index_prefix_specs: &[],
            index_range_specs: &[],
            continuation: AccessScanContinuationInput::new(None, Direction::Asc),
        },
        None,
        None,
    );
    let runtime = TraversalRuntime::new(
        ctx.structural_store().expect("test store should resolve"),
        FastStreamInvariantEntity::ENTITY_TAG,
    );

    let fast = execute_structural_fast_stream_request(
        &runtime,
        &access.plan,
        access.bindings,
        access.physical_fetch_hint,
        access.index_predicate_execution,
        ExecutionOptimization::PrimaryKey,
    )
    .expect("unbounded primary fast-stream request should build lazily");

    assert_eq!(
        fast.rows_scanned, None,
        "unbounded primary streams must not pre-count access candidates before consumption"
    );
    assert_eq!(
        fast.ordered_key_stream
            .exact_diagnostic_access_candidate_count(),
        Some(0),
        "exact primary candidate counting remains an explicit diagnostics-only operation"
    );
}
