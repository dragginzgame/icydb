//! Module: db::executor::scan::fast_stream::tests
//! Covers fast-stream scan behavior and continuation handling.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        Db,
        access::AccessPlan,
        data::DataStore,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, Context, ExecutableAccess,
            ExecutionOptimization, stream::access::TraversalRuntime,
        },
        index::IndexStore,
        registry::StoreRegistry,
    },
    error::ErrorClass,
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
    static FAST_STREAM_INVARIANT_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(
            FastStreamInvariantStore::PATH,
            &FAST_STREAM_INVARIANT_DATA_STORE,
            &FAST_STREAM_INVARIANT_INDEX_STORE,
        )
        .expect("fast-stream invariant test store registration should succeed");
        reg
    };
}

static FAST_STREAM_INVARIANT_DB: Db<FastStreamInvariantCanister> =
    Db::new(&FAST_STREAM_INVARIANT_REGISTRY);

#[test]
fn fast_stream_requires_exact_key_count_hint() {
    let ctx = Context::<FastStreamInvariantEntity>::new(&FAST_STREAM_INVARIANT_DB);
    let id1 = Ulid::from_u128(1);
    let id2 = Ulid::from_u128(2);
    let access =
        AccessPlan::Union(vec![AccessPlan::by_key(id1), AccessPlan::by_key(id2)]).into_value_plan();
    let access = ExecutableAccess::from_executable_plan(
        access.resolve_strategy().into_executable(),
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

    let Err(err) =
        execute_structural_fast_stream_request(&runtime, access, ExecutionOptimization::PrimaryKey)
    else {
        panic!("fast-path execution must reject streams without exact count hints")
    };

    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "missing exact-count hint must classify as invariant violation"
    );
    assert!(
        err.message
            .contains("fast-path stream must expose an exact key-count hint"),
        "missing exact-count hint must emit a clear invariant message"
    );
}
