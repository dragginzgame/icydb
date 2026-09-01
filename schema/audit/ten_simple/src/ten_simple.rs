use icydb_model::prelude::*;
use icydb_testing_wasm_helpers::{
    define_fixture_canister, define_fixture_store, define_simple_audit_entities,
};

define_fixture_canister!(
    TenSimpleCanister = "TenSimpleCanister",
    namespace = "ten_simple",
    memory_min = 155,
    memory_max = 161,
    commit_memory_id = 159,
    startup_memory_id = 161,
    integrity_progress_memory_id = 160,
);

define_fixture_store!(
    TenSimpleStore,
    canister = "TenSimpleCanister",
    storage(journaled(
        data_memory_id = 155,
        index_memory_id = 156,
        schema_memory_id = 157,
        journal_memory_id = 158,
    )),
);

#[enum_(
    variant(name = "Ready"),
    variant(name = "Weighted", value(item(prim = "Nat64")))
)]
pub struct ReachableInputChoice {}

#[record(fields(
    field(name = "label", value(item(prim = "Text", max_len = 64))),
    field(name = "choice", value(item(is = "ReachableInputChoice"))),
    field(name = "note", value(opt, item(prim = "Text", max_len = 64)))
))]
pub struct ReachableInputProfile {}

#[entity(
    store = "TenSimpleStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(
            name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(
            name = "profiles",
            value(many, item(is = "ReachableInputProfile"))
        )
    ),
    timestamps
)]
pub struct TenSimpleEntity01 {}

define_simple_audit_entities!(
    "TenSimpleStore";
    TenSimpleEntity02,
    TenSimpleEntity03,
    TenSimpleEntity04,
    TenSimpleEntity05,
    TenSimpleEntity06,
    TenSimpleEntity07,
    TenSimpleEntity08,
    TenSimpleEntity09,
    TenSimpleEntity10,
);
