use icydb_model::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

define_fixture_canister!(
    OneSimpleCanister = "OneSimpleCanister",
    namespace = "one_simple",
    memory_min = 100,
    memory_max = 106,
    commit_memory_id = 104,
    startup_memory_id = 106,
    integrity_progress_memory_id = 105,
);

define_fixture_store!(
    OneSimpleStore,
    canister = "OneSimpleCanister",
    storage(journaled(
        data_memory_id = 100,
        index_memory_id = 101,
        schema_memory_id = 102,
        journal_memory_id = 103,
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
    store = "OneSimpleStore",
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
pub struct OneSimpleEntity01 {}

#[cfg(feature = "u256-audit")]
#[entity(
    store = "OneSimpleStore",
    version = 1,
    pk(field = "id"),
    index(field = "amount", unique),
    index(field = "optional_amount"),
    index(fields = ["bucket", "amount"]),
    fields(
        field(name = "id", value(item(prim = "U256"))),
        field(name = "amount", value(item(prim = "U256"))),
        field(name = "optional_amount", value(opt, item(prim = "U256"))),
        field(name = "bucket", value(item(prim = "Nat64"))),
        field(name = "label", value(item(prim = "Text", max_len = 64)))
    )
)]
pub struct U256AuditEntity {}
