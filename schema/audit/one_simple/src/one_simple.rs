use icydb_model::prelude::*;
use icydb_testing_wasm_helpers::{
    define_fixture_canister, define_fixture_store, define_simple_audit_entities,
};

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

define_simple_audit_entities!(
    "OneSimpleStore";
    OneSimpleEntity01
);

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
