use icydb::design::prelude::*;

///
/// SqlTestCanister
///
/// Small canister model dedicated to lightweight SQL smoke-test coverage.
///

#[canister(memory_min = 155, memory_max = 165, commit_memory_id = 157)]
pub struct SqlTestCanister {}

///
/// SqlTestStore
///
/// Single-store fixture used to keep the lightweight SQL test canister small
/// while still exercising one indexed entity surface.
///

#[store(
    ident = "SQL_TEST_STORE",
    canister = "SqlTestCanister",
    data_memory_id = 155,
    index_memory_id = 156
)]
pub struct SqlTestStore {}

///
/// SqlTestUser
///
/// Small indexed user fixture used by generated-vs-typed SQL smoke tests.
///

#[entity(
    store = "SqlTestStore",
    pk(field = "id"),
    index(fields = "name"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "age", value(item(prim = "Int32")))
    )
)]
pub struct SqlTestUser {}
