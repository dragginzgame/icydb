use icydb::design::prelude::*;

///
/// SqlParityCanister
///
/// Broad SQL parity canister model used by generated-vs-typed unit tests.
///

#[canister(memory_min = 104, memory_max = 154, commit_memory_id = 154)]
pub struct SqlParityCanister {}

///
/// SqlParityStore
///
/// Shared store used by the broad SQL parity fixture entities.
///

#[store(
    ident = "SQL_PARITY_STORE",
    canister = "SqlParityCanister",
    data_memory_id = 104,
    index_memory_id = 105
)]
pub struct SqlParityStore {}

///
/// Customer
///
/// Technical customer fixture that carries the plain SQL parity coverage.
///

#[entity(
    store = "SqlParityStore",
    pk(field = "id"),
    index(fields = "name"),
    index(fields = "name", key_items = "LOWER(name)"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "age", value(item(prim = "Int32")))
    )
)]
pub struct Customer {}

///
/// CustomerAccount
///
/// Technical customer-account fixture that carries the filtered and
/// expression-backed SQL parity coverage.
///

#[entity(
    store = "SqlParityStore",
    pk(field = "id"),
    index(fields = "name", predicate = "active = true"),
    index(fields = "tier,handle", predicate = "active = true"),
    index(
        fields = "handle",
        key_items = "LOWER(handle)",
        predicate = "active = true"
    ),
    index(
        fields = "tier,handle",
        key_items = "tier, LOWER(handle)",
        predicate = "active = true"
    ),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "active", value(item(prim = "Bool"))),
        field(ident = "tier", value(item(prim = "Text"))),
        field(ident = "handle", value(item(prim = "Text")))
    )
)]
pub struct CustomerAccount {}

///
/// CustomerOrder
///
/// Technical order fixture that carries the text-prefix and composite-order SQL
/// parity coverage previously mixed into the demo surface.
///

#[entity(
    store = "SqlParityStore",
    pk(field = "id"),
    index(fields = "name"),
    index(fields = "priority, status"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "priority", value(item(prim = "Nat16"))),
        field(ident = "status", value(item(prim = "Text")))
    )
)]
pub struct CustomerOrder {}
