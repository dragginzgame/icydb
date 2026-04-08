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
/// CustomerOrderProfile
///
/// Technical structured order profile used to keep one value-backed
/// non-scalar field on the broad sql_parity order fixture.
///

#[record(fields(
    field(ident = "summary", value(item(prim = "Text")), default = "String::new"),
    field(ident = "bucket", value(item(prim = "Nat16")), default = 0u16)
))]
pub struct CustomerOrderProfile {}

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
        field(ident = "status", value(item(prim = "Text"))),
        field(ident = "labels", value(many, item(prim = "Text"))),
        field(ident = "profile", value(item(is = "CustomerOrderProfile")))
    )
)]
pub struct CustomerOrder {}

///
/// PlannerChoice
///
/// Technical deterministic-planning fixture used to lock public SQL route
/// selection when multiple visible indexes tie on structural eligibility.
///

#[entity(
    store = "SqlParityStore",
    pk(field = "id"),
    index(fields = "tier,alpha,label"),
    index(fields = "tier,handle,label"),
    index(fields = "tier,label,alpha"),
    index(fields = "tier,label,handle"),
    index(fields = "beta"),
    index(fields = "alpha"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "tier", value(item(prim = "Text"))),
        field(ident = "score", value(item(prim = "Nat16"))),
        field(ident = "handle", value(item(prim = "Text"))),
        field(ident = "label", value(item(prim = "Text"))),
        field(ident = "alpha", value(item(prim = "Text"))),
        field(ident = "beta", value(item(prim = "Text")))
    )
)]
pub struct PlannerChoice {}

///
/// PlannerPrefixChoice
///
/// Technical deterministic-planning fixture used to lock public SQL
/// equality-prefix route selection when competing visible indexes tie on
/// prefix length.
///

#[entity(
    store = "SqlParityStore",
    pk(field = "id"),
    index(fields = "tier,label"),
    index(fields = "tier,handle"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "tier", value(item(prim = "Text"))),
        field(ident = "handle", value(item(prim = "Text"))),
        field(ident = "label", value(item(prim = "Text")))
    )
)]
pub struct PlannerPrefixChoice {}
