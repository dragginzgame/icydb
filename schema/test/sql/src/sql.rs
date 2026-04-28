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
        field(ident = "age", value(item(prim = "Int32"))),
        field(ident = "rank", value(item(prim = "Int32")))
    )
)]
pub struct SqlTestUser {}

///
/// SqlTestNumericTypes
///
/// Dedicated SQL fixture for mixed-width numeric expression and aggregate
/// coverage on the lightweight schema/test SQL canister.
///

#[entity(
    store = "SqlTestStore",
    pk(field = "id"),
    index(fields = "label"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "label", value(item(prim = "Text"))),
        field(ident = "group_name", value(item(prim = "Text"))),
        field(ident = "int8_value", value(item(prim = "Int8"))),
        field(ident = "int16_value", value(item(prim = "Int16"))),
        field(ident = "int32_value", value(item(prim = "Int32"))),
        field(ident = "int64_value", value(item(prim = "Int64"))),
        field(ident = "nat8_value", value(item(prim = "Nat8"))),
        field(ident = "nat16_value", value(item(prim = "Nat16"))),
        field(ident = "nat32_value", value(item(prim = "Nat32"))),
        field(ident = "nat64_value", value(item(prim = "Nat64"))),
        field(ident = "decimal_value", value(item(prim = "Decimal", scale = 2))),
        field(ident = "float32_value", value(item(prim = "Float32"))),
        field(ident = "float64_value", value(item(prim = "Float64")))
    )
)]
pub struct SqlTestNumericTypes {}
