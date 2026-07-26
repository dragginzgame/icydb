use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

define_fixture_canister!(
    SqlTestCanister = "SqlTestCanister",
    namespace = "test_sql",
    memory_min = 155,
    memory_max = 160,
    commit_memory_id = 159,
);

define_fixture_store!(
    SqlTestStore = "SQL_TEST_STORE",
    canister = "SqlTestCanister",
    storage(journaled(
        data_memory_id = 155,
        index_memory_id = 156,
        schema_memory_id = 157,
        journal_memory_id = 158,
    )),
);

///
/// SqlTestUser
///
/// Small indexed user fixture used by generated-vs-typed SQL smoke tests.
///

#[entity(source_key = "schema/test/sql/src/sql.rs::entity::1",
    store = "SqlTestStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.1", fields = ["name"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "name", ident = "name", value(item(prim = "Text", unbounded))),
        field(source_key = "age", ident = "age", value(item(prim = "Int32"))),
        field(source_key = "rank", ident = "rank", value(item(prim = "Int32")))
    )
)]
pub struct SqlTestUser {}

///
/// SqlTestNumericTypes
///
/// Dedicated SQL fixture for mixed-width numeric expression and aggregate
/// coverage on the lightweight schema/test SQL canister.
///

#[entity(source_key = "schema/test/sql/src/sql.rs::entity::2",
    store = "SqlTestStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.2", fields = ["label"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "label", ident = "label", value(item(prim = "Text", unbounded))),
        field(source_key = "group_name", ident = "group_name", value(item(prim = "Text", unbounded))),
        field(source_key = "int8_value", ident = "int8_value", value(item(prim = "Int8"))),
        field(source_key = "int16_value", ident = "int16_value", value(item(prim = "Int16"))),
        field(source_key = "int32_value", ident = "int32_value", value(item(prim = "Int32"))),
        field(source_key = "int64_value", ident = "int64_value", value(item(prim = "Int64"))),
        field(source_key = "nat8_value", ident = "nat8_value", value(item(prim = "Nat8"))),
        field(source_key = "nat16_value", ident = "nat16_value", value(item(prim = "Nat16"))),
        field(source_key = "nat32_value", ident = "nat32_value", value(item(prim = "Nat32"))),
        field(source_key = "nat64_value", ident = "nat64_value", value(item(prim = "Nat64"))),
        field(source_key = "decimal_value", ident = "decimal_value", value(item(prim = "Decimal", scale = 2))),
        field(source_key = "float32_value", ident = "float32_value", value(item(prim = "Float32"))),
        field(source_key = "float64_value", ident = "float64_value", value(item(prim = "Float64")))
    )
)]
pub struct SqlTestNumericTypes {}
