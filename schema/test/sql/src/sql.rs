use icydb_model::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

#[cfg(not(feature = "migration-v2"))]
define_fixture_canister!(
    SqlTestCanister = "SqlTestCanister",
    namespace = "test_sql",
    memory_min = 155,
    memory_max = 161,
    commit_memory_id = 159,
    startup_memory_id = 161,
    integrity_progress_memory_id = 160,
);

#[cfg(feature = "migration-v2")]
define_fixture_canister!(
    SqlTestCanister = "SqlTestCanister",
    namespace = "test_sql",
    memory_min = 155,
    memory_max = 161,
    commit_memory_id = 159,
    startup_memory_id = 161,
    integrity_progress_memory_id = 160,
    migrations(entity_migration(
        entity = "SqlTestUser",
        from = 1,
        renames(field(from = "rank", to = "score")),
        transforms(rewrite(
            from = "age",
            to = "age",
            checked_cast(to = "Nat16")
        ))
    )),
);

define_fixture_store!(
    SqlTestStore,
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

#[cfg(not(feature = "migration-v2"))]
#[entity(store = "SqlTestStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["name"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(name = "age", value(item(prim = "Int32"))),
        field(name = "rank", value(item(prim = "Int32")))
    ),
    timestamps
)]
pub struct SqlTestUser {}

#[cfg(feature = "migration-v2")]
#[entity(store = "SqlTestStore",
    version = 2,
    pk(fields = ["id"]),
    index(fields = ["name"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(name = "age", value(item(prim = "Nat16"))),
        field(name = "score", value(item(prim = "Int32")))
    ),
    timestamps
)]
pub struct SqlTestUser {}

///
/// SqlTestNumericTypes
///
/// Dedicated SQL fixture for mixed-width numeric expression and aggregate
/// coverage on the lightweight schema/test SQL canister.
///

#[entity(store = "SqlTestStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["label"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "label", value(item(prim = "Text", unbounded))),
        field(name = "group_name", value(item(prim = "Text", unbounded))),
        field(name = "int8_value", value(item(prim = "Int8"))),
        field(name = "int16_value", value(item(prim = "Int16"))),
        field(name = "int32_value", value(item(prim = "Int32"))),
        field(name = "int64_value", value(item(prim = "Int64"))),
        field(name = "nat8_value", value(item(prim = "Nat8"))),
        field(name = "nat16_value", value(item(prim = "Nat16"))),
        field(name = "nat32_value", value(item(prim = "Nat32"))),
        field(name = "nat64_value", value(item(prim = "Nat64"))),
        field(name = "decimal_value", value(item(prim = "Decimal", scale = 2))),
        field(name = "float32_value", value(item(prim = "Float32"))),
        field(name = "float64_value", value(item(prim = "Float64")))
    ),
    timestamps
)]
pub struct SqlTestNumericTypes {}

/// Caller-authored Nat64 control for the Identity closeout instruction probe.
#[entity(
    store = "SqlTestStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(name = "payload", value(item(prim = "Nat64")))
    )
)]
pub struct SqlTestCallerNat64 {}

/// Generated Nat64 subject for the Identity closeout instruction probe.
#[entity(
    store = "SqlTestStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(
            name = "id",
            value(item(prim = "Nat64")),
            generated(insert = "Identity::next")
        ),
        field(name = "payload", value(item(prim = "Nat64")))
    )
)]
pub struct SqlTestIdentityNat64 {}

/// Generated Nat128 subject for the Identity closeout instruction probe.
#[entity(
    store = "SqlTestStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(
            name = "id",
            value(item(prim = "Nat128")),
            generated(insert = "Identity::next")
        ),
        field(name = "payload", value(item(prim = "Nat64")))
    )
)]
pub struct SqlTestIdentityNat128 {}

/// Isolated generated owner for one-row and record-cap batch measurements.
#[entity(
    store = "SqlTestStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(
            name = "id",
            value(item(prim = "Nat64")),
            generated(insert = "Identity::next")
        ),
        field(name = "payload", value(item(prim = "Nat64")))
    )
)]
pub struct SqlTestIdentityBatch {}
