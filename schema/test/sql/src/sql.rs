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

// Minimal same-store entities used only to measure the exact 64-context
// structural batch boundary in the maintained PocketIC closeout probe.
macro_rules! define_context_boundary_entities {
    ($($entity:ident),+ $(,)?) => {
        $(
            #[entity(
                store = "SqlTestStore",
                version = 1,
                pk(field = "id"),
                fields(
                    field(name = "id", value(item(prim = "Nat64"))),
                    field(name = "payload", value(item(prim = "Nat64")))
                )
            )]
            pub struct $entity {}
        )+
    };
}

define_context_boundary_entities!(
    SqlTestContext00,
    SqlTestContext01,
    SqlTestContext02,
    SqlTestContext03,
    SqlTestContext04,
    SqlTestContext05,
    SqlTestContext06,
    SqlTestContext07,
    SqlTestContext08,
    SqlTestContext09,
    SqlTestContext10,
    SqlTestContext11,
    SqlTestContext12,
    SqlTestContext13,
    SqlTestContext14,
    SqlTestContext15,
    SqlTestContext16,
    SqlTestContext17,
    SqlTestContext18,
    SqlTestContext19,
    SqlTestContext20,
    SqlTestContext21,
    SqlTestContext22,
    SqlTestContext23,
    SqlTestContext24,
    SqlTestContext25,
    SqlTestContext26,
    SqlTestContext27,
    SqlTestContext28,
    SqlTestContext29,
    SqlTestContext30,
    SqlTestContext31,
    SqlTestContext32,
    SqlTestContext33,
    SqlTestContext34,
    SqlTestContext35,
    SqlTestContext36,
    SqlTestContext37,
    SqlTestContext38,
    SqlTestContext39,
    SqlTestContext40,
    SqlTestContext41,
    SqlTestContext42,
    SqlTestContext43,
    SqlTestContext44,
    SqlTestContext45,
    SqlTestContext46,
    SqlTestContext47,
    SqlTestContext48,
    SqlTestContext49,
    SqlTestContext50,
    SqlTestContext51,
    SqlTestContext52,
    SqlTestContext53,
    SqlTestContext54,
    SqlTestContext55,
    SqlTestContext56,
    SqlTestContext57,
    SqlTestContext58,
    SqlTestContext59,
    SqlTestContext60,
    SqlTestContext61,
    SqlTestContext62,
    SqlTestContext63,
);

/// Application-keyed user in the maintained Toko-shaped enrollment proof.
#[entity(
    store = "SqlTestStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "display_name", value(item(prim = "Text", max_len = 64)))
    ),
    timestamps
)]
pub struct SqlTestEnrollmentUser {}

/// Principal membership retaining both lookup and compound uniqueness.
#[entity(
    store = "SqlTestStore",
    version = 1,
    pk(field = "authentication_principal"),
    index(field = "user_id"),
    index(fields = ["user_id", "authentication_principal"], unique),
    fields(
        field(name = "authentication_principal", value(item(prim = "Principal"))),
        field(
            name = "user_id",
            value(item(rel = "SqlTestEnrollmentUser", prim = "Ulid"))
        )
    ),
    timestamps
)]
pub struct SqlTestEnrollmentUserPrincipal {}

/// Generated-key robot related to the application-keyed enrollment user.
#[entity(
    store = "SqlTestStore",
    version = 1,
    pk(field = "id"),
    index(field = "user_id"),
    fields(
        field(
            name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(
            name = "user_id",
            value(item(rel = "SqlTestEnrollmentUser", prim = "Ulid"))
        ),
        field(name = "label", value(item(prim = "Text", max_len = 64)))
    ),
    timestamps
)]
pub struct SqlTestEnrollmentRobot {}

/// Reference-shaped accepted schema for typed rejected-field diagnostics.
#[entity(
    store = "SqlTestStore",
    version = 1,
    pk(field = "pokemon_card_id"),
    index(fields = ["hp", "pokemon_card_id"]),
    fields(
        field(name = "pokemon_card_id", value(item(prim = "Ulid"))),
        field(name = "hp", value(item(prim = "Int32")))
    )
)]
pub struct PokemonCardMetadata {}
