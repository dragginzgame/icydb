use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// Entity
///

#[entity(
    source_key = "schema/test/fixtures/src/macro_test/entity.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(field = "id"),
    index(source_key = "index.1", field = "a"),
    fields(
        field(
            source_key = "id",
            ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(
            source_key = "a",
            ident = "a",
            value(item(prim = "Int32")),
            default = 3
        ),
    )
)]
pub struct Entity {}

///
/// UnitKey
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/entity.rs::entity::2",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id", value(item(prim = "Unit"))),
        field(source_key = "a", ident = "a", value(item(prim = "Int32")), default = 3),
    )
)]
pub struct UnitKey {}

///
/// RenamedEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/entity.rs::entity::3",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    name = "Potato",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ))
)]
pub struct RenamedEntity {}

///
/// BoundedTextEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/entity.rs::entity::4",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "name", ident = "name", value(item(prim = "Text", max_len = 12))),
    )
)]
pub struct BoundedTextEntity {}

///
/// BoundedBlobEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/entity.rs::entity::5",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "payload", ident = "payload", value(item(prim = "Blob", max_len = 4))),
    )
)]
pub struct BoundedBlobEntity {}

///
/// DatabaseDefaultEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/entity.rs::entity::6",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "rank", ident = "rank", value(item(prim = "Int32")), default = 7i32),
        field(source_key = "label", ident = "label",
            value(item(prim = "Text", max_len = 8)),
            default = "unknown"
        ),
    )
)]
pub struct DatabaseDefaultEntity {}

///
/// ExternalPrimaryKeyEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/entity.rs::entity::7",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["pid"], source = "external"),
    fields(
        field(source_key = "pid", ident = "pid", value(item(prim = "Principal")), default = "2vxsx-fae"),
        field(source_key = "a", ident = "a", value(item(prim = "Int32")), default = 7),
    )
)]
pub struct ExternalPrimaryKeyEntity {}

///
/// CompositePrimaryKeyEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/entity.rs::entity::8",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["tenant_id", "local_id"]),
    fields(
        field(source_key = "tenant_id", ident = "tenant_id", value(item(prim = "Nat64")), default = 1u64),
        field(source_key = "local_id", ident = "local_id", value(item(prim = "Nat64")), default = 2u64),
        field(source_key = "rank", ident = "rank", value(item(prim = "Int32")), default = 7),
    )
)]
pub struct CompositePrimaryKeyEntity {}
