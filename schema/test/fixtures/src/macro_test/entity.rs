use crate::{macro_test::normalize::case::LowerCaseText, schema::test::TestStore};
use icydb_model::prelude::*;

///
/// Entity
///

#[entity(
    store = "TestStore",
    version = 1,
    pk(field = "id"),
    index(field = "a"),
    fields(
        field(
            name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "a", value(item(prim = "Int32")), default = 3),
    ),
    timestamps
)]
pub struct Entity {}

///
/// UnitKey
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id", value(item(prim = "Unit"))),
        field(name = "a", value(item(prim = "Int32")), default = 3),
    ),
    timestamps
)]
pub struct UnitKey {}

///
/// RenamedEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    )),
    timestamps(
        created_at(name = "inserted_at"),
        updated_at(name = "modified_at")
    )
)]
pub struct RenamedEntity {}

///
/// BoundedTextEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "name", value(item(prim = "Text", max_len = 12))),
    ),
    timestamps
)]
pub struct BoundedTextEntity {}

///
/// BoundedBlobEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "payload", value(item(prim = "Blob", max_len = 4))),
    ),
    timestamps
)]
pub struct BoundedBlobEntity {}

///
/// DatabaseDefaultEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "rank", value(item(prim = "Int32")), default = 7i32),
        field(name = "label",
            value(item(prim = "Text", max_len = 8)),
            default = "unknown"
        ),
    ),
    timestamps
)]
pub struct DatabaseDefaultEntity {}

///
/// ExternalPrimaryKeyEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["pid"], source = "external"),
    fields(
        field(name = "pid", value(item(prim = "Principal")), default = "2vxsx-fae"),
        field(name = "a", value(item(prim = "Int32")), default = 7),
    ),
    timestamps
)]
pub struct ExternalPrimaryKeyEntity {}

///
/// CompositePrimaryKeyEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["tenant_id", "local_id"]),
    fields(
        field(name = "tenant_id", value(item(prim = "Nat64")), default = 1u64),
        field(name = "local_id", value(item(prim = "Nat64")), default = 2u64),
        field(name = "rank", value(item(prim = "Int32")), default = 7),
    ),
    timestamps
)]
pub struct CompositePrimaryKeyEntity {}

/// Compile proof for nullable, normalized and compound generated index fields.
/// These combinations share one maintained declaration instead of separate
/// E2E/admin schemas without behavioral consumers.
#[entity(
    store = "TestStore",
    version = 1,
    pk(field = "id"),
    index(field = "optional_code", unique),
    index(field = "optional_label", unique),
    index(field = "username", unique),
    index(fields = ["owner_id", "owner_principal", "score"]),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "optional_code", value(opt, item(prim = "Nat8"))),
        field(name = "optional_label", value(opt, item(prim = "Text", unbounded))),
        field(name = "username", value(item(is = "LowerCaseText"))),
        field(name = "owner_id", value(item(rel = "Entity", prim = "Ulid"))),
        field(name = "owner_principal", value(item(prim = "Principal"))),
        field(name = "score", value(item(prim = "Nat32"))),
        field(name = "payload", value(opt, item(prim = "Blob", unbounded)))
    ),
    timestamps
)]
pub struct IndexFieldCombinations {}
