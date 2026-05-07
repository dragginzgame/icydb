use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// Entity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(
            ident = "id",
            value(item(prim = "Ulid")),
            default = "Ulid::generate",
            generated(insert = "Ulid::generate")
        ),
        field(ident = "a", value(item(prim = "Int32")), default = 3),
    )
)]
pub struct Entity {}

///
/// UnitKey
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Unit"))),
        field(ident = "a", value(item(prim = "Int32")), default = 3),
    )
)]
pub struct UnitKey {}

///
/// RenamedEntity
///

#[entity(
    name = "Potato",
    store = "TestStore",
    pk(field = "id"),
    fields(field(
        ident = "id",
        value(item(prim = "Ulid")),
        default = "Ulid::generate",
        generated(insert = "Ulid::generate")
    ))
)]
pub struct RenamedEntity {}

///
/// BoundedTextEntity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(
            ident = "id",
            value(item(prim = "Ulid")),
            default = "Ulid::generate",
            generated(insert = "Ulid::generate")
        ),
        field(ident = "name", value(item(prim = "Text", max_len = 12))),
    )
)]
pub struct BoundedTextEntity {}

///
/// BoundedBlobEntity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(
            ident = "id",
            value(item(prim = "Ulid")),
            default = "Ulid::generate",
            generated(insert = "Ulid::generate")
        ),
        field(ident = "payload", value(item(prim = "Blob", max_len = 4))),
    )
)]
pub struct BoundedBlobEntity {}

///
/// DatabaseDefaultEntity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(
            ident = "id",
            value(item(prim = "Ulid")),
            default = "Ulid::generate",
            generated(insert = "Ulid::generate")
        ),
        field(
            ident = "rank",
            value(item(prim = "Int32")),
            default = 3i32,
            db_default = 7i32
        ),
        field(
            ident = "label",
            value(item(prim = "Text", max_len = 8)),
            default = "unknown"
        ),
    )
)]
pub struct DatabaseDefaultEntity {}

///
/// ExternalPrimaryKeyEntity
///

#[entity(
    store = "TestStore",
    pk(field = "pid", source = "external"),
    fields(
        field(
            ident = "pid",
            value(item(prim = "Principal")),
            default = "Principal::anonymous"
        ),
        field(ident = "a", value(item(prim = "Int32")), default = 7),
    )
)]
pub struct ExternalPrimaryKeyEntity {}
