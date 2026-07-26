use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// Indexable
///

#[entity(source_key = "schema/test/fixtures/src/e2e/index.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.1", fields = ["pid", "ulid", "score"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "pid", ident = "pid", value(item(prim = "Principal"))),
        field(source_key = "ulid", ident = "ulid", value(item(prim = "Ulid"))),
        field(source_key = "score", ident = "score", value(item(prim = "Nat32"))),
    )
)]
pub struct Indexable {}

///
/// NotIndexable
///

#[entity(source_key = "schema/test/fixtures/src/e2e/index.rs::entity::2",
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
        field(source_key = "pid", ident = "pid", value(item(prim = "Principal"))),
        field(source_key = "ulid", ident = "ulid", value(item(prim = "Ulid"))),
        field(source_key = "score", ident = "score", value(item(prim = "Nat32"))),
    )
)]
pub struct NotIndexable {}

///
/// IndexableOptText
///

#[entity(source_key = "schema/test/fixtures/src/e2e/index.rs::entity::3",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.2", fields = ["username"], unique),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "username", ident = "username", value(opt, item(prim = "Text", unbounded))),
    )
)]
pub struct IndexableOptText {}
