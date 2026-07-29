use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// Indexable
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["pid", "ulid", "score"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "pid", value(item(prim = "Principal"))),
        field(name = "ulid", value(item(prim = "Ulid"))),
        field(name = "score", value(item(prim = "Nat32"))),
    ),
    timestamps
)]
pub struct Indexable {}

///
/// NotIndexable
///

#[entity(
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "pid", value(item(prim = "Principal"))),
        field(name = "ulid", value(item(prim = "Ulid"))),
        field(name = "score", value(item(prim = "Nat32"))),
    ),
    timestamps
)]
pub struct NotIndexable {}

///
/// IndexableOptText
///

#[entity(
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["username"], unique),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "username", value(opt, item(prim = "Text", unbounded))),
    ),
    timestamps
)]
pub struct IndexableOptText {}
