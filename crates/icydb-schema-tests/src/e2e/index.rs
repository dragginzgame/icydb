use crate::prelude::*;

///
/// Indexable
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    index(store = "TestIndexStore", fields = "pid, ulid, score"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "pid", value(item(prim = "Principal"))),
        field(ident = "ulid", value(item(prim = "Ulid"))),
        field(ident = "score", value(item(prim = "Nat32"))),
    )
)]
pub struct Indexable {}

///
/// NotIndexable
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "pid", value(item(prim = "Principal"))),
        field(ident = "ulid", value(item(prim = "Ulid"))),
        field(ident = "score", value(item(prim = "Nat32"))),
    )
)]
pub struct NotIndexable {}

///
/// IndexableOptText
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    index(store = "TestIndexStore", fields = "username", unique),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "username", value(opt, item(prim = "Text"))),
    )
)]
pub struct IndexableOptText {}
