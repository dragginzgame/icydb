use crate::prelude::*;

///
/// SimpleEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct SimpleEntity {}

///
/// BlobEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "bytes", value(item(prim = "Blob")))
    )
)]
pub struct BlobEntity {}

///
/// Searchable
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "description", value(item(prim = "Text")))
    )
)]
pub struct Searchable {}

///
/// Limit
///

#[entity(
    store = "TestDataStore",
    pk = "value",
    fields(field(ident = "value", value(item(prim = "Nat32"))))
)]
pub struct Limit {}

///
/// DataKeyOrder
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct DataKeyOrder {}

///
/// MissingFieldSmall
///

#[record(fields(
    field(ident = "a_id", value(item(prim = "Ulid"))),
    field(ident = "b_id", value(item(prim = "Ulid"))),
))]
pub struct MissingFieldSmall {}

///
/// MissingFieldLarge
///

#[record(fields(
    field(ident = "a_id", value(item(prim = "Ulid"))),
    field(ident = "b_id", value(item(prim = "Ulid"))),
    field(ident = "c_id", value(item(prim = "Ulid"))),
))]
pub struct MissingFieldLarge {}

///
/// ContainsBlob
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "bytes", value(opt, item(prim = "Blob")))
    )
)]
pub struct ContainsBlob {}

///
/// ContainsOpts
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "opt_a", value(opt, item(prim = "Principal"))),
        field(ident = "opt_b", value(opt, item(prim = "Principal"))),
        field(ident = "opt_c", value(opt, item(prim = "Principal"))),
        field(ident = "opt_d", value(opt, item(prim = "Principal"))),
        field(ident = "opt_e", value(opt, item(prim = "Principal"))),
        field(ident = "opt_f", value(opt, item(prim = "Principal"))),
        field(ident = "opt_g", value(opt, item(prim = "Principal"))),
        field(ident = "opt_h", value(opt, item(prim = "Principal"))),
        field(ident = "opt_i", value(opt, item(prim = "Principal"))),
        field(ident = "opt_j", value(opt, item(prim = "Principal"))),
        field(ident = "opt_k", value(opt, item(prim = "Principal"))),
        field(ident = "opt_l", value(opt, item(prim = "Principal")))
    )
)]
pub struct ContainsOpts {}

///
/// ContainsManyRelations
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "a_ids", value(many, item(rel = "ContainsBlob"))),
        field(ident = "b_ids", value(many, item(rel = "ContainsBlob"))),
        field(ident = "c_ids", value(many, item(rel = "ContainsBlob"))),
        field(ident = "d_ids", value(many, item(rel = "ContainsBlob"))),
        field(ident = "e_ids", value(many, item(rel = "ContainsBlob"))),
        field(ident = "f_ids", value(many, item(rel = "ContainsBlob"))),
        field(ident = "g_ids", value(many, item(rel = "ContainsBlob"))),
        field(ident = "h_ids", value(many, item(rel = "ContainsBlob"))),
        field(ident = "i_ids", value(many, item(rel = "ContainsBlob"))),
        field(ident = "j_ids", value(many, item(rel = "ContainsBlob")))
    )
)]
pub struct ContainsManyRelations {}

///
/// Index
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    index(store = "TestIndexStore", fields = "x"),
    index(store = "TestIndexStore", fields = "y", unique),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "x", value(item(prim = "Int32"))),
        field(ident = "y", value(item(prim = "Int32")))
    )
)]
pub struct Index {}

impl Index {
    #[must_use]
    pub fn new(x: i32, y: i32) -> Self {
        Self {
            x,
            y,
            ..Default::default()
        }
    }
}

///
/// LowerIndexText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(sanitizer(path = "base::sanitizer::text::case::Lower"))
)]
pub struct LowerIndexText {}

///
/// IndexSanitized
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    index(store = "TestIndexStore", fields = "username", unique),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "username", value(item(is = "LowerIndexText"))),
        field(ident = "score", value(item(prim = "Int32")))
    )
)]
pub struct IndexSanitized {}

///
/// IndexRelation
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    index(store = "TestIndexStore", fields = "create_blob_id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "create_blob_id", value(item(rel = "BlobEntity")))
    )
)]
pub struct IndexRelation {}

///
/// IndexUniqueOpt
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    index(store = "TestIndexStore", fields = "value", unique),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "value", value(opt, item(prim = "Nat8")))
    )
)]
pub struct IndexUniqueOpt {}
