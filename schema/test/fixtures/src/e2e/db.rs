use crate::schema::test::TestStore;
use icydb_model::{base, prelude::*};

///
/// SimpleEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    )),
    timestamps
)]
pub struct SimpleEntity {}

///
/// BlobEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "bytes", value(item(prim = "Blob", unbounded)))
    ),
    timestamps
)]
pub struct BlobEntity {}

///
/// Searchable
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(name = "description", value(item(prim = "Text", unbounded)))
    ),
    timestamps
)]
pub struct Searchable {}

///
/// Limit
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["value"]),
    fields(field(name = "value", value(item(prim = "Nat32")))),
    timestamps
)]
pub struct Limit {}

///
/// DecodedDataStoreKeyOrder
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    )),
    timestamps
)]
pub struct DecodedDataStoreKeyOrder {}

///
/// MissingFieldSmall
///

#[record(fields(
    field(name = "a", value(item(prim = "Ulid"))),
    field(name = "b", value(item(prim = "Ulid"))),
))]
pub struct MissingFieldSmall {}

///
/// MissingFieldLarge
///

#[record(fields(
    field(name = "a", value(item(prim = "Ulid"))),
    field(name = "b", value(item(prim = "Ulid"))),
    field(name = "c", value(item(prim = "Ulid"))),
))]
pub struct MissingFieldLarge {}

///
/// ContainsBlob
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "bytes", value(opt, item(prim = "Blob", unbounded)))
    ),
    timestamps
)]
pub struct ContainsBlob {}

///
/// ContainsOpts
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "opt_a", value(opt, item(prim = "Principal"))),
        field(name = "opt_b", value(opt, item(prim = "Principal"))),
        field(name = "opt_c", value(opt, item(prim = "Principal"))),
        field(name = "opt_d", value(opt, item(prim = "Principal"))),
        field(name = "opt_e", value(opt, item(prim = "Principal"))),
        field(name = "opt_f", value(opt, item(prim = "Principal"))),
        field(name = "opt_g", value(opt, item(prim = "Principal"))),
        field(name = "opt_h", value(opt, item(prim = "Principal"))),
        field(name = "opt_i", value(opt, item(prim = "Principal"))),
        field(name = "opt_j", value(opt, item(prim = "Principal"))),
        field(name = "opt_k", value(opt, item(prim = "Principal"))),
        field(name = "opt_l", value(opt, item(prim = "Principal")))
    ),
    timestamps
)]
pub struct ContainsOpts {}

///
/// ContainsManyRelations
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "a_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(name = "b_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(name = "c_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(name = "d_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(name = "e_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(name = "f_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(name = "g_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(name = "h_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(name = "i_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(name = "j_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        )
    ),
    timestamps
)]
pub struct ContainsManyRelations {}

///
/// Index
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["x"]),
    index(fields = ["y"], unique),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "x", value(item(prim = "Int32"))),
        field(name = "y", value(item(prim = "Int32")))
    ),
    timestamps
)]
pub struct Index {}

impl Index {
    #[must_use]
    pub fn new(x: i32, y: i32) -> Self {
        Self {
            id: Ulid::nil(),
            x,
            y,
            created_at: Timestamp::default(),
            updated_at: Timestamp::default(),
        }
    }
}

///
/// LowerIndexText
///

#[newtype(
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Lower"))
)]
pub struct LowerIndexText {}

///
/// IndexNormalized
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["username"], unique),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "username", value(item(is = "LowerIndexText"))),
        field(name = "score", value(item(prim = "Int32")))
    ),
    timestamps
)]
pub struct IndexNormalized {}

///
/// IndexRelation
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["create_blob_id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "create_blob_id",
            value(item(rel = "BlobEntity", prim = "Ulid"))
        )
    ),
    timestamps
)]
pub struct IndexRelation {}

///
/// IndexUniqueOpt
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["value"], unique),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "value", value(opt, item(prim = "Nat8")))
    ),
    timestamps
)]
pub struct IndexUniqueOpt {}
