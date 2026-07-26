use crate::schema::test::TestStore;
use icydb::{base, design::prelude::*};

///
/// SimpleEntity
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ))
)]
pub struct SimpleEntity {}

///
/// BlobEntity
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::2",
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
        field(source_key = "bytes", ident = "bytes", value(item(prim = "Blob", unbounded)))
    )
)]
pub struct BlobEntity {}

///
/// Searchable
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::3",
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
        field(source_key = "name", ident = "name", value(item(prim = "Text", unbounded))),
        field(source_key = "description", ident = "description", value(item(prim = "Text", unbounded)))
    )
)]
pub struct Searchable {}

///
/// Limit
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::4",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["value"]),
    fields(field(source_key = "value", ident = "value", value(item(prim = "Nat32"))))
)]
pub struct Limit {}

///
/// DecodedDataStoreKeyOrder
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::5",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ))
)]
pub struct DecodedDataStoreKeyOrder {}

///
/// MissingFieldSmall
///

#[record(
    source_key = "schema/test/fixtures/src/e2e/db.rs::record::1",
    fields(
        field(source_key = "a", ident = "a", value(item(prim = "Ulid"))),
        field(source_key = "b", ident = "b", value(item(prim = "Ulid"))),
    )
)]
pub struct MissingFieldSmall {}

///
/// MissingFieldLarge
///

#[record(
    source_key = "schema/test/fixtures/src/e2e/db.rs::record::2",
    fields(
        field(source_key = "a", ident = "a", value(item(prim = "Ulid"))),
        field(source_key = "b", ident = "b", value(item(prim = "Ulid"))),
        field(source_key = "c", ident = "c", value(item(prim = "Ulid"))),
    )
)]
pub struct MissingFieldLarge {}

///
/// ContainsBlob
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::6",
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
        field(source_key = "bytes", ident = "bytes", value(opt, item(prim = "Blob", unbounded)))
    )
)]
pub struct ContainsBlob {}

///
/// ContainsOpts
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::7",
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
        field(source_key = "opt_a", ident = "opt_a", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_b", ident = "opt_b", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_c", ident = "opt_c", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_d", ident = "opt_d", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_e", ident = "opt_e", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_f", ident = "opt_f", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_g", ident = "opt_g", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_h", ident = "opt_h", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_i", ident = "opt_i", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_j", ident = "opt_j", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_k", ident = "opt_k", value(opt, item(prim = "Principal"))),
        field(source_key = "opt_l", ident = "opt_l", value(opt, item(prim = "Principal")))
    )
)]
pub struct ContainsOpts {}

///
/// ContainsManyRelations
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::8",
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
        field(source_key = "a_ids", ident = "a_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(source_key = "b_ids", ident = "b_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(source_key = "c_ids", ident = "c_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(source_key = "d_ids", ident = "d_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(source_key = "e_ids", ident = "e_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(source_key = "f_ids", ident = "f_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(source_key = "g_ids", ident = "g_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(source_key = "h_ids", ident = "h_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(source_key = "i_ids", ident = "i_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        ),
        field(source_key = "j_ids", ident = "j_ids",
            value(many, item(rel = "ContainsBlob", prim = "Ulid"))
        )
    )
)]
pub struct ContainsManyRelations {}

///
/// Index
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::9",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.1", fields = ["x"]),
    index(source_key = "index.2", fields = ["y"], unique),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "x", ident = "x", value(item(prim = "Int32"))),
        field(source_key = "y", ident = "y", value(item(prim = "Int32")))
    )
)]
pub struct Index {}

impl Index {
    #[must_use]
    pub fn new(x: i32, y: i32) -> Self {
        Self {
            id: Ulid::generate(),
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
    source_key = "schema/test/fixtures/src/e2e/db.rs::newtype::1",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(normalizer(path = "base::normalizer::text::case::Lower"))
)]
pub struct LowerIndexText {}

///
/// IndexNormalized
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::10",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.3", fields = ["username"], unique),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "username", ident = "username", value(item(is = "LowerIndexText"))),
        field(source_key = "score", ident = "score", value(item(prim = "Int32")))
    )
)]
pub struct IndexNormalized {}

///
/// IndexRelation
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::11",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.4", fields = ["create_blob_id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "create_blob_id", ident = "create_blob_id",
            value(item(rel = "BlobEntity", prim = "Ulid"))
        )
    )
)]
pub struct IndexRelation {}

///
/// IndexUniqueOpt
///

#[entity(source_key = "schema/test/fixtures/src/e2e/db.rs::entity::12",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.5", fields = ["value"], unique),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "value", ident = "value", value(opt, item(prim = "Nat8")))
    )
)]
pub struct IndexUniqueOpt {}
