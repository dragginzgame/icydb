use crate::prelude::*;

///
/// SimpleEntity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct SimpleEntity {}

///
/// BlobEntity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
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
    store = "TestStore",
    pk(field = "id"),
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
    store = "TestStore",
    pk(field = "value"),
    fields(field(ident = "value", value(item(prim = "Nat32"))))
)]
pub struct Limit {}

///
/// DataKeyOrder
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct DataKeyOrder {}

///
/// MissingFieldSmall
///

#[record(fields(
    field(ident = "a", value(item(prim = "Ulid"))),
    field(ident = "b", value(item(prim = "Ulid"))),
))]
pub struct MissingFieldSmall {}

///
/// MissingFieldLarge
///

#[record(fields(
    field(ident = "a", value(item(prim = "Ulid"))),
    field(ident = "b", value(item(prim = "Ulid"))),
    field(ident = "c", value(item(prim = "Ulid"))),
))]
pub struct MissingFieldLarge {}

///
/// ContainsBlob
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
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
    store = "TestStore",
    pk(field = "id"),
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
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "a", value(many, item(rel = "ContainsBlob", prim = "Ulid"))),
        field(ident = "b", value(many, item(rel = "ContainsBlob", prim = "Ulid"))),
        field(ident = "c", value(many, item(rel = "ContainsBlob", prim = "Ulid"))),
        field(ident = "d", value(many, item(rel = "ContainsBlob", prim = "Ulid"))),
        field(ident = "e", value(many, item(rel = "ContainsBlob", prim = "Ulid"))),
        field(ident = "f", value(many, item(rel = "ContainsBlob", prim = "Ulid"))),
        field(ident = "g", value(many, item(rel = "ContainsBlob", prim = "Ulid"))),
        field(ident = "h", value(many, item(rel = "ContainsBlob", prim = "Ulid"))),
        field(ident = "i", value(many, item(rel = "ContainsBlob", prim = "Ulid"))),
        field(ident = "j", value(many, item(rel = "ContainsBlob", prim = "Ulid")))
    )
)]
pub struct ContainsManyRelations {}

///
/// Index
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    index(fields = "x"),
    index(fields = "y", unique),
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
    store = "TestStore",
    pk(field = "id"),
    index(fields = "username", unique),
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
    store = "TestStore",
    pk(field = "id"),
    index(fields = "create_blob"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "create_blob", value(item(rel = "BlobEntity", prim = "Ulid")))
    )
)]
pub struct IndexRelation {}

///
/// IndexUniqueOpt
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    index(fields = "value", unique),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "value", value(opt, item(prim = "Nat8")))
    )
)]
pub struct IndexUniqueOpt {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{deserialize, serialize};

    #[test]
    fn missing_field_round_trip() {
        let small = MissingFieldSmall {
            a: Ulid::generate(),
            b: Ulid::generate(),
        };

        let bytes = serialize(&small).expect("serialize MissingFieldSmall");
        let large =
            deserialize::<MissingFieldLarge>(&bytes).expect("deserialize MissingFieldLarge");

        assert!(!large.a.is_nil());
        assert!(!large.b.is_nil());
        assert!(large.c.is_nil());
    }
}
