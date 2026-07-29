use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// MergeEntity
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
        field(name = "score", value(item(prim = "Nat32"))),
        field(name = "nickname", value(opt, item(prim = "Text", unbounded))),
        field(name = "scores", value(many, item(prim = "Nat32"))),
        field(name = "tags", value(item(is = "MergeTags"))),
        field(name = "settings", value(item(is = "MergeSettings"))),
        field(name = "profile", value(item(is = "MergeProfile"))),
        field(name = "wrapper", value(item(is = "MergeWrapper"))),
        field(name = "tuple_field", value(item(is = "MergeTuple"))),
        field(name = "opt_profile", value(opt, item(is = "MergeProfile")))
    ),
    timestamps
)]
pub struct MergeEntity {}

///
/// MergeSettings
///

#[map(key(prim = "Text", unbounded), value(item(prim = "Nat32")))]
pub struct MergeSettings {}

///
/// MergeTags
///

#[set(item(prim = "Text", unbounded))]
pub struct MergeTags {}

///
/// MergeProfile
///

#[record(fields(
    field(name = "bio", value(item(prim = "Text", unbounded))),
    field(name = "visits", value(item(prim = "Nat32"))),
    field(name = "favorite_numbers", value(many, item(prim = "Nat32")))
))]
pub struct MergeProfile {}

///
/// MergeWrapper
///

#[newtype(item(is = "MergeProfile"))]
pub struct MergeWrapper {}

///
/// MergeTuple
///

#[tuple(value(item(prim = "Text", unbounded)), value(item(prim = "Nat32")))]
pub struct MergeTuple {}
