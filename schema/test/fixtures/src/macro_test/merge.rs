use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// MergeEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/merge.rs::entity::1",
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
        field(source_key = "score", ident = "score", value(item(prim = "Nat32"))),
        field(source_key = "nickname", ident = "nickname", value(opt, item(prim = "Text", unbounded))),
        field(source_key = "scores", ident = "scores", value(many, item(prim = "Nat32"))),
        field(source_key = "tags", ident = "tags", value(item(is = "MergeTags"))),
        field(source_key = "settings", ident = "settings", value(item(is = "MergeSettings"))),
        field(source_key = "profile", ident = "profile", value(item(is = "MergeProfile"))),
        field(source_key = "wrapper", ident = "wrapper", value(item(is = "MergeWrapper"))),
        field(source_key = "tuple_field", ident = "tuple_field", value(item(is = "MergeTuple"))),
        field(source_key = "opt_profile", ident = "opt_profile", value(opt, item(is = "MergeProfile")))
    )
)]
pub struct MergeEntity {}

///
/// MergeSettings
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/merge.rs::map::1",
    key(prim = "Text", unbounded),
    value(item(prim = "Nat32"))
)]
pub struct MergeSettings {}

///
/// MergeTags
///

#[set(
    source_key = "schema/test/fixtures/src/macro_test/merge.rs::set::1",
    item(prim = "Text", unbounded)
)]
pub struct MergeTags {}

///
/// MergeProfile
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/merge.rs::record::1",
    fields(
        field(
            source_key = "bio",
            ident = "bio",
            value(item(prim = "Text", unbounded))
        ),
        field(source_key = "visits", ident = "visits", value(item(prim = "Nat32"))),
        field(
            source_key = "favorite_numbers",
            ident = "favorite_numbers",
            value(many, item(prim = "Nat32"))
        )
    )
)]
pub struct MergeProfile {}

///
/// MergeWrapper
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/merge.rs::newtype::1",
    item(is = "MergeProfile")
)]
pub struct MergeWrapper {}

///
/// MergeTuple
///

#[tuple(
    source_key = "schema/test/fixtures/src/macro_test/merge.rs::tuple::1",
    value(item(prim = "Text", unbounded)),
    value(item(prim = "Nat32"))
)]
pub struct MergeTuple {}
