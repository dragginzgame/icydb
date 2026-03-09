#[cfg(test)]
mod tests;

use crate::prelude::*;

///
/// MergeEntity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text"))),
        field(ident = "score", value(item(prim = "Nat32"))),
        field(ident = "nickname", value(opt, item(prim = "Text"))),
        field(ident = "scores", value(many, item(prim = "Nat32"))),
        field(ident = "tags", value(item(is = "MergeTags"))),
        field(ident = "settings", value(item(is = "MergeSettings"))),
        field(ident = "profile", value(item(is = "MergeProfile"))),
        field(ident = "wrapper", value(item(is = "MergeWrapper"))),
        field(ident = "tuple_field", value(item(is = "MergeTuple"))),
        field(ident = "opt_profile", value(opt, item(is = "MergeProfile")))
    )
)]
pub struct MergeEntity {}

///
/// MergeSettings
///

#[map(key(prim = "Text"), value(item(prim = "Nat32")))]
pub struct MergeSettings {}

///
/// MergeTags
///

#[set(item(prim = "Text"))]
pub struct MergeTags {}

///
/// MergeProfile
///

#[record(fields(
    field(ident = "bio", value(item(prim = "Text"))),
    field(ident = "visits", value(item(prim = "Nat32"))),
    field(ident = "favorite_numbers", value(many, item(prim = "Nat32")))
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

#[tuple(value(item(prim = "Text")), value(item(prim = "Nat32")))]
pub struct MergeTuple {}
