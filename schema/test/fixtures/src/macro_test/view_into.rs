use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// ViewIntoRoundTrip
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text", unbounded))),
        field(ident = "score", value(item(prim = "Nat32"))),
        field(ident = "tags", value(many, item(prim = "Text", unbounded))),
        field(ident = "nickname", value(opt, item(prim = "Text", unbounded)))
    )
)]
pub struct ViewIntoRoundTrip {}
