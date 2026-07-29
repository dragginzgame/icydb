use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// ViewIntoRoundTrip
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
        field(name = "tags", value(many, item(prim = "Text", unbounded))),
        field(name = "nickname", value(opt, item(prim = "Text", unbounded)))
    ),
    timestamps
)]
pub struct ViewIntoRoundTrip {}
