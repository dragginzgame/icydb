use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// ProjectionOrderEntity
///
/// Representative entity used to lock field-order alignment between
/// generated field declaration and typed-projection output.
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "title", value(item(prim = "Text", unbounded))),
        field(name = "score", value(item(prim = "Nat32"))),
        field(name = "nickname", value(opt, item(prim = "Text", unbounded))),
        field(name = "tags", value(many, item(prim = "Text", unbounded)))
    ),
    timestamps
)]
pub struct ProjectionOrderEntity {}
