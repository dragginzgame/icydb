use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// ViewIntoRoundTrip
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/view_into.rs::entity::1",
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
        field(source_key = "tags", ident = "tags", value(many, item(prim = "Text", unbounded))),
        field(source_key = "nickname", ident = "nickname", value(opt, item(prim = "Text", unbounded)))
    )
)]
pub struct ViewIntoRoundTrip {}
