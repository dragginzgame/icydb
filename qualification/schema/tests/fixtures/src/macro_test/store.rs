use icydb::model::prelude::*;

///
/// StoreTestEntity
///

#[entity(store = "crate::schema::test::TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id", value(item(prim = "Nat64")))),
    timestamps
)]
pub struct StoreTestEntity {}
