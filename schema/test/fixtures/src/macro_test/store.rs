use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// StoreTestEntity
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id", value(item(prim = "Nat64")))),
    timestamps
)]
pub struct StoreTestEntity {}
