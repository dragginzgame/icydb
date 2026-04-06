use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// StoreTestEntity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Nat64"))))
)]
pub struct StoreTestEntity {}
