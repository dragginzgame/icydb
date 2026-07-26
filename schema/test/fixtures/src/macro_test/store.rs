use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// StoreTestEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/store.rs::entity::1",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id", value(item(prim = "Nat64"))))
)]
pub struct StoreTestEntity {}
