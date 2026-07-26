use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// StoreTestEntity
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/store.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id", value(item(prim = "Nat64"))))
)]
pub struct StoreTestEntity {}
