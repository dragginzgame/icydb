use crate::macro_schema::test::TestStore;
use icydb::design::prelude::*;

///
/// User
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct User;

///
/// UserProjects
///

#[entity(
    store = "TestStore",
    pk(field = "user_id"),
    fields(field(ident = "user_id", value(item(rel = "User", prim = "Ulid"))))
)]
pub struct UserProjects;
