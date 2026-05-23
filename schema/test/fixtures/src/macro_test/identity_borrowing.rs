use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// User
///

#[entity(
    store = "TestStore",
    pk(fields = ["id"]),
    fields(field(
        ident = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ))
)]
pub struct User;

///
/// UserProjects
///

#[entity(
    store = "TestStore",
    pk(fields = ["user_id"]),
    fields(field(ident = "user_id", value(item(rel = "User", prim = "Ulid"))))
)]
pub struct UserProjects;
