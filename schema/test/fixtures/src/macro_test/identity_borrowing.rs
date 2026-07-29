use crate::schema::test::TestStore;
use icydb_model::prelude::*;

///
/// User
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    )),
    timestamps
)]
pub struct User;

///
/// UserProjects
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["user_id"]),
    fields(field(name = "user_id", value(item(rel = "User", prim = "Ulid")))),
    timestamps
)]
pub struct UserProjects;

///
/// Int128RelationTarget
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id", value(item(prim = "Int128")))),
    timestamps
)]
pub struct Int128RelationTarget;

///
/// Nat128RelationTarget
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id", value(item(prim = "Nat128")))),
    timestamps
)]
pub struct Nat128RelationTarget;

///
/// Int128RelationOwner
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "target_id", value(item(rel = "Int128RelationTarget", prim = "Int128")))
    ),
    timestamps
)]
pub struct Int128RelationOwner;

///
/// Nat128RelationOwner
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "target_id", value(item(rel = "Nat128RelationTarget", prim = "Nat128")))
    ),
    timestamps
)]
pub struct Nat128RelationOwner;
