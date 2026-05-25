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

///
/// Int128RelationTarget
///

#[entity(
    store = "TestStore",
    pk(fields = ["id"]),
    fields(field(ident = "id", value(item(prim = "Int128"))))
)]
pub struct Int128RelationTarget;

///
/// Nat128RelationTarget
///

#[entity(
    store = "TestStore",
    pk(fields = ["id"]),
    fields(field(ident = "id", value(item(prim = "Nat128"))))
)]
pub struct Nat128RelationTarget;

///
/// Int128RelationOwner
///

#[entity(
    store = "TestStore",
    pk(fields = ["id"]),
    fields(
        field(
            ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(ident = "target_id", value(item(rel = "Int128RelationTarget", prim = "Int128")))
    )
)]
pub struct Int128RelationOwner;

///
/// Nat128RelationOwner
///

#[entity(
    store = "TestStore",
    pk(fields = ["id"]),
    fields(
        field(
            ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(ident = "target_id", value(item(rel = "Nat128RelationTarget", prim = "Nat128")))
    )
)]
pub struct Nat128RelationOwner;
