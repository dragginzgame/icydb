use crate::schema::test::TestStore;
use icydb::design::prelude::*;

///
/// User
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/identity_borrowing.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ))
)]
pub struct User;

///
/// UserProjects
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/identity_borrowing.rs::entity::2",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["user_id"]),
    fields(field(source_key = "user_id", ident = "user_id", value(item(rel = "User", prim = "Ulid"))))
)]
pub struct UserProjects;

///
/// Int128RelationTarget
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/identity_borrowing.rs::entity::3",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id", value(item(prim = "Int128"))))
)]
pub struct Int128RelationTarget;

///
/// Nat128RelationTarget
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/identity_borrowing.rs::entity::4",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id", value(item(prim = "Nat128"))))
)]
pub struct Nat128RelationTarget;

///
/// Int128RelationOwner
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/identity_borrowing.rs::entity::5",
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
        field(source_key = "target_id", ident = "target_id", value(item(rel = "Int128RelationTarget", prim = "Int128")))
    )
)]
pub struct Int128RelationOwner;

///
/// Nat128RelationOwner
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/identity_borrowing.rs::entity::6",
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
        field(source_key = "target_id", ident = "target_id", value(item(rel = "Nat128RelationTarget", prim = "Nat128")))
    )
)]
pub struct Nat128RelationOwner;
