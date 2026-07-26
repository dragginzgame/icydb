use crate::schema::{relation::RelationDataStore, test::TestStore};
use icydb::design::prelude::*;

///
/// HasRelation
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::1",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "a_id", ident = "a_id", value(item(rel = "EntityA", prim = "Ulid"))),
        field(source_key = "b_id", ident = "b_id", value(item(rel = "EntityB", prim = "Nat16"))),
        field(source_key = "c_id", ident = "c_id", value(item(rel = "EntityC", prim = "Principal"))),
    )
)]
pub struct HasRelation;

///
/// HasManyRelation
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::2",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "a_ids", ident = "a_ids", value(many, item(rel = "EntityA", prim = "Ulid"))),
    )
)]
pub struct HasManyRelation;

///
/// HasPluralRelation
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::3",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "orders_ids", ident = "orders_ids", value(many, item(rel = "Orders", prim = "Ulid"))),
    )
)]
pub struct HasPluralRelation;

///
/// EntityA
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::4",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ))
)]
pub struct EntityA;

///
/// EntityB
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::5",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id", value(item(prim = "Nat16"))))
)]
pub struct EntityB;

///
/// EntityC
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::6",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id", value(item(prim = "Principal"))))
)]
pub struct EntityC;

///
/// Orders
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::7",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ))
)]
pub struct Orders;

///
/// RelationOwner
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::8",
    store = "RelationDataStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(source_key = "id", ident = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ))
)]
pub struct RelationOwner;

///
/// RelationOwned
///

#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::9",
    store = "RelationDataStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "owner_id", ident = "owner_id", value(item(rel = "RelationOwner", prim = "Ulid"))),
    )
)]
pub struct RelationOwned;

///
/// RelationRecord
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/relation.rs::record::1",
    fields(
        field(
            source_key = "owner_id",
            ident = "owner_id",
            value(item(rel = "RelationOwner", prim = "Ulid"))
        ),
        field(
            source_key = "optional_owner_id",
            ident = "optional_owner_id",
            value(opt, item(rel = "RelationOwner", prim = "Ulid"))
        ),
        field(
            source_key = "many_owners_ids",
            ident = "many_owners_ids",
            value(many, item(rel = "RelationOwner", prim = "Ulid"))
        ),
    )
)]
pub struct RelationRecord;

///
/// CrossCanisterRelation
///

#[cfg(test)]
#[entity(source_key = "schema/test/fixtures/src/macro_test/relation.rs::entity::10",
    store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "owner_id", ident = "owner_id", value(item(rel = "RelationOwner", prim = "Ulid"))),
    )
)]
pub struct CrossCanisterRelation;
