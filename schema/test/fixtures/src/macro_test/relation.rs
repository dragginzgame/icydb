use crate::schema::{relation::RelationDataStore, test::TestStore};
use icydb_model::prelude::*;

///
/// HasRelation
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "a_id", value(item(rel = "EntityA", prim = "Ulid"))),
        field(name = "b_id", value(item(rel = "EntityB", prim = "Nat16"))),
        field(name = "c_id", value(item(rel = "EntityC", prim = "Principal"))),
    ),
    timestamps
)]
pub struct HasRelation;

///
/// HasManyRelation
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "a_ids", value(many, item(rel = "EntityA", prim = "Ulid"))),
    ),
    timestamps
)]
pub struct HasManyRelation;

///
/// HasPluralRelation
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "orders_ids", value(many, item(rel = "Orders", prim = "Ulid"))),
    ),
    timestamps
)]
pub struct HasPluralRelation;

///
/// EntityA
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
pub struct EntityA;

///
/// EntityB
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id", value(item(prim = "Nat16")))),
    timestamps
)]
pub struct EntityB;

///
/// EntityC
///

#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id", value(item(prim = "Principal")))),
    timestamps
)]
pub struct EntityC;

///
/// Orders
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
pub struct Orders;

///
/// RelationOwner
///

#[entity(store = "RelationDataStore",
    version = 1,
    pk(fields = ["id"]),
    fields(field(name = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    )),
    timestamps
)]
pub struct RelationOwner;

///
/// RelationOwned
///

#[entity(store = "RelationDataStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "owner_id", value(item(rel = "RelationOwner", prim = "Ulid"))),
    ),
    timestamps
)]
pub struct RelationOwned;

///
/// RelationRecord
///

#[record(fields(
    field(name = "owner_id", value(item(rel = "RelationOwner", prim = "Ulid"))),
    field(
        name = "optional_owner_id",
        value(opt, item(rel = "RelationOwner", prim = "Ulid"))
    ),
    field(
        name = "many_owners_ids",
        value(many, item(rel = "RelationOwner", prim = "Ulid"))
    ),
))]
pub struct RelationRecord;

///
/// CrossCanisterRelation
///

#[cfg(test)]
#[entity(store = "TestStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "owner_id", value(item(rel = "RelationOwner", prim = "Ulid"))),
    ),
    timestamps
)]
pub struct CrossCanisterRelation;
