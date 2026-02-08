use crate::prelude::*;

///
/// HasRelation
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "a", value(item(rel = "EntityA", prim = "Ulid"))),
        field(ident = "b", value(item(rel = "EntityB", prim = "Nat16"))),
        field(ident = "c", value(item(rel = "EntityC", prim = "Principal"))),
    )
)]
pub struct HasRelation;

///
/// HasManyRelation
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "a", value(many, item(rel = "EntityA", prim = "Ulid"))),
    )
)]
pub struct HasManyRelation;

///
/// EntityA
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct EntityA;

///
/// EntityB
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Nat16"))))
)]
pub struct EntityB;

///
/// EntityC
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Principal"))))
)]
pub struct EntityC;

///
/// RelationOwner
///

#[entity(
    store = "RelationDataStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct RelationOwner;

///
/// RelationOwned
///

#[entity(
    store = "RelationDataStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "owner", value(item(rel = "RelationOwner", prim = "Ulid"))),
    )
)]
pub struct RelationOwned;

///
/// CrossCanisterRelation
///

#[cfg(test)]
#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "owner", value(item(rel = "RelationOwner", prim = "Ulid"))),
    )
)]
pub struct CrossCanisterRelation;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relation_fields_use_primitive_key_storage_types() {
        let row = HasRelation {
            id: Ulid::from_parts(1, 1),
            a: Ulid::from_parts(1, 2),
            b: 7u16,
            c: Principal::anonymous(),
            ..Default::default()
        };

        let _: Ulid = row.id;
        let _: Ulid = row.a;
        let _: u16 = row.b;
        let _: Principal = row.c;
    }

    #[test]
    fn relation_many_field_uses_primitive_collection_type() {
        let _ = HasManyRelation {
            id: Ulid::from_parts(2, 1),
            a: vec![Ulid::from_parts(2, 2)],
            ..Default::default()
        };
    }
}
