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
/// HasPluralRelation
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "orders", value(many, item(rel = "Orders", prim = "Ulid"))),
    )
)]
pub struct HasPluralRelation;

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
/// Orders
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct Orders;

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
/// RelationRecord
///

#[record(fields(
    field(ident = "owner", value(item(rel = "RelationOwner", prim = "Ulid"))),
    field(
        ident = "optional_owner",
        value(opt, item(rel = "RelationOwner", prim = "Ulid"))
    ),
    field(
        ident = "many_owners",
        value(many, item(rel = "RelationOwner", prim = "Ulid"))
    ),
))]
pub struct RelationRecord;

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

    #[test]
    fn entity_relation_accessors_return_typed_ids() {
        let owner_key = Ulid::from_parts(3, 1);
        let row = RelationOwned {
            id: Ulid::from_parts(3, 2),
            owner: owner_key,
            ..Default::default()
        };
        let owner_id: Id<RelationOwner> = row.owner_id();
        assert_eq!(owner_id.key(), owner_key);
    }

    #[test]
    fn entity_many_relation_accessors_return_typed_ids() {
        let owner_key = Ulid::from_parts(4, 1);
        let row = HasManyRelation {
            id: Ulid::from_parts(4, 2),
            a: vec![owner_key],
            ..Default::default()
        };

        let ids: Vec<_> = row.a_ids().collect();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].key(), owner_key);
    }

    #[test]
    fn plural_relation_accessor_keeps_field_name_prefix() {
        let order_key = Ulid::from_parts(4, 10);
        let row = HasPluralRelation {
            id: Ulid::from_parts(4, 11),
            orders: vec![order_key],
            ..Default::default()
        };

        let ids: Vec<_> = row.orders_ids().collect();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].key(), order_key);
    }

    #[test]
    fn record_relation_accessors_return_typed_ids() {
        let owner_a = Ulid::from_parts(5, 1);
        let owner_b = Ulid::from_parts(5, 2);
        let record = RelationRecord {
            owner: owner_a,
            optional_owner: Some(owner_b),
            many_owners: vec![owner_a, owner_b],
        };

        let owner_id: Id<RelationOwner> = record.owner_id();
        assert_eq!(owner_id.key(), owner_a);

        let optional_owner: Option<Id<RelationOwner>> = record.optional_owner_id();
        assert_eq!(optional_owner.map(|id| id.key()), Some(owner_b));

        let keys: Vec<Ulid> = record
            .many_owners_ids()
            .into_iter()
            .map(|id| id.key())
            .collect();
        assert_eq!(keys, vec![owner_a, owner_b]);
    }
}
