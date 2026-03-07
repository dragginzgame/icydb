use crate::prelude::*;

///
/// HasRelation
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "a_id", value(item(rel = "EntityA", prim = "Ulid"))),
        field(ident = "b_id", value(item(rel = "EntityB", prim = "Nat16"))),
        field(ident = "c_id", value(item(rel = "EntityC", prim = "Principal"))),
    )
)]
pub struct HasRelation;

///
/// HasManyRelation
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "a_ids", value(many, item(rel = "EntityA", prim = "Ulid"))),
    )
)]
pub struct HasManyRelation;

///
/// HasPluralRelation
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "orders_ids", value(many, item(rel = "Orders", prim = "Ulid"))),
    )
)]
pub struct HasPluralRelation;

///
/// EntityA
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct EntityA;

///
/// EntityB
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Nat16"))))
)]
pub struct EntityB;

///
/// EntityC
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Principal"))))
)]
pub struct EntityC;

///
/// Orders
///

#[entity(
    store = "TestStore",
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
        field(ident = "owner_id", value(item(rel = "RelationOwner", prim = "Ulid"))),
    )
)]
pub struct RelationOwned;

///
/// RelationRecord
///

#[record(fields(
    field(ident = "owner_id", value(item(rel = "RelationOwner", prim = "Ulid"))),
    field(
        ident = "optional_owner_id",
        value(opt, item(rel = "RelationOwner", prim = "Ulid"))
    ),
    field(
        ident = "many_owners_ids",
        value(many, item(rel = "RelationOwner", prim = "Ulid"))
    ),
))]
pub struct RelationRecord;

///
/// CrossCanisterRelation
///

#[cfg(test)]
#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "owner_id", value(item(rel = "RelationOwner", prim = "Ulid"))),
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
            a_id: Ulid::from_parts(1, 2),
            b_id: 7u16,
            c_id: Principal::anonymous(),
            ..Default::default()
        };

        let _: Ulid = row.id;
        let _: Ulid = row.a_id;
        let _: u16 = row.b_id;
        let _: Principal = row.c_id;
    }

    #[test]
    fn relation_many_field_uses_primitive_collection_type() {
        let _ = HasManyRelation {
            id: Ulid::from_parts(2, 1),
            a_ids: vec![Ulid::from_parts(2, 2)],
            ..Default::default()
        };
    }

    #[test]
    fn entity_relation_accessors_return_typed_ids() {
        let owner_key = Ulid::from_parts(3, 1);
        let row = RelationOwned {
            id: Ulid::from_parts(3, 2),
            owner_id: owner_key,
            ..Default::default()
        };
        let owner_id: Id<RelationOwner> = row.owner_id();
        assert_eq!(owner_id.key(), owner_key);
    }

    #[test]
    fn entity_relation_setter_accepts_typed_id() {
        let owner_key = Ulid::from_parts(3, 10);
        let mut row = RelationOwned {
            id: Ulid::from_parts(3, 11),
            owner_id: Ulid::from_parts(3, 12),
            ..Default::default()
        };

        row.set_owner_id(Id::from_key(owner_key));
        assert_eq!(row.owner_id, owner_key);
    }

    #[test]
    fn entity_many_relation_accessors_return_typed_ids() {
        let owner_key = Ulid::from_parts(4, 1);
        let row = HasManyRelation {
            id: Ulid::from_parts(4, 2),
            a_ids: vec![owner_key],
            ..Default::default()
        };

        let ids: Vec<_> = row.a_ids().collect();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].key(), owner_key);
    }

    #[test]
    fn entity_many_relation_add_remove_helpers_use_typed_ids() {
        let owner_a = Ulid::from_parts(4, 20);
        let owner_b = Ulid::from_parts(4, 21);
        let mut row = HasManyRelation {
            id: Ulid::from_parts(4, 22),
            ..Default::default()
        };

        row.add_a_id(Id::from_key(owner_a));
        row.add_a_id(Id::from_key(owner_b));
        assert_eq!(row.a_ids, vec![owner_a, owner_b]);

        assert!(row.remove_a_id(Id::from_key(owner_a)));
        assert_eq!(row.a_ids, vec![owner_b]);
        assert!(!row.remove_a_id(Id::from_key(owner_a)));
    }

    #[test]
    fn plural_relation_accessor_keeps_field_name_prefix() {
        let order_key = Ulid::from_parts(4, 10);
        let row = HasPluralRelation {
            id: Ulid::from_parts(4, 11),
            orders_ids: vec![order_key],
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
            owner_id: owner_a,
            optional_owner_id: Some(owner_b),
            many_owners_ids: vec![owner_a, owner_b],
        };

        let owner_id: Id<RelationOwner> = record.owner_id();
        assert_eq!(owner_id.key(), owner_a);

        let optional_owner: Option<Id<RelationOwner>> = record.optional_owner_id();
        assert_eq!(optional_owner.map(|id| id.key()), Some(owner_b));

        let keys: Vec<Ulid> = record.many_owners_ids().map(|id| id.key()).collect();
        assert_eq!(keys, vec![owner_a, owner_b]);
    }
}
