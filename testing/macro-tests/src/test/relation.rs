#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_fixtures::macro_test::relation::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::traits::EntityKey;
    use icydb::types::Id;

    fn assert_entity_key_type<T>()
    where
        T: EntityKey<Key = Ulid>,
    {
    }

    #[test]
    fn relation_fields_use_primitive_key_storage_types() {
        assert_entity_key_type::<HasRelation>();

        let entity = HasRelation {
            a_id: Ulid::from_parts(1, 2),
            b_id: 7u16,
            c_id: Principal::anonymous(),
            ..Default::default()
        };

        let _: Ulid = entity.a_id;
        let _: u16 = entity.b_id;
        let _: Principal = entity.c_id;

        let row: HasRelation = entity;
        assert_eq!(row.a_id, Ulid::from_parts(1, 2));
    }

    #[test]
    fn relation_many_field_uses_primitive_collection_type() {
        let _row: HasManyRelation = HasManyRelation {
            a_ids: vec![Ulid::from_parts(2, 2)],
            ..Default::default()
        };
    }

    #[test]
    fn entity_relation_accessors_return_typed_ids() {
        let owner_key = Ulid::from_parts(3, 1);
        let row: RelationOwned = RelationOwned {
            owner_id: owner_key,
            ..Default::default()
        };
        let owner_id: Id<RelationOwner> = row.owner_id();
        assert_eq!(owner_id.key(), owner_key);
    }

    #[test]
    fn entity_relation_setter_accepts_typed_id() {
        let owner_key = Ulid::from_parts(3, 10);
        let mut row: RelationOwned = RelationOwned {
            owner_id: Ulid::from_parts(3, 12),
            ..Default::default()
        };

        row.set_owner_id(Id::from_key(owner_key));
        assert_eq!(row.owner_id().key(), owner_key);
    }

    #[test]
    fn entity_many_relation_accessors_return_typed_ids() {
        let owner_key = Ulid::from_parts(4, 1);
        let row: HasManyRelation = HasManyRelation {
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
        let mut row: HasManyRelation = HasManyRelation {
            a_ids: Vec::new(),
            ..Default::default()
        };

        row.add_a_id(Id::from_key(owner_a));
        row.add_a_id(Id::from_key(owner_b));
        assert_eq!(
            row.a_ids().map(|id| id.key()).collect::<Vec<_>>(),
            vec![owner_a, owner_b]
        );

        assert!(row.remove_a_id(Id::from_key(owner_a)));
        assert_eq!(
            row.a_ids().map(|id| id.key()).collect::<Vec<_>>(),
            vec![owner_b]
        );
        assert!(!row.remove_a_id(Id::from_key(owner_a)));
    }

    #[test]
    fn plural_relation_accessor_keeps_field_name_prefix() {
        let order_key = Ulid::from_parts(4, 10);
        let row: HasPluralRelation = HasPluralRelation {
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
        assert_eq!(
            optional_owner.map(|id: Id<RelationOwner>| id.key()),
            Some(owner_b)
        );

        let keys: Vec<Ulid> = record
            .many_owners_ids()
            .map(|id: Id<RelationOwner>| id.key())
            .collect();
        assert_eq!(keys, vec![owner_a, owner_b]);
    }
}
