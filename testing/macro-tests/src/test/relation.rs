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
    fn entity_relation_fields_stay_plain_primitive_storage() {
        let owner_key = Ulid::from_parts(3, 1);
        let row: RelationOwned = RelationOwned {
            owner_id: owner_key,
            ..Default::default()
        };

        let _: Ulid = row.owner_id;
        assert_eq!(row.owner_id, owner_key);
    }

    #[test]
    fn entity_many_relation_fields_stay_plain_key_collections() {
        let owner_a = Ulid::from_parts(4, 20);
        let owner_b = Ulid::from_parts(4, 21);
        let mut row: HasManyRelation = HasManyRelation {
            a_ids: vec![owner_a],
            ..Default::default()
        };

        row.a_ids.push(owner_b);
        assert_eq!(row.a_ids, vec![owner_a, owner_b]);

        row.a_ids.retain(|existing| *existing != owner_a);
        assert_eq!(row.a_ids, vec![owner_b]);
    }

    #[test]
    fn plural_relation_field_keeps_declared_storage_name() {
        let order_key = Ulid::from_parts(4, 10);
        let row: HasPluralRelation = HasPluralRelation {
            orders_ids: vec![order_key],
            ..Default::default()
        };

        assert_eq!(row.orders_ids, vec![order_key]);
    }

    #[test]
    fn record_relation_fields_stay_plain_storage_members() {
        let owner_a = Ulid::from_parts(5, 1);
        let owner_b = Ulid::from_parts(5, 2);

        let record = RelationRecord {
            owner_id: owner_a,
            optional_owner_id: Some(owner_b),
            many_owners_ids: vec![owner_a, owner_b],
        };

        assert_eq!(record.owner_id, owner_a);
        assert_eq!(record.optional_owner_id, Some(owner_b));
        assert_eq!(record.many_owners_ids, vec![owner_a, owner_b]);
    }
}
