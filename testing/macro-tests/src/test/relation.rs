#[cfg(test)]
use crate::prelude::*;

pub use icydb_testing_test_fixtures::macro_test::relation::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::model::field::FieldKind;
    use icydb::traits::{EntityDeclaration, EntityKey};

    fn assert_entity_key_type<T>()
    where
        T: EntityKey<Key = Ulid>,
    {
    }

    #[test]
    fn relation_fields_use_primitive_key_storage_types() {
        assert_entity_key_type::<HasRelation>();

        let entity = HasRelation {
            id: Ulid::generate(),
            a_id: test_ulid(1, 2),
            b_id: 7u16,
            c_id: Principal::anonymous(),
            created_at: icydb::types::Timestamp::default(),
            updated_at: icydb::types::Timestamp::default(),
        };

        let _: Ulid = entity.a_id;
        let _: u16 = entity.b_id;
        let _: Principal = entity.c_id;

        let row: HasRelation = entity;
        assert_eq!(row.a_id, test_ulid(1, 2));
    }

    #[test]
    fn relation_declarations_retain_target_metadata() {
        let relation_target = |field_name| {
            let field = HasRelation::MODEL
                .fields()
                .iter()
                .find(|field| field.name() == field_name)
                .expect("relation field should be present");

            match field.kind() {
                FieldKind::Relation {
                    target_entity_name, ..
                } => target_entity_name,
                _ => panic!("relation field should retain relation metadata"),
            }
        };

        assert_eq!(relation_target("a_id"), "EntityA");
        assert_eq!(relation_target("b_id"), "EntityB");
        assert_eq!(relation_target("c_id"), "EntityC");
    }

    #[test]
    fn relation_many_field_uses_primitive_collection_type() {
        let _row: HasManyRelation = HasManyRelation {
            id: Ulid::generate(),
            a_ids: vec![test_ulid(2, 2)],
            created_at: icydb::types::Timestamp::default(),
            updated_at: icydb::types::Timestamp::default(),
        };
    }

    #[test]
    fn entity_relation_fields_stay_plain_primitive_storage() {
        let owner_key = test_ulid(3, 1);
        let row: RelationOwned = RelationOwned {
            id: Ulid::generate(),
            owner_id: owner_key,
            created_at: icydb::types::Timestamp::default(),
            updated_at: icydb::types::Timestamp::default(),
        };

        let _: Ulid = row.owner_id;
        assert_eq!(row.owner_id, owner_key);
    }

    #[test]
    fn entity_many_relation_fields_stay_plain_key_collections() {
        let owner_a = test_ulid(4, 20);
        let owner_b = test_ulid(4, 21);
        let mut row: HasManyRelation = HasManyRelation {
            id: Ulid::generate(),
            a_ids: vec![owner_a],
            created_at: icydb::types::Timestamp::default(),
            updated_at: icydb::types::Timestamp::default(),
        };

        row.a_ids.push(owner_b);
        assert_eq!(row.a_ids, vec![owner_a, owner_b]);

        row.a_ids.retain(|existing| *existing != owner_a);
        assert_eq!(row.a_ids, vec![owner_b]);
    }

    #[test]
    fn plural_relation_field_keeps_declared_storage_name() {
        let order_key = test_ulid(4, 10);
        let row: HasPluralRelation = HasPluralRelation {
            id: Ulid::generate(),
            orders_ids: vec![order_key],
            created_at: icydb::types::Timestamp::default(),
            updated_at: icydb::types::Timestamp::default(),
        };

        assert_eq!(row.orders_ids, vec![order_key]);
    }

    #[test]
    fn record_relation_fields_stay_plain_storage_members() {
        let owner_a = test_ulid(5, 1);
        let owner_b = test_ulid(5, 2);

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
