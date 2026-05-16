pub use icydb_testing_test_fixtures::macro_test::entity::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        db::DbSession,
        model::field::{FieldDatabaseDefault, FieldKind},
        traits::{EntityKey, EntitySchema},
        types::Ulid,
    };
    use icydb_testing_test_fixtures::schema::test::TestCanister;

    fn assert_entity_key<T>()
    where
        T: EntityKey<Key = Ulid>,
    {
    }

    #[test]
    fn internal_primary_key_uses_declared_field_type() {
        assert_entity_key::<Entity>();
    }

    #[test]
    fn entity_field_name_constants_are_generated() {
        assert_eq!(Entity::ID, "id");
        assert_eq!(Entity::A, "a");
        assert_eq!(BoundedTextEntity::NAME, "name");
    }

    #[allow(dead_code)]
    fn fluent_filter_eq_accepts_generated_field_constants(session: &DbSession<TestCanister>) {
        let _ = session
            .load::<Entity>()
            .filter_eq(Entity::A, 3_i32)
            .one_opt();
    }

    #[allow(dead_code)]
    fn fluent_filter_helpers_accept_generated_field_constants(session: &DbSession<TestCanister>) {
        let _ = session
            .load::<Entity>()
            .filter_ne(Entity::A, 4_i32)
            .filter_lt(Entity::A, 10_i32)
            .filter_lte(Entity::A, 10_i32)
            .filter_gt(Entity::A, 1_i32)
            .filter_gte(Entity::A, 1_i32)
            .filter_eq_field(Entity::A, Entity::A)
            .filter_ne_field(Entity::A, Entity::A)
            .filter_lt_field(Entity::A, Entity::A)
            .filter_lte_field(Entity::A, Entity::A)
            .filter_gt_field(Entity::A, Entity::A)
            .filter_gte_field(Entity::A, Entity::A)
            .filter_in(Entity::A, [1_i32, 2_i32])
            .filter_not_in(Entity::A, [3_i32, 4_i32])
            .filter_contains(Entity::A, 3_i32)
            .filter_is_null(Entity::A)
            .filter_is_not_null(Entity::A)
            .filter_is_missing(Entity::A)
            .filter_is_empty(Entity::A)
            .filter_is_not_empty(Entity::A)
            .filter_between(Entity::A, 1_i32, 10_i32)
            .filter_between_fields(Entity::A, Entity::A, Entity::A)
            .filter_not_between(Entity::A, 1_i32, 10_i32)
            .filter_not_between_fields(Entity::A, Entity::A, Entity::A)
            .one_opt();

        let _ = session
            .load::<BoundedTextEntity>()
            .filter_text_eq_ci(BoundedTextEntity::NAME, "Ada")
            .filter_text_contains(BoundedTextEntity::NAME, "da")
            .filter_text_contains_ci(BoundedTextEntity::NAME, "DA")
            .filter_text_starts_with(BoundedTextEntity::NAME, "A")
            .filter_text_starts_with_ci(BoundedTextEntity::NAME, "a")
            .filter_text_ends_with(BoundedTextEntity::NAME, "a")
            .filter_text_ends_with_ci(BoundedTextEntity::NAME, "A")
            .one_opt();
    }

    #[test]
    fn entity_name_defaults_and_override() {
        assert_eq!(<RenamedEntity as EntitySchema>::MODEL.name(), "Potato");
    }

    #[test]
    fn text_max_len_directive_reaches_entity_model() {
        let name = BoundedTextEntity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "name")
            .expect("bounded text field should be present");

        assert!(matches!(name.kind(), FieldKind::Text { max_len: Some(12) }));
    }

    #[test]
    fn blob_max_len_directive_reaches_entity_model() {
        let payload = BoundedBlobEntity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "payload")
            .expect("bounded blob field should be present");

        assert!(matches!(
            payload.kind(),
            FieldKind::Blob { max_len: Some(4) }
        ));
    }

    #[test]
    fn function_construction_defaults_do_not_become_database_defaults() {
        let generated_id_default = Entity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "id")
            .expect("field with function construction default should be present");

        assert_eq!(
            generated_id_default.database_default(),
            FieldDatabaseDefault::None,
        );
    }

    #[test]
    fn supported_literal_construction_defaults_reach_entity_model_as_slot_payloads() {
        let literal_default = Entity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "a")
            .expect("field with literal construction default should be present");

        assert_eq!(
            literal_default.database_default(),
            FieldDatabaseDefault::EncodedSlotPayload(&[0xFF, 0x01, 3, 0, 0, 0, 0, 0, 0, 0]),
        );
    }

    #[test]
    fn explicit_database_defaults_reach_entity_model_as_slot_payloads() {
        let rank = DatabaseDefaultEntity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "rank")
            .expect("rank field should be present");
        let label = DatabaseDefaultEntity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "label")
            .expect("label field should be present");

        assert_eq!(
            rank.database_default(),
            FieldDatabaseDefault::EncodedSlotPayload(&[0xFF, 0x01, 7, 0, 0, 0, 0, 0, 0, 0]),
        );
        assert_eq!(
            label.database_default(),
            FieldDatabaseDefault::EncodedSlotPayload(&[
                0xFF, 0x01, b'u', b'n', b'k', b'n', b'o', b'w', b'n'
            ]),
        );
    }
}
