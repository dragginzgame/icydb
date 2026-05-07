pub use icydb_testing_test_fixtures::macro_test::entity::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        model::field::{FieldDatabaseDefault, FieldKind},
        traits::{EntityKey, EntitySchema},
        types::Ulid,
    };

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
