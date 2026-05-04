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
    fn rust_construction_defaults_do_not_become_database_defaults() {
        let explicit_rust_default = Entity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "a")
            .expect("field with Rust construction default should be present");

        assert_eq!(
            explicit_rust_default.database_default(),
            FieldDatabaseDefault::None,
        );
    }
}
