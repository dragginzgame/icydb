pub use icydb_testing_fixtures::macro_test::entity::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
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
}
