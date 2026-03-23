pub use icydb_testing_fixtures::macro_test::identity_borrowing::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        traits::{EntityKey, EntityValue},
        types::{Id, Ulid},
    };

    fn assert_primary_key_type<T>()
    where
        T: EntityKey<Key = Ulid>,
    {
    }

    #[test]
    fn relation_primary_key_uses_declared_primitive_type() {
        assert_primary_key_type::<UserProjects>();
    }

    #[test]
    fn identity_keeps_typed_ids_without_generated_relation_accessors() {
        let _: fn(&UserProjects) -> Id<UserProjects> = UserProjects::id;

        let row = UserProjects {
            user_id: Ulid::from_parts(7, 1),
            ..Default::default()
        };
        let _: Ulid = row.user_id;
    }
}
