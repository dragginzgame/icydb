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
    fn relation_accessor_and_identity_keep_typed_ids() {
        let _: fn(&UserProjects) -> Id<User> = UserProjects::user_id;
        let _: fn(&UserProjects) -> Id<UserProjects> = UserProjects::id;
    }
}
