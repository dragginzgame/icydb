pub use icydb_testing_test_fixtures::macro_test::identity_borrowing::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        traits::EntityKey,
        types::{Id, Ulid},
    };

    fn assert_primary_key_type<T>()
    where
        T: EntityKey<Key = Ulid>,
    {
    }

    fn assert_int128_primary_key_type<T>()
    where
        T: EntityKey<Key = i128>,
    {
    }

    fn assert_nat128_primary_key_type<T>()
    where
        T: EntityKey<Key = u128>,
    {
    }

    #[test]
    fn relation_primary_key_uses_declared_primitive_type() {
        assert_primary_key_type::<UserProjects>();
    }

    #[test]
    fn scalar_128_relation_targets_use_declared_primitive_type() {
        assert_int128_primary_key_type::<Int128RelationTarget>();
        assert_nat128_primary_key_type::<Nat128RelationTarget>();

        let _: fn(&Int128RelationOwner) -> Id<Int128RelationOwner> =
            <Int128RelationOwner as icydb::__macro::EntityValue>::id;
        let _: fn(&Nat128RelationOwner) -> Id<Nat128RelationOwner> =
            <Nat128RelationOwner as icydb::__macro::EntityValue>::id;
    }

    #[test]
    fn identity_keeps_typed_ids_without_generated_relation_accessors() {
        let _: fn(&UserProjects) -> Id<UserProjects> =
            <UserProjects as icydb::__macro::EntityValue>::id;

        let row = UserProjects {
            user_id: Ulid::from_parts(7, 1),
            created_at: icydb::types::Timestamp::default(),
            updated_at: icydb::types::Timestamp::default(),
        };
        let _: Ulid = row.user_id;
    }
}
