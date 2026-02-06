use crate::prelude::*;

///
/// User
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct User;

///
/// UserProjects
///

#[entity(
    store = "TestDataStore",
    pk = "user",
    fields(field(ident = "user", value(item(rel = "User"))))
)]
pub struct UserProjects;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        traits::{EntityStorageKey, EntityValue},
        types::{Ref, Ulid},
    };

    fn assert_storage_key<E: EntityStorageKey<Key = Ulid>>() {}

    #[test]
    fn relation_primary_key_borrows_storage_key() {
        assert_storage_key::<UserProjects>();

        let user_ref: Ref<User> = Ref::from(<Id<User> as ::icydb::traits::View>::from_view(
            Ulid::from_parts(1, 42),
        ));
        let projects = UserProjects {
            user: user_ref,
            ..Default::default()
        };

        // Field type is still a Ref<User>
        let _: Ref<User> = projects.user;

        // Semantic identity is now Id<UserProjects>
        let id: Id<UserProjects> = projects.id();

        // Identity unwraps to the borrowed storage key
        assert_eq!(id.to_value(), user_ref.to_value());
    }
}
