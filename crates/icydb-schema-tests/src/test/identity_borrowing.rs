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
    use icydb::traits::{EntityIdentity, EntityValue};
    use icydb::types::{Ref, Ulid};

    #[test]
    fn relation_primary_key_emits_ref() {
        let user_ref = Ref::<User>::new(Ulid::from_parts(1, 42));
        let projects = UserProjects {
            user: user_ref,
            ..Default::default()
        };

        let _: Ref<User> = projects.user;
        let id: <UserProjects as EntityIdentity>::Id = projects.id();
        let _: Ref<User> = id;
    }
}
