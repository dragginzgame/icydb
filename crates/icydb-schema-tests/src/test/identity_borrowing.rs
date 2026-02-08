use crate::prelude::*;

///
/// User
///

#[entity(
    store = "TestDataStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct User;

///
/// UserProjects
///

#[entity(
    store = "TestDataStore",
    pk(field = "user"),
    fields(field(ident = "user", value(item(rel = "User", prim = "Ulid"))))
)]
pub struct UserProjects;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{traits::EntityValue, types::Ulid};

    #[test]
    fn relation_primary_key_uses_declared_primitive_type() {
        let user = Ulid::from_parts(1, 42);
        let projects = UserProjects {
            user,
            ..Default::default()
        };

        // Field type stores the declared primitive key.
        let _: Ulid = projects.user;

        // Entity has its own identity.
        let _id: Id<UserProjects> = projects.id();

        // No identity ↔ primitive equivalence is asserted.
    }
}
