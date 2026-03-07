use crate::prelude::*;

///
/// User
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct User;

///
/// UserProjects
///

#[entity(
    store = "TestStore",
    pk(field = "user_id"),
    fields(field(ident = "user_id", value(item(rel = "User", prim = "Ulid"))))
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
            user_id: user,
            ..Default::default()
        };

        // Field type stores the declared primitive key.
        let _: Ulid = projects.user_id;

        // Entity has its own identity.
        let _id: Id<UserProjects> = projects.id();

        // No identity ↔ primitive equivalence is asserted.
    }
}
