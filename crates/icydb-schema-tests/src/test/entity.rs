use crate::prelude::*;

///
/// Entity
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "a", value(item(prim = "Int32")), default = 3),
    )
)]
pub struct Entity {}

///
/// UnitKey
///

#[entity(
    store = "TestStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Unit"))),
        field(ident = "a", value(item(prim = "Int32")), default = 3),
    )
)]
pub struct UnitKey {}

///
/// RenamedEntity
///

#[entity(
    name = "Potato",
    store = "TestStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct RenamedEntity {}

///
/// ExternalPrimaryKeyEntity
///

#[entity(
    store = "TestStore",
    pk(field = "pid", source = "external"),
    fields(
        field(
            ident = "pid",
            value(item(prim = "Principal")),
            default = "Principal::anonymous"
        ),
        field(ident = "a", value(item(prim = "Int32")), default = 7),
    )
)]
pub struct ExternalPrimaryKeyEntity {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_primary_key_uses_declared_field_type() {
        let entity = Entity::default();
        let _: Ulid = entity.id;
    }

    #[test]
    fn entity_name_defaults_and_override() {
        assert_eq!(
            <RenamedEntity as ::icydb::traits::EntityIdentity>::ENTITY_NAME,
            "Potato"
        );
    }
}
