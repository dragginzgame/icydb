use crate::prelude::*;

///
/// Entity
///

#[entity(
    store = "TestDataStore",
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
    store = "TestDataStore",
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
    store = "TestDataStore",
    pk(field = "id"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct RenamedEntity {}

///
/// ExternalPrimaryKeyEntity
///

#[entity(
    store = "TestDataStore",
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
    use icydb::traits::EntityValue;

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

    #[test]
    fn external_primary_key_uses_declared_field_type_and_projects_identity() {
        let wallet_pid = Principal::anonymous();
        let entity = ExternalPrimaryKeyEntity {
            pid: wallet_pid,
            ..Default::default()
        };

        let _: Principal = entity.pid;

        let id: Id<ExternalPrimaryKeyEntity> = entity.id();
        assert_eq!(id.to_value(), wallet_pid.to_value());
    }
}
