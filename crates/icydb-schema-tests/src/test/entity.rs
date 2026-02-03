use crate::prelude::*;

///
/// Entity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
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
    pk = "id",
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
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct RenamedEntity {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn entity_name_defaults_and_override() {
        assert_eq!(
            <RenamedEntity as ::icydb::traits::EntityIdentity>::ENTITY_NAME,
            "Potato"
        );
    }
}
