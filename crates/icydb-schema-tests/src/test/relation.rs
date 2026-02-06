use crate::prelude::*;

///
/// HasRelation
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "a", value(item(rel = "EntityA"))),
        field(ident = "b", value(item(rel = "EntityB", prim = "Nat16"))),
        field(ident = "c", value(item(rel = "EntityC", prim = "Principal"))),
    )
)]
pub struct HasRelation;

///
/// HasManyRelation
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "a", value(many, item(rel = "EntityA"))),
    )
)]
pub struct HasManyRelation;

///
/// EntityA
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct EntityA;

///
/// EntityB
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Nat16"))))
)]
pub struct EntityB;

///
/// EntityC
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Principal"))))
)]
pub struct EntityC;

///
/// RelationOwner
///

#[entity(
    store = "RelationDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct RelationOwner;

///
/// RelationOwned
///

#[entity(
    store = "RelationDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "owner", value(item(rel = "RelationOwner"))),
    )
)]
pub struct RelationOwned;

///
/// CrossCanisterRelation
///

#[cfg(test)]
#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "owner", value(item(rel = "RelationOwner"))),
    )
)]
pub struct CrossCanisterRelation;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_set_normalizes_duplicates_and_orders_by_key() {
        let id_a = Ulid::from_parts(1, 10);
        let id_b = Ulid::from_parts(1, 20);
        let ref_a: Ref<EntityA> =
            Ref::from(<Id<EntityA> as ::icydb::traits::View>::from_view(id_a));
        let ref_b: Ref<EntityA> =
            Ref::from(<Id<EntityA> as ::icydb::traits::View>::from_view(id_b));
        let refs = vec![ref_b, ref_a, ref_b];

        let set = RefSet::<EntityA>::from_refs(refs);

        assert_eq!(set.len(), 2);
        let refs: Vec<Ref<EntityA>> = set.iter().copied().collect();
        assert_eq!(refs, vec![ref_a, ref_b]);
    }
}
