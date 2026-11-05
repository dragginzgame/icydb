//! Automatic and explicit labels survive real macro expansion and proposal sealing.
#![cfg(feature = "build")]

use icydb_model::{build::get_schema, node::Entity, prelude::*};
use icydb_schema::canonical_index_name;

#[canister(memory_namespace = "index_names")]
pub struct NamesCanister;
#[store(canister = "NamesCanister", storage(heap()))]
pub struct NamesStore;
#[entity(
    store = "NamesStore", version = 1, pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(name = "email", value(item(prim = "Text", max_len = 32))),
        field(name = "lower_email", value(item(prim = "Text", max_len = 32))),
        field(name = "score", value(item(prim = "Nat64"))),
        field(name = "active", value(item(prim = "Bool"))),
        field(name = "a_b", value(item(prim = "Nat64"))),
        field(name = "a", value(item(prim = "Nat64"))),
        field(name = "b", value(item(prim = "Nat64")))
    ),
    index(field = "email"),
    index(name = "chosen_unique", field = "email", unique),
    index(field = "lower_email"),
    index(field = "LOWER(email)"),
    index(field = "score", predicate = "active = true"),
    index(field = "score", predicate = "active = false"),
    index(field = "a_b"),
    index(fields = ["a", "b"])
)]
pub struct NamesRow;

#[test]
fn resolved_names_reach_the_sealed_fragment_without_aliasing_definitions() {
    let schema = get_schema().unwrap();
    let (_, entity) = schema
        .get_nodes::<Entity>()
        .find(|(_, entity)| entity.def().ident() == "NamesRow")
        .unwrap();
    let names = entity
        .indexes()
        .iter()
        .map(icydb_model::node::Index::name)
        .collect::<Vec<_>>();
    assert_eq!(names.len(), 8);
    assert_eq!(
        names.iter().collect::<std::collections::HashSet<_>>().len(),
        8
    );
    assert_eq!(
        names[0],
        canonical_index_name("NamesRow", &["email"], false, None)
    );
    assert_eq!(names[1], "chosen_unique");
    for (position, terms) in [
        (2, vec!["lower_email"]),
        (3, vec!["LOWER(email)"]),
        (6, vec!["a_b"]),
        (7, vec!["a", "b"]),
    ] {
        assert_eq!(
            names[position],
            canonical_index_name("NamesRow", &terms, false, None)
        );
    }
    assert!(!entity.indexes()[0].is_unique());
    assert!(entity.indexes()[1].is_unique());
    let fragment = schema
        .schema_fragment_for_canister(&format!("{}::NamesCanister", module_path!()))
        .unwrap();
    let accepted_source = fragment
        .entities()
        .iter()
        .find(|entity| entity.name().as_str() == "NamesRow")
        .unwrap();
    assert_eq!(accepted_source.indexes().len(), names.len());
    for source in accepted_source.indexes() {
        assert!(names.contains(&source.name().as_str()));
        assert_eq!(source.source_key().as_str(), source.name().as_str());
    }
    drop(schema);
}
