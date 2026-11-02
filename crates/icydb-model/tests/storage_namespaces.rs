//! Macro-to-model coverage for durable Storage namespaces.

use icydb_model::{build::get_schema, canister, node::Canister};

#[canister(memory_namespace = "storage_namespace")]
pub struct StorageCanister;

#[test]
fn macro_namespace_reaches_the_schema_model() {
    let schema = get_schema().expect("fixture schema should seal");
    let (_, canister) = schema
        .get_nodes::<Canister>()
        .find(|(_, canister)| canister.def().ident() == "StorageCanister")
        .expect("macro should register its model");
    assert_eq!(canister.memory_namespace(), "storage_namespace");
    assert_eq!(
        canister.commit_stable_key(),
        "icydb.storage_namespace.commit.control.v1"
    );
    drop(schema);
}
