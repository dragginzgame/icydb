use icydb::schema::{
    build::schema_write,
    node::{Canister, Def, SchemaNode, Store},
};
use std::{env::var, fs::File, io::Write, path::PathBuf};

const MINIMAL_MODULE_PATH: &str = "icydb_testing_audit_minimal_fixtures::minimal";
const MINIMAL_CANISTER_PATH: &str =
    "icydb_testing_audit_minimal_fixtures::minimal::MinimalCanister";

fn main() -> std::io::Result<()> {
    // Register the generated-code cfg knobs expected by the emitted actor glue.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(icydb)");
    println!("cargo:rustc-check-cfg=cfg(feature, values(\"sql\"))");
    println!("cargo:rustc-cfg=icydb");

    // The minimal audit fixture is intentionally store-only. Seed its schema
    // nodes directly here so build-time codegen does not depend on ctor-based
    // registration surviving every host-tooling mode.
    let mut schema = schema_write();
    schema.insert_node(SchemaNode::Canister(Canister::new(
        Def::new(MINIMAL_MODULE_PATH, "MinimalCanister"),
        61,
        71,
        63,
    )));
    schema.insert_node(SchemaNode::Store(Store::new(
        Def::new(MINIMAL_MODULE_PATH, "MinimalStore"),
        "MINIMAL_STORE",
        MINIMAL_CANISTER_PATH,
        61,
        62,
    )));
    drop(schema);

    // Render the actor glue into Cargo's output directory.
    let out_dir = var("OUT_DIR").expect("OUT_DIR not set");
    let output = icydb::build::generate(MINIMAL_CANISTER_PATH);
    let actor_file = PathBuf::from(out_dir).join("actor.rs");
    let mut file = File::create(actor_file)?;
    file.write_all(output.as_bytes())?;

    Ok(())
}
