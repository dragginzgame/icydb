//! Generate the upgrade fixture from its ordinary shared-source schema.

mod schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=src/schema.rs");
    icydb::build_canister!(schema::MemoryCanister)
}
