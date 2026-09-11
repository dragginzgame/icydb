//! Generate the actor from the same normal module used by the runtime.

mod design;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=src/design");
    runtime_api::build_canister!(design::FacadeCanister)
}
