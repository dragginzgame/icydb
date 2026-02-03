//! Build-script helper that runs IcyDB actor codegen for the specified canister path.
//!
//! This does not register schema nodes; schema registration is handled by derives at compile time.
//! Formerly named `build!`; renamed to make the actor-only scope explicit.
#[macro_export]
macro_rules! build {
    ($actor:expr) => {
        use std::{env::var, fs::File, io::Write, path::PathBuf};

        //
        // CARGO
        //
        // should include the build flags we need to get
        // different targets working
        //

        // all
        println!("cargo:rerun-if-changed=build.rs");

        // add the cfg flag
        println!("cargo:rustc-check-cfg=cfg(icydb)");
        println!("cargo:rustc-cfg=icydb");

        // Get the output directory set by Cargo
        let out_dir = var("OUT_DIR").expect("OUT_DIR not set");

        //
        // ACTOR CODE
        //

        let output = ::icydb::build::generate($actor);

        // write the file
        let actor_file = PathBuf::from(out_dir.clone()).join("actor.rs");
        let mut file = File::create(actor_file)?;
        file.write_all(output.as_bytes())?;
    };
}
