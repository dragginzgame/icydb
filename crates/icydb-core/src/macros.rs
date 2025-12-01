// start
// macro to be included at the start of each canister lib.rs file
#[macro_export]
macro_rules! start {
    () => {
        // actor.rs
        include!(concat!(env!("OUT_DIR"), "/actor.rs"));
    };
}

// build
// for the various build.rs files
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

// db
#[macro_export]
#[allow(clippy::crate_in_macro_def)]
macro_rules! db {
    () => {
        crate::db()
    };

    (debug) => {
        crate::db().debug()
    };
}
