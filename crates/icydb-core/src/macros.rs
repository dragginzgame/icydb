// start
// macro to be included at the start of each canister lib.rs file
/// Include the generated actor module emitted by `build!` (placed in `OUT_DIR/actor.rs`).
#[macro_export]
macro_rules! start {
    () => {
        // actor.rs
        include!(concat!(env!("OUT_DIR"), "/actor.rs"));
    };
}

// db
/// Access the current canister's database session; use `db!().debug()` for verbose tracing.
#[macro_export]
#[allow(clippy::crate_in_macro_def)]
macro_rules! db {
    () => {
        crate::db()
    };
}
