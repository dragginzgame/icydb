// Re-export the actual facade so a missing optional module is diagnosed here,
// without embedding the external package's absolute source path in snapshots.
mod __icydb_facade {
    pub use icydb::*;
}
use canic_icydb::__private::cdk as __icydb_host_sdk;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();
}

canic_icydb::mount! {
    icydb_metrics(authorization = public);
}

fn main() {}
