use icydb::design::prelude::*;

#[record(source_key = "testing/macro-tests/tests/ui/default_remove_without_inheritance.rs::record::1", traits(remove(Default)))]
pub struct InvalidDefaultRemoval;

fn main() {}
