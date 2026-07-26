use icydb::design::prelude::*;

#[enum_(source_key = "testing/macro-tests/tests/ui/default_enum_marker_without_trait.rs::enum_::nested::1", variant(source_key = "Pending", ident = "Pending", default), variant(source_key = "Active", ident = "Active"))]
pub struct UnusedEnumDefault;

fn main() {}
