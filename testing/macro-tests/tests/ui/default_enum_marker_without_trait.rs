use icydb::design::prelude::*;

#[enum_(variant(ident = "Pending", default), variant(ident = "Active"))]
pub struct UnusedEnumDefault;

fn main() {}
