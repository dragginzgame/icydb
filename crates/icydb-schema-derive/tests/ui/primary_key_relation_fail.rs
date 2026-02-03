use icydb_schema_derive::entity;

#[entity(
    store = "TestDataStore",
    pk = "owner",
    fields(field(ident = "owner", value(item(rel = "Owner"))))
)]
/// Compile-fail fixture for relation primary keys.
/// Intentionally uses a relation as the primary key.
/// Expected to fail with a fatal schema error.

pub struct BadOwner;

fn main() {}
