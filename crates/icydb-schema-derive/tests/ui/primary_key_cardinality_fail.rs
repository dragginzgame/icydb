use icydb_schema_derive::entity;

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(opt, item(prim = "Ulid"))))
)]
/// Compile-fail fixture for primary key cardinality.
/// Intentionally marks the primary key as optional.
/// Expected to fail with a fatal schema error.

pub struct BadPrimaryKeyCardinality;

fn main() {}
