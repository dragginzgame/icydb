use icydb_model::canister;

#[canister(
    migrations(
        entity_migration(entity = "CatalogItem", from = 1, from_name = "Item"),
        entity_migration(entity = "Holder", from = 1)
    ),
    memory_namespace = "test",
)]
pub struct ApplicationCanister;

fn main() {}
