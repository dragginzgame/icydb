use icydb_model::canister;

#[canister(
    migrations(entity_migration(entity = "Account", from = 1)),
    memory_namespace = "test",
    )]
pub struct ApplicationCanister;

fn main() {}
