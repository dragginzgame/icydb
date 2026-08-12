use icydb_model::canister;

#[canister(
    migrations(entity_migration(entity = "Account", from = 1)),
    memory_namespace = "test",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 109,
    startup_memory_id = 108
)]
pub struct ApplicationCanister;

fn main() {}
