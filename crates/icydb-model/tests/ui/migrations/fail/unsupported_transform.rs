use icydb_model::canister;

#[canister(
    migrations(entity_migration(
        entity = "Account",
        from = 1,
        transforms(rewrite(from = "age", to = "age", arbitrary))
    )),
    memory_namespace = "test",
    )]
pub struct ApplicationCanister;

fn main() {}
