use icydb_core::{
    entity::EntityPlacement,
    traits::{CanisterKind, Path, StoreKind},
};

struct StoreCanister;

impl Path for StoreCanister {
    const PATH: &'static str = "store_canister";
}

impl CanisterKind for StoreCanister {
    const COMMIT_MEMORY_ID: u8 = 1;
    const COMMIT_STABLE_KEY: &'static str = "store_canister_commit";
}

struct DeclaredCanister;

impl Path for DeclaredCanister {
    const PATH: &'static str = "declared_canister";
}

impl CanisterKind for DeclaredCanister {
    const COMMIT_MEMORY_ID: u8 = 2;
    const COMMIT_STABLE_KEY: &'static str = "declared_canister_commit";
}

struct EntityStore;

impl Path for EntityStore {
    const PATH: &'static str = "entity_store";
}

impl StoreKind for EntityStore {
    type Canister = StoreCanister;
}

struct MisplacedEntity;

impl EntityPlacement for MisplacedEntity {
    type Store = EntityStore;
    type Canister = DeclaredCanister;
}

fn main() {}
