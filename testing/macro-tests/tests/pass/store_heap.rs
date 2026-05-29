use icydb::design::prelude::*;

#[canister(memory_namespace = "heap_test", memory_min = 100, memory_max = 110, commit_memory_id = 110)]
pub struct HeapCanister {}

#[store(
    ident = "HEAP_DATA_STORE",
    store_name = "heap_data",
    canister = "HeapCanister",
    storage(heap())
)]
pub struct HeapDataStore {}

fn main() {}
