pub mod relation {
    use icydb::design::prelude::*;

    ///
    /// RelationCanister
    ///

    #[canister(
        memory_namespace = "relation",
        memory_min = 100,
        memory_max = 120,
        commit_memory_id = 120
    )]
    pub struct RelationCanister {}

    ///
    /// RelationStore
    ///
    #[store(
        ident = "RELATION_DATA_STORE",
        store_name = "main",
        canister = "RelationCanister",
        data_memory_id = 100,
        index_memory_id = 101,
        schema_memory_id = 102
    )]
    pub struct RelationDataStore {}
}

pub mod test {
    use icydb::design::prelude::*;

    ///
    /// TestCanister
    ///

    #[canister(
        memory_namespace = "test",
        memory_min = 130,
        memory_max = 150,
        commit_memory_id = 150
    )]
    pub struct TestCanister {}

    /// TestStore
    ///
    #[store(
        ident = "TEST_STORE",
        store_name = "main",
        canister = "TestCanister",
        data_memory_id = 130,
        index_memory_id = 131,
        schema_memory_id = 132
    )]
    pub struct TestStore {}
}
