pub mod relation {
    use icydb::design::prelude::*;

    ///
    /// RelationCanister
    ///

    #[canister(memory_min = 10, memory_max = 20, commit_memory_id = 20)]
    pub struct RelationCanister {}

    ///
    /// RelationStore
    ///
    #[store(
        ident = "RELATION_DATA_STORE",
        canister = "RelationCanister",
        data_memory_id = 10,
        index_memory_id = 11,
        schema_memory_id = 12
    )]
    pub struct RelationDataStore {}
}

pub mod test {
    use icydb::design::prelude::*;

    ///
    /// TestCanister
    ///

    #[canister(memory_min = 50, memory_max = 100, commit_memory_id = 100)]
    pub struct TestCanister {}

    /// TestStore
    ///
    #[store(
        ident = "TEST_STORE",
        canister = "TestCanister",
        data_memory_id = 50,
        index_memory_id = 51,
        schema_memory_id = 52
    )]
    pub struct TestStore {}
}
