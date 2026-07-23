pub mod relation {
    use icydb::design::prelude::*;

    ///
    /// RelationCanister
    ///

    #[canister(
        memory_namespace = "relation",
        memory_min = 100,
        memory_max = 105,
        commit_memory_id = 104,
        integrity_progress_memory_id = 105
    )]
    pub struct RelationCanister {}

    ///
    /// RelationStore
    ///
    #[store(
        ident = "RELATION_DATA_STORE",
        store_name = "main",
        canister = "RelationCanister",
        storage(journaled(
            data_memory_id = 100,
            index_memory_id = 101,
            schema_memory_id = 102,
            journal_memory_id = 103,
        ))
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
        memory_max = 135,
        commit_memory_id = 134,
        integrity_progress_memory_id = 135
    )]
    pub struct TestCanister {}

    /// TestStore
    ///
    #[store(
        ident = "TEST_STORE",
        store_name = "main",
        canister = "TestCanister",
        storage(journaled(
            data_memory_id = 130,
            index_memory_id = 131,
            schema_memory_id = 132,
            journal_memory_id = 133,
        ))
    )]
    pub struct TestStore {}
}
