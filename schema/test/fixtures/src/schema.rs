pub mod relation {
    use icydb_model::prelude::*;

    ///
    /// RelationCanister
    ///

    #[canister(
        memory_namespace = "relation",
        memory_min = 100,
        memory_max = 106,
        commit_memory_id = 104,
        startup_memory_id = 106,
        integrity_progress_memory_id = 105
    )]
    pub struct RelationCanister {}

    ///
    /// RelationStore
    ///
    #[store(
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
    use icydb_model::prelude::*;

    ///
    /// TestCanister
    ///

    #[canister(
        memory_namespace = "test",
        memory_min = 130,
        memory_max = 136,
        commit_memory_id = 134,
        startup_memory_id = 136,
        integrity_progress_memory_id = 135
    )]
    pub struct TestCanister {}

    /// TestStore
    ///
    #[store(
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
