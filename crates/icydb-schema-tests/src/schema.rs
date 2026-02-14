pub use crate::prelude::*;

pub mod test {
    use super::*;

    ///
    /// TestCanister
    ///

    #[canister(memory_min = 50, memory_max = 100)]
    pub struct TestCanister {}

    ///
    /// TestIndexStore
    ///

    #[index_store(
        ident = "TEST_INDEX_STORE",
        canister = "TestCanister",
        entry_memory_id = 51
    )]
    pub struct TestIndexStore {}

    ///
    /// TestDataStore
    ///

    #[data_store(ident = "TEST_DATA_STORE", canister = "TestCanister", memory_id = 50)]
    pub struct TestDataStore {}
}

pub mod relation {
    use super::*;

    ///
    /// RelationCanister
    ///

    #[canister(memory_min = 10, memory_max = 20)]
    pub struct RelationCanister {}

    ///
    /// RelationDataStore
    ///

    #[data_store(
        ident = "RELATION_DATA_STORE",
        canister = "RelationCanister",
        memory_id = 10
    )]
    pub struct RelationDataStore {}
}
