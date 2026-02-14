pub use crate::prelude::*;

pub mod test {
    use super::*;

    ///
    /// TestCanister
    ///

    #[canister(memory_min = 50, memory_max = 100)]
    pub struct TestCanister {}

    /// TestStore
    ///
    #[store(
        ident = "TEST_STORE",
        canister = "TestCanister",
        data_memory_id = 50,
        index_memory_id = 51
    )]
    pub struct TestStore {}
}

pub mod relation {
    use super::*;

    ///
    /// RelationCanister
    ///

    #[canister(memory_min = 10, memory_max = 20)]
    pub struct RelationCanister {}

    /// RelationStore
    ///
    #[store(
        ident = "RELATION_DATA_STORE",
        canister = "RelationCanister",
        data_memory_id = 10,
        index_memory_id = 11
    )]
    pub struct RelationDataStore {}
}
