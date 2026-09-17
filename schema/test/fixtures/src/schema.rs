pub mod relation {
    use icydb_model::prelude::*;

    ///
    /// RelationCanister
    ///

    #[canister(memory_namespace = "relation")]
    pub struct RelationCanister {}

    ///
    /// RelationStore
    ///
    #[store(canister = "RelationCanister", storage(journaled(key = "main")))]
    pub struct RelationDataStore {}
}

pub mod test {
    use icydb_model::prelude::*;

    ///
    /// TestCanister
    ///

    #[canister(memory_namespace = "test")]
    pub struct TestCanister {}

    /// TestStore
    ///
    #[store(canister = "TestCanister", storage(journaled(key = "main")))]
    pub struct TestStore {}
}
