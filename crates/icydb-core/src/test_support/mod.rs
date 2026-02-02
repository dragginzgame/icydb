pub mod entity;

use crate::{
    model::field::{EntityFieldKind, EntityFieldModel},
    traits::{CanisterKind, DataStoreKind, Path},
};

/// Default test canister path for core-only test entities.
pub const TEST_CANISTER_PATH: &str = "icydb_core::test_support::TestCanister";

/// Default test data store path for core-only test entities.
pub const TEST_DATA_STORE_PATH: &str = "icydb_core::test_support::TestDataStore";

/// Default test index store path for core-only test entities.
pub const TEST_INDEX_STORE_PATH: &str = "icydb_core::test_support::TestIndexStore";

///
/// TestCanister
///
/// Shared test-only canister marker for core tests.
/// Use this for EntityKind implementations in test support.
///

#[derive(Clone, Copy)]
pub struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = TEST_CANISTER_PATH;
}

impl CanisterKind for TestCanister {}

///
/// TestDataStore
///
/// Shared test-only data store marker for core tests.
/// Use this for EntityKind implementations in test support.
///

pub struct TestDataStore;

impl Path for TestDataStore {
    const PATH: &'static str = TEST_DATA_STORE_PATH;
}

impl DataStoreKind for TestDataStore {
    type Canister = TestCanister;
}

/// Build a runtime field model for test entities.
#[must_use]
pub const fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
    EntityFieldModel { name, kind }
}
