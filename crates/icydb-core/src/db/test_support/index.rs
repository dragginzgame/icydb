//! Index-key fixtures for database-local tests.
//!
//! Semantic index encoding belongs to the index layer. Executor tests use
//! this helper so production executor modules remain byte-only even when the
//! tests need to populate a realistic index store.

use crate::db::{
    index::{IndexId, IndexKey, IndexKeyKind},
    key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
};

pub(in crate::db) fn nat64_index_key(
    index_id: &IndexId,
    component: &[u8],
    primary_key: u64,
) -> IndexKey {
    IndexKey::new_from_components_with_primary_key_value(
        index_id,
        IndexKeyKind::User,
        &[component.to_vec()],
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(primary_key)),
    )
    .expect("test index key should build")
}
