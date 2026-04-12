//! Module: db::session::write
//! Responsibility: session-owned typed write APIs for insert, replace, update,
//! and structural mutation entrypoints over the shared save pipeline.
//! Does not own: commit staging, mutation execution, or persistence encoding.
//! Boundary: keeps public session write semantics above the executor save surface.

#[cfg(test)]
use crate::db::{DataStore, IndexStore};
use crate::{
    db::{DbSession, PersistedRow, WriteBatchResponse, data::UpdatePatch, executor::MutationMode},
    error::InternalError,
    traits::{CanisterKind, EntityCreateInput, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    /// Insert one entity row.
    pub fn insert<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.insert(entity))
    }

    /// Insert one authored typed input.
    pub fn create<I>(&self, input: I) -> Result<I::Entity, InternalError>
    where
        I: EntityCreateInput,
        I::Entity: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.create(input))
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.insert_many_atomic(entities))
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.insert_many_non_atomic(entities))
    }

    /// Replace one existing entity row.
    pub fn replace<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.replace(entity))
    }

    /// Apply one structural mutation under one explicit write-mode contract.
    ///
    /// This is the public core session boundary for structural writes:
    /// callers provide the key, field patch, and intended mutation mode, and
    /// the session routes that through the shared structural mutation pipeline.
    pub fn mutate_structural<E>(
        &self,
        key: E::Key,
        patch: UpdatePatch,
        mode: MutationMode,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.apply_structural_mutation(mode, key, patch))
    }

    /// Apply one structural replacement, inserting if missing.
    ///
    /// Replace semantics still do not inherit omitted fields from the old row.
    /// Missing fields must materialize through explicit defaults or managed
    /// field preflight, or the write fails closed.
    #[cfg(test)]
    pub(in crate::db) fn replace_structural<E>(
        &self,
        key: E::Key,
        patch: UpdatePatch,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.mutate_structural(key, patch, MutationMode::Replace)
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.replace_many_atomic(entities))
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.replace_many_non_atomic(entities))
    }

    /// Update one existing entity row.
    pub fn update<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_entity(|save| save.update(entity))
    }

    /// Apply one structural insert from a patch-defined after-image.
    ///
    /// Insert semantics no longer require a pre-built full row image.
    /// Missing fields still fail closed unless derive-owned materialization can
    /// supply them through explicit defaults or managed-field preflight.
    #[cfg(test)]
    pub(in crate::db) fn insert_structural<E>(
        &self,
        key: E::Key,
        patch: UpdatePatch,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.mutate_structural(key, patch, MutationMode::Insert)
    }

    /// Apply one structural field patch to an existing entity row.
    ///
    /// This session-owned boundary keeps structural mutation out of the raw
    /// executor surface while still routing through the same typed save
    /// preflight before commit staging.
    #[cfg(test)]
    pub(in crate::db) fn update_structural<E>(
        &self,
        key: E::Key,
        patch: UpdatePatch,
    ) -> Result<E, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.mutate_structural(key, patch, MutationMode::Update)
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.update_many_atomic(entities))
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_save_batch(|save| save.update_many_non_atomic(entities))
    }

    /// TEST ONLY: clear all registered data and index stores for this database.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn clear_stores_for_tests(&self) {
        self.db.with_store_registry(|reg| {
            // Test cleanup only: clearing all stores is set-like and does not
            // depend on registry iteration order.
            for (_, store) in reg.iter() {
                store.with_data_mut(DataStore::clear);
                store.with_index_mut(IndexStore::clear);
            }
        });
    }
}
