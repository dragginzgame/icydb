//! Module: session::read_set
//! Responsibility: capture and recompare bounded physical source proofs.
//! Does not own: cursor execution or durable application job state.
//! Boundary: accepted entity names -> registered store revisions.

use crate::{
    db::{
        DbSession, ExhaustiveReadError, ReadSetRevisionError, ReadSetRevisionProof,
        ReadSetStoreIdentity, ReadSetStoreRevision, StoreHandle, StoreRuntimeStorageMode,
        commit::database_incarnation_id, executor::budget::HardExecutionContext,
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};

const READ_SET_CAPTURE_SHAPE: u64 = 0x7265_6164_7365_7401;

impl<C: CanisterKind> DbSession<C> {
    /// Capture one canonical proof covering every physical store behind the
    /// named accepted entities.
    pub fn capture_read_set_revision_proof(
        &self,
        entity_names: &[&str],
    ) -> Result<ReadSetRevisionProof, ExhaustiveReadError> {
        self.db
            .request_execution_scope()
            .charge(
                HardExecutionContext::new(
                    DiagnosticExecutionBudgetScope::Execution,
                    DiagnosticExecutionLane::TrustedRead,
                    READ_SET_CAPTURE_SHAPE,
                ),
                DiagnosticExecutionBudgetResource::QueryExecutions,
                1,
            )
            .map_err(crate::error::InternalError::from)?;
        if entity_names.is_empty() {
            return Err(ReadSetRevisionError::Empty.into());
        }
        let root = self.current_accepted_runtime_root_identity()?;
        let mut store_paths = Vec::with_capacity(entity_names.len());
        for entity in entity_names {
            let catalog = self
                .find_accepted_schema_catalog_context_for_entity_name(entity)?
                .ok_or(ReadSetRevisionError::UnknownEntity)?;
            store_paths.push(catalog.identity().store_path());
        }
        store_paths.sort_unstable();
        store_paths.dedup();
        let stores = store_paths
            .into_iter()
            .map(|store_path| {
                let handle = self.db.recovered_store(store_path)?;
                Self::capture_store_revision(store_path, handle)
            })
            .collect::<Result<Vec<_>, ExhaustiveReadError>>()?;
        ReadSetRevisionProof::new(root, stores).map_err(Into::into)
    }

    pub(in crate::db) fn capture_entity_read_set_revision_proof(
        &self,
        store_path: &'static str,
    ) -> Result<ReadSetRevisionProof, ExhaustiveReadError> {
        let root = self.current_accepted_runtime_root_identity()?;
        let handle = self.db.recovered_store(store_path)?;
        ReadSetRevisionProof::new(
            root,
            vec![Self::capture_store_revision(store_path, handle)?],
        )
        .map_err(Into::into)
    }

    pub(in crate::db) fn verify_read_set_revision_proof(
        &self,
        proof: &ReadSetRevisionProof,
    ) -> Result<(), ExhaustiveReadError> {
        proof.validate()?;
        let incarnation = database_incarnation_id()?;
        let root = self.current_accepted_runtime_root_identity()?;
        if proof.database_incarnation() != incarnation.to_bytes() {
            return Err(ReadSetRevisionError::DatabaseIncarnationChanged.into());
        }
        if !proof.root_matches(incarnation, root) {
            return Err(ReadSetRevisionError::AcceptedRootChanged.into());
        }
        for expected in proof.stores() {
            let (store_path, handle) = self
                .store_for_read_set_identity(expected.store())
                .ok_or(ReadSetRevisionError::NonCanonical)?;
            let current = Self::capture_store_revision(store_path, handle)?;
            if current.data_revision() != expected.data_revision() {
                return Err(ReadSetRevisionError::StoreDataChanged {
                    store: expected.store(),
                }
                .into());
            }
            if current.access_state_revision() != expected.access_state_revision() {
                return Err(ReadSetRevisionError::StoreAccessChanged {
                    store: expected.store(),
                }
                .into());
            }
        }
        Ok(())
    }

    pub(in crate::db) fn verify_durable_read_set_revision_proof(
        &self,
        proof: &ReadSetRevisionProof,
    ) -> Result<(), ExhaustiveReadError> {
        self.verify_read_set_revision_proof(proof)?;
        for store in proof.stores() {
            let (_, handle) = self
                .store_for_read_set_identity(store.store())
                .ok_or(ReadSetRevisionError::NonCanonical)?;
            if handle.storage_capabilities().storage_mode() != StoreRuntimeStorageMode::Journaled {
                return Err(ReadSetRevisionError::DurableStoreRequired {
                    store: store.store(),
                }
                .into());
            }
        }
        Ok(())
    }

    pub(in crate::db) fn ensure_read_set_contains_store(
        proof: &ReadSetRevisionProof,
        store_path: &str,
    ) -> Result<(), ExhaustiveReadError> {
        let store = ReadSetStoreIdentity::for_store_path(store_path);
        if !proof.contains_store(store) {
            return Err(ReadSetRevisionError::StoreMissingFromProof { store }.into());
        }
        Ok(())
    }

    fn capture_store_revision(
        store_path: &str,
        handle: StoreHandle,
    ) -> Result<ReadSetStoreRevision, ExhaustiveReadError> {
        let data_revision = handle.journal_tail_store().map_or_else(
            || {
                handle
                    .with_data(crate::db::data::DataStore::generation)
                    .checked_add(1)
                    .ok_or_else(crate::error::InternalError::store_invariant)
            },
            |journal| {
                journal.with_borrow(crate::db::journal::JournalTailStore::data_mutation_revision)
            },
        )?;
        let access_state_revision = handle.access_state_revision()?;
        Ok(ReadSetStoreRevision::new(
            ReadSetStoreIdentity::for_store_path(store_path),
            data_revision,
            access_state_revision,
        ))
    }

    fn store_for_read_set_identity(
        &self,
        identity: ReadSetStoreIdentity,
    ) -> Option<(&'static str, StoreHandle)> {
        self.db.with_store_registry(|registry| {
            registry.iter().find(|(store_path, _)| {
                ReadSetStoreIdentity::for_store_path(store_path) == identity
            })
        })
    }
}
