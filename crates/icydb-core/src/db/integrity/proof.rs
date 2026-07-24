//! Module: db::integrity::proof
//! Responsibility: capture the exact immutable read set for one Deep sweep.
//! Does not own: job persistence, page advancement, or accepted-schema meaning.
//! Boundary: accepted inspection plan + registered physical stores -> canonical proof vector.

use crate::{
    db::{
        Db,
        commit::{database_control_proof_identity, database_incarnation_id, ensure_recovered},
        integrity::{DatabaseIncarnationId, accepted_relation_projections},
        journal::JournalTailProofIdentity,
        registry::StoreRuntimeStorageMode,
        schema::AcceptedInspectionPlan,
    },
    error::InternalError,
    traits::CanisterKind,
};
use candid::CandidType;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

/// Exact physical state read for one participating journaled store.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct IntegrityStoreProof {
    store_path: String,
    data_generation: u64,
    index_generation: u64,
    journal: JournalTailProofIdentity,
}

impl IntegrityStoreProof {
    /// Borrow the canonical registered store path.
    #[must_use]
    pub(in crate::db) const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    /// Return the captured journal interval used to validate private progress.
    #[must_use]
    pub(super) const fn journal_interval(&self) -> (u64, u64) {
        (
            self.journal.fold_sequence(),
            self.journal.next_append_sequence(),
        )
    }
}

/// Exact active forward-index generation read by the selected plan.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct IntegrityIndexGenerationProof {
    store_path: String,
    schema_index_id: u32,
    physical_generation: u64,
}

/// Exact active source-owned reverse generation read by the selected plan.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct IntegrityRelationGenerationProof {
    target_store_path: String,
    relation_id: u32,
    physical_generation: u64,
}

/// Canonical proof vector captured before and after every Deep page.
///
/// Progress-store bytes are intentionally absent. Equality is the only
/// advancement authority; no generic "store changed" flag can stand in for
/// one of these typed components.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct IntegrityProofVector {
    database_incarnation_id: DatabaseIncarnationId,
    accepted_schema_version: u32,
    accepted_schema_fingerprint: [u8; 16],
    inspection_plan_fingerprint: [u8; 32],
    database_control_fingerprint: [u8; 32],
    allocation_registry_generation: u64,
    stores: Vec<IntegrityStoreProof>,
    index_generations: Vec<IntegrityIndexGenerationProof>,
    relation_generations: Vec<IntegrityRelationGenerationProof>,
}

impl IntegrityProofVector {
    /// Validate decoded proof components before they can authorize advancement.
    pub(super) fn validate(&self) -> Result<(), InternalError> {
        if self.accepted_schema_version == 0
            || self.stores.is_empty()
            || !self.stores.iter().all(|proof| !proof.store_path.is_empty())
            || !self
                .stores
                .iter()
                .all(|proof| proof.journal.is_well_formed())
            || !self
                .stores
                .windows(2)
                .all(|pair| pair[0].store_path < pair[1].store_path)
            || !self
                .index_generations
                .iter()
                .all(|proof| !proof.store_path.is_empty() && proof.schema_index_id != 0)
            || !self.index_generations.windows(2).all(|pair| {
                (&pair[0].store_path, pair[0].schema_index_id)
                    < (&pair[1].store_path, pair[1].schema_index_id)
            })
            || !self
                .relation_generations
                .iter()
                .all(|proof| !proof.target_store_path.is_empty() && proof.relation_id != 0)
            || !self.relation_generations.windows(2).all(|pair| {
                (&pair[0].target_store_path, pair[0].relation_id)
                    < (&pair[1].target_store_path, pair[1].relation_id)
            })
        {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }

    /// Return the number of active forward-index identities in this proof.
    #[must_use]
    pub(super) const fn index_generation_count(&self) -> usize {
        self.index_generations.len()
    }

    /// Return the number of active source-owned relation identities.
    #[must_use]
    pub(super) const fn relation_generation_count(&self) -> usize {
        self.relation_generations.len()
    }

    /// Return the durable database lifecycle identity.
    #[must_use]
    pub(in crate::db) const fn database_incarnation_id(&self) -> DatabaseIncarnationId {
        self.database_incarnation_id
    }

    /// Borrow the canonical store proof components.
    #[must_use]
    pub(in crate::db) const fn stores(&self) -> &[IntegrityStoreProof] {
        self.stores.as_slice()
    }

    /// Return the accepted entity schema version.
    #[must_use]
    pub(in crate::db) const fn accepted_schema_version(&self) -> u32 {
        self.accepted_schema_version
    }

    /// Return the accepted entity schema fingerprint.
    #[must_use]
    pub(in crate::db) const fn accepted_schema_fingerprint(&self) -> [u8; 16] {
        self.accepted_schema_fingerprint
    }

    /// Return the complete accepted inspection-plan fingerprint.
    #[must_use]
    pub(in crate::db) const fn inspection_plan_fingerprint(&self) -> [u8; 32] {
        self.inspection_plan_fingerprint
    }
}

/// Capture the exact proof vector for one accepted inspection plan.
pub(in crate::db) fn capture_integrity_proof_vector<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
) -> Result<IntegrityProofVector, InternalError> {
    ensure_recovered(db)?;

    let identity = plan.identity();
    let source_store = db.store_handle(identity.store_path())?;
    let relations = accepted_relation_projections(db, plan)?;
    let mut participating_stores =
        BTreeMap::from([(identity.store_path().to_string(), source_store)]);
    for relation in &relations {
        participating_stores
            .entry(relation.target_store_path().to_string())
            .or_insert_with(|| relation.target_store());
    }

    let stores = participating_stores
        .into_iter()
        .map(|(store_path, handle)| {
            if handle.storage_capabilities().storage_mode() != StoreRuntimeStorageMode::Journaled {
                return Err(InternalError::store_unsupported());
            }
            let journal = handle
                .journal_tail_store()
                .ok_or_else(InternalError::store_invariant)?
                .with_borrow(crate::db::journal::JournalTailStore::proof_identity)?;
            Ok(IntegrityStoreProof {
                store_path,
                data_generation: handle.with_data(crate::db::data::DataStore::generation),
                index_generation: handle.with_index(crate::db::index::IndexStore::generation),
                journal,
            })
        })
        .collect::<Result<Vec<_>, InternalError>>()?;

    let mut index_generations = (0..plan.index_inspection().len())
        .map(|ordinal| {
            let domain = plan
                .index_inspection()
                .domain(ordinal, identity.entity_tag())?;
            Ok(IntegrityIndexGenerationProof {
                store_path: domain.store_path().to_string(),
                schema_index_id: domain.schema_index_id().get(),
                physical_generation: domain.physical_generation(),
            })
        })
        .collect::<Result<Vec<_>, InternalError>>()?;
    index_generations.sort();

    let mut relation_generations = relations
        .iter()
        .map(|relation| IntegrityRelationGenerationProof {
            target_store_path: relation.target_store_path().to_string(),
            relation_id: relation.relation_id().get(),
            physical_generation: relation.physical_generation(),
        })
        .collect::<Vec<_>>();
    relation_generations.sort();

    Ok(IntegrityProofVector {
        database_incarnation_id: database_incarnation_id()?,
        accepted_schema_version: identity.accepted_schema_version().get(),
        accepted_schema_fingerprint: identity.accepted_schema_fingerprint(),
        inspection_plan_fingerprint: plan.fingerprint().to_bytes(),
        database_control_fingerprint: database_control_proof_identity()?,
        allocation_registry_generation: allocation_registry_generation()?,
        stores,
        index_generations,
        relation_generations,
    })
}

#[cfg(test)]
#[expect(
    clippy::unnecessary_wraps,
    reason = "the test backend supplies generation zero while preserving the fallible production signature"
)]
fn allocation_registry_generation() -> Result<u64, InternalError> {
    Ok(ic_memory::committed_allocations().map_or(0, |allocations| allocations.generation()))
}

#[cfg(not(test))]
fn allocation_registry_generation() -> Result<u64, InternalError> {
    let allocations = ic_memory::committed_allocations()
        .map_err(InternalError::database_format_memory_registration_failed)?;
    validate_committed_allocation_declarations(allocations.declarations())?;
    Ok(allocations.generation())
}

#[cfg(test)]
#[expect(
    clippy::unnecessary_wraps,
    reason = "the test backend has no default ic-memory runtime; declaration closure is covered by focused unit tests"
)]
pub(super) const fn validate_integrity_allocation_registry() -> Result<(), InternalError> {
    Ok(())
}

#[cfg(not(test))]
pub(super) fn validate_integrity_allocation_registry() -> Result<(), InternalError> {
    let allocations = ic_memory::committed_allocations()
        .map_err(InternalError::database_format_memory_registration_failed)?;
    validate_committed_allocation_declarations(allocations.declarations())
}

fn validate_committed_allocation_declarations(
    declarations: &[ic_memory::AllocationDeclaration],
) -> Result<(), InternalError> {
    if declarations.len() > usize::from(ic_memory::MEMORY_MANAGER_MAX_ID) + 1 {
        return Err(InternalError::store_invariant());
    }

    let mut stable_keys = BTreeSet::new();
    let mut slots = BTreeSet::new();
    for declaration in declarations {
        declaration
            .validate()
            .map_err(|_| InternalError::store_invariant())?;
        if !stable_keys.insert(declaration.stable_key()) || !slots.insert(declaration.slot()) {
            return Err(InternalError::store_invariant());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration(key: &str, memory_id: u8) -> ic_memory::AllocationDeclaration {
        ic_memory::AllocationDeclaration::memory_manager(key, memory_id, key)
            .expect("test declaration should admit")
    }

    #[test]
    fn quick_allocation_registry_closure_requires_unique_keys_and_slots() {
        let first = declaration("tests.integrity.first.v1", 21);
        let second = declaration("tests.integrity.second.v1", 22);
        assert!(
            validate_committed_allocation_declarations(&[first.clone(), second.clone()]).is_ok(),
        );

        let duplicate_key = declaration("tests.integrity.first.v1", 23);
        assert!(
            validate_committed_allocation_declarations(&[first.clone(), duplicate_key]).is_err(),
        );

        let duplicate_slot = declaration("tests.integrity.third.v1", 22);
        assert!(
            validate_committed_allocation_declarations(&[first, second, duplicate_slot]).is_err(),
        );
    }
}
