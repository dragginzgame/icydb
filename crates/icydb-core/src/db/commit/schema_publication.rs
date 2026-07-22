//! Module: db::commit::schema_publication
//! Responsibility: marker-bound accepted-schema bundle/root publication.
//! Does not own: candidate construction, schema compatibility, or root codecs.
//! Boundary: schema reconciliation -> commit marker/journal -> schema live projection.

use crate::{
    db::{
        commit::{CommitMarker, begin_commit, finish_commit, generate_commit_id},
        data::DataStore,
        index::IndexStore,
        journal::{JournalBatch, JournalRecord},
        registry::{StoreHandle, StoreRecoveryCapability},
        schema::{
            AcceptedSchemaRevision, CandidateSchemaRevision, StagedDerivedDomainReplacement,
            StagedUserIndexDomainReplacement,
            accepted_schema_cache_fingerprint_for_persisted_snapshot,
        },
    },
    error::InternalError,
};
use std::collections::BTreeSet;

enum StagedSchemaDomains {
    None,
    #[cfg(feature = "sql")]
    UserIndexes(Vec<StagedUserIndexDomainReplacement>),
    Derived(Vec<StagedDerivedDomainReplacement>),
}

pub(in crate::db) fn publish_accepted_schema_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<(), InternalError> {
    if candidate.store_path() != store_path {
        return Err(InternalError::store_invariant());
    }

    publish_accepted_schema_candidate_with_prepared_domains(
        store_path,
        store,
        expected_revision,
        candidate,
        StagedSchemaDomains::None,
    )
}

/// Publish one accepted-schema candidate and its prevalidated per-entity
/// user-index domains through the same marker window.
#[cfg(feature = "sql")]
pub(in crate::db) fn publish_accepted_schema_candidate_with_user_index_domains(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    replacements: Vec<StagedUserIndexDomainReplacement>,
) -> Result<(), InternalError> {
    validate_user_index_domain_candidates(
        store_path,
        store,
        expected_revision,
        candidate,
        replacements.as_slice(),
    )?;
    publish_accepted_schema_candidate_with_prepared_domains(
        store_path,
        store,
        expected_revision,
        candidate,
        StagedSchemaDomains::UserIndexes(replacements),
    )
}

/// Publish one accepted-schema candidate with complete user-index domains and
/// candidate-logical reverse-relation effects through the same marker window.
pub(in crate::db) fn publish_accepted_schema_candidate_with_derived_domains(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domains: Vec<StagedDerivedDomainReplacement>,
) -> Result<(), InternalError> {
    validate_derived_domain_candidates(
        store_path,
        store,
        expected_revision,
        candidate,
        domains.as_slice(),
    )?;
    publish_accepted_schema_candidate_with_prepared_domains(
        store_path,
        store,
        expected_revision,
        candidate,
        StagedSchemaDomains::Derived(domains),
    )
}

fn publish_accepted_schema_candidate_with_prepared_domains(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domains: StagedSchemaDomains,
) -> Result<(), InternalError> {
    match store.storage_capabilities().recovery() {
        StoreRecoveryCapability::None => {
            store.with_schema_mut(|schema_store| {
                schema_store.publish_accepted_schema_candidate(expected_revision, candidate)
            })?;
            apply_staged_schema_domains(store, domains);
            Ok(())
        }
        StoreRecoveryCapability::StableBasePlusJournalReplay => {
            publish_journaled_candidate(store_path, store, expected_revision, candidate, domains)
        }
    }
}

fn publish_journaled_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domains: StagedSchemaDomains,
) -> Result<(), InternalError> {
    let journal_store = store
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?;
    let marker_id = generate_commit_id()?;
    let sequence = journal_store
        .with_borrow(crate::db::journal::JournalTailStore::next_mutation_append_sequence)?;
    let schema_record = JournalRecord::accepted_schema_publish(
        store_path,
        expected_revision,
        candidate.encoded_bundle().to_vec(),
        candidate.encoded_root().to_vec(),
    )?;
    let batch = JournalBatch::new(marker_id, marker_id, sequence, vec![schema_record])?;
    let marker = CommitMarker::from_parts(marker_id, vec![batch.clone()])?;
    let commit = begin_commit(marker)?;

    finish_commit(commit, |_guard| {
        journal_store.with_borrow_mut(|journal| journal.append_batch(&batch))?;
        store.with_schema_mut(|schema_store| {
            schema_store.apply_journaled_accepted_schema_candidate(expected_revision, candidate)
        })?;
        apply_staged_schema_domains(store, domains);
        Ok(())
    })
}

#[cfg(feature = "sql")]
fn validate_user_index_domain_candidates(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    replacements: &[StagedUserIndexDomainReplacement],
) -> Result<(), InternalError> {
    if replacements.is_empty() || candidate.store_path() != store_path {
        return Err(InternalError::store_invariant());
    }
    let mut entities = BTreeSet::new();
    for replacement in replacements {
        validate_user_index_domain_candidate(
            store_path,
            store,
            expected_revision,
            candidate,
            replacement,
            &mut entities,
        )?;
    }
    if store.index_state() != crate::db::index::IndexState::Ready {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

fn validate_derived_domain_candidates(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domains: &[StagedDerivedDomainReplacement],
) -> Result<(), InternalError> {
    if domains.is_empty() || candidate.store_path() != store_path {
        return Err(InternalError::store_invariant());
    }
    let mut entities = BTreeSet::new();
    for domain in domains {
        validate_user_index_domain_candidate(
            store_path,
            store,
            expected_revision,
            candidate,
            domain.user_indexes(),
            &mut entities,
        )?;
    }
    if store.index_state() != crate::db::index::IndexState::Ready {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

fn validate_user_index_domain_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    replacement: &StagedUserIndexDomainReplacement,
    entities: &mut BTreeSet<crate::types::EntityTag>,
) -> Result<(), InternalError> {
    let accepted_before_identity = replacement.accepted_before_identity();
    if !entities.insert(replacement.entity_tag())
        || replacement.store_path() != store_path
        || accepted_before_identity.store_path() != store_path
        || accepted_before_identity.accepted_schema_revision() != expected_revision
    {
        return Err(InternalError::store_invariant());
    }
    let current_identity = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(
                replacement.entity_tag(),
                accepted_before_identity.entity_path(),
                store_path,
            )
        })?
        .ok_or_else(InternalError::store_corruption)?
        .identity();
    if current_identity != accepted_before_identity {
        return Err(InternalError::store_invariant());
    }
    let accepted_after = candidate
        .bundle()
        .entity_snapshots()
        .get(&replacement.entity_tag())
        .ok_or_else(InternalError::store_corruption)?;
    let accepted_after_fingerprint =
        accepted_schema_cache_fingerprint_for_persisted_snapshot(accepted_after)?;
    let entity_path_matches =
        accepted_after.entity_path() == accepted_before_identity.entity_path();
    let schema_version_matches = accepted_after.version() == replacement.accepted_after_version();
    let schema_fingerprint_matches =
        accepted_after_fingerprint == replacement.accepted_after_fingerprint();
    if !(entity_path_matches && schema_version_matches && schema_fingerprint_matches) {
        return Err(InternalError::store_invariant());
    }

    Ok(())
}

fn apply_staged_schema_domains(store: StoreHandle, domains: StagedSchemaDomains) {
    match domains {
        StagedSchemaDomains::None => {}
        #[cfg(feature = "sql")]
        StagedSchemaDomains::UserIndexes(replacements) => {
            apply_user_index_domain_replacements(store, replacements);
        }
        StagedSchemaDomains::Derived(domains) => {
            apply_derived_domain_replacements(store, domains);
        }
    }
}

#[cfg(feature = "sql")]
fn apply_user_index_domain_replacements(
    store: StoreHandle,
    replacements: Vec<StagedUserIndexDomainReplacement>,
) {
    let data_generation = store.with_data(DataStore::generation);
    store.with_index_mut(|index_store| {
        index_store.mark_building();
        for replacement in replacements {
            apply_user_index_domain_replacement(index_store, replacement);
        }
        index_store.mark_prefix_cardinality_data_generation(data_generation);
        index_store.mark_ready();
    });
}

fn apply_derived_domain_replacements(
    store: StoreHandle,
    domains: Vec<StagedDerivedDomainReplacement>,
) {
    let data_generation = store.with_data(DataStore::generation);
    let mut reverse_relation_effects = Vec::new();
    store.with_index_mut(|index_store| {
        index_store.mark_building();
        for domain in domains {
            let (user_indexes, mut relation_effects) = domain.into_apply_parts();
            apply_user_index_domain_replacement(index_store, user_indexes);
            reverse_relation_effects.append(&mut relation_effects);
        }
        index_store.mark_prefix_cardinality_data_generation(data_generation);
        index_store.mark_ready();
    });
    for effect in reverse_relation_effects {
        effect.apply();
    }
}

fn apply_user_index_domain_replacement(
    index_store: &mut IndexStore,
    replacement: StagedUserIndexDomainReplacement,
) {
    let (deletion_keys, final_entries) = replacement.into_apply_parts();
    for key in deletion_keys {
        index_store.remove(&key);
    }
    for entry in final_entries {
        let (key, value) = entry.into_parts();
        index_store.insert(key, value);
    }
}
