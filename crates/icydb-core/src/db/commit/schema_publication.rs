//! Module: db::commit::schema_publication
//! Responsibility: marker-bound accepted-schema bundle/root publication.
//! Does not own: candidate construction, schema compatibility, or root codecs.
//! Boundary: schema reconciliation -> commit marker/journal -> schema live projection.

use crate::{
    db::{
        commit::{CommitMarker, begin_commit, finish_commit, generate_commit_id},
        data::DataStore,
        journal::{JournalBatch, JournalRecord},
        registry::{StoreHandle, StoreRecoveryCapability},
        relation::StagedReverseRelationDomainEffects,
        schema::{
            AcceptedSchemaRevision, CandidateSchemaRevision, StagedUserIndexDomainReplacement,
            accepted_schema_cache_fingerprint_for_persisted_snapshot,
        },
    },
    error::InternalError,
};
use std::collections::BTreeSet;

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
        Vec::new(),
        Vec::new(),
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
        replacements,
        Vec::new(),
    )
}

/// Publish one accepted-schema candidate with complete user-index domains and
/// candidate-logical reverse-relation effects through the same marker window.
pub(in crate::db) fn publish_accepted_schema_candidate_with_derived_domains(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    user_index_domains: Vec<StagedUserIndexDomainReplacement>,
    reverse_relation_domains: Vec<StagedReverseRelationDomainEffects>,
) -> Result<(), InternalError> {
    validate_user_index_domain_candidates(
        store_path,
        store,
        expected_revision,
        candidate,
        user_index_domains.as_slice(),
    )?;
    validate_reverse_relation_domain_candidates(
        store_path,
        expected_revision,
        candidate,
        reverse_relation_domains.as_slice(),
    )?;
    let user_entities = user_index_domains
        .iter()
        .map(StagedUserIndexDomainReplacement::entity_tag)
        .collect::<BTreeSet<_>>();
    let relation_entities = reverse_relation_domains
        .iter()
        .map(StagedReverseRelationDomainEffects::entity_tag)
        .collect::<BTreeSet<_>>();
    if user_entities != relation_entities {
        return Err(InternalError::store_invariant());
    }
    publish_accepted_schema_candidate_with_prepared_domains(
        store_path,
        store,
        expected_revision,
        candidate,
        user_index_domains,
        reverse_relation_domains,
    )
}

fn publish_accepted_schema_candidate_with_prepared_domains(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    replacements: Vec<StagedUserIndexDomainReplacement>,
    reverse_relation_domains: Vec<StagedReverseRelationDomainEffects>,
) -> Result<(), InternalError> {
    match store.storage_capabilities().recovery() {
        StoreRecoveryCapability::None => {
            store.with_schema_mut(|schema_store| {
                schema_store.publish_accepted_schema_candidate(expected_revision, candidate)
            })?;
            if !replacements.is_empty() {
                apply_user_index_domain_replacements(store, replacements);
            }
            apply_reverse_relation_domain_effects(reverse_relation_domains);
            Ok(())
        }
        StoreRecoveryCapability::StableBasePlusJournalReplay => publish_journaled_candidate(
            store_path,
            store,
            expected_revision,
            candidate,
            replacements,
            reverse_relation_domains,
        ),
    }
}

fn publish_journaled_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    replacements: Vec<StagedUserIndexDomainReplacement>,
    reverse_relation_domains: Vec<StagedReverseRelationDomainEffects>,
) -> Result<(), InternalError> {
    let journal_store = store
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?;
    let marker_id = generate_commit_id()?;
    let sequence =
        journal_store.with_borrow(crate::db::journal::JournalTailStore::next_append_sequence)?;
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
        if !replacements.is_empty() {
            apply_user_index_domain_replacements(store, replacements);
        }
        apply_reverse_relation_domain_effects(reverse_relation_domains);
        Ok(())
    })
}

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
        let schema_version_matches =
            accepted_after.version() == replacement.accepted_after_version();
        let schema_fingerprint_matches =
            accepted_after_fingerprint == replacement.accepted_after_fingerprint();
        if !(entity_path_matches && schema_version_matches && schema_fingerprint_matches) {
            return Err(InternalError::store_invariant());
        }
    }
    if store.index_state() != crate::db::index::IndexState::Ready {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

fn validate_reverse_relation_domain_candidates(
    store_path: &'static str,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domains: &[StagedReverseRelationDomainEffects],
) -> Result<(), InternalError> {
    if domains.is_empty() || candidate.store_path() != store_path {
        return Err(InternalError::store_invariant());
    }
    let mut entities = BTreeSet::new();
    for domain in domains {
        let accepted_before_identity = domain.accepted_before_identity();
        if !entities.insert(domain.entity_tag())
            || domain.store_path() != store_path
            || accepted_before_identity.store_path() != store_path
            || accepted_before_identity.accepted_schema_revision() != expected_revision
        {
            return Err(InternalError::store_invariant());
        }
        let accepted_after = candidate
            .bundle()
            .entity_snapshots()
            .get(&domain.entity_tag())
            .ok_or_else(InternalError::store_corruption)?;
        let accepted_after_fingerprint =
            accepted_schema_cache_fingerprint_for_persisted_snapshot(accepted_after)?;
        let entity_path_matches =
            accepted_after.entity_path() == accepted_before_identity.entity_path();
        let schema_version_matches = accepted_after.version() == domain.accepted_after_version();
        let schema_fingerprint_matches =
            accepted_after_fingerprint == domain.accepted_after_fingerprint();
        if !(entity_path_matches && schema_version_matches && schema_fingerprint_matches) {
            return Err(InternalError::store_invariant());
        }
    }

    Ok(())
}

fn apply_user_index_domain_replacements(
    store: StoreHandle,
    replacements: Vec<StagedUserIndexDomainReplacement>,
) {
    let data_generation = store.with_data(DataStore::generation);
    store.with_index_mut(|index_store| {
        index_store.mark_building();
        for replacement in replacements {
            let (deletion_keys, final_entries) = replacement.into_apply_parts();
            for key in deletion_keys {
                index_store.remove(&key);
            }
            for entry in final_entries {
                let (key, value) = entry.into_parts();
                index_store.insert(key, value);
            }
        }
        index_store.mark_prefix_cardinality_data_generation(data_generation);
        index_store.mark_ready();
    });
}

fn apply_reverse_relation_domain_effects(domains: Vec<StagedReverseRelationDomainEffects>) {
    for domain in domains {
        for effect in domain.into_effects() {
            effect.apply();
        }
    }
}
