//! Module: db::commit::schema_publication
//! Responsibility: marker-bound accepted-schema bundle/root publication.
//! Does not own: candidate construction, schema compatibility, or root codecs.
//! Boundary: schema reconciliation -> commit marker/journal -> schema live projection.

use crate::{
    db::{
        commit::{CommitMarker, begin_commit, finish_commit, generate_commit_id},
        data::DataStore,
        index::{IndexEntryValue, IndexKey, IndexStore, RawIndexStoreKey},
        journal::{JournalBatch, JournalRecord},
        registry::{StoreHandle, StoreRecoveryCapability},
        relation::{RelationConstraintIndexEntry, RelationConstraintProjection},
        schema::{
            AcceptedSchemaRevision, CandidateSchemaRevision, ConstraintId, ConstraintValidationJob,
            StagedDerivedDomainReplacement, StagedUserIndexDomainReplacement,
            accepted_schema_cache_fingerprint_for_persisted_snapshot,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use std::collections::BTreeSet;

/// Prepared derived domains that cross the same accepted-schema marker.

enum StagedSchemaDomains {
    None,
    #[cfg(feature = "sql")]
    UserIndexes(Vec<StagedUserIndexDomainReplacement>),
    Derived(Vec<StagedDerivedDomainReplacement>),
}

/// Exact validation-job mutation paired with one accepted-schema publication.

#[derive(Clone, Copy)]
enum ConstraintValidationJobChange<'a> {
    None,
    Put(&'a ConstraintValidationJob),
    Delete {
        entity_tag: EntityTag,
        constraint_id: ConstraintId,
    },
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
        ConstraintValidationJobChange::None,
    )
}

/// Publish one accepted candidate and the exact validation job its new
/// `Validating` activation requires through one marker boundary.
pub(in crate::db) fn publish_accepted_schema_candidate_with_constraint_validation_job(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    job: &ConstraintValidationJob,
) -> Result<(), InternalError> {
    publish_accepted_schema_candidate_with_prepared_domains(
        store_path,
        store,
        expected_revision,
        candidate,
        StagedSchemaDomains::None,
        ConstraintValidationJobChange::Put(job),
    )
}

/// Publish one promotion or abort and remove its validation job through the
/// same marker boundary.
pub(in crate::db) fn publish_accepted_schema_candidate_with_constraint_validation_job_removal(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
) -> Result<(), InternalError> {
    publish_accepted_schema_candidate_with_prepared_domains(
        store_path,
        store,
        expected_revision,
        candidate,
        StagedSchemaDomains::None,
        ConstraintValidationJobChange::Delete {
            entity_tag,
            constraint_id,
        },
    )
}

/// Advance one validation job without changing accepted constraint meaning.
pub(in crate::db) fn publish_constraint_validation_job(
    store_path: &'static str,
    store: StoreHandle,
    job: &ConstraintValidationJob,
) -> Result<(), InternalError> {
    let bundle = store
        .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
        .ok_or_else(InternalError::store_corruption)?;
    if bundle.store_path() != store_path {
        return Err(InternalError::store_corruption());
    }
    store.with_schema(|schema_store| {
        schema_store.validate_constraint_validation_job_closure_with_change(
            &bundle,
            Some(job),
            None,
        )
    })?;

    match store.storage_capabilities().recovery() {
        StoreRecoveryCapability::None => {
            store.with_schema_mut(|schema_store| schema_store.apply_constraint_validation_job(job))
        }
        StoreRecoveryCapability::StableBasePlusJournalReplay => {
            publish_journaled_constraint_validation_job(store_path, store, job)
        }
    }
}

/// Advance one unique-index validation page and its isolated candidate writes
/// through the same marker-owned checkpoint boundary.
pub(in crate::db) fn publish_constraint_validation_job_with_candidate_index_entries(
    store_path: &'static str,
    store: StoreHandle,
    job: &ConstraintValidationJob,
    entries: Vec<RawIndexStoreKey>,
) -> Result<(), InternalError> {
    let bundle = store
        .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
        .ok_or_else(InternalError::store_corruption)?;
    if bundle.store_path() != store_path {
        return Err(InternalError::store_corruption());
    }
    store.with_schema(|schema_store| {
        schema_store.validate_constraint_validation_job_closure_with_change(
            &bundle,
            Some(job),
            None,
        )
    })?;
    validate_candidate_index_entries(&bundle, job, entries.as_slice())?;
    if store.storage_capabilities().recovery()
        != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_unsupported());
    }

    publish_journaled_constraint_validation_job_with_candidate_index_entries(
        store_path, store, job, entries,
    )
}

/// Advance one relation validation page and its isolated reverse writes through
/// the same marker-owned checkpoint boundary.
pub(in crate::db) fn publish_constraint_validation_job_with_candidate_relation_entries(
    store_path: &'static str,
    store: StoreHandle,
    job: &ConstraintValidationJob,
    projection: &RelationConstraintProjection,
    entries: Vec<RelationConstraintIndexEntry>,
) -> Result<(), InternalError> {
    let bundle = store
        .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
        .ok_or_else(InternalError::store_corruption)?;
    if bundle.store_path() != store_path {
        return Err(InternalError::store_corruption());
    }
    store.with_schema(|schema_store| {
        schema_store.validate_constraint_validation_job_closure_with_change(
            &bundle,
            Some(job),
            None,
        )
    })?;
    if job.staged_generation() != Some(projection.physical_generation())
        || entries.windows(2).any(|pair| {
            (pair[0].target_store_path(), pair[0].key())
                >= (pair[1].target_store_path(), pair[1].key())
        })
        || entries
            .iter()
            .any(|entry| !projection.validates_entry(entry))
        || store.storage_capabilities().recovery()
            != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_corruption());
    }

    publish_journaled_constraint_validation_job_with_candidate_relation_entries(
        store_path, store, job, entries,
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
        ConstraintValidationJobChange::None,
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
        ConstraintValidationJobChange::None,
    )
}

fn publish_accepted_schema_candidate_with_prepared_domains(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domains: StagedSchemaDomains,
    job_change: ConstraintValidationJobChange<'_>,
) -> Result<(), InternalError> {
    validate_constraint_validation_job_change(store, candidate, job_change)?;
    match store.storage_capabilities().recovery() {
        StoreRecoveryCapability::None => {
            publish_heap_candidate_with_constraint_validation_job_change(
                store,
                expected_revision,
                candidate,
                job_change,
            )?;
            apply_staged_schema_domains(store, domains);
            Ok(())
        }
        StoreRecoveryCapability::StableBasePlusJournalReplay => publish_journaled_candidate(
            store_path,
            store,
            expected_revision,
            candidate,
            domains,
            job_change,
        ),
    }
}

fn publish_journaled_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domains: StagedSchemaDomains,
    job_change: ConstraintValidationJobChange<'_>,
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
    let mut records = vec![schema_record];
    if let Some(record) = constraint_validation_job_journal_record(store_path, job_change)? {
        records.push(record);
    }
    let batch = JournalBatch::new(marker_id, marker_id, sequence, records)?;
    let marker = CommitMarker::from_parts(marker_id, vec![batch.clone()])?;
    let commit = begin_commit(marker)?;

    finish_commit(commit, |_guard| {
        journal_store.with_borrow_mut(|journal| journal.append_batch(&batch))?;
        store.with_schema_mut(|schema_store| {
            schema_store.apply_journaled_accepted_schema_candidate(expected_revision, candidate)
        })?;
        apply_constraint_validation_job_change(store, job_change)?;
        apply_staged_schema_domains(store, domains);
        Ok(())
    })
}

fn publish_journaled_constraint_validation_job(
    store_path: &'static str,
    store: StoreHandle,
    job: &ConstraintValidationJob,
) -> Result<(), InternalError> {
    let journal_store = store
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?;
    let marker_id = generate_commit_id()?;
    let sequence = journal_store
        .with_borrow(crate::db::journal::JournalTailStore::next_mutation_append_sequence)?;
    let record = JournalRecord::constraint_validation_job_put(store_path, job)?;
    let batch = JournalBatch::new(marker_id, marker_id, sequence, vec![record])?;
    let marker = CommitMarker::from_parts(marker_id, vec![batch.clone()])?;
    let commit = begin_commit(marker)?;

    finish_commit(commit, |_guard| {
        journal_store.with_borrow_mut(|journal| journal.append_batch(&batch))?;
        store.with_schema_mut(|schema_store| schema_store.apply_constraint_validation_job(job))
    })
}

fn publish_journaled_constraint_validation_job_with_candidate_index_entries(
    store_path: &'static str,
    store: StoreHandle,
    job: &ConstraintValidationJob,
    entries: Vec<RawIndexStoreKey>,
) -> Result<(), InternalError> {
    let journal_store = store
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?;
    let marker_id = generate_commit_id()?;
    let sequence = journal_store
        .with_borrow(crate::db::journal::JournalTailStore::next_mutation_append_sequence)?;
    let record = JournalRecord::constraint_validation_job_put(store_path, job)?;
    let batch = JournalBatch::new(marker_id, marker_id, sequence, vec![record])?;
    let marker = CommitMarker::from_parts(marker_id, vec![batch.clone()])?;
    let commit = begin_commit(marker)?;

    finish_commit(commit, |_guard| {
        journal_store.with_borrow_mut(|journal| journal.append_batch(&batch))?;
        store.with_index_mut(|index_store| {
            for key in entries {
                index_store.insert(key, IndexEntryValue::presence());
            }
        });
        store.with_schema_mut(|schema_store| schema_store.apply_constraint_validation_job(job))
    })
}

fn publish_journaled_constraint_validation_job_with_candidate_relation_entries(
    store_path: &'static str,
    store: StoreHandle,
    job: &ConstraintValidationJob,
    entries: Vec<RelationConstraintIndexEntry>,
) -> Result<(), InternalError> {
    let journal_store = store
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?;
    let marker_id = generate_commit_id()?;
    let sequence = journal_store
        .with_borrow(crate::db::journal::JournalTailStore::next_mutation_append_sequence)?;
    let record = JournalRecord::constraint_validation_job_put(store_path, job)?;
    let batch = JournalBatch::new(marker_id, marker_id, sequence, vec![record])?;
    let marker = CommitMarker::from_parts(marker_id, vec![batch.clone()])?;
    let commit = begin_commit(marker)?;

    finish_commit(commit, |_guard| {
        journal_store.with_borrow_mut(|journal| journal.append_batch(&batch))?;
        for entry in entries {
            entry.target_store().with_index_mut(|index_store| {
                index_store.insert(entry.key().clone(), IndexEntryValue::presence());
            });
        }
        store.with_schema_mut(|schema_store| schema_store.apply_constraint_validation_job(job))
    })
}

fn validate_candidate_index_entries(
    bundle: &crate::db::schema::AcceptedSchemaRevisionBundle,
    job: &ConstraintValidationJob,
    entries: &[RawIndexStoreKey],
) -> Result<(), InternalError> {
    let snapshot = bundle
        .entity_snapshots()
        .get(&job.entity_tag())
        .ok_or_else(InternalError::store_corruption)?;
    let activation = snapshot
        .constraint_catalog()
        .activation(job.constraint_id())
        .ok_or_else(InternalError::store_corruption)?;
    let crate::db::schema::ConstraintActivationKind::Unique { index_id } = activation.kind() else {
        return Err(InternalError::store_corruption());
    };
    let candidate = snapshot
        .candidate_indexes()
        .iter()
        .find(|index| index.schema_id() == *index_id)
        .ok_or_else(InternalError::store_corruption)?;
    let expected = crate::db::index::IndexId::new_with_generation(
        job.entity_tag(),
        candidate.ordinal(),
        candidate.physical_generation(),
    );
    if job.staged_generation() != Some(candidate.physical_generation())
        || entries.windows(2).any(|pair| pair[0] >= pair[1])
        || entries.iter().any(|raw| {
            IndexKey::try_from_raw(raw).map_or(true, |key| {
                key.key_kind() != crate::db::index::IndexKeyKind::User
                    || *key.index_id() != expected
            })
        })
    {
        return Err(InternalError::store_corruption());
    }
    Ok(())
}

fn validate_constraint_validation_job_change(
    store: StoreHandle,
    candidate: &CandidateSchemaRevision,
    change: ConstraintValidationJobChange<'_>,
) -> Result<(), InternalError> {
    store.with_schema(|schema_store| {
        schema_store.validate_live_activation_transition(candidate.bundle())?;
        match change {
            ConstraintValidationJobChange::None => {
                schema_store.validate_constraint_validation_job_closure(candidate.bundle())
            }
            ConstraintValidationJobChange::Put(job) => schema_store
                .validate_constraint_validation_job_closure_with_change(
                    candidate.bundle(),
                    Some(job),
                    None,
                ),
            ConstraintValidationJobChange::Delete {
                entity_tag,
                constraint_id,
            } => schema_store.validate_constraint_validation_job_closure_with_change(
                candidate.bundle(),
                None,
                Some((entity_tag, constraint_id)),
            ),
        }
    })
}

fn publish_heap_candidate_with_constraint_validation_job_change(
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    change: ConstraintValidationJobChange<'_>,
) -> Result<(), InternalError> {
    store.with_schema_mut(|schema_store| match change {
        ConstraintValidationJobChange::None => {
            schema_store.publish_accepted_schema_candidate(expected_revision, candidate)
        }
        ConstraintValidationJobChange::Put(job) => {
            schema_store.apply_constraint_validation_job(job)?;
            if let Err(error) =
                schema_store.publish_accepted_schema_candidate(expected_revision, candidate)
            {
                schema_store.apply_constraint_validation_job_removal(
                    job.entity_tag(),
                    job.constraint_id(),
                )?;
                return Err(error);
            }
            Ok(())
        }
        ConstraintValidationJobChange::Delete {
            entity_tag,
            constraint_id,
        } => {
            schema_store.publish_accepted_schema_candidate(expected_revision, candidate)?;
            schema_store.apply_constraint_validation_job_removal(entity_tag, constraint_id)
        }
    })
}

fn constraint_validation_job_journal_record(
    store_path: &'static str,
    change: ConstraintValidationJobChange<'_>,
) -> Result<Option<JournalRecord>, InternalError> {
    match change {
        ConstraintValidationJobChange::None => Ok(None),
        ConstraintValidationJobChange::Put(job) => {
            JournalRecord::constraint_validation_job_put(store_path, job).map(Some)
        }
        ConstraintValidationJobChange::Delete {
            entity_tag,
            constraint_id,
        } => JournalRecord::constraint_validation_job_delete(store_path, entity_tag, constraint_id)
            .map(Some),
    }
}

fn apply_constraint_validation_job_change(
    store: StoreHandle,
    change: ConstraintValidationJobChange<'_>,
) -> Result<(), InternalError> {
    store.with_schema_mut(|schema_store| match change {
        ConstraintValidationJobChange::None => Ok(()),
        ConstraintValidationJobChange::Put(job) => {
            schema_store.apply_constraint_validation_job(job)
        }
        ConstraintValidationJobChange::Delete {
            entity_tag,
            constraint_id,
        } => schema_store.apply_constraint_validation_job_removal(entity_tag, constraint_id),
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
