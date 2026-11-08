//! Module: db::commit::schema_publication
//! Responsibility: marker-bound accepted-schema bundle/root publication.
//! Does not own: candidate construction, schema compatibility, or root codecs.
//! Boundary: schema reconciliation -> commit marker/journal -> schema live projection.

// Test interruption hooks stay at the publication edges they qualify; their state lives in helper_tests.

use crate::db::index::{
    IndexEntryValue, IndexKey, PreparedIndexPositionPublication, RawIndexStoreKey,
};
#[cfg(feature = "sql")]
use crate::db::journal::MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD;
#[cfg(feature = "sql")]
use crate::db::{
    data::DataStore,
    schema::{
        StagedUserIndexDomainReplacement, accepted_schema_cache_fingerprint_for_persisted_snapshot,
    },
};
use crate::{
    db::{
        commit::{
            CommitMarker, DatabaseControlOp, begin_commit, database_incarnation_id, finish_commit,
            generate_commit_id, generate_marker_batch_id, next_database_commit_sequence,
        },
        journal::{DatabaseCommitSequence, JournalBatch, JournalRecord, JournalSequence},
        positioned_overlay::JournalOverlayPosition,
        registry::{StoreHandle, StoreRecoveryCapability, StoreSchemaMetadataCapability},
        schema::{
            AcceptedSchemaRevision, CandidateSchemaRevision, ConstraintId, ConstraintValidationJob,
            PreparedSchemaPositionPublication, SchemaApplicationRecordOp,
            apply_live_schema_checkpoint, apply_schema_application_record_op,
            ensure_schema_migration_ready_for_schema_changes, preflight_live_schema_checkpoint,
        },
    },
    error::InternalError,
    runtime::now_millis,
    types::EntityTag,
};
/// Optional prepared derived domain that crosses the same accepted-schema marker.

struct StagedSchemaDomain {
    #[cfg(feature = "sql")]
    user_index: Option<StagedUserIndexDomainReplacement>,
}

impl StagedSchemaDomain {
    const fn none() -> Self {
        Self {
            #[cfg(feature = "sql")]
            user_index: None,
        }
    }

    #[cfg(feature = "sql")]
    const fn user_index(replacement: StagedUserIndexDomainReplacement) -> Self {
        Self {
            user_index: Some(replacement),
        }
    }
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

struct PreparedJournaledSchemaPositions {
    schema: PreparedSchemaPositionPublication,
    index: PreparedIndexPositionPublication,
}

fn prepare_journaled_schema_positions(
    store: StoreHandle,
    batch: &JournalBatch,
) -> Result<PreparedJournaledSchemaPositions, InternalError> {
    let allocation = store
        .journal_allocation()
        .ok_or_else(InternalError::store_invariant)?;
    let position = JournalOverlayPosition::new(allocation, batch.journal_sequence());
    let incarnation = database_incarnation_id()?;
    let schema = store.with_schema(|schema_store| {
        schema_store.prepare_positioned_journal_batch_publication(incarnation, batch, position)
    })?;
    let index_keys = batch.records().iter().flat_map(|record| match record {
        JournalRecord::AcceptedSchemaIndexDelete { keys, .. }
        | JournalRecord::AcceptedSchemaIndexPut { keys, .. } => keys.as_slice(),
        JournalRecord::ConstraintValidationIndexPut { key, .. } => std::slice::from_ref(key),
        _ => &[],
    });
    let index = store.with_index(|index_store| {
        index_store.prepare_position_publication(index_keys.cloned(), position)
    })?;
    Ok(PreparedJournaledSchemaPositions { schema, index })
}

fn publish_journaled_schema_positions(
    store: StoreHandle,
    prepared: PreparedJournaledSchemaPositions,
) {
    store.with_index_mut(|index_store| index_store.publish_prepared_positions(prepared.index));
    store.with_schema_mut(|schema_store| {
        schema_store.publish_prepared_journal_batch_positions(prepared.schema);
    });
}

///
/// AcceptedSchemaPublication
///
/// One preconstructed store-local candidate participating in an atomic
/// database-scoped accepted-schema publication.
///

pub(in crate::db) struct AcceptedSchemaPublication<'a> {
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &'a CandidateSchemaRevision,
}

impl<'a> AcceptedSchemaPublication<'a> {
    /// Bind one catalog-native candidate to its registered store authority.
    pub(in crate::db) const fn new(
        store_path: &'static str,
        store: StoreHandle,
        expected_revision: AcceptedSchemaRevision,
        candidate: &'a CandidateSchemaRevision,
    ) -> Self {
        Self {
            store_path,
            store,
            expected_revision,
            candidate,
        }
    }
}

pub(in crate::db) fn publish_accepted_schema_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<(), InternalError> {
    publish_accepted_schema_candidates_atomically(vec![AcceptedSchemaPublication::new(
        store_path,
        store,
        expected_revision,
        candidate,
    )])
}

/// Publish one or more catalog-native accepted candidates through one durable
/// marker boundary.
///
/// Stores with live-only schema allocations checkpoint the exact accepted
/// candidate in database-control memory under the same marker. Recovery can
/// therefore restore accepted catalog authority without consulting authored
/// or generated proposal material.
pub(in crate::db) fn publish_accepted_schema_candidates_atomically(
    publications: Vec<AcceptedSchemaPublication<'_>>,
) -> Result<(), InternalError> {
    publish_accepted_schema_candidates_with_database_control(publications, Vec::new())
}

/// Publish zero or more accepted candidates and one terminal application
/// receipt through the same database-wide marker boundary.
pub(in crate::db) fn publish_accepted_schema_candidates_with_application_record(
    publications: Vec<AcceptedSchemaPublication<'_>>,
    application_record: SchemaApplicationRecordOp,
) -> Result<(), InternalError> {
    publish_accepted_schema_candidates_with_database_control(
        publications,
        vec![DatabaseControlOp::SchemaApplication(application_record)],
    )
}

/// Publish accepted candidates and one bounded database-control transaction
/// through the same marker boundary.
pub(in crate::db) fn publish_accepted_schema_candidates_with_database_control(
    mut publications: Vec<AcceptedSchemaPublication<'_>>,
    database_control: Vec<DatabaseControlOp>,
) -> Result<(), InternalError> {
    // Only an exact migration record operation may accompany publication
    // while that migration owns the predecessor head. Marker validation owns
    // the operation's CAS and legal transition; ordinary publishers cannot
    // change the head merely because Prepared still permits row operations.
    #[cfg(any(test, feature = "migration"))]
    let migration_publication = database_control
        .iter()
        .any(|operation| matches!(operation, DatabaseControlOp::SchemaMigration(_)));
    #[cfg(not(any(test, feature = "migration")))]
    let migration_publication = false;
    if !migration_publication {
        ensure_schema_migration_ready_for_schema_changes()?;
    }
    if publications.is_empty() {
        if database_control.is_empty() {
            return Err(InternalError::store_invariant());
        }
        return publish_database_control_atomically(database_control);
    }
    icydb_schema::compact_sort_unstable_by(&mut publications, |left, right| {
        left.store_path.cmp(right.store_path)
    });
    if publications
        .windows(2)
        .any(|pair| pair[0].store_path == pair[1].store_path)
        || publications
            .iter()
            .any(|publication| publication.candidate.store_path() != publication.store_path)
    {
        return Err(InternalError::store_invariant());
    }
    for publication in &publications {
        validate_constraint_validation_job_change(
            publication.store,
            publication.candidate,
            ConstraintValidationJobChange::None,
        )?;
    }

    let incarnation = database_incarnation_id()?;
    let mut all_current = true;
    let mut any_current = false;
    // Complete preflight in sorted store order before classifying a mixed publication.
    // A later preflight error must retain priority over the mixed-state rejection.
    for publication in &publications {
        let current = publication.store.with_schema(|schema_store| {
            schema_store.preflight_accepted_schema_candidate(
                incarnation,
                publication.expected_revision,
                publication.candidate,
            )
        })?;
        all_current &= current;
        any_current |= current;
    }
    if all_current {
        return if database_control.is_empty() {
            Ok(())
        } else {
            Err(InternalError::store_invariant())
        };
    }
    if any_current {
        return Err(InternalError::store_invariant());
    }

    for publication in &publications {
        if publication.store.storage_capabilities().schema_metadata()
            == StoreSchemaMetadataCapability::LiveRebuiltMetadata
        {
            preflight_live_schema_checkpoint(
                incarnation,
                publication.store_path,
                publication.expected_revision,
                publication.candidate,
            )?;
        }
    }

    publish_candidates_atomically(publications.as_slice(), database_control)
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
        StagedSchemaDomain::none(),
        ConstraintValidationJobChange::Put(job),
        None,
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
        StagedSchemaDomain::none(),
        ConstraintValidationJobChange::Delete {
            entity_tag,
            constraint_id,
        },
        None,
    )
}

/// Publish one generated row-local abort, validation-job removal, and terminal
/// application receipt through the same marker boundary.
pub(in crate::db) fn publish_generated_row_local_abort_with_application_record(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    application_record: SchemaApplicationRecordOp,
) -> Result<(), InternalError> {
    publish_accepted_schema_candidate_with_prepared_domains(
        store_path,
        store,
        expected_revision,
        candidate,
        StagedSchemaDomain::none(),
        ConstraintValidationJobChange::Delete {
            entity_tag,
            constraint_id,
        },
        Some(application_record),
    )
}

/// Advance one validation job without changing accepted constraint meaning.
pub(in crate::db) fn publish_constraint_validation_job(
    store_path: &'static str,
    store: StoreHandle,
    job: &ConstraintValidationJob,
) -> Result<(), InternalError> {
    validated_job_checkpoint_bundle(store_path, store, job)?;

    match store.storage_capabilities().recovery() {
        StoreRecoveryCapability::None => {
            store.with_schema_mut(|schema_store| schema_store.apply_constraint_validation_job(job))
        }
        StoreRecoveryCapability::StableBasePlusJournalReplay => {
            publish_journaled_constraint_validation_job(store_path, store, job, Vec::new())
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
    let bundle = validated_job_checkpoint_bundle(store_path, store, job)?;
    validate_candidate_index_entries(&bundle, job, entries.as_slice())?;
    if store.storage_capabilities().recovery()
        != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_unsupported());
    }

    publish_journaled_constraint_validation_job(store_path, store, job, entries)
}

/// Publish one accepted-schema candidate and its prevalidated entity-owned
/// user-index domain through the same marker window.
#[cfg(feature = "sql")]
pub(in crate::db) fn publish_accepted_schema_candidate_with_user_index_domain(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    replacement: StagedUserIndexDomainReplacement,
    validation_job_removal: Option<(EntityTag, ConstraintId)>,
) -> Result<(), InternalError> {
    validate_user_index_domain_candidate(
        store_path,
        store,
        expected_revision,
        candidate,
        &replacement,
    )?;
    publish_accepted_schema_candidate_with_prepared_domains(
        store_path,
        store,
        expected_revision,
        candidate,
        StagedSchemaDomain::user_index(replacement),
        validation_job_removal.map_or(
            ConstraintValidationJobChange::None,
            |(entity_tag, constraint_id)| ConstraintValidationJobChange::Delete {
                entity_tag,
                constraint_id,
            },
        ),
        None,
    )
}

fn publish_accepted_schema_candidate_with_prepared_domains(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domain: StagedSchemaDomain,
    job_change: ConstraintValidationJobChange<'_>,
    application_record: Option<SchemaApplicationRecordOp>,
) -> Result<(), InternalError> {
    ensure_schema_migration_ready_for_schema_changes()?;
    validate_constraint_validation_job_change(store, candidate, job_change)?;
    match store.storage_capabilities().recovery() {
        StoreRecoveryCapability::None => publish_live_candidate_with_prepared_domains(
            store_path,
            store,
            expected_revision,
            candidate,
            domain,
            job_change,
            application_record,
        ),
        StoreRecoveryCapability::StableBasePlusJournalReplay => publish_journaled_candidate(
            store_path,
            store,
            expected_revision,
            candidate,
            domain,
            job_change,
            application_record,
        ),
    }
}

fn publish_live_candidate_with_prepared_domains(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domain: StagedSchemaDomain,
    job_change: ConstraintValidationJobChange<'_>,
    application_record: Option<SchemaApplicationRecordOp>,
) -> Result<(), InternalError> {
    if !matches!(job_change, ConstraintValidationJobChange::None)
        || application_record.is_some()
        || store.storage_capabilities().schema_metadata()
            != StoreSchemaMetadataCapability::LiveRebuiltMetadata
    {
        return Err(InternalError::store_unsupported());
    }
    let incarnation = database_incarnation_id()?;
    store.with_schema(|schema_store| {
        schema_store.preflight_accepted_schema_candidate(incarnation, expected_revision, candidate)
    })?;
    preflight_live_schema_checkpoint(incarnation, store_path, expected_revision, candidate)?;
    let marker_id = generate_commit_id(now_millis())?;
    let database_commit_sequence = DatabaseCommitSequence::new(next_database_commit_sequence()?);
    let record = JournalRecord::accepted_schema_publish(
        store_path,
        expected_revision,
        candidate.encoded_bundle().to_vec(),
        candidate.encoded_root().to_vec(),
    )?;
    let batch = JournalBatch::new_with_database_commit_sequence(
        marker_id,
        marker_id,
        JournalSequence::new(0),
        database_commit_sequence,
        vec![record],
    )?;
    let marker = CommitMarker::from_parts(marker_id, vec![batch])?;
    let commit = begin_commit(&marker)?;
    drop(marker);

    finish_commit(commit, |_guard| {
        apply_live_schema_checkpoint(incarnation, store_path, expected_revision, candidate)?;
        store.with_schema_mut(|schema_store| {
            schema_store.publish_accepted_schema_candidate(
                incarnation,
                expected_revision,
                candidate,
            )
        })?;
        apply_staged_schema_domain(store, domain)?;
        Ok(())
    })
}

fn publish_journaled_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    domain: StagedSchemaDomain,
    job_change: ConstraintValidationJobChange<'_>,
    application_record: Option<SchemaApplicationRecordOp>,
) -> Result<(), InternalError> {
    let incarnation = database_incarnation_id()?;
    store.with_schema(|schema_store| {
        schema_store.preflight_accepted_schema_candidate(incarnation, expected_revision, candidate)
    })?;
    let journal_store = store
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?;
    let marker_id = generate_commit_id(now_millis())?;
    let database_commit_sequence = DatabaseCommitSequence::new(next_database_commit_sequence()?);
    let sequence = journal_store
        .with_borrow(crate::db::journal::JournalTailStore::next_mutation_append_sequence)?;
    let schema_record = JournalRecord::accepted_schema_publish(
        store_path,
        expected_revision,
        candidate.encoded_bundle().to_vec(),
        candidate.encoded_root().to_vec(),
    )?;
    let mut records = vec![schema_record];
    append_staged_schema_domain_journal_records(store_path, &domain, &mut records)?;
    if let Some(record) = constraint_validation_job_journal_record(store_path, job_change)? {
        records.push(record);
    }
    let batch = JournalBatch::new_with_database_commit_sequence(
        marker_id,
        marker_id,
        sequence,
        database_commit_sequence,
        records,
    )?;
    let positions = prepare_journaled_schema_positions(store, &batch)?;
    let marker = CommitMarker::from_parts_with_schema_application(
        marker_id,
        vec![batch],
        application_record,
    )?;
    // Keep the marker as the sole batch owner through journal publication.
    let batch = marker
        .journal_batches()
        .first()
        .ok_or_else(InternalError::store_invariant)?;
    let commit = begin_commit(&marker)?;

    finish_commit(commit, |guard| {
        #[cfg(test)]
        #[cfg(feature = "sql")]
        interrupt_schema_publication_at(SchemaPublicationInterruption::MarkerPersisted)?;
        let marker_bytes = guard.journal_batch_bytes(0)?;
        journal_store
            .with_borrow_mut(|journal| journal.append_marker_encoded_batch(batch, marker_bytes))?;
        store.with_schema_mut(|schema_store| {
            schema_store.apply_journaled_accepted_schema_candidate(
                incarnation,
                expected_revision,
                candidate,
            )
        })?;
        apply_constraint_validation_job_change(store, job_change)?;
        apply_staged_schema_domain(store, domain)?;
        apply_database_control_ops(marker.database_control())?;
        publish_journaled_schema_positions(store, positions);
        Ok(())
    })
}

#[cfg(not(feature = "sql"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "the no-SQL domain preserves the shared fallible journal-record preparation boundary"
)]
const fn append_staged_schema_domain_journal_records(
    _store_path: &'static str,
    _domain: &StagedSchemaDomain,
    _records: &mut Vec<JournalRecord>,
) -> Result<(), InternalError> {
    Ok(())
}

#[cfg(feature = "sql")]
fn append_staged_schema_domain_journal_records(
    store_path: &'static str,
    domain: &StagedSchemaDomain,
    records: &mut Vec<JournalRecord>,
) -> Result<(), InternalError> {
    let Some(replacement) = domain.user_index.as_ref() else {
        return Ok(());
    };
    let entity_tag = replacement.entity_tag();
    let fingerprint = replacement.accepted_after_fingerprint();
    for keys in replacement
        .deletion_keys()
        .chunks(MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD)
    {
        records.push(JournalRecord::accepted_schema_index_delete(
            store_path,
            entity_tag,
            fingerprint,
            keys.to_vec(),
        )?);
    }
    for entries in replacement
        .insertion_entries()
        .chunks(MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD)
    {
        records.push(JournalRecord::accepted_schema_index_put(
            store_path,
            entity_tag,
            fingerprint,
            entries.iter().map(|entry| entry.key().clone()).collect(),
        )?);
    }
    Ok(())
}

fn publish_candidate_journal_authority(
    publication: &AcceptedSchemaPublication<'_>,
    batch: &JournalBatch,
    marker_bytes: Option<&[u8]>,
) -> Result<(), InternalError> {
    let Some(journal_store) = publication.store.journal_tail_store() else {
        return Ok(());
    };
    if batch.journal_sequence() == JournalSequence::new(0) {
        let accepted_entity_tags = publication
            .candidate
            .bundle()
            .entity_snapshots()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        return journal_store.with_borrow_mut(|journal| {
            journal.publish_accepted_entity_mutation_revisions(accepted_entity_tags.as_slice())
        });
    }
    let marker_bytes = marker_bytes.ok_or_else(InternalError::store_invariant)?;
    journal_store
        .with_borrow_mut(|journal| journal.append_marker_encoded_batch(batch, marker_bytes))
}

fn publish_candidates_atomically(
    publications: &[AcceptedSchemaPublication<'_>],
    database_control: Vec<DatabaseControlOp>,
) -> Result<(), InternalError> {
    let incarnation = database_incarnation_id()?;
    let marker_id = generate_commit_id(now_millis())?;
    let database_commit_sequence = DatabaseCommitSequence::new(next_database_commit_sequence()?);
    let mut batches = Vec::with_capacity(publications.len());
    for (ordinal, publication) in publications.iter().enumerate() {
        let sequence = match (
            publication.expected_revision,
            publication.store.journal_tail_store(),
        ) {
            (AcceptedSchemaRevision::NONE, _) | (_, None) => JournalSequence::new(0),
            (_, Some(journal_store)) => journal_store
                .with_borrow(crate::db::journal::JournalTailStore::next_mutation_append_sequence)?,
        };
        let record = JournalRecord::accepted_schema_publish(
            publication.store_path,
            publication.expected_revision,
            publication.candidate.encoded_bundle().to_vec(),
            publication.candidate.encoded_root().to_vec(),
        )?;
        let batch_id = generate_marker_batch_id(marker_id, ordinal)?;
        batches.push(JournalBatch::new_with_database_commit_sequence(
            batch_id,
            marker_id,
            sequence,
            database_commit_sequence,
            vec![record],
        )?);
    }
    let positions = publications
        .iter()
        .zip(&batches)
        .map(|(publication, batch)| {
            (batch.journal_sequence() != JournalSequence::new(0))
                .then(|| prepare_journaled_schema_positions(publication.store, batch))
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let marker =
        CommitMarker::from_parts_with_database_control(marker_id, batches, database_control)?;
    let commit = begin_commit(&marker)?;

    finish_commit(commit, |guard| {
        for (batch_ordinal, (publication, batch)) in publications
            .iter()
            .zip(marker.journal_batches())
            .enumerate()
        {
            let marker_bytes = (batch.journal_sequence() != JournalSequence::new(0))
                .then(|| guard.journal_batch_bytes(batch_ordinal))
                .transpose()?;
            publish_candidate_journal_authority(publication, batch, marker_bytes)?;
        }
        for publication in publications {
            if publication.store.storage_capabilities().schema_metadata()
                == StoreSchemaMetadataCapability::LiveRebuiltMetadata
            {
                apply_live_schema_checkpoint(
                    incarnation,
                    publication.store_path,
                    publication.expected_revision,
                    publication.candidate,
                )?;
            }
        }
        for publication in publications {
            publication.store.with_schema_mut(|schema_store| {
                match (
                    publication.expected_revision,
                    publication.store.storage_capabilities().recovery(),
                ) {
                    (AcceptedSchemaRevision::NONE, _) | (_, StoreRecoveryCapability::None) => {
                        schema_store.publish_accepted_schema_candidate(
                            incarnation,
                            publication.expected_revision,
                            publication.candidate,
                        )
                    }
                    (_, StoreRecoveryCapability::StableBasePlusJournalReplay) => schema_store
                        .apply_journaled_accepted_schema_candidate(
                            incarnation,
                            publication.expected_revision,
                            publication.candidate,
                        ),
                }
            })?;
        }
        apply_database_control_ops(marker.database_control())?;
        for (publication, positions) in publications.iter().zip(positions) {
            if let Some(positions) = positions {
                publish_journaled_schema_positions(publication.store, positions);
            }
        }
        Ok(())
    })
}

fn publish_database_control_atomically(
    database_control: Vec<DatabaseControlOp>,
) -> Result<(), InternalError> {
    let marker_id = generate_commit_id(now_millis())?;
    let marker =
        CommitMarker::from_parts_with_database_control(marker_id, Vec::new(), database_control)?;
    let commit = begin_commit(&marker)?;
    finish_commit(commit, |_guard| {
        apply_database_control_ops(marker.database_control())
    })
}

fn apply_database_control_ops(operations: &[DatabaseControlOp]) -> Result<(), InternalError> {
    for operation in operations {
        match operation {
            DatabaseControlOp::SchemaApplication(operation) => {
                apply_schema_application_record_op(operation)?;
            }
            DatabaseControlOp::EntitySourceLineage(operation) => {
                crate::db::schema::apply_entity_source_lineage_catalog_op(operation)?;
            }
            #[cfg(any(test, feature = "migration"))]
            DatabaseControlOp::SchemaMigration(operation) => {
                crate::db::schema::apply_schema_migration_record_op(operation)?;
            }
            DatabaseControlOp::MutationProgress(_) => {
                return Err(InternalError::store_invariant());
            }
        }
    }
    Ok(())
}

// Publish a prevalidated job and any candidate keys through one marker. Job-only
// checkpoints use an empty entry list and skip the candidate-index write borrow.
fn publish_journaled_constraint_validation_job(
    store_path: &'static str,
    store: StoreHandle,
    job: &ConstraintValidationJob,
    entries: Vec<RawIndexStoreKey>,
) -> Result<(), InternalError> {
    let journal_store = store
        .journal_tail_store()
        .ok_or_else(InternalError::store_invariant)?;
    let marker_id = generate_commit_id(now_millis())?;
    let database_commit_sequence = DatabaseCommitSequence::new(next_database_commit_sequence()?);
    let sequence = journal_store
        .with_borrow(crate::db::journal::JournalTailStore::next_mutation_append_sequence)?;
    let mut records = Vec::with_capacity(entries.len().saturating_add(1));
    records.push(JournalRecord::constraint_validation_job_put(
        store_path, job,
    )?);
    for key in &entries {
        records.push(JournalRecord::constraint_validation_index_put(
            store_path,
            job.entity_tag(),
            job.constraint_id(),
            key.clone(),
        )?);
    }
    let batch = JournalBatch::new_with_database_commit_sequence(
        marker_id,
        marker_id,
        sequence,
        database_commit_sequence,
        records,
    )?;
    let positions = prepare_journaled_schema_positions(store, &batch)?;
    let marker = CommitMarker::from_parts(marker_id, vec![batch])?;
    let batch = marker
        .journal_batches()
        .first()
        .ok_or_else(InternalError::store_invariant)?;
    let commit = begin_commit(&marker)?;

    finish_commit(commit, |guard| {
        let marker_bytes = guard.journal_batch_bytes(0)?;
        journal_store
            .with_borrow_mut(|journal| journal.append_marker_encoded_batch(batch, marker_bytes))?;
        if !entries.is_empty() {
            store.with_index_mut(|index_store| {
                for key in entries {
                    index_store.insert(key, IndexEntryValue::presence());
                }
                Ok::<_, InternalError>(())
            })?;
        }
        store.with_schema_mut(|schema_store| schema_store.apply_constraint_validation_job(job))?;
        publish_journaled_schema_positions(store, positions);
        Ok(())
    })
}

pub(in crate::db::commit) fn validate_candidate_index_entries<'key>(
    bundle: &crate::db::schema::AcceptedSchemaRevisionBundle,
    job: &ConstraintValidationJob,
    entries: impl IntoIterator<Item = &'key RawIndexStoreKey>,
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
    if job.staged_generation() != Some(candidate.physical_generation()) {
        return Err(InternalError::store_corruption());
    }
    let mut previous = None;
    for raw in entries {
        let key = IndexKey::try_from_raw(raw).map_err(|_| InternalError::store_corruption())?;
        if previous.is_some_and(|previous| previous >= raw)
            || key.key_kind() != crate::db::index::IndexKeyKind::User
            || *key.index_id() != expected
        {
            return Err(InternalError::store_corruption());
        }
        previous = Some(raw);
    }
    Ok(())
}

// Check checkpoint ownership and activation/job closure once for both publication
// paths. Candidate-index validation uses this same accepted bundle afterward.
fn validated_job_checkpoint_bundle(
    store_path: &'static str,
    store: StoreHandle,
    job: &ConstraintValidationJob,
) -> Result<crate::db::schema::AcceptedSchemaRevisionBundle, InternalError> {
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

    Ok(bundle)
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
fn validate_user_index_domain_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    replacement: &StagedUserIndexDomainReplacement,
) -> Result<(), InternalError> {
    let accepted_before_identity = replacement.accepted_before_identity();
    if candidate.store_path() != store_path
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
    if store.index_state() != crate::db::index::IndexState::Ready {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

#[cfg(not(feature = "sql"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "the no-SQL domain is infallible while preserving the shared fallible publication callback"
)]
const fn apply_staged_schema_domain(
    _store: StoreHandle,
    _domain: StagedSchemaDomain,
) -> Result<(), InternalError> {
    Ok(())
}

#[cfg(feature = "sql")]
fn apply_staged_schema_domain(
    store: StoreHandle,
    domain: StagedSchemaDomain,
) -> Result<(), InternalError> {
    if let Some(replacement) = domain.user_index {
        apply_user_index_domain_replacement(store, replacement)?;
    }
    Ok(())
}

#[cfg(feature = "sql")]
fn apply_user_index_domain_replacement(
    store: StoreHandle,
    replacement: StagedUserIndexDomainReplacement,
) -> Result<(), InternalError> {
    let data_generation = store.with_data(DataStore::generation);
    store.mark_index_building()?;
    let (deletion_keys, insertion_entries) = replacement.into_apply_parts();
    store.with_index_mut(|index_store| {
        for key in deletion_keys {
            index_store.remove(&key);
            #[cfg(test)]
            interrupt_schema_publication_at(SchemaPublicationInterruption::IndexDeleted)?;
        }
        for entry in insertion_entries {
            let (key, value) = entry.into_parts();
            index_store.insert_preflighted_absent(key, value);
            #[cfg(test)]
            interrupt_schema_publication_at(SchemaPublicationInterruption::IndexInserted)?;
        }
        index_store.mark_prefix_cardinality_data_generation(data_generation);
        Ok::<(), InternalError>(())
    })?;
    store.mark_index_ready()?;
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[cfg(feature = "sql")]
mod helper_tests;

#[cfg(test)]
#[cfg(feature = "sql")]
pub(in crate::db) use helper_tests::interrupt_next_schema_publication_for_tests;
#[cfg(test)]
#[cfg(feature = "sql")]
use helper_tests::interrupt_schema_publication_at;

#[cfg(test)]
#[cfg(feature = "sql")]
pub(in crate::db) use helper_tests::SchemaPublicationInterruption;
