//! Module: db::commit::schema_publication
//! Responsibility: marker-bound accepted-schema bundle/root publication.
//! Does not own: candidate construction, schema compatibility, or root codecs.
//! Boundary: schema reconciliation -> commit marker/journal -> schema live projection.

use crate::{
    db::{
        commit::{CommitMarker, begin_commit, finish_commit, generate_commit_id},
        journal::{JournalBatch, JournalRecord},
        registry::{StoreHandle, StoreRecoveryCapability},
        schema::{AcceptedSchemaRevision, CandidateSchemaRevision},
    },
    error::InternalError,
};

pub(in crate::db) fn publish_accepted_schema_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<(), InternalError> {
    publish_accepted_schema_candidate_with_row_puts(
        store_path,
        store,
        expected_revision,
        candidate,
        Vec::new(),
    )
}

pub(in crate::db) fn publish_accepted_schema_candidate_with_row_puts(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    row_puts: Vec<JournalRecord>,
) -> Result<(), InternalError> {
    if candidate.store_path() != store_path {
        return Err(InternalError::store_invariant());
    }
    if row_puts
        .iter()
        .any(|record| !matches!(record, JournalRecord::RowPut { .. }))
    {
        return Err(InternalError::store_invariant());
    }

    match store.storage_capabilities().recovery() {
        StoreRecoveryCapability::None => store.with_schema_mut(|schema_store| {
            schema_store.publish_accepted_schema_candidate(expected_revision, candidate)
        }),
        StoreRecoveryCapability::StableBasePlusJournalReplay => {
            publish_journaled_candidate(store_path, store, expected_revision, candidate, row_puts)
        }
    }
}

fn publish_journaled_candidate(
    store_path: &'static str,
    store: StoreHandle,
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
    row_puts: Vec<JournalRecord>,
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
    let mut records = Vec::with_capacity(row_puts.len().saturating_add(1));
    records.push(schema_record);
    records.extend(row_puts);
    let batch = JournalBatch::new(marker_id, marker_id, sequence, records)?;
    let marker = CommitMarker::from_parts(marker_id, vec![batch.clone()])?;
    let commit = begin_commit(marker)?;

    finish_commit(commit, |_guard| {
        journal_store.with_borrow_mut(|journal| journal.append_batch(&batch))?;
        store.with_schema_mut(|schema_store| {
            schema_store.apply_journaled_accepted_schema_candidate(expected_revision, candidate)
        })
    })
}
