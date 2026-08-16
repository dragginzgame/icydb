//! Bounded physical execution for one validated source migration.
//!
//! Candidate rows and derived keys remain private while predecessor authority
//! is gated. Every rewrite page is journaled with the same marker that advances
//! its durable cursor; final validation reads only the resulting candidate form.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

use icydb_diagnostic_code::SchemaMigrationCode;

use crate::{
    db::{
        Db,
        commit::{
            CommitMarker, DatabaseControlOp, begin_commit, finish_commit, generate_commit_id,
            generate_marker_batch_id, next_database_commit_sequence,
        },
        data::{DecodedDataStoreKey, RawDataStoreKey, RawRow, StoreVisit, StructuralSlotReader},
        direction::Direction,
        index::{IndexEntryValue, IndexId, IndexKey, RawIndexStoreKey},
        journal::{
            DatabaseCommitSequence, JournalBatch, JournalRecord, MAX_JOURNAL_BATCH_RECORDS,
            journal_record_payload_len,
        },
        key_taxonomy::RawDataStoreKeyRange,
        registry::{StoreHandle, StoreRecoveryCapability},
        relation::{RelationConstraintProjection, ReverseRelationSourceInfo},
        schema::{
            AcceptedCatalogSnapshotSelection, CompiledAcceptedRowConstraints,
            MigrationIndexProjection, PersistedSchemaMigrationIndexCursor,
            PersistedSchemaMigrationProgress, PersistedSchemaMigrationRowCursor,
            SchemaMigrationRecordOp, accepted_commit_schema_fingerprint,
            apply_schema_migration_record_op, migration_planner::PlannedSchemaMigration,
            migration_transform::CompiledMigrationEntityProgram,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};
use icydb_schema::TargetStoreIdentity;

#[cfg(test)]
thread_local! {
    static MIGRATION_REWRITE_INTERRUPTION: std::cell::Cell<Option<MigrationRewriteInterruption>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::db::schema) enum MigrationRewriteInterruption {
    MarkerPersisted,
    JournalPublished,
    PhysicalApplied,
}

#[cfg(test)]
pub(in crate::db::schema) fn interrupt_next_migration_rewrite_at(
    interruption: MigrationRewriteInterruption,
) {
    MIGRATION_REWRITE_INTERRUPTION.with(|next| next.set(Some(interruption)));
}

#[cfg(test)]
fn take_migration_rewrite_interruption(interruption: MigrationRewriteInterruption) -> bool {
    MIGRATION_REWRITE_INTERRUPTION.with(|next| {
        if next.get() == Some(interruption) {
            next.set(None);
            true
        } else {
            false
        }
    })
}

#[cfg(test)]
fn migration_rewrite_row_limit() -> usize {
    MIGRATION_REWRITE_INTERRUPTION.with(|next| {
        if next.get().is_some() {
            1
        } else {
            MAX_MIGRATION_REWRITE_ROWS_PER_PAGE
        }
    })
}

const MAX_MIGRATION_REWRITE_ROWS_PER_PAGE: usize = 256;
const MAX_MIGRATION_REWRITE_ROW_BYTES_PER_PAGE: usize = 1024 * 1024;
const MAX_MIGRATION_REWRITE_INDEX_BYTES_PER_PAGE: usize = 1024 * 1024;
const MAX_MIGRATION_REWRITE_JOURNAL_BYTES_PER_PAGE: usize = 8 * 1024 * 1024;
const MAX_MIGRATION_FINAL_VALIDATION_ROWS_PER_PAGE: usize = 256;
const MAX_MIGRATION_FINAL_VALIDATION_BYTES_PER_PAGE: usize = 1024 * 1024;
const MAX_MIGRATION_ABORT_INDEX_ENTRIES_PER_PAGE: usize = 512;
const MAX_MIGRATION_ABORT_INDEX_BYTES_PER_PAGE: usize = 1024 * 1024;

const _: () = {
    assert!(MAX_MIGRATION_REWRITE_ROWS_PER_PAGE > 0);
    assert!(MAX_MIGRATION_REWRITE_ROW_BYTES_PER_PAGE <= 1024 * 1024);
    assert!(MAX_MIGRATION_REWRITE_INDEX_BYTES_PER_PAGE <= 1024 * 1024);
    assert!(MAX_MIGRATION_REWRITE_JOURNAL_BYTES_PER_PAGE <= 8 * 1024 * 1024);
};

pub(in crate::db::schema) struct MigrationRewritePage {
    progress: PersistedSchemaMigrationProgress,
    effects: Vec<MigrationPhysicalEffect>,
    exhausted: bool,
}

impl MigrationRewritePage {
    pub(in crate::db::schema) fn into_parts(
        self,
    ) -> (
        PersistedSchemaMigrationProgress,
        Vec<MigrationPhysicalEffect>,
        bool,
    ) {
        (self.progress, self.effects, self.exhausted)
    }
}

pub(in crate::db::schema) struct MigrationFinalValidationPage {
    progress: PersistedSchemaMigrationProgress,
    exhausted: bool,
}

impl MigrationFinalValidationPage {
    pub(in crate::db::schema) fn into_parts(self) -> (PersistedSchemaMigrationProgress, bool) {
        (self.progress, self.exhausted)
    }
}

pub(in crate::db::schema) struct MigrationPhysicalEffect {
    store_path: &'static str,
    store: StoreHandle,
    record: JournalRecord,
}

struct PreparedCandidateEntity {
    candidate_contract: crate::db::data::StructuralRowContract,
    candidate_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    constraints: CompiledAcceptedRowConstraints,
    indexes: Vec<MigrationIndexProjection>,
    relations: Vec<RelationConstraintProjection>,
}

pub(in crate::db::schema) fn rewrite_migration_page<C: CanisterKind>(
    db: &Db<C>,
    planned: &PlannedSchemaMigration,
    before_progress: &PersistedSchemaMigrationProgress,
    plan_digest: icydb_schema::SchemaMigrationPlanDigest,
) -> Result<MigrationRewritePage, InternalError> {
    #[cfg(not(test))]
    let mut remaining_rows = MAX_MIGRATION_REWRITE_ROWS_PER_PAGE;
    #[cfg(test)]
    let mut remaining_rows = migration_rewrite_row_limit();
    let mut remaining_row_bytes = MAX_MIGRATION_REWRITE_ROW_BYTES_PER_PAGE;
    let mut remaining_index_bytes = MAX_MIGRATION_REWRITE_INDEX_BYTES_PER_PAGE;
    let mut remaining_effects = MAX_JOURNAL_BATCH_RECORDS;
    let mut remaining_journal_bytes = MAX_MIGRATION_REWRITE_JOURNAL_BYTES_PER_PAGE;
    let mut rows_rewritten = 0_u64;
    let mut effects = Vec::new();
    let mut final_cursor = before_progress.row_cursor().cloned();
    let mut exhausted = true;

    for program in planned.programs() {
        let cursor = before_progress.row_cursor();
        if cursor.is_some_and(|cursor| {
            (program.store(), program.entity()) < (cursor.store(), cursor.entity())
        }) {
            continue;
        }
        if remaining_rows == 0
            || remaining_row_bytes == 0
            || remaining_index_bytes == 0
            || remaining_effects == 0
            || remaining_journal_bytes == 0
        {
            exhausted = false;
            break;
        }
        let candidate = candidate_for_program(planned, program)?;
        let prepared = prepare_candidate_entity(db, program, candidate)?;
        let store = db.store_handle(program.store_path())?;
        require_journaled(store)?;
        let page = rewrite_entity_page(
            store,
            program,
            &prepared,
            cursor.filter(|cursor| {
                cursor.store() == program.store() && cursor.entity() == program.entity()
            }),
            remaining_rows,
            remaining_row_bytes,
            remaining_index_bytes,
            remaining_effects,
            remaining_journal_bytes,
            plan_digest,
        )?;
        remaining_rows = remaining_rows.saturating_sub(page.rows);
        remaining_row_bytes = remaining_row_bytes.saturating_sub(page.row_bytes);
        remaining_index_bytes = remaining_index_bytes.saturating_sub(page.index_bytes);
        remaining_effects = remaining_effects.saturating_sub(page.effects.len());
        remaining_journal_bytes = remaining_journal_bytes.saturating_sub(page.journal_bytes);
        rows_rewritten = rows_rewritten
            .checked_add(u64::try_from(page.rows).map_err(|_| InternalError::store_invariant())?)
            .ok_or_else(InternalError::store_invariant)?;
        if let Some(cursor) = page.cursor {
            final_cursor = Some(cursor);
        }
        effects.extend(page.effects);
        if !page.exhausted {
            exhausted = false;
            break;
        }
    }
    let progress = before_progress.with_rewrite_page(final_cursor, rows_rewritten)?;
    Ok(MigrationRewritePage {
        progress,
        effects,
        exhausted,
    })
}

struct EntityRewritePage {
    cursor: Option<PersistedSchemaMigrationRowCursor>,
    rows: usize,
    row_bytes: usize,
    index_bytes: usize,
    journal_bytes: usize,
    effects: Vec<MigrationPhysicalEffect>,
    exhausted: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "the bounded rewrite loop keeps all engine-owned budgets explicit"
)]
#[expect(
    clippy::too_many_lines,
    reason = "one bounded row visit keeps transform, candidate admission, and exact row/index effect construction adjacent"
)]
fn rewrite_entity_page(
    store: StoreHandle,
    program: &CompiledMigrationEntityProgram,
    prepared: &PreparedCandidateEntity,
    checkpoint: Option<&PersistedSchemaMigrationRowCursor>,
    row_budget: usize,
    row_byte_budget: usize,
    index_byte_budget: usize,
    effect_budget: usize,
    journal_byte_budget: usize,
    plan_digest: icydb_schema::SchemaMigrationPlanDigest,
) -> Result<EntityRewritePage, InternalError> {
    let before_selection = store
        .with_schema(|schema| {
            schema.current_accepted_catalog_selection(
                program.entity(),
                program.before_path(),
                program.store_path(),
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let before_contract = crate::db::data::AcceptedStructuralRowAuthority::from_catalog_selection(
        program.before_path(),
        &before_selection,
    )?
    .into_row_contract();
    let range = RawDataStoreKeyRange::entity_prefix(program.entity());
    let lower = match checkpoint {
        None => Bound::Included(RawDataStoreKey::store_range_lower_key(&range)),
        Some(cursor) => Bound::Excluded(RawDataStoreKey::from_persisted_bytes(
            cursor.primary_key().to_vec(),
        )),
    };
    let upper = range
        .upper_exclusive()
        .map(RawDataStoreKey::from_store_range_bound)
        .map_or(Bound::Unbounded, Bound::Excluded);
    let mut page = EntityRewritePage {
        cursor: checkpoint.cloned(),
        rows: 0,
        row_bytes: 0,
        index_bytes: 0,
        journal_bytes: 0,
        effects: Vec::new(),
        exhausted: true,
    };
    store.with_data(|data| {
        data.visit_range(
            (lower, upper),
            |raw_key, raw_row| -> Result<StoreVisit, InternalError> {
                if page.rows == row_budget {
                    page.exhausted = false;
                    return Ok(StoreVisit::Stop);
                }
                let before = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                    raw_row,
                    &before_contract,
                )?;
                let decoded = DecodedDataStoreKey::try_from_raw(raw_key)
                    .map_err(|_| InternalError::store_corruption())?;
                before.validate_primary_key(&decoded)?;
                let candidate = program
                    .evaluate(&before, &prepared.candidate_contract, &decoded)
                    .map_err(|_| {
                        InternalError::schema_migration(SchemaMigrationCode::CandidateMismatch)
                    })?;
                let candidate_bytes = candidate.as_raw_row().as_bytes().to_vec();
                let next_row_bytes = page
                    .row_bytes
                    .checked_add(candidate_bytes.len())
                    .ok_or_else(InternalError::store_invariant)?;
                let candidate_reader =
                    StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                        candidate.as_raw_row(),
                        &prepared.candidate_contract,
                    )?;
                candidate_reader.validate_primary_key(&decoded)?;
                let mut row_effects = Vec::new();
                let mut row_index_bytes = 0usize;
                for projection in &prepared.indexes {
                    let Some(key) =
                        projection.derive_key(&decoded.primary_key_value(), &candidate_reader)?
                    else {
                        continue;
                    };
                    row_index_bytes = row_index_bytes
                        .checked_add(key.as_bytes().len())
                        .ok_or_else(InternalError::store_invariant)?;
                    row_effects.push(MigrationPhysicalEffect {
                        store_path: program.store_path(),
                        store,
                        record: JournalRecord::schema_migration_index_put(
                            program.store_path(),
                            key,
                            plan_digest,
                        )?,
                    });
                }
                for relation in &prepared.relations {
                    let projected = relation.project_row(
                        &decoded.primary_key_value(),
                        &candidate_reader,
                        true,
                    )?;
                    if !projected.missing_targets().is_empty() {
                        return Err(InternalError::schema_migration(
                            SchemaMigrationCode::CandidateMismatch,
                        ));
                    }
                    for entry in projected.into_entries() {
                        row_index_bytes = row_index_bytes
                            .checked_add(entry.key().as_bytes().len())
                            .ok_or_else(InternalError::store_invariant)?;
                        row_effects.push(MigrationPhysicalEffect {
                            store_path: entry.target_store_path(),
                            store: entry.target_store(),
                            record: JournalRecord::schema_migration_index_put(
                                entry.target_store_path(),
                                entry.key().clone(),
                                plan_digest,
                            )?,
                        });
                    }
                }
                let next_index_bytes = page
                    .index_bytes
                    .checked_add(row_index_bytes)
                    .ok_or_else(InternalError::store_invariant)?;
                if next_row_bytes > row_byte_budget || next_index_bytes > index_byte_budget {
                    if page.rows == 0 {
                        return Err(InternalError::store_unsupported());
                    }
                    page.exhausted = false;
                    return Ok(StoreVisit::Stop);
                }
                let row_record = JournalRecord::schema_migration_row_put(
                    program.store_path(),
                    raw_key.clone(),
                    candidate_bytes,
                    prepared.candidate_fingerprint,
                    plan_digest,
                )?;
                let row_effect_count = row_effects.len().saturating_add(1);
                let next_effect_count = page.effects.len().saturating_add(row_effect_count);
                let row_journal_bytes = row_effects.iter().fold(
                    journal_record_payload_len(&row_record),
                    |bytes, effect| {
                        bytes.saturating_add(journal_record_payload_len(&effect.record))
                    },
                );
                let next_journal_bytes = page.journal_bytes.saturating_add(row_journal_bytes);
                if next_effect_count > effect_budget || next_journal_bytes > journal_byte_budget {
                    if page.rows == 0 {
                        return Err(InternalError::store_unsupported());
                    }
                    page.exhausted = false;
                    return Ok(StoreVisit::Stop);
                }
                page.effects.push(MigrationPhysicalEffect {
                    store_path: program.store_path(),
                    store,
                    record: row_record,
                });
                page.effects.extend(row_effects);
                page.rows = page.rows.saturating_add(1);
                page.row_bytes = next_row_bytes;
                page.index_bytes = next_index_bytes;
                page.journal_bytes = next_journal_bytes;
                page.cursor = Some(PersistedSchemaMigrationRowCursor::try_new(
                    program.store(),
                    program.entity(),
                    raw_key.as_bytes().to_vec(),
                )?);
                Ok(StoreVisit::Continue)
            },
        )
    })?;
    Ok(page)
}

pub(in crate::db::schema) fn publish_migration_rewrite_page(
    mut effects: Vec<MigrationPhysicalEffect>,
    operation: SchemaMigrationRecordOp,
) -> Result<(), InternalError> {
    effects.sort_by(|left, right| {
        left.store_path
            .cmp(right.store_path)
            .then_with(|| {
                migration_effect_kind(&left.record).cmp(&migration_effect_kind(&right.record))
            })
            .then_with(|| {
                migration_effect_key(&left.record).cmp(migration_effect_key(&right.record))
            })
    });
    effects
        .dedup_by(|left, right| left.store_path == right.store_path && left.record == right.record);
    let marker_id = generate_commit_id()?;
    let database_commit_sequence = DatabaseCommitSequence::new(next_database_commit_sequence()?);
    let mut grouped = BTreeMap::<&'static str, (StoreHandle, Vec<JournalRecord>)>::new();
    for effect in effects {
        require_journaled(effect.store)?;
        grouped
            .entry(effect.store_path)
            .or_insert_with(|| (effect.store, Vec::new()))
            .1
            .push(effect.record);
    }
    let mut batches = Vec::with_capacity(grouped.len());
    for (ordinal, (store_path, (store, records))) in grouped.into_iter().enumerate() {
        preflight_migration_effects(store_path, store, &records)?;
        let journal = store
            .journal_tail_store()
            .ok_or_else(InternalError::store_invariant)?;
        let sequence = journal
            .with_borrow(crate::db::journal::JournalTailStore::next_mutation_append_sequence)?;
        let batch_id = generate_marker_batch_id(marker_id, ordinal)?;
        batches.push((
            store_path,
            store,
            JournalBatch::new_with_database_commit_sequence(
                batch_id,
                marker_id,
                sequence,
                database_commit_sequence,
                records,
            )?,
        ));
    }
    let marker = CommitMarker::from_parts_with_database_control(
        marker_id,
        batches.iter().map(|(_, _, batch)| batch.clone()).collect(),
        vec![DatabaseControlOp::SchemaMigration(operation.clone())],
    )?;
    let commit = begin_commit(marker)?;
    #[cfg(test)]
    if take_migration_rewrite_interruption(MigrationRewriteInterruption::MarkerPersisted) {
        return Err(InternalError::executor_invariant());
    }
    finish_commit(commit, |_guard| {
        for (_, store, batch) in &batches {
            let journal = store
                .journal_tail_store()
                .ok_or_else(InternalError::store_invariant)?;
            journal.with_borrow_mut(|tail| tail.append_batch(batch))?;
        }
        #[cfg(test)]
        if take_migration_rewrite_interruption(MigrationRewriteInterruption::JournalPublished) {
            return Err(InternalError::executor_invariant());
        }
        for (store_path, store, batch) in &batches {
            for record in batch.records() {
                apply_migration_effect(store_path, *store, record)?;
            }
        }
        #[cfg(test)]
        if take_migration_rewrite_interruption(MigrationRewriteInterruption::PhysicalApplied) {
            return Err(InternalError::executor_invariant());
        }
        apply_schema_migration_record_op(&operation)
    })
}

fn apply_migration_effect(
    store_path: &'static str,
    store: StoreHandle,
    record: &JournalRecord,
) -> Result<(), InternalError> {
    match record {
        JournalRecord::SchemaMigrationRowPut {
            store_path: record_store_path,
            primary_key,
            row_bytes,
            ..
        } if record_store_path == store_path => {
            let row =
                RawRow::from_untrusted_bytes(row_bytes.clone()).map_err(InternalError::from)?;
            store.with_data_mut(|data| {
                data.apply_recovered_journal_put(primary_key.clone(), row)
                    .map(drop)
            })
        }
        JournalRecord::SchemaMigrationIndexPut {
            store_path: record_store_path,
            key,
            ..
        } if record_store_path == store_path => store.with_index_mut(|index| {
            match index.get(key) {
                None => {
                    index.insert(key.clone(), IndexEntryValue::presence());
                }
                Some(value) if value == IndexEntryValue::presence() => {}
                Some(_) => return Err(InternalError::store_corruption()),
            }
            Ok(())
        }),
        _ => Err(InternalError::store_invariant()),
    }
}

fn migration_effect_key(record: &JournalRecord) -> &[u8] {
    match record {
        JournalRecord::SchemaMigrationRowPut { primary_key, .. } => primary_key.as_bytes(),
        JournalRecord::SchemaMigrationIndexPut { key, .. } => key.as_bytes(),
        _ => &[],
    }
}

const fn migration_effect_kind(record: &JournalRecord) -> u8 {
    match record {
        JournalRecord::SchemaMigrationRowPut { .. } => 0,
        JournalRecord::SchemaMigrationIndexPut { .. } => 1,
        _ => u8::MAX,
    }
}

fn preflight_migration_effects(
    store_path: &'static str,
    store: StoreHandle,
    records: &[JournalRecord],
) -> Result<(), InternalError> {
    for record in records {
        match record {
            JournalRecord::SchemaMigrationRowPut {
                store_path: record_store_path,
                primary_key,
                ..
            } if *record_store_path == store_path => {
                if store.with_data(|data| data.get(primary_key)).is_none() {
                    return Err(InternalError::store_corruption());
                }
            }
            JournalRecord::SchemaMigrationIndexPut {
                store_path: record_store_path,
                key,
                ..
            } if *record_store_path == store_path => {
                if store
                    .with_index(|index| index.get(key))
                    .is_some_and(|value| value != IndexEntryValue::presence())
                {
                    return Err(InternalError::store_corruption());
                }
            }
            _ => return Err(InternalError::store_invariant()),
        }
    }
    Ok(())
}

pub(in crate::db::schema) fn final_validate_migration_page<C: CanisterKind>(
    db: &Db<C>,
    planned: &PlannedSchemaMigration,
    before_progress: &PersistedSchemaMigrationProgress,
) -> Result<MigrationFinalValidationPage, InternalError> {
    let mut remaining_rows = MAX_MIGRATION_FINAL_VALIDATION_ROWS_PER_PAGE;
    let mut remaining_bytes = MAX_MIGRATION_FINAL_VALIDATION_BYTES_PER_PAGE;
    let mut final_cursor = before_progress.row_cursor().cloned();
    let mut exhausted = true;
    for program in planned.programs() {
        let cursor = before_progress.row_cursor();
        if cursor.is_some_and(|cursor| {
            (program.store(), program.entity()) < (cursor.store(), cursor.entity())
        }) {
            continue;
        }
        if remaining_rows == 0 || remaining_bytes == 0 {
            exhausted = false;
            break;
        }
        let candidate = candidate_for_program(planned, program)?;
        let prepared = prepare_candidate_entity(db, program, candidate)?;
        let store = db.store_handle(program.store_path())?;
        let page = final_validate_entity_page(
            store,
            program,
            &prepared,
            cursor.filter(|cursor| {
                cursor.store() == program.store() && cursor.entity() == program.entity()
            }),
            remaining_rows,
            remaining_bytes,
        )?;
        remaining_rows = remaining_rows.saturating_sub(page.rows);
        remaining_bytes = remaining_bytes.saturating_sub(page.bytes);
        if let Some(cursor) = page.cursor {
            final_cursor = Some(cursor);
        }
        if !page.exhausted {
            exhausted = false;
            break;
        }
    }
    let progress = before_progress.with_rewrite_page(final_cursor, 0)?;
    Ok(MigrationFinalValidationPage {
        progress,
        exhausted,
    })
}

struct FinalEntityPage {
    cursor: Option<PersistedSchemaMigrationRowCursor>,
    rows: usize,
    bytes: usize,
    exhausted: bool,
}

fn final_validate_entity_page(
    store: StoreHandle,
    program: &CompiledMigrationEntityProgram,
    prepared: &PreparedCandidateEntity,
    checkpoint: Option<&PersistedSchemaMigrationRowCursor>,
    row_budget: usize,
    byte_budget: usize,
) -> Result<FinalEntityPage, InternalError> {
    let range = RawDataStoreKeyRange::entity_prefix(program.entity());
    let lower = match checkpoint {
        None => Bound::Included(RawDataStoreKey::store_range_lower_key(&range)),
        Some(cursor) => Bound::Excluded(RawDataStoreKey::from_persisted_bytes(
            cursor.primary_key().to_vec(),
        )),
    };
    let upper = range
        .upper_exclusive()
        .map(RawDataStoreKey::from_store_range_bound)
        .map_or(Bound::Unbounded, Bound::Excluded);
    let mut page = FinalEntityPage {
        cursor: checkpoint.cloned(),
        rows: 0,
        bytes: 0,
        exhausted: true,
    };
    store.with_data(|data| {
        data.visit_range((lower, upper), |raw_key, raw_row| {
            if page.rows == row_budget {
                page.exhausted = false;
                return Ok(StoreVisit::Stop);
            }
            let next_bytes = page
                .bytes
                .checked_add(raw_row.len())
                .ok_or_else(InternalError::store_invariant)?;
            if next_bytes > byte_budget {
                if page.rows == 0 {
                    return Err(InternalError::store_unsupported());
                }
                page.exhausted = false;
                return Ok(StoreVisit::Stop);
            }
            let decoded = DecodedDataStoreKey::try_from_raw(raw_key)
                .map_err(|_| InternalError::store_corruption())?;
            let row = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                raw_row,
                &prepared.candidate_contract,
            )?;
            row.validate_primary_key(&decoded)?;
            let values = row.decode_selected_slot_values(prepared.constraints.required_slots())?;
            prepared
                .constraints
                .evaluate(prepared.candidate_fingerprint, values.as_slice())
                .map_err(|_| {
                    InternalError::schema_migration(SchemaMigrationCode::CandidateMismatch)
                })?;
            for projection in &prepared.indexes {
                if let Some(key) = projection.derive_key(&decoded.primary_key_value(), &row)?
                    && store.with_index(|index| index.get(&key))
                        != Some(IndexEntryValue::presence())
                {
                    return Err(InternalError::schema_migration(
                        SchemaMigrationCode::CandidateMismatch,
                    ));
                }
            }
            for relation in &prepared.relations {
                let projected = relation.project_row(&decoded.primary_key_value(), &row, true)?;
                if !projected.missing_targets().is_empty()
                    || projected.into_entries().iter().any(|entry| {
                        entry
                            .target_store()
                            .with_index(|index| index.get(entry.key()))
                            != Some(IndexEntryValue::presence())
                    })
                {
                    return Err(InternalError::schema_migration(
                        SchemaMigrationCode::CandidateMismatch,
                    ));
                }
            }
            page.rows = page.rows.saturating_add(1);
            page.bytes = next_bytes;
            page.cursor = Some(PersistedSchemaMigrationRowCursor::try_new(
                program.store(),
                program.entity(),
                raw_key.as_bytes().to_vec(),
            )?);
            Ok(StoreVisit::Continue)
        })
    })?;
    Ok(page)
}

fn candidate_for_program<'a>(
    planned: &'a PlannedSchemaMigration,
    program: &CompiledMigrationEntityProgram,
) -> Result<&'a crate::db::schema::CandidateSchemaRevision, InternalError> {
    planned
        .candidates()
        .iter()
        .find(|candidate| candidate.store_path() == program.store_path())
        .ok_or_else(InternalError::store_invariant)
}

fn prepare_candidate_entity<C: CanisterKind>(
    db: &Db<C>,
    program: &CompiledMigrationEntityProgram,
    candidate: &crate::db::schema::CandidateSchemaRevision,
) -> Result<PreparedCandidateEntity, InternalError> {
    let candidate_selection = AcceptedCatalogSnapshotSelection::from_candidate(
        candidate,
        program.entity(),
        program.candidate_path(),
        program.store_path(),
    )?
    .ok_or_else(InternalError::store_invariant)?;
    let candidate_schema = candidate_selection.decode_verified()?;
    let candidate_contract =
        crate::db::data::AcceptedStructuralRowAuthority::from_catalog_selection(
            program.candidate_path(),
            &candidate_selection,
        )?
        .into_row_contract();
    let store = db.store_handle(program.store_path())?;
    let before_schema = store
        .with_schema(|schema| {
            schema.current_accepted_catalog_selection(
                program.entity(),
                program.before_path(),
                program.store_path(),
            )
        })?
        .ok_or_else(InternalError::store_corruption)?
        .decode_verified()?;
    let indexes = candidate_schema
        .persisted_snapshot()
        .indexes()
        .iter()
        .filter(|index| {
            before_schema
                .persisted_snapshot()
                .indexes()
                .iter()
                .find(|before| before.schema_id() == index.schema_id())
                .is_none_or(|before| before.physical_generation() != index.physical_generation())
        })
        .chain(candidate_schema.persisted_snapshot().candidate_indexes())
        .map(|index| MigrationIndexProjection::new(program.entity(), index, &candidate_contract))
        .collect::<Result<Vec<_>, _>>()?;
    let source = ReverseRelationSourceInfo::new(program.candidate_path(), program.entity());
    let relations = candidate_schema
        .persisted_snapshot()
        .relations()
        .iter()
        .filter(|edge| {
            before_schema
                .persisted_snapshot()
                .relations()
                .iter()
                .find(|before| before.id() == edge.id())
                .is_none_or(|before| before.physical_generation() != edge.physical_generation())
        })
        .map(|edge| {
            RelationConstraintProjection::new_active(
                db,
                source.clone(),
                candidate_schema.persisted_snapshot(),
                &candidate_contract,
                edge,
            )
        })
        .chain(
            candidate_schema
                .persisted_snapshot()
                .candidate_relations()
                .iter()
                .map(|edge| {
                    RelationConstraintProjection::new(
                        db,
                        source.clone(),
                        candidate_schema.persisted_snapshot(),
                        &candidate_contract,
                        edge,
                    )
                }),
        )
        .collect::<Result<Vec<_>, _>>()?;
    if relations.iter().any(|relation| {
        relation.target_store().storage_capabilities().recovery()
            != StoreRecoveryCapability::StableBasePlusJournalReplay
    }) {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PhysicalRunnerMissing,
        ));
    }
    let candidate_fingerprint = accepted_commit_schema_fingerprint(&candidate_schema)?;
    let constraints = CompiledAcceptedRowConstraints::compile(
        &candidate_schema,
        candidate_selection.value_catalog_handle(),
        candidate_fingerprint,
    )
    .map_err(|_| InternalError::accepted_row_constraint_program_corrupt())?;
    Ok(PreparedCandidateEntity {
        candidate_contract,
        candidate_fingerprint,
        constraints,
        indexes,
        relations,
    })
}

pub(in crate::db::schema) fn migration_derived_domain_count<C: CanisterKind>(
    db: &Db<C>,
    planned: &PlannedSchemaMigration,
) -> Result<u32, InternalError> {
    let mut count = 0_u32;
    for program in planned.programs() {
        let candidate = candidate_for_program(planned, program)?;
        let prepared = prepare_candidate_entity(db, program, candidate)?;
        count = count
            .checked_add(
                u32::try_from(
                    prepared
                        .indexes
                        .len()
                        .saturating_add(prepared.relations.len()),
                )
                .map_err(|_| InternalError::store_invariant())?,
            )
            .ok_or_else(InternalError::store_invariant)?;
    }
    Ok(count)
}

struct MigrationDerivedDomain {
    store_path: &'static str,
    store: StoreHandle,
    index_id: IndexId,
}

/// Delete one bounded page of planner-invisible candidate generations.
///
/// Progress advances over every visited raw entry, not only deleted entries,
/// so an abort cannot hide an unbounded scan behind sparse candidate domains.
pub(in crate::db::schema) fn cleanup_migration_staging_page<C: CanisterKind>(
    db: &Db<C>,
    planned: &PlannedSchemaMigration,
    before_progress: &PersistedSchemaMigrationProgress,
    store_identities: &BTreeMap<&'static str, TargetStoreIdentity>,
) -> Result<(PersistedSchemaMigrationProgress, bool), InternalError> {
    let domains = migration_derived_domains(db, planned)?;
    let mut by_store = BTreeMap::<&'static str, (StoreHandle, BTreeSet<IndexId>)>::new();
    for domain in domains {
        by_store
            .entry(domain.store_path)
            .or_insert_with(|| (domain.store, BTreeSet::new()))
            .1
            .insert(domain.index_id);
    }
    let cursor = before_progress.index_cursor();
    let cursor_path = cursor
        .map(|cursor| {
            store_identities
                .iter()
                .find_map(|(path, identity)| (*identity == cursor.store()).then_some(*path))
                .ok_or_else(InternalError::store_corruption)
        })
        .transpose()?;
    let mut remaining_entries = MAX_MIGRATION_ABORT_INDEX_ENTRIES_PER_PAGE;
    let mut remaining_bytes = MAX_MIGRATION_ABORT_INDEX_BYTES_PER_PAGE;
    let mut final_cursor = cursor.cloned();
    let mut exhausted = true;
    let mut reached_cursor_store = cursor_path.is_none();

    for (store_path, (store, candidate_ids)) in by_store {
        if !reached_cursor_store {
            if cursor_path != Some(store_path) {
                continue;
            }
            reached_cursor_store = true;
        }
        if remaining_entries == 0 || remaining_bytes == 0 {
            exhausted = false;
            break;
        }
        let lower = if cursor_path == Some(store_path) {
            cursor.map_or(Bound::Unbounded, |cursor| {
                Bound::Excluded(RawIndexStoreKey::from_persisted_bytes(
                    cursor.key().to_vec(),
                ))
            })
        } else {
            Bound::Unbounded
        };
        let upper = Bound::Unbounded;
        let mut removals = Vec::new();
        let mut store_exhausted = true;
        store.with_index(|index| {
            index.visit_raw_entries_in_range((&lower, &upper), Direction::Asc, |raw, _| {
                if remaining_entries == 0 || remaining_bytes < raw.as_bytes().len() {
                    store_exhausted = false;
                    return Ok(true);
                }
                let decoded =
                    IndexKey::try_from_raw(raw).map_err(|_| InternalError::store_corruption())?;
                remaining_entries = remaining_entries.saturating_sub(1);
                remaining_bytes = remaining_bytes.saturating_sub(raw.as_bytes().len());
                let index_id = decoded.index_id();
                final_cursor = Some(PersistedSchemaMigrationIndexCursor::try_new(
                    *store_identities
                        .get(store_path)
                        .ok_or_else(InternalError::store_invariant)?,
                    index_id.entity_tag(),
                    u64::from(index_id.ordinal()).saturating_add(1),
                    raw.as_bytes().to_vec(),
                )?);
                if candidate_ids.contains(index_id) {
                    removals.push(raw.clone());
                }
                Ok(false)
            })
        })?;
        store.with_index_mut(|index| {
            for key in removals {
                index.remove(&key);
            }
            index.fold_journaled_materialized_view()
        })?;
        if !store_exhausted {
            exhausted = false;
            break;
        }
    }
    if cursor_path.is_some() && !reached_cursor_store {
        return Err(InternalError::store_corruption());
    }
    let progress =
        before_progress.with_index_progress(if exhausted { None } else { final_cursor }, 0)?;
    Ok((progress, exhausted))
}

fn migration_derived_domains<C: CanisterKind>(
    db: &Db<C>,
    planned: &PlannedSchemaMigration,
) -> Result<Vec<MigrationDerivedDomain>, InternalError> {
    let mut domains = Vec::new();
    for program in planned.programs() {
        let candidate = candidate_for_program(planned, program)?;
        let prepared = prepare_candidate_entity(db, program, candidate)?;
        let source_store = db.store_handle(program.store_path())?;
        domains.extend(
            prepared
                .indexes
                .iter()
                .map(|projection| MigrationDerivedDomain {
                    store_path: program.store_path(),
                    store: source_store,
                    index_id: projection.index_id(),
                }),
        );
        for relation in &prepared.relations {
            domains.push(MigrationDerivedDomain {
                store_path: relation.target_store_path(),
                store: relation.target_store(),
                index_id: relation.index_id()?,
            });
        }
    }
    domains.sort_unstable_by(|left, right| {
        (left.store_path, left.index_id).cmp(&(right.store_path, right.index_id))
    });
    domains.dedup_by(|left, right| {
        left.store_path == right.store_path && left.index_id == right.index_id
    });
    Ok(domains)
}

fn require_journaled(store: StoreHandle) -> Result<(), InternalError> {
    if store.storage_capabilities().recovery()
        != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PhysicalRunnerMissing,
        ));
    }
    Ok(())
}
