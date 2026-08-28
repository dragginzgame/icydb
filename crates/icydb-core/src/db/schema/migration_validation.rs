//! Bounded historical validation for one prepared source migration.
//!
//! Each call scans a deterministic accepted-before key page, materializes
//! transient candidate rows, and proves candidate constraints, relations, and
//! isolated unique-index keys. It never writes an accepted row or publishes a
//! candidate catalog.

use std::{collections::BTreeMap, ops::Bound};

use icydb_diagnostic_code::SchemaMigrationCode;

use super::migration_record::MAX_SCHEMA_MIGRATION_FINDINGS;
use crate::{
    db::{
        Db,
        data::{DecodedDataStoreKey, RawDataStoreKey, StoreVisit, StructuralSlotReader},
        direction::Direction,
        index::{IndexEntryValue, IndexKey, IndexKeyKind, RawIndexStoreKey},
        key_taxonomy::RawDataStoreKeyRange,
        registry::{StoreHandle, StoreRecoveryCapability},
        relation::{RelationConstraintProjection, ReverseRelationSourceInfo},
        schema::{
            AcceptedCatalogSnapshotSelection, CompiledAcceptedRowConstraints,
            PersistedSchemaMigrationFinding, PersistedSchemaMigrationFindingKind,
            PersistedSchemaMigrationProgress, PersistedSchemaMigrationRowCursor,
            UniqueConstraintProjection, accepted_schema_cache_fingerprint,
            migration_planner::PlannedSchemaMigration,
            migration_transform::CompiledMigrationEntityProgram,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};

const MAX_MIGRATION_VALIDATION_ROWS_PER_PAGE: usize = 256;
const MAX_MIGRATION_VALIDATION_DECODED_BYTES_PER_PAGE: usize = 1024 * 1024;
const MAX_MIGRATION_VALIDATION_STAGED_BYTES_PER_PAGE: usize = 1024 * 1024;

const _: () = {
    assert!(MAX_MIGRATION_VALIDATION_ROWS_PER_PAGE > 0);
    assert!(MAX_MIGRATION_VALIDATION_DECODED_BYTES_PER_PAGE <= 1024 * 1024);
    assert!(MAX_MIGRATION_VALIDATION_STAGED_BYTES_PER_PAGE <= 1024 * 1024);
};

/// One bounded validation result ready for durable progress publication.
pub(in crate::db::schema) struct MigrationValidationPage {
    progress: PersistedSchemaMigrationProgress,
    staged_entries: Vec<MigrationStagedIndexEntry>,
    exhausted: bool,
}

impl MigrationValidationPage {
    pub(in crate::db::schema) fn into_parts(
        self,
    ) -> (
        PersistedSchemaMigrationProgress,
        Vec<MigrationStagedIndexEntry>,
        bool,
    ) {
        (self.progress, self.staged_entries, self.exhausted)
    }
}

/// One already-proven planner-invisible unique-index write.
pub(in crate::db::schema) struct MigrationStagedIndexEntry {
    store_path: &'static str,
    store: StoreHandle,
    key: RawIndexStoreKey,
}

impl MigrationStagedIndexEntry {
    #[must_use]
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexStoreKey {
        &self.key
    }
}

/// Validate at most one engine-bounded page over all physical programs.
pub(in crate::db::schema) fn validate_migration_page<C: CanisterKind>(
    db: &Db<C>,
    planned: &PlannedSchemaMigration,
    before_progress: &PersistedSchemaMigrationProgress,
) -> Result<MigrationValidationPage, InternalError> {
    let mut remaining_rows = MAX_MIGRATION_VALIDATION_ROWS_PER_PAGE;
    let mut remaining_decoded_bytes = MAX_MIGRATION_VALIDATION_DECODED_BYTES_PER_PAGE;
    let mut remaining_staged_bytes = MAX_MIGRATION_VALIDATION_STAGED_BYTES_PER_PAGE;
    let mut rows_validated = 0_u64;
    let mut findings = Vec::new();
    let mut staged_entries = Vec::new();
    let mut final_cursor = before_progress.row_cursor().cloned();
    let mut exhausted = true;

    for program in planned.programs() {
        let cursor = before_progress.row_cursor();
        if cursor.is_some_and(|cursor| {
            (program.store(), program.entity()) < (cursor.store(), cursor.entity())
        }) {
            continue;
        }
        if remaining_rows == 0 || remaining_decoded_bytes == 0 || remaining_staged_bytes == 0 {
            exhausted = false;
            break;
        }
        let candidate = planned
            .candidates()
            .iter()
            .find(|candidate| candidate.store_path() == program.store_path())
            .ok_or_else(InternalError::store_invariant)?;
        let store = db.store_handle(program.store_path())?;
        if store.storage_capabilities().recovery()
            != StoreRecoveryCapability::StableBasePlusJournalReplay
        {
            return Err(InternalError::store_unsupported());
        }
        let page = validate_entity_page(
            db,
            store,
            program,
            candidate,
            cursor.filter(|cursor| {
                cursor.store() == program.store() && cursor.entity() == program.entity()
            }),
            remaining_rows,
            remaining_decoded_bytes,
            remaining_staged_bytes,
            MAX_SCHEMA_MIGRATION_FINDINGS.saturating_sub(findings.len()),
        )?;
        remaining_rows = remaining_rows.saturating_sub(page.rows);
        remaining_decoded_bytes = remaining_decoded_bytes.saturating_sub(page.decoded_bytes);
        remaining_staged_bytes = remaining_staged_bytes.saturating_sub(page.staged_bytes);
        rows_validated = rows_validated
            .checked_add(u64::try_from(page.rows).map_err(|_| InternalError::store_invariant())?)
            .ok_or_else(InternalError::store_invariant)?;
        if let Some(cursor) = page.cursor {
            final_cursor = Some(cursor);
        }
        findings.extend(page.findings);
        staged_entries.extend(page.staged_entries);
        if !page.exhausted {
            exhausted = false;
            break;
        }
        if !findings.is_empty() {
            exhausted = false;
            break;
        }
    }
    let progress = before_progress.with_validation_page(final_cursor, rows_validated, findings)?;
    Ok(MigrationValidationPage {
        progress,
        staged_entries,
        exhausted,
    })
}

struct EntityValidationPage {
    cursor: Option<PersistedSchemaMigrationRowCursor>,
    rows: usize,
    decoded_bytes: usize,
    staged_bytes: usize,
    findings: Vec<PersistedSchemaMigrationFinding>,
    staged_entries: Vec<MigrationStagedIndexEntry>,
    exhausted: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "one migration page keeps all engine-owned budgets explicit"
)]
#[expect(
    clippy::too_many_lines,
    reason = "the page loop keeps transform, constraint, relation, and unique precedence explicit"
)]
fn validate_entity_page<C: CanisterKind>(
    db: &Db<C>,
    store: StoreHandle,
    program: &CompiledMigrationEntityProgram,
    candidate: &crate::db::schema::CandidateSchemaRevision,
    checkpoint: Option<&PersistedSchemaMigrationRowCursor>,
    row_budget: usize,
    decoded_budget: usize,
    staged_budget: usize,
    finding_budget: usize,
) -> Result<EntityValidationPage, InternalError> {
    let before_selection = store
        .with_schema(|schema| {
            schema.current_accepted_catalog_selection(
                program.entity(),
                program.before_path(),
                program.store_path(),
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let before_schema = before_selection.decode_verified()?;
    let before_contract = crate::db::data::AcceptedStructuralRowAuthority::from_catalog_selection(
        program.before_path(),
        &before_selection,
    )?
    .into_row_contract();
    let candidate_selection = AcceptedCatalogSnapshotSelection::from_candidate(
        candidate,
        program.entity(),
        program.candidate_path(),
        program.store_path(),
    )?
    .ok_or_else(InternalError::store_invariant)?;
    let candidate_schema = candidate_selection.decode_verified()?;
    let candidate_authority =
        crate::db::data::AcceptedStructuralRowAuthority::from_catalog_selection(
            program.candidate_path(),
            &candidate_selection,
        )?;
    let (_, candidate_contract) = candidate_authority.into_parts();
    let fingerprint = accepted_schema_cache_fingerprint(&candidate_schema)?;
    let constraints = CompiledAcceptedRowConstraints::compile(
        &candidate_schema,
        candidate_selection.value_catalog_handle(),
        fingerprint,
    )
    .map_err(|_| InternalError::accepted_row_constraint_program_corrupt())?;
    let unique = candidate_schema
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
        .filter(|index| index.unique())
        .map(|index| UniqueConstraintProjection::new(program.entity(), index, &candidate_contract))
        .collect::<Result<Vec<_>, _>>()?;
    let source = ReverseRelationSourceInfo::new(program.candidate_path(), program.entity());
    let relations = candidate_schema
        .persisted_snapshot()
        .relations()
        .iter()
        .map(|edge| {
            let staged = before_schema
                .persisted_snapshot()
                .relations()
                .iter()
                .find(|before| before.id() == edge.id())
                .is_none_or(|before| before.physical_generation() != edge.physical_generation());
            RelationConstraintProjection::new_active(
                db,
                source.clone(),
                candidate_schema.persisted_snapshot(),
                &candidate_contract,
                edge,
            )
            .map(|projection| (projection, staged))
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
                    .map(|projection| (projection, true))
                }),
        )
        .collect::<Result<Vec<_>, _>>()?;
    if relations.iter().any(|(relation, staged)| {
        *staged
            && relation.target_store().storage_capabilities().recovery()
                != StoreRecoveryCapability::StableBasePlusJournalReplay
    }) {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PhysicalRunnerMissing,
        ));
    }

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
    let mut page = EntityValidationPage {
        cursor: checkpoint.cloned(),
        rows: 0,
        decoded_bytes: 0,
        staged_bytes: 0,
        findings: Vec::new(),
        staged_entries: Vec::new(),
        exhausted: true,
    };
    store.with_data(|data| {
        data.visit_range(
            (lower, upper),
            |raw_key, raw_row| -> Result<StoreVisit, InternalError> {
                if page.rows == row_budget || page.findings.len() == finding_budget {
                    page.exhausted = false;
                    return Ok(StoreVisit::Stop);
                }
                let next_decoded_bytes = page
                    .decoded_bytes
                    .checked_add(raw_row.len())
                    .ok_or_else(InternalError::store_invariant)?;
                if next_decoded_bytes > decoded_budget {
                    if page.rows == 0 {
                        return Err(InternalError::store_unsupported());
                    }
                    page.exhausted = false;
                    return Ok(StoreVisit::Stop);
                }
                let decoded = DecodedDataStoreKey::try_from_raw(raw_key)
                    .map_err(|_| InternalError::identity_corruption())?;
                let before = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                    raw_row,
                    &before_contract,
                )?;
                before.validate_primary_key(&decoded)?;
                let candidate_row = match program.evaluate(&before, &candidate_contract, &decoded) {
                    Ok(row) => row,
                    Err(finding) => {
                        page.findings
                            .push(migration_transform_finding(program, raw_key, finding)?);
                        observe_row_progress(&mut page, program, raw_key, raw_row.len())?;
                        return Ok(StoreVisit::Continue);
                    }
                };
                let candidate_reader =
                    StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                        candidate_row.as_raw_row(),
                        &candidate_contract,
                    )?;
                let values =
                    candidate_reader.decode_selected_slot_values(constraints.required_slots())?;
                if constraints
                    .evaluate(fingerprint, values.as_slice())
                    .is_err()
                {
                    page.findings.push(migration_finding(
                        PersistedSchemaMigrationFindingKind::Constraint,
                        program,
                        raw_key,
                    )?);
                    observe_row_progress(&mut page, program, raw_key, raw_row.len())?;
                    return Ok(StoreVisit::Continue);
                }
                let mut row_staged_entries = Vec::new();
                let mut row_staged_bytes = 0usize;
                for (relation, stage_relation) in &relations {
                    let projected = relation.project_row(
                        &decoded.primary_key_value(),
                        &candidate_reader,
                        true,
                    )?;
                    if !projected.missing_targets().is_empty() {
                        page.findings.push(migration_finding(
                            PersistedSchemaMigrationFindingKind::Relation,
                            program,
                            raw_key,
                        )?);
                        observe_row_progress(&mut page, program, raw_key, raw_row.len())?;
                        return Ok(StoreVisit::Continue);
                    }
                    if *stage_relation {
                        for entry in projected.into_entries() {
                            row_staged_bytes = row_staged_bytes
                                .checked_add(entry.key().as_bytes().len())
                                .ok_or_else(InternalError::store_invariant)?;
                            row_staged_entries.push(MigrationStagedIndexEntry {
                                store_path: entry.target_store_path(),
                                store: entry.target_store(),
                                key: entry.key().clone(),
                            });
                        }
                    }
                }
                let mut row_unique_keys = Vec::new();
                for projection in &unique {
                    let Some(key) =
                        projection.derive_key(&decoded.primary_key_value(), &candidate_reader)?
                    else {
                        continue;
                    };
                    row_staged_bytes = row_staged_bytes
                        .checked_add(key.as_bytes().len())
                        .ok_or_else(InternalError::store_invariant)?;
                    if candidate_unique_key_conflicts(
                        store,
                        &key,
                        page.staged_entries
                            .iter()
                            .map(MigrationStagedIndexEntry::key)
                            .chain(row_unique_keys.iter()),
                    )? {
                        page.findings.push(migration_finding(
                            PersistedSchemaMigrationFindingKind::UniqueIndex,
                            program,
                            raw_key,
                        )?);
                        observe_row_progress(&mut page, program, raw_key, raw_row.len())?;
                        return Ok(StoreVisit::Continue);
                    }
                    row_unique_keys.push(key.clone());
                    row_staged_entries.push(MigrationStagedIndexEntry {
                        store_path: program.store_path(),
                        store,
                        key,
                    });
                }
                let next_bytes = page
                    .staged_bytes
                    .checked_add(row_staged_bytes)
                    .ok_or_else(InternalError::store_invariant)?;
                if next_bytes > staged_budget {
                    if page.rows == 0 {
                        return Err(InternalError::store_unsupported());
                    }
                    page.exhausted = false;
                    return Ok(StoreVisit::Stop);
                }
                page.staged_bytes = next_bytes;
                page.staged_entries.extend(row_staged_entries);
                observe_row_progress(&mut page, program, raw_key, raw_row.len())?;
                Ok(StoreVisit::Continue)
            },
        )
    })?;
    Ok(page)
}

fn observe_row_progress(
    page: &mut EntityValidationPage,
    program: &CompiledMigrationEntityProgram,
    raw_key: &RawDataStoreKey,
    raw_row_bytes: usize,
) -> Result<(), InternalError> {
    page.rows = page.rows.saturating_add(1);
    page.decoded_bytes = page.decoded_bytes.saturating_add(raw_row_bytes);
    page.cursor = Some(PersistedSchemaMigrationRowCursor::try_new(
        program.store(),
        program.entity(),
        raw_key.as_bytes().to_vec(),
    )?);
    Ok(())
}

fn migration_finding(
    kind: PersistedSchemaMigrationFindingKind,
    program: &CompiledMigrationEntityProgram,
    raw_key: &RawDataStoreKey,
) -> Result<PersistedSchemaMigrationFinding, InternalError> {
    PersistedSchemaMigrationFinding::try_new(
        kind,
        program.store(),
        program.entity(),
        raw_key
            .encoded_primary_key_bytes()
            .ok_or_else(InternalError::store_corruption)?
            .to_vec(),
    )
}

fn migration_transform_finding(
    program: &CompiledMigrationEntityProgram,
    raw_key: &RawDataStoreKey,
    finding: crate::db::schema::migration_transform::MigrationTransformFinding,
) -> Result<PersistedSchemaMigrationFinding, InternalError> {
    PersistedSchemaMigrationFinding::try_new_transform(
        program.store(),
        program.entity(),
        raw_key
            .encoded_primary_key_bytes()
            .ok_or_else(InternalError::store_corruption)?
            .to_vec(),
        finding.source_field(),
        finding.target_field(),
        finding.reason(),
    )
}

fn candidate_unique_key_conflicts<'a>(
    store: StoreHandle,
    candidate_raw: &RawIndexStoreKey,
    page_keys: impl IntoIterator<Item = &'a RawIndexStoreKey>,
) -> Result<bool, InternalError> {
    let candidate =
        IndexKey::try_from_raw(candidate_raw).map_err(|_| InternalError::index_invariant())?;
    if candidate.key_kind() != IndexKeyKind::User {
        return Err(InternalError::index_invariant());
    }
    for page_raw in page_keys {
        let page =
            IndexKey::try_from_raw(page_raw).map_err(|_| InternalError::index_invariant())?;
        if page.key_kind() == IndexKeyKind::User
            && page.index_id() == candidate.index_id()
            && page.has_same_components(&candidate)
        {
            return Ok(page_raw != candidate_raw);
        }
    }
    let (lower, upper) = candidate
        .raw_bounds_for_all_components()
        .map_err(|_| InternalError::index_invariant())?;
    let mut conflict = false;
    store.with_index(|index_store| {
        index_store.visit_raw_entries_in_range(
            (&Bound::Included(lower), &Bound::Included(upper)),
            Direction::Asc,
            |raw, _| {
                if raw != candidate_raw {
                    conflict = true;
                }
                Ok(conflict)
            },
        )
    })?;
    Ok(conflict)
}

/// Persist an invisible candidate page before its cursor is advanced. Exact
/// repeated keys are idempotent; a differing prior value is corruption.
pub(in crate::db::schema) fn stage_migration_index_entries(
    entries: Vec<MigrationStagedIndexEntry>,
) -> Result<(), InternalError> {
    if entries.iter().any(|entry| {
        entry.store.storage_capabilities().recovery()
            != StoreRecoveryCapability::StableBasePlusJournalReplay
    }) {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PhysicalRunnerMissing,
        ));
    }
    let mut grouped = BTreeMap::<&'static str, (StoreHandle, Vec<RawIndexStoreKey>)>::new();
    for entry in entries {
        let group = grouped
            .entry(entry.store_path)
            .or_insert_with(|| (entry.store, Vec::new()));
        group.1.push(entry.key);
    }
    for (_path, (store, mut keys)) in grouped {
        icydb_schema::compact_sort_unstable_by(&mut keys, Ord::cmp);
        if keys.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(InternalError::index_conflict());
        }
        store.with_index_mut(|index| {
            for key in keys {
                if index.get(&key).is_none() {
                    index.insert(key, IndexEntryValue::presence());
                }
            }
            index.fold_journaled_materialized_view()
        })?;
    }
    Ok(())
}
