//! Startup expression-index schema mutation adapter.
//!
//! This module mirrors the field-path DDL publication boundary while keeping
//! expression-index key construction on accepted mutation targets.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow, StoreVisit, StructuralRowContract},
        index::{IndexId, IndexKey, IndexState, IndexStore, IndexStoreVisit, RawIndexStoreKey},
        predicate::{PredicateProgram, normalize, parse_sql_predicate},
        registry::StoreHandle,
        schema::{
            PersistedSchemaSnapshot, SchemaExpressionIndexRebuildRow,
            SchemaExpressionIndexRebuildTarget, SchemaExpressionIndexStagedEntry,
            SchemaExpressionIndexStagedRebuild, SchemaMutationRunnerInput,
            SchemaTransitionPlanKind, transition::SchemaTransitionPlan,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use sha2::{Digest, Sha256};

use super::startup_field_path::{
    SchemaPublicationGate, StartupDecodedFieldPathRebuildRow, StartupFieldPathRebuildRow,
    catalog_backed_row_contract_for_rebuild, decode_field_path_rebuild_rows,
    field_path_rebuild_raw_rows_for_entity,
};

pub(super) fn execute_supported_expression_index_addition(
    store: StoreHandle,
    publication_gate: SchemaPublicationGate,
    entity_path: &'static str,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
    plan: &SchemaTransitionPlan,
    target: &SchemaExpressionIndexRebuildTarget,
) -> Result<(usize, usize), InternalError> {
    let entity_tag = publication_gate.entity_tag();
    if plan.kind() != SchemaTransitionPlanKind::AddExpressionIndex {
        return Err(InternalError::store_unsupported());
    }
    validate_expression_mutation_plan(plan, target, entity_path)?;
    let input = SchemaMutationRunnerInput::new(
        accepted_before,
        accepted_after,
        plan.mutation_plan().clone(),
    )
    .map_err(|_error| InternalError::store_unsupported())?;
    let row_contract = catalog_backed_row_contract_for_rebuild(
        store,
        publication_gate,
        entity_path,
        accepted_before,
    )?;
    let predicate_program = expression_rebuild_predicate_program(target, &row_contract)?;
    let raw_rows = field_path_rebuild_raw_rows_for_entity(store, entity_tag, entity_path)?;
    let rebuild_gate = StartupExpressionRebuildGate::from_raw_rows(
        entity_tag,
        entity_path,
        accepted_before,
        raw_rows.as_slice(),
    )?;
    let rows =
        decode_field_path_rebuild_rows(raw_rows.as_slice(), entity_tag, entity_path, row_contract)?;
    rebuild_gate.validate_before_physical_work(store, rows.len())?;

    let physical_mutation = store.with_index_mut(|index_store| {
        execute_expression_index_store_mutation(
            index_store,
            entity_tag,
            entity_path,
            target,
            predicate_program.as_ref(),
            rows.as_slice(),
            &input,
        )
    })?;
    let publication_result = (|| {
        rebuild_gate.validate_before_schema_publication(store, physical_mutation.rows_scanned())?;
        validate_expression_physical_store_before_schema_publication(
            store,
            entity_tag,
            entity_path,
            target,
            physical_mutation.index_keys_written(),
        )?;
        publication_gate.publish_accepted_snapshot(store, accepted_after)
    })();
    if let Err(error) = publication_result {
        store.with_index_mut(|index_store| physical_mutation.rollback(index_store));
        return Err(error);
    }

    Ok((
        physical_mutation.rows_scanned(),
        physical_mutation.index_keys_written(),
    ))
}

fn validate_expression_mutation_plan(
    plan: &SchemaTransitionPlan,
    target: &SchemaExpressionIndexRebuildTarget,
    _entity_path: &'static str,
) -> Result<(), InternalError> {
    if plan.expression_index_target() != Some(target) {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

fn expression_rebuild_predicate_program(
    target: &SchemaExpressionIndexRebuildTarget,
    row_contract: &StructuralRowContract,
) -> Result<Option<PredicateProgram>, InternalError> {
    let Some(predicate_sql) = target.predicate_sql() else {
        return Ok(None);
    };
    let predicate =
        parse_sql_predicate(predicate_sql).map_err(|_error| InternalError::store_unsupported())?;

    Ok(Some(PredicateProgram::compile_with_row_contract(
        row_contract,
        &normalize(&predicate),
    )))
}

fn execute_expression_index_store_mutation(
    index_store: &mut IndexStore,
    entity_tag: EntityTag,
    entity_path: &'static str,
    target: &SchemaExpressionIndexRebuildTarget,
    predicate_program: Option<&PredicateProgram>,
    rows: &[StartupDecodedFieldPathRebuildRow<'_>],
    input: &SchemaMutationRunnerInput<'_>,
) -> Result<StartupExpressionPhysicalMutation, InternalError> {
    if index_store.state() != IndexState::Ready {
        return Err(InternalError::store_unsupported());
    }
    let target_index_id = IndexId::new(entity_tag, target.ordinal());
    let preflight =
        expression_startup_index_store_preflight(index_store, entity_tag, target, entity_path)?;
    if preflight.target != 0 {
        return Err(InternalError::store_unsupported());
    }

    let rebuild_rows = rows
        .iter()
        .map(|row| SchemaExpressionIndexRebuildRow::new(row.primary_key_value, &row.slots));
    let staged = SchemaExpressionIndexStagedRebuild::from_rows(
        input.accepted_after().entity_path(),
        entity_tag,
        target.clone(),
        predicate_program,
        rebuild_rows,
    )?;
    let validation = staged
        .validate()
        .map_err(|_error| InternalError::store_unsupported())?;

    publish_expression_index_store_batch(
        index_store,
        entity_path,
        target,
        &target_index_id,
        staged.entries(),
    )?;

    Ok(StartupExpressionPhysicalMutation {
        rows_scanned: validation.source_rows(),
        entries: staged.entries().to_vec(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StartupExpressionPhysicalMutation {
    rows_scanned: usize,
    entries: Vec<SchemaExpressionIndexStagedEntry>,
}

impl StartupExpressionPhysicalMutation {
    const fn rows_scanned(&self) -> usize {
        self.rows_scanned
    }

    const fn index_keys_written(&self) -> usize {
        self.entries.len()
    }

    fn rollback(&self, index_store: &mut IndexStore) {
        index_store.mark_building();
        rollback_expression_index_store_batch(index_store, &self.entries);
        index_store.mark_ready();
    }
}

fn publish_expression_index_store_batch(
    index_store: &mut IndexStore,
    entity_path: &'static str,
    target: &SchemaExpressionIndexRebuildTarget,
    target_index_id: &IndexId,
    entries: &[SchemaExpressionIndexStagedEntry],
) -> Result<(), InternalError> {
    index_store.mark_building();
    let result = insert_and_validate_expression_index_store_batch(
        index_store,
        entity_path,
        target,
        target_index_id,
        entries,
    );
    match result {
        Ok(()) => {
            index_store.mark_ready();
            Ok(())
        }
        Err(error) => {
            rollback_expression_index_store_batch(index_store, entries);
            index_store.mark_ready();
            Err(error)
        }
    }
}

fn insert_and_validate_expression_index_store_batch(
    index_store: &mut IndexStore,
    entity_path: &'static str,
    target: &SchemaExpressionIndexRebuildTarget,
    target_index_id: &IndexId,
    entries: &[SchemaExpressionIndexStagedEntry],
) -> Result<(), InternalError> {
    for entry in entries {
        index_store.insert(entry.key().clone(), entry.entry().clone());
    }

    validate_expression_index_store_batch(
        index_store,
        entity_path,
        target,
        target_index_id,
        entries,
    )
}

fn rollback_expression_index_store_batch(
    index_store: &mut IndexStore,
    entries: &[SchemaExpressionIndexStagedEntry],
) {
    for entry in entries {
        index_store.remove(entry.key());
    }
}

#[cfg(test)]
pub(super) fn publish_expression_index_store_batch_for_test(
    index_store: &mut IndexStore,
    entity_path: &'static str,
    target: &SchemaExpressionIndexRebuildTarget,
    target_index_id: &IndexId,
    entries: &[SchemaExpressionIndexStagedEntry],
) -> Result<(), InternalError> {
    publish_expression_index_store_batch(index_store, entity_path, target, target_index_id, entries)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StartupExpressionIndexStorePreflight {
    target: u64,
    other: u64,
    total: u64,
}

fn expression_startup_index_store_preflight(
    index_store: &IndexStore,
    entity_tag: EntityTag,
    target: &SchemaExpressionIndexRebuildTarget,
    _entity_path: &'static str,
) -> Result<StartupExpressionIndexStorePreflight, InternalError> {
    let target_index_id = IndexId::new(entity_tag, target.ordinal());
    let mut preflight = StartupExpressionIndexStorePreflight {
        target: 0,
        other: 0,
        total: 0,
    };

    let result: Result<(), InternalError> = index_store.visit_entries(|raw_key, _| {
        let index_key =
            IndexKey::try_from_raw(raw_key).map_err(|_error| InternalError::store_corruption())?;
        if *index_key.index_id() == target_index_id {
            preflight.target += 1;
        } else {
            preflight.other += 1;
        }
        preflight.total += 1;
        Ok(IndexStoreVisit::Continue)
    });
    result?;

    Ok(preflight)
}

fn validate_expression_index_store_batch(
    index_store: &IndexStore,
    entity_path: &'static str,
    target: &SchemaExpressionIndexRebuildTarget,
    target_index_id: &IndexId,
    entries: &[SchemaExpressionIndexStagedEntry],
) -> Result<(), InternalError> {
    if index_store.state() != IndexState::Building {
        return Err(InternalError::store_unsupported());
    }
    let expected_entry_count =
        u64::try_from(entries.len()).map_err(|_| InternalError::store_unsupported())?;
    let actual_entry_count =
        expression_target_index_entry_count(index_store, target_index_id, entity_path, target)?;
    if actual_entry_count != expected_entry_count {
        return Err(InternalError::store_unsupported());
    }
    for entry in entries {
        let index_key = IndexKey::try_from_raw(entry.key())
            .map_err(|_error| InternalError::store_corruption())?;
        if index_key.index_id() != target_index_id {
            return Err(InternalError::store_unsupported());
        }
        let Some(index_entry) = index_store.get(entry.key()) else {
            return Err(InternalError::store_unsupported());
        };
        if index_entry != *entry.entry() {
            return Err(InternalError::store_unsupported());
        }
    }

    Ok(())
}

fn validate_expression_physical_store_before_schema_publication(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    target: &SchemaExpressionIndexRebuildTarget,
    expected_entries: usize,
) -> Result<(), InternalError> {
    store.with_index(|index_store| {
        if index_store.state() != IndexState::Ready {
            return Err(InternalError::store_unsupported());
        }
        let target_index_id = IndexId::new(entity_tag, target.ordinal());
        let actual = expression_target_index_entry_count(
            index_store,
            &target_index_id,
            entity_path,
            target,
        )?;
        let expected =
            u64::try_from(expected_entries).map_err(|_| InternalError::store_unsupported())?;
        if actual == expected {
            return Ok(());
        }

        Err(InternalError::store_unsupported())
    })
}

fn expression_target_index_entry_count(
    index_store: &IndexStore,
    target_index_id: &IndexId,
    entity_path: &'static str,
    target: &SchemaExpressionIndexRebuildTarget,
) -> Result<u64, InternalError> {
    let mut count = 0u64;
    let result: Result<(), InternalError> = index_store.visit_entries(|raw_key, _| {
        if expression_key_targets_index(raw_key, target_index_id, entity_path, target)? {
            count += 1;
        }
        Ok(IndexStoreVisit::Continue)
    });
    result?;

    Ok(count)
}

fn expression_key_targets_index(
    raw_key: &RawIndexStoreKey,
    target_index_id: &IndexId,
    _entity_path: &'static str,
    _target: &SchemaExpressionIndexRebuildTarget,
) -> Result<bool, InternalError> {
    let index_key =
        IndexKey::try_from_raw(raw_key).map_err(|_error| InternalError::store_corruption())?;

    Ok(index_key.index_id() == target_index_id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StartupExpressionRebuildRowFingerprint {
    rows: usize,
    digest: [u8; 32],
}

impl StartupExpressionRebuildRowFingerprint {
    const fn new(rows: usize, digest: [u8; 32]) -> Self {
        Self { rows, digest }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StartupExpressionRebuildGate {
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: PersistedSchemaSnapshot,
    row_fingerprint: StartupExpressionRebuildRowFingerprint,
}

impl StartupExpressionRebuildGate {
    fn from_raw_rows(
        entity_tag: EntityTag,
        entity_path: &'static str,
        accepted_before: &PersistedSchemaSnapshot,
        rows: &[StartupFieldPathRebuildRow],
    ) -> Result<Self, InternalError> {
        Ok(Self {
            entity_tag,
            entity_path,
            accepted_before: accepted_before.clone(),
            row_fingerprint: expression_rebuild_row_fingerprint_from_rows(entity_tag, rows)?,
        })
    }

    fn validate_before_physical_work(
        &self,
        store: StoreHandle,
        rows_scanned: usize,
    ) -> Result<(), InternalError> {
        self.validate_current_state(store, rows_scanned, "before physical work")
    }

    fn validate_before_schema_publication(
        &self,
        store: StoreHandle,
        rows_scanned: usize,
    ) -> Result<(), InternalError> {
        self.validate_current_state(store, rows_scanned, "before schema publication")
    }

    fn validate_current_state(
        &self,
        store: StoreHandle,
        _rows_scanned: usize,
        _boundary: &'static str,
    ) -> Result<(), InternalError> {
        let current =
            expression_rebuild_row_fingerprint_for_store(store, self.entity_tag, self.entity_path)?;
        if current != self.row_fingerprint {
            return Err(InternalError::store_unsupported());
        }

        let latest = store.with_schema_mut(|schema_store| {
            schema_store.latest_staged_persisted_snapshot(self.entity_tag)
        })?;
        if latest.as_ref() != Some(&self.accepted_before) {
            return Err(InternalError::store_unsupported());
        }

        Ok(())
    }
}

fn expression_rebuild_row_fingerprint_from_rows(
    entity_tag: EntityTag,
    rows: &[StartupFieldPathRebuildRow],
) -> Result<StartupExpressionRebuildRowFingerprint, InternalError> {
    let mut hasher = Sha256::new();
    for row in rows {
        let raw_key =
            DecodedDataStoreKey::new_primary_key_value(entity_tag, &row.primary_key_value)
                .to_raw()?;
        hash_expression_rebuild_row(&mut hasher, raw_key.as_bytes(), &row.row);
    }

    Ok(StartupExpressionRebuildRowFingerprint::new(
        rows.len(),
        hasher.finalize().into(),
    ))
}

fn expression_rebuild_row_fingerprint_for_store(
    store: StoreHandle,
    entity_tag: EntityTag,
    _entity_path: &'static str,
) -> Result<StartupExpressionRebuildRowFingerprint, InternalError> {
    store.with_data(|data_store| {
        let mut rows = 0usize;
        let mut hasher = Sha256::new();
        data_store.visit_entries(|raw_key, raw_row| {
            let data_key = DecodedDataStoreKey::try_from_raw(raw_key)
                .map_err(|_error| InternalError::store_corruption())?;
            if data_key.entity_tag() != entity_tag {
                return Ok::<StoreVisit, InternalError>(StoreVisit::Continue);
            }
            rows += 1;
            hash_expression_rebuild_row(&mut hasher, raw_key.as_bytes(), raw_row);
            Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
        })?;

        Ok(StartupExpressionRebuildRowFingerprint::new(
            rows,
            hasher.finalize().into(),
        ))
    })
}

fn hash_expression_rebuild_row(hasher: &mut Sha256, raw_key: &[u8], row: &RawRow) {
    hasher.update(raw_key);
    hasher.update((row.len() as u64).to_be_bytes());
    hasher.update(row.as_bytes());
}
