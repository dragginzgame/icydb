//! Startup expression-index schema mutation adapter.
//!
//! This module mirrors the field-path DDL publication boundary while keeping
//! expression-index key construction on accepted mutation targets.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow, StructuralRowContract},
        index::{IndexId, IndexKey, IndexState, IndexStore, RawIndexStoreKey},
        predicate::{PredicateProgram, normalize, parse_sql_predicate},
        registry::StoreHandle,
        schema::{
            AcceptedSchemaSnapshot, PersistedSchemaSnapshot, SchemaExpressionIndexRebuildRow,
            SchemaExpressionIndexRebuildTarget, SchemaExpressionIndexStagedEntry,
            SchemaExpressionIndexStagedRebuild, SchemaMutationExecutionStep,
            SchemaMutationRunnerInput, SchemaTransitionPlanKind, transition::SchemaTransitionPlan,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use sha2::{Digest, Sha256};

use super::startup_field_path::{
    StartupDecodedFieldPathRebuildRow, StartupFieldPathRebuildRow, decode_field_path_rebuild_rows,
    field_path_rebuild_raw_rows_for_entity,
};

pub(super) fn execute_supported_expression_index_addition(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
    plan: &SchemaTransitionPlan,
    target: &SchemaExpressionIndexRebuildTarget,
) -> Result<(usize, usize), InternalError> {
    if plan.kind() != SchemaTransitionPlanKind::AddExpressionIndex {
        return Err(InternalError::store_unsupported(format!(
            "schema mutation expression-index execution rejected for entity '{entity_path}': plan_kind={:?}",
            plan.kind(),
        )));
    }
    validate_expression_execution_plan(plan, target, entity_path)?;
    let input =
        SchemaMutationRunnerInput::new(accepted_before, accepted_after, plan.execution_plan())
            .map_err(|error| {
                InternalError::store_unsupported(format!(
                    "schema mutation expression-index runner input rejected for entity '{entity_path}': error={error:?}",
                ))
            })?;
    let accepted = AcceptedSchemaSnapshot::try_new(accepted_before.clone())?;
    let row_contract =
        StructuralRowContract::from_accepted_schema_snapshot(entity_path, &accepted)?;
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

    let (rows_scanned, index_keys_written) = store.with_index_mut(|index_store| {
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
    rebuild_gate.validate_before_schema_publication(store, rows_scanned)?;
    validate_expression_physical_store_before_schema_publication(
        store,
        entity_tag,
        entity_path,
        target,
        index_keys_written,
    )?;
    store.with_schema_mut(|schema_store| {
        schema_store.insert_persisted_snapshot(entity_tag, accepted_after)
    })?;

    Ok((rows_scanned, index_keys_written))
}

fn validate_expression_execution_plan(
    plan: &SchemaTransitionPlan,
    target: &SchemaExpressionIndexRebuildTarget,
    entity_path: &'static str,
) -> Result<(), InternalError> {
    let execution_plan = plan.execution_plan();
    let [
        SchemaMutationExecutionStep::BuildExpressionIndex {
            target: planned_target,
        },
        SchemaMutationExecutionStep::ValidatePhysicalWork,
        SchemaMutationExecutionStep::InvalidateRuntimeState,
    ] = execution_plan.steps()
    else {
        return Err(InternalError::store_unsupported(format!(
            "schema mutation expression-index execution rejected unsupported plan shape for entity '{entity_path}'",
        )));
    };
    if planned_target != target {
        return Err(InternalError::store_unsupported(format!(
            "schema mutation expression-index execution target drifted for entity '{entity_path}': prepared='{}' actual='{}'",
            target.name(),
            planned_target.name(),
        )));
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
    let predicate = parse_sql_predicate(predicate_sql).map_err(|error| {
        InternalError::store_unsupported(format!(
            "schema mutation expression rebuild predicate failed to parse for target '{}': {error}",
            target.name(),
        ))
    })?;

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
) -> Result<(usize, usize), InternalError> {
    if index_store.state() != IndexState::Ready {
        return Err(InternalError::store_unsupported(format!(
            "schema mutation expression-index rebuild requires a ready physical index store before rebuild for entity '{entity_path}': target_index={} index_state={}",
            target.name(),
            index_store.state().as_str(),
        )));
    }
    let target_index_id = IndexId::new(entity_tag, target.ordinal());
    let preflight =
        expression_startup_index_store_preflight(index_store, entity_tag, target, entity_path)?;
    if preflight.target != 0 {
        return Err(InternalError::store_unsupported(format!(
            "schema mutation expression-index rebuild requires an empty target physical index for entity '{entity_path}': target_index={} target_index_entries={} other_index_entries={} total_entries={}",
            target.name(),
            preflight.target,
            preflight.other,
            preflight.total,
        )));
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
    let validation = staged.validate().map_err(|error| {
        InternalError::store_unsupported(format!(
            "schema mutation expression-index staged validation failed for entity '{entity_path}': target_index={} error={error:?}",
            target.name(),
        ))
    })?;

    index_store.mark_building();
    for entry in staged.entries() {
        index_store.insert(entry.key().clone(), entry.entry().clone());
    }
    validate_expression_index_store_batch(
        index_store,
        entity_path,
        target,
        &target_index_id,
        staged.entries(),
    )?;
    index_store.mark_ready();

    Ok((validation.source_rows(), validation.entry_count()))
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
    entity_path: &'static str,
) -> Result<StartupExpressionIndexStorePreflight, InternalError> {
    let target_index_id = IndexId::new(entity_tag, target.ordinal());
    let mut preflight = StartupExpressionIndexStorePreflight {
        target: 0,
        other: 0,
        total: 0,
    };

    for (raw_key, _) in index_store.entries() {
        let index_key = IndexKey::try_from_raw(&raw_key).map_err(|error| {
            InternalError::store_corruption(format!(
                "schema mutation expression-index key decode failed for entity '{entity_path}' while preflighting target index '{}': {error}",
                target.name(),
            ))
        })?;
        if *index_key.index_id() == target_index_id {
            preflight.target += 1;
        } else {
            preflight.other += 1;
        }
        preflight.total += 1;
    }

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
        return Err(InternalError::store_unsupported(format!(
            "schema mutation expression-index validation requires a building index store for entity '{entity_path}': target_index={} index_state={}",
            target.name(),
            index_store.state().as_str(),
        )));
    }
    let expected_entry_count = u64::try_from(entries.len()).map_err(|_| {
        InternalError::store_unsupported(format!(
            "schema mutation expression-index produced too many entries for entity '{entity_path}': target_index={}",
            target.name(),
        ))
    })?;
    let actual_entry_count =
        expression_target_index_entry_count(index_store, target_index_id, entity_path, target)?;
    if actual_entry_count != expected_entry_count {
        return Err(InternalError::store_unsupported(format!(
            "schema mutation expression-index entry count mismatch for entity '{entity_path}': target_index={} expected={} actual={actual_entry_count}",
            target.name(),
            expected_entry_count,
        )));
    }
    for entry in entries {
        let index_key = IndexKey::try_from_raw(entry.key()).map_err(|error| {
            InternalError::store_corruption(format!(
                "schema mutation expression-index key decode failed for entity '{entity_path}' while validating target index '{}': {error}",
                target.name(),
            ))
        })?;
        if index_key.index_id() != target_index_id {
            return Err(InternalError::store_unsupported(format!(
                "schema mutation expression-index target mismatch for entity '{entity_path}': target_index={}",
                target.name(),
            )));
        }
        let Some(index_entry) = index_store.get(entry.key()) else {
            return Err(InternalError::store_unsupported(format!(
                "schema mutation expression-index missing written entry for entity '{entity_path}': target_index={}",
                target.name(),
            )));
        };
        if index_entry != *entry.entry() {
            return Err(InternalError::store_unsupported(format!(
                "schema mutation expression-index written entry mismatch for entity '{entity_path}': target_index={}",
                target.name(),
            )));
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
            return Err(InternalError::store_unsupported(format!(
                "schema mutation expression-index publication requires a ready physical index store for entity '{entity_path}': target_index={} index_state={}",
                target.name(),
                index_store.state().as_str(),
            )));
        }
        let target_index_id = IndexId::new(entity_tag, target.ordinal());
        let actual = expression_target_index_entry_count(
            index_store,
            &target_index_id,
            entity_path,
            target,
        )?;
        let expected = u64::try_from(expected_entries).map_err(|_| {
            InternalError::store_unsupported(format!(
                "schema mutation expression-index expected-entry count is unpublishable for entity '{entity_path}': target_index={}",
                target.name(),
            ))
        })?;
        if actual == expected {
            return Ok(());
        }

        Err(InternalError::store_unsupported(format!(
            "schema mutation expression-index physical store changed before schema publication for entity '{entity_path}': target_index={} expected_entries={expected} actual_entries={actual}",
            target.name(),
        )))
    })
}

fn expression_target_index_entry_count(
    index_store: &IndexStore,
    target_index_id: &IndexId,
    entity_path: &'static str,
    target: &SchemaExpressionIndexRebuildTarget,
) -> Result<u64, InternalError> {
    let mut count = 0u64;
    for (raw_key, _) in index_store.entries() {
        if expression_key_targets_index(&raw_key, target_index_id, entity_path, target)? {
            count += 1;
        }
    }

    Ok(count)
}

fn expression_key_targets_index(
    raw_key: &RawIndexStoreKey,
    target_index_id: &IndexId,
    entity_path: &'static str,
    target: &SchemaExpressionIndexRebuildTarget,
) -> Result<bool, InternalError> {
    let index_key = IndexKey::try_from_raw(raw_key).map_err(|error| {
        InternalError::store_corruption(format!(
            "schema mutation expression-index key decode failed for entity '{entity_path}' while counting target index '{}': {error}",
            target.name(),
        ))
    })?;

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

    const fn rows(&self) -> usize {
        self.rows
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
        rows_scanned: usize,
        boundary: &'static str,
    ) -> Result<(), InternalError> {
        let current =
            expression_rebuild_row_fingerprint_for_store(store, self.entity_tag, self.entity_path)?;
        if current != self.row_fingerprint {
            return Err(InternalError::store_unsupported(format!(
                "schema mutation expression-index rebuild lost exclusive row gate {boundary} for entity '{}': expected_rows={} actual_rows={} rows_scanned={rows_scanned}",
                self.entity_path,
                self.row_fingerprint.rows(),
                current.rows(),
            )));
        }

        let latest = store.with_schema_mut(|schema_store| {
            schema_store.latest_persisted_snapshot(self.entity_tag)
        })?;
        if latest.as_ref() != Some(&self.accepted_before) {
            return Err(InternalError::store_unsupported(format!(
                "schema mutation expression-index rebuild lost exclusive schema gate {boundary} for entity '{}'",
                self.entity_path,
            )));
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
        let raw_key = DecodedDataStoreKey::new(entity_tag, row.primary_key_value).to_raw()?;
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
    entity_path: &'static str,
) -> Result<StartupExpressionRebuildRowFingerprint, InternalError> {
    store.with_data(|data_store| {
        let mut rows = 0usize;
        let mut hasher = Sha256::new();
        for entry in data_store.entries() {
            let data_key = DecodedDataStoreKey::try_from_raw(entry.key()).map_err(|error| {
                InternalError::store_corruption(format!(
                    "schema mutation expression-index data key decode failed for entity '{entity_path}' while validating startup rebuild gate: {error}",
                ))
            })?;
            if data_key.entity_tag() != entity_tag {
                continue;
            }
            rows += 1;
            hash_expression_rebuild_row(&mut hasher, entry.key().as_bytes(), &entry.value());
        }

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
