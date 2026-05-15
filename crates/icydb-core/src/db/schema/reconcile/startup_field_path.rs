//! Startup field-path schema mutation reconciliation adapter.
//!
//! This module owns the runtime-startup bridge from an accepted schema
//! transition plan into the field-path index runner. General reconciliation
//! remains in `reconcile.rs`; this file must not make metadata-only
//! reconciliation capable of physical work.

use crate::{
    db::{
        data::{DataKey, RawRow, StorageKey, StructuralRowContract, StructuralSlotReader},
        index::{IndexId, IndexKey, IndexState, IndexStore},
        registry::StoreHandle,
        schema::{
            AcceptedSchemaSnapshot, PersistedSchemaSnapshot, SchemaFieldPathIndexRebuildRow,
            SchemaFieldPathIndexRebuildTarget, SchemaFieldPathIndexRunner,
            SchemaFieldPathIndexRunnerFailure, SchemaFieldPathIndexRunnerReport,
            SchemaMutationAcceptedSnapshotPublicationSink, SchemaMutationDeveloperReport,
            SchemaMutationPublishStatus, SchemaMutationRunnerInput, SchemaMutationRunnerPhase,
            SchemaMutationRuntimeEpoch, SchemaMutationRuntimeInvalidationSink,
            SchemaMutationValidationStatus, transition::SchemaTransitionPlan,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use sha2::{Digest, Sha256};

pub(super) fn execute_supported_field_path_index_addition(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
    plan: &SchemaTransitionPlan,
) -> Result<SchemaMutationDeveloperReport, InternalError> {
    let supported = plan
        .supported_developer_physical_path()
        .map_err(|rejection| {
            InternalError::store_unsupported(format!(
                "schema mutation physical startup execution rejected for entity '{entity_path}': supported_path_rejection={rejection:?}",
            ))
        })?;
    let input = SchemaMutationRunnerInput::new(
        accepted_before,
        accepted_after,
        plan.execution_plan(),
    )
    .map_err(|error| {
        InternalError::store_unsupported(format!(
            "schema mutation runner input rejected for entity '{entity_path}': error={error:?}",
        ))
    })?;
    let accepted = AcceptedSchemaSnapshot::try_new(accepted_before.clone())?;
    let row_contract =
        StructuralRowContract::from_accepted_schema_snapshot(entity_path, &accepted)?;
    let raw_rows = field_path_rebuild_raw_rows_for_entity(store, entity_tag, entity_path)?;
    let rebuild_gate = StartupFieldPathRebuildGate::from_raw_rows(
        entity_tag,
        entity_path,
        accepted_before,
        raw_rows.as_slice(),
    )?;
    let rows =
        decode_field_path_rebuild_rows(raw_rows.as_slice(), entity_tag, entity_path, row_contract)?;
    rebuild_gate.validate_before_physical_work(store, supported.target(), rows.len())?;

    let mut invalidation_sink = StartupSchemaMutationInvalidationSink;
    let mut publication_sink = StartupSchemaMutationPublicationSink;
    let report = store.with_index_mut(|index_store| {
        if index_store.state() != IndexState::Ready {
            let diagnostic = SchemaMutationDeveloperReport::field_path_index_addition(
                SchemaMutationRunnerPhase::Preflight,
                entity_path,
                supported.target(),
                rows.len(),
                0,
                SchemaMutationValidationStatus::Failed,
                SchemaMutationPublishStatus::NotStarted,
            );
            return Err(InternalError::store_unsupported(format!(
                "schema mutation startup index rebuild requires a ready target physical index store before rebuild: {} index_state={}",
                diagnostic.summary(),
                index_store.state().as_str(),
            )));
        }
        let preflight = field_path_startup_index_store_preflight(
            index_store,
            entity_tag,
            supported.target(),
            entity_path,
        )?;
        if preflight.target_index_entries() != 0 {
            let diagnostic = SchemaMutationDeveloperReport::field_path_index_addition(
                SchemaMutationRunnerPhase::Preflight,
                entity_path,
                supported.target(),
                rows.len(),
                0,
                SchemaMutationValidationStatus::Failed,
                SchemaMutationPublishStatus::NotStarted,
            );
            return Err(InternalError::store_unsupported(format!(
                "schema mutation startup index rebuild requires an empty target physical index: {} target_index_entries={} other_index_entries={} total_entries={}",
                diagnostic.summary(),
                preflight.target_index_entries(),
                preflight.other_index_entries(),
                preflight.total_entries(),
            )));
        }

        let rebuild_rows = rows
            .iter()
            .map(|row| SchemaFieldPathIndexRebuildRow::new(row.storage_key, &row.slots));

        SchemaFieldPathIndexRunner::run(
            &input,
            entity_tag,
            supported.target().clone(),
            rebuild_rows,
            index_store,
            &mut invalidation_sink,
            &mut publication_sink,
        )
        .map_err(|failure| {
            field_path_runner_failure_error(entity_path, supported.target(), rows.len(), failure)
        })
    })?;

    let publication = StartupFieldPathPublicationDecision::from_runner_report(
        store,
        &rebuild_gate,
        supported.target(),
        &report,
    )?;
    publication.publish_accepted_snapshot(store, entity_tag, accepted_after)?;

    Ok(publication.diagnostic)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct StartupFieldPathIndexStorePreflight {
    target_index_id: IndexId,
    target_index_entries: u64,
    other_index_entries: u64,
    total_entries: u64,
}

impl StartupFieldPathIndexStorePreflight {
    const fn new(target_index_id: IndexId) -> Self {
        Self {
            target_index_id,
            target_index_entries: 0,
            other_index_entries: 0,
            total_entries: 0,
        }
    }

    fn record(&mut self, index_id: &IndexId) {
        if *index_id == self.target_index_id {
            self.target_index_entries += 1;
        } else {
            self.other_index_entries += 1;
        }
        self.total_entries += 1;
    }

    pub(super) const fn target_index_entries(&self) -> u64 {
        self.target_index_entries
    }

    pub(super) const fn other_index_entries(&self) -> u64 {
        self.other_index_entries
    }

    pub(super) const fn total_entries(&self) -> u64 {
        self.total_entries
    }

    const fn target_index_id(&self) -> IndexId {
        self.target_index_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StartupFieldPathRebuildGate {
    entity_tag: EntityTag,
    pub(super) entity_path: &'static str,
    accepted_before: PersistedSchemaSnapshot,
    row_fingerprint: StartupFieldPathRebuildRowFingerprint,
}

impl StartupFieldPathRebuildGate {
    pub(super) fn from_raw_rows(
        entity_tag: EntityTag,
        entity_path: &'static str,
        accepted_before: &PersistedSchemaSnapshot,
        rows: &[StartupFieldPathRebuildRow],
    ) -> Result<Self, InternalError> {
        Ok(Self {
            entity_tag,
            entity_path,
            accepted_before: accepted_before.clone(),
            row_fingerprint: field_path_rebuild_row_fingerprint_from_rows(entity_tag, rows)?,
        })
    }

    pub(super) fn validate_before_physical_work(
        &self,
        store: StoreHandle,
        target: &SchemaFieldPathIndexRebuildTarget,
        rows_scanned: usize,
    ) -> Result<(), InternalError> {
        self.validate_current_state(store, target, rows_scanned, "before physical work")
    }

    fn validate_before_schema_publication(
        &self,
        store: StoreHandle,
        target: &SchemaFieldPathIndexRebuildTarget,
        rows_scanned: usize,
    ) -> Result<(), InternalError> {
        self.validate_current_state(store, target, rows_scanned, "before schema publication")
    }

    fn validate_current_state(
        &self,
        store: StoreHandle,
        target: &SchemaFieldPathIndexRebuildTarget,
        rows_scanned: usize,
        boundary: &'static str,
    ) -> Result<(), InternalError> {
        let current =
            field_path_rebuild_row_fingerprint_for_store(store, self.entity_tag, self.entity_path)?;
        if current != self.row_fingerprint {
            let diagnostic = SchemaMutationDeveloperReport::field_path_index_addition(
                SchemaMutationRunnerPhase::Preflight,
                self.entity_path,
                target,
                rows_scanned,
                0,
                SchemaMutationValidationStatus::Failed,
                SchemaMutationPublishStatus::NotStarted,
            );
            return Err(InternalError::store_unsupported(format!(
                "schema mutation startup rebuild lost exclusive row gate {boundary}: {} expected_rows={} actual_rows={}",
                diagnostic.summary(),
                self.row_fingerprint.rows(),
                current.rows(),
            )));
        }

        let latest = store.with_schema_mut(|schema_store| {
            schema_store.latest_persisted_snapshot(self.entity_tag)
        })?;
        if latest.as_ref() != Some(&self.accepted_before) {
            let diagnostic = SchemaMutationDeveloperReport::field_path_index_addition(
                SchemaMutationRunnerPhase::Preflight,
                self.entity_path,
                target,
                rows_scanned,
                0,
                SchemaMutationValidationStatus::Failed,
                SchemaMutationPublishStatus::NotStarted,
            );
            return Err(InternalError::store_unsupported(format!(
                "schema mutation startup rebuild lost exclusive schema gate {boundary}: {}",
                diagnostic.summary(),
            )));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StartupFieldPathPublicationDecision {
    diagnostic: SchemaMutationDeveloperReport,
    target: SchemaFieldPathIndexRebuildTarget,
    target_index_id: IndexId,
    target_entries: u64,
}

impl StartupFieldPathPublicationDecision {
    pub(super) fn from_runner_report(
        store: StoreHandle,
        rebuild_gate: &StartupFieldPathRebuildGate,
        target: &SchemaFieldPathIndexRebuildTarget,
        report: &SchemaFieldPathIndexRunnerReport,
    ) -> Result<Self, InternalError> {
        let diagnostic = report.developer_report(rebuild_gate.entity_path, target);
        if !report.runner_report().physical_work_allows_publication() {
            return Err(InternalError::store_unsupported(format!(
                "schema mutation field-path index runner did not produce publishable physical work: {}",
                diagnostic.summary(),
            )));
        }
        let target_entries =
            u64::try_from(report.runner_report().index_keys_written()).map_err(|_| {
                InternalError::store_unsupported(format!(
                    "schema mutation field-path index runner produced an unpublishable target-entry count: {}",
                    diagnostic.summary(),
                ))
            })?;

        rebuild_gate.validate_before_schema_publication(
            store,
            target,
            report.runner_report().rows_scanned(),
        )?;

        Ok(Self {
            diagnostic,
            target: target.clone(),
            target_index_id: IndexId::new(rebuild_gate.entity_tag, target.ordinal()),
            target_entries,
        })
    }

    pub(super) fn publish_accepted_snapshot(
        &self,
        store: StoreHandle,
        entity_tag: EntityTag,
        accepted_after: &PersistedSchemaSnapshot,
    ) -> Result<(), InternalError> {
        self.validate_physical_store_before_schema_publication(store, entity_tag)?;
        store.with_schema_mut(|schema_store| {
            schema_store.insert_persisted_snapshot(entity_tag, accepted_after)
        })?;

        Ok(())
    }

    fn validate_physical_store_before_schema_publication(
        &self,
        store: StoreHandle,
        entity_tag: EntityTag,
    ) -> Result<(), InternalError> {
        let preflight = store.with_index(|index_store| {
            if index_store.state() != IndexState::Ready {
                return Err(InternalError::store_unsupported(format!(
                    "schema mutation field-path index physical store was not ready before schema publication: {} index_state={}",
                    self.diagnostic.summary(),
                    index_store.state().as_str(),
                )));
            }
            field_path_startup_index_store_preflight(
                index_store,
                entity_tag,
                &self.target,
                self.diagnostic.entity_path(),
            )
        })?;
        if preflight.target_index_entries() != self.target_entries {
            return Err(InternalError::store_unsupported(format!(
                "schema mutation field-path index physical store changed before schema publication: {} expected_target_entries={} actual_target_entries={} other_index_entries={} total_entries={}",
                self.diagnostic.summary(),
                self.target_entries,
                preflight.target_index_entries(),
                preflight.other_index_entries(),
                preflight.total_entries(),
            )));
        }
        if preflight.target_index_id() != self.target_index_id {
            return Err(InternalError::store_unsupported(format!(
                "schema mutation field-path index physical store target changed before schema publication: {}",
                self.diagnostic.summary(),
            )));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StartupFieldPathRebuildRowFingerprint {
    rows: usize,
    digest: [u8; 32],
}

impl StartupFieldPathRebuildRowFingerprint {
    const fn new(rows: usize, digest: [u8; 32]) -> Self {
        Self { rows, digest }
    }

    const fn rows(&self) -> usize {
        self.rows
    }
}

fn field_path_rebuild_row_fingerprint_from_rows(
    entity_tag: EntityTag,
    rows: &[StartupFieldPathRebuildRow],
) -> Result<StartupFieldPathRebuildRowFingerprint, InternalError> {
    let mut hasher = Sha256::new();
    for row in rows {
        let raw_key = DataKey::new(entity_tag, row.storage_key).to_raw()?;
        hash_field_path_rebuild_row(&mut hasher, raw_key.as_bytes(), &row.row);
    }

    Ok(StartupFieldPathRebuildRowFingerprint::new(
        rows.len(),
        hasher.finalize().into(),
    ))
}

fn field_path_rebuild_row_fingerprint_for_store(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
) -> Result<StartupFieldPathRebuildRowFingerprint, InternalError> {
    store.with_data(|data_store| {
        let mut rows = 0usize;
        let mut hasher = Sha256::new();
        for entry in data_store.entries() {
            let data_key = DataKey::try_from_raw(entry.key()).map_err(|error| {
                InternalError::store_corruption(format!(
                    "schema mutation field-path rebuild data key decode failed for entity '{entity_path}' while validating startup rebuild gate: {error}",
                ))
            })?;
            if data_key.entity_tag() != entity_tag {
                continue;
            }
            rows += 1;
            hash_field_path_rebuild_row(&mut hasher, entry.key().as_bytes(), &entry.value());
        }

        Ok(StartupFieldPathRebuildRowFingerprint::new(
            rows,
            hasher.finalize().into(),
        ))
    })
}

fn hash_field_path_rebuild_row(hasher: &mut Sha256, raw_key: &[u8], row: &RawRow) {
    hasher.update(raw_key);
    hasher.update((row.len() as u64).to_be_bytes());
    hasher.update(row.as_bytes());
}

pub(super) fn field_path_startup_index_store_preflight(
    index_store: &IndexStore,
    entity_tag: EntityTag,
    target: &SchemaFieldPathIndexRebuildTarget,
    entity_path: &'static str,
) -> Result<StartupFieldPathIndexStorePreflight, InternalError> {
    let mut preflight =
        StartupFieldPathIndexStorePreflight::new(IndexId::new(entity_tag, target.ordinal()));

    for (raw_key, _) in index_store.entries() {
        let index_key = IndexKey::try_from_raw(&raw_key).map_err(|error| {
            InternalError::store_corruption(format!(
                "schema mutation field-path startup index key decode failed for entity '{entity_path}' while preflighting target index '{}': {error}",
                target.name(),
            ))
        })?;
        preflight.record(index_key.index_id());
    }

    Ok(preflight)
}

pub(super) struct StartupFieldPathRebuildRow {
    storage_key: StorageKey,
    row: RawRow,
}

pub(super) struct StartupDecodedFieldPathRebuildRow<'a> {
    pub(super) storage_key: StorageKey,
    pub(super) slots: StructuralSlotReader<'a>,
}

pub(super) fn field_path_rebuild_raw_rows_for_entity(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
) -> Result<Vec<StartupFieldPathRebuildRow>, InternalError> {
    store.with_data(|data_store| {
        let mut rows = Vec::new();
        for entry in data_store.entries() {
            let data_key = DataKey::try_from_raw(entry.key()).map_err(|error| {
                InternalError::store_corruption(format!(
                    "schema mutation field-path rebuild data key decode failed for entity '{entity_path}': {error}",
                ))
            })?;
            if data_key.entity_tag() != entity_tag {
                continue;
            }
            rows.push(StartupFieldPathRebuildRow {
                storage_key: data_key.storage_key(),
                row: entry.value().clone(),
            });
        }

        Ok::<_, InternalError>(rows)
    })
}

pub(super) fn decode_field_path_rebuild_rows<'a>(
    rows: &'a [StartupFieldPathRebuildRow],
    entity_tag: EntityTag,
    _entity_path: &'static str,
    row_contract: StructuralRowContract,
) -> Result<Vec<StartupDecodedFieldPathRebuildRow<'a>>, InternalError> {
    rows.iter()
        .map(|row| {
            let slots = StructuralSlotReader::from_raw_row_with_validated_contract(
                &row.row,
                row_contract.clone(),
            )?;
            let data_key = DataKey::new(entity_tag, row.storage_key);
            slots.validate_storage_key(&data_key)?;

            Ok(StartupDecodedFieldPathRebuildRow {
                storage_key: row.storage_key,
                slots,
            })
        })
        .collect()
}

fn field_path_runner_failure_error(
    entity_path: &'static str,
    target: &SchemaFieldPathIndexRebuildTarget,
    rows_scanned: usize,
    failure: SchemaFieldPathIndexRunnerFailure,
) -> InternalError {
    let diagnostic = failure.developer_report(entity_path, target, rows_scanned);
    InternalError::store_unsupported(format!(
        "schema mutation field-path index startup runner failed: {} error={:?} rollback={}",
        diagnostic.summary(),
        failure.error(),
        failure.rollback_report().is_some(),
    ))
}

pub(super) struct StartupSchemaMutationInvalidationSink;

impl SchemaMutationRuntimeInvalidationSink for StartupSchemaMutationInvalidationSink {
    fn invalidate_runtime_schema(
        &mut self,
        _store: &str,
        _before: &SchemaMutationRuntimeEpoch,
        _after: &SchemaMutationRuntimeEpoch,
    ) {
    }
}

pub(super) struct StartupSchemaMutationPublicationSink;

impl SchemaMutationAcceptedSnapshotPublicationSink for StartupSchemaMutationPublicationSink {
    fn publish_accepted_schema(
        &mut self,
        _store: &str,
        _accepted_after: &PersistedSchemaSnapshot,
        _before: &SchemaMutationRuntimeEpoch,
        _after: &SchemaMutationRuntimeEpoch,
    ) {
    }
}
