//! Startup field-path schema mutation reconciliation adapter.
//!
//! This module owns the runtime-startup bridge from an accepted schema
//! transition plan into the field-path index runner. General reconciliation
//! remains in `reconcile.rs`; this file must not make metadata-only
//! reconciliation capable of physical work.

use crate::{
    db::{
        data::{
            AcceptedStructuralRowAuthority, DecodedDataStoreKey, RawRow, StoreVisit,
            StructuralRowContract, StructuralSlotReader,
        },
        index::{IndexId, IndexKey, IndexState, IndexStore, IndexStoreVisit},
        key_taxonomy::PrimaryKeyValue,
        predicate::{PredicateProgram, normalize, parse_sql_predicate},
        registry::StoreHandle,
        schema::{
            AcceptedCatalogIdentity, PersistedSchemaSnapshot, SchemaFieldPathIndexMutationMetrics,
            SchemaFieldPathIndexRebuildRow, SchemaFieldPathIndexRebuildTarget,
            SchemaFieldPathIndexRunner, SchemaFieldPathIndexRunnerError,
            SchemaFieldPathIndexRunnerReport, SchemaMutationRunnerInput,
            transition::SchemaTransitionPlan,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use sha2::{Digest, Sha256};

#[cfg(feature = "sql")]
use super::publish_accepted_entity_snapshot_revision;

pub(super) fn execute_supported_field_path_index_addition(
    store: StoreHandle,
    publication_gate: SchemaPublicationGate,
    entity_path: &'static str,
    accepted_before: &PersistedSchemaSnapshot,
    accepted_after: &PersistedSchemaSnapshot,
    plan: &SchemaTransitionPlan,
) -> Result<SchemaFieldPathIndexMutationMetrics, InternalError> {
    let entity_tag = publication_gate.entity_tag();
    let target = plan
        .field_path_index_target()
        .ok_or_else(InternalError::store_unsupported)?;
    let input = field_path_runner_input(entity_path, accepted_before, accepted_after, plan)?;
    let row_contract = catalog_backed_row_contract_for_rebuild(
        store,
        publication_gate,
        entity_path,
        accepted_before,
    )?;
    let predicate_program = field_path_rebuild_predicate_program(target, &row_contract)?;
    let raw_rows = field_path_rebuild_raw_rows_for_entity(store, entity_tag, entity_path)?;
    let rebuild_gate = StartupFieldPathRebuildGate::from_raw_rows(
        entity_tag,
        entity_path,
        accepted_before,
        raw_rows.as_slice(),
    )?;
    let rows =
        decode_field_path_rebuild_rows(raw_rows.as_slice(), entity_tag, entity_path, row_contract)?;
    rebuild_gate.validate_before_physical_work(store, target, rows.len())?;

    let report = store.with_index_mut(|index_store| {
        if index_store.state() != IndexState::Ready {
            return Err(InternalError::store_unsupported());
        }
        let preflight =
            field_path_startup_index_store_preflight(index_store, entity_tag, target, entity_path)?;
        if preflight.target_index_entries() != 0 {
            return Err(InternalError::store_unsupported());
        }

        let rebuild_rows = rows
            .iter()
            .map(|row| SchemaFieldPathIndexRebuildRow::new(row.primary_key_value, &row.slots));

        SchemaFieldPathIndexRunner::run(
            &input,
            entity_tag,
            target.clone(),
            predicate_program.as_ref(),
            rebuild_rows,
            index_store,
        )
        .map_err(SchemaFieldPathIndexRunnerError::into_internal_error)
    })?;

    let publication = match StartupFieldPathPublicationDecision::from_runner_report(
        store,
        &rebuild_gate,
        target,
        &report,
    ) {
        Ok(publication) => publication,
        Err(error) => {
            store.with_index_mut(|index_store| report.rollback_physical_work(index_store));
            return Err(error);
        }
    };
    if let Err(error) =
        publication.publish_accepted_snapshot(store, publication_gate, accepted_after)
    {
        store.with_index_mut(|index_store| report.rollback_physical_work(index_store));
        return Err(error);
    }

    Ok(publication.metrics)
}

pub(super) fn catalog_backed_row_contract_for_rebuild(
    store: StoreHandle,
    publication_gate: SchemaPublicationGate,
    entity_path: &'static str,
    accepted_before: &PersistedSchemaSnapshot,
) -> Result<StructuralRowContract, InternalError> {
    let selection = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(
                publication_gate.entity_tag(),
                entity_path,
                publication_gate.store_path(),
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let authority =
        AcceptedStructuralRowAuthority::from_catalog_selection(entity_path, &selection)?;
    if authority.accepted_schema().persisted_snapshot() != accepted_before {
        return Err(InternalError::store_unsupported());
    }

    Ok(authority.into_row_contract())
}

fn field_path_runner_input<'a>(
    _entity_path: &'static str,
    accepted_before: &'a PersistedSchemaSnapshot,
    accepted_after: &'a PersistedSchemaSnapshot,
    plan: &SchemaTransitionPlan,
) -> Result<SchemaMutationRunnerInput<'a>, InternalError> {
    SchemaMutationRunnerInput::new(
        accepted_before,
        accepted_after,
        plan.mutation_plan().clone(),
    )
    .map_err(|_error| InternalError::store_unsupported())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SchemaPublicationGate {
    entity_tag: EntityTag,
    store_path: &'static str,
    accepted_before_identity: Option<AcceptedCatalogIdentity>,
}

impl SchemaPublicationGate {
    pub(super) const fn startup(entity_tag: EntityTag, store_path: &'static str) -> Self {
        Self {
            entity_tag,
            store_path,
            accepted_before_identity: None,
        }
    }

    #[cfg(feature = "sql")]
    pub(super) const fn sql_ddl(
        entity_tag: EntityTag,
        accepted_before_identity: AcceptedCatalogIdentity,
    ) -> Self {
        Self {
            entity_tag,
            store_path: accepted_before_identity.store_path(),
            accepted_before_identity: Some(accepted_before_identity),
        }
    }

    pub(super) const fn entity_tag(self) -> EntityTag {
        self.entity_tag
    }

    pub(super) const fn store_path(self) -> &'static str {
        self.store_path
    }

    pub(super) fn publish_accepted_snapshot(
        self,
        store: StoreHandle,
        accepted_after: &PersistedSchemaSnapshot,
    ) -> Result<(), InternalError> {
        if let Some(expected) = self.accepted_before_identity {
            debug_assert_eq!(self.entity_tag, expected.entity_tag());
            #[cfg(feature = "sql")]
            return publish_accepted_entity_snapshot_revision(store, expected, accepted_after);

            #[cfg(not(feature = "sql"))]
            return Err(InternalError::store_invariant());
        }

        store.with_schema_mut(|schema_store| {
            schema_store.insert_persisted_snapshot(self.entity_tag, accepted_after)
        })
    }
}

fn field_path_rebuild_predicate_program(
    target: &SchemaFieldPathIndexRebuildTarget,
    row_contract: &StructuralRowContract,
) -> Result<Option<PredicateProgram>, InternalError> {
    let Some(predicate_sql) = target.predicate_sql() else {
        return Ok(None);
    };
    let predicate = parse_sql_predicate(predicate_sql).map_err(|error| {
        let _ = (&error, target);

        InternalError::store_unsupported()
    })?;

    Ok(Some(PredicateProgram::compile_with_row_contract(
        row_contract,
        &normalize(&predicate),
    )))
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

    #[cfg(test)]
    pub(super) const fn other_index_entries(&self) -> u64 {
        self.other_index_entries
    }

    #[cfg(test)]
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
        let _ = (target, rows_scanned, boundary);

        let current =
            field_path_rebuild_row_fingerprint_for_store(store, self.entity_tag, self.entity_path)?;
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StartupFieldPathPublicationDecision {
    metrics: SchemaFieldPathIndexMutationMetrics,
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
        let metrics = report.mutation_metrics(rebuild_gate.entity_path);
        let target_entries = u64::try_from(report.staged_validation().entry_count())
            .map_err(|_| InternalError::store_unsupported())?;

        rebuild_gate.validate_before_schema_publication(
            store,
            target,
            report.staged_validation().source_rows(),
        )?;

        Ok(Self {
            metrics,
            target: target.clone(),
            target_index_id: IndexId::new(rebuild_gate.entity_tag, target.ordinal()),
            target_entries,
        })
    }

    pub(super) fn publish_accepted_snapshot(
        &self,
        store: StoreHandle,
        publication_gate: SchemaPublicationGate,
        accepted_after: &PersistedSchemaSnapshot,
    ) -> Result<(), InternalError> {
        let entity_tag = publication_gate.entity_tag();
        self.validate_physical_store_before_schema_publication(store, entity_tag)?;
        publication_gate.publish_accepted_snapshot(store, accepted_after)?;

        Ok(())
    }

    fn validate_physical_store_before_schema_publication(
        &self,
        store: StoreHandle,
        entity_tag: EntityTag,
    ) -> Result<(), InternalError> {
        let preflight = store.with_index(|index_store| {
            if index_store.state() != IndexState::Ready {
                return Err(InternalError::store_unsupported());
            }
            field_path_startup_index_store_preflight(
                index_store,
                entity_tag,
                &self.target,
                self.metrics.entity_path(),
            )
        })?;
        if preflight.target_index_entries() != self.target_entries {
            return Err(InternalError::store_unsupported());
        }
        if preflight.target_index_id() != self.target_index_id {
            return Err(InternalError::store_unsupported());
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
}

fn field_path_rebuild_row_fingerprint_from_rows(
    entity_tag: EntityTag,
    rows: &[StartupFieldPathRebuildRow],
) -> Result<StartupFieldPathRebuildRowFingerprint, InternalError> {
    let mut hasher = Sha256::new();
    for row in rows {
        let raw_key =
            DecodedDataStoreKey::new_primary_key_value(entity_tag, &row.primary_key_value)
                .to_raw()?;
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
        data_store.visit_entries(|raw_key, raw_row| {
            let data_key = DecodedDataStoreKey::try_from_raw(raw_key).map_err(|error| {
                let _ = (&error, entity_path);

                InternalError::store_corruption()
            })?;
            if data_key.entity_tag() != entity_tag {
                return Ok::<StoreVisit, InternalError>(StoreVisit::Continue);
            }
            rows += 1;
            hash_field_path_rebuild_row(&mut hasher, raw_key.as_bytes(), raw_row);
            Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
        })?;

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

    let result: Result<(), InternalError> = index_store.visit_entries(|raw_key, _| {
        let index_key = IndexKey::try_from_raw(raw_key).map_err(|error| {
            let _ = (&error, entity_path, target);

            InternalError::store_corruption()
        })?;
        preflight.record(index_key.index_id());
        Ok(IndexStoreVisit::Continue)
    });
    result?;

    Ok(preflight)
}

pub(super) struct StartupFieldPathRebuildRow {
    pub(super) primary_key_value: PrimaryKeyValue,
    pub(super) row: RawRow,
}

pub(super) struct StartupDecodedFieldPathRebuildRow<'a> {
    pub(super) primary_key_value: PrimaryKeyValue,
    pub(super) slots: StructuralSlotReader<'a>,
}

pub(super) fn field_path_rebuild_raw_rows_for_entity(
    store: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &'static str,
) -> Result<Vec<StartupFieldPathRebuildRow>, InternalError> {
    store.with_data(|data_store| {
        let mut rows = Vec::new();
        data_store.visit_entries(|raw_key, raw_row| {
            let data_key = DecodedDataStoreKey::try_from_raw(raw_key).map_err(|error| {
                let _ = (&error, entity_path);

                InternalError::store_corruption()
            })?;
            if data_key.entity_tag() != entity_tag {
                return Ok::<StoreVisit, InternalError>(StoreVisit::Continue);
            }
            rows.push(StartupFieldPathRebuildRow {
                primary_key_value: data_key.primary_key_value(),
                row: raw_row.clone(),
            });
            Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
        })?;

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
            let data_key =
                DecodedDataStoreKey::new_primary_key_value(entity_tag, &row.primary_key_value);
            slots.validate_primary_key(&data_key)?;

            Ok(StartupDecodedFieldPathRebuildRow {
                primary_key_value: row.primary_key_value,
                slots,
            })
        })
        .collect()
}
