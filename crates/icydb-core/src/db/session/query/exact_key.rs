//! Module: db::session::query::exact_key
//! Responsibility: bounded planner-free typed primary-key batches.
//! Does not own: generated row decoding, caller authorization, or aggregate request scopes.
//! Boundary: validates one accepted typed binding, charges caller positions,
//! deduplicates canonical keys, and reads each distinct stored row at most once.

use crate::{
    db::{
        DbSession, DynamicTypedEntityBinding, ExactKeyBatchProjectionOutput, PrimaryKeyValue,
        QueryError,
        data::{DecodedDataStoreKey, RawDataStoreKey, RawRow, StructuralSlotReader},
        executor::budget::{
            HardExecutionBudget, HardExecutionBudgetTracker, HardExecutionContext,
            HardExecutionFailureHeadroom,
        },
        registry::StoreHandle,
        schema::output_value_from_runtime,
    },
    error::InternalError,
    traits::CanisterKind,
    value::OutputValue,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};
#[cfg(feature = "diagnostics")]
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Maximum caller positions in one typed exact-key batch.
pub const MAX_TYPED_EXACT_KEY_BATCH_ITEMS: usize = 1_024;
/// Maximum canonical stored-key bytes charged before deduplication.
pub const MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES: usize = 256 * 1_024;
/// Maximum raw stored-row bytes read for distinct keys.
pub const MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES: usize = 4 * 1_024 * 1_024;
/// Maximum encoded logical projection bytes charged by caller position.
pub const MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES: usize = 4 * 1_024 * 1_024;

const EXACT_KEY_FAILURE_HEADROOM: HardExecutionFailureHeadroom =
    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1_024);
static EXACT_KEY_HARD_BUDGET: HardExecutionBudget = HardExecutionBudget::new(
    [
        1,                 // query executions
        0,                 // planning steps
        0,                 // plan compilations
        1_024,             // key/index entries visited
        1_024,             // rows visited
        4 * 1_024 * 1_024, // stored bytes read
        0,                 // predicate/expression steps
        256 * 1_024,       // nested value steps
        4 * 1_024 * 1_024, // decoded bytes
        4 * 1_024 * 1_024, // materialized bytes
        0,                 // sort entries
        0,                 // sort comparisons
        0,                 // sort temporary bytes
        0,                 // group/distinct entries
        0,                 // group/distinct state bytes
        0,                 // cursor steps
        256 * 1_024,       // temporary bytes
        0,                 // diagnostic steps
        1_024,             // logical result rows
        4 * 1_024 * 1_024, // logical result bytes
        4_500_000_000,     // instrumented instruction units
    ],
    EXACT_KEY_FAILURE_HEADROOM,
);
const EXACT_KEY_SHAPE_DOMAIN: u64 = 0x6963_7964_622d_676b;

struct LoweredExactKeys {
    distinct: Vec<(DecodedDataStoreKey, RawDataStoreKey)>,
    positions: Vec<u32>,
    #[cfg(feature = "diagnostics")]
    diagnostic_key_hashes: Vec<[u8; 16]>,
}

fn budget_error(error: impl Into<InternalError>) -> QueryError {
    QueryError::execute(error.into())
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

const fn exact_key_shape_fingerprint_prefix(binding: &DynamicTypedEntityBinding) -> u64 {
    let fingerprint = binding.accepted_fingerprint;
    u64::from_be_bytes([
        fingerprint[0],
        fingerprint[1],
        fingerprint[2],
        fingerprint[3],
        fingerprint[4],
        fingerprint[5],
        fingerprint[6],
        fingerprint[7],
    ]) ^ binding.entity_tag.rotate_left(17)
        ^ EXACT_KEY_SHAPE_DOMAIN
}

const fn exact_key_budget_context(binding: &DynamicTypedEntityBinding) -> HardExecutionContext {
    HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::PublicRead,
        exact_key_shape_fingerprint_prefix(binding),
    )
}

fn checked_add_bytes(
    total: &mut usize,
    amount: usize,
    limit: usize,
    error: fn(usize, usize) -> InternalError,
) -> Result<(), InternalError> {
    *total = total
        .checked_add(amount)
        .ok_or_else(|| error(usize::MAX, limit))?;
    if *total > limit {
        return Err(error(*total, limit));
    }
    Ok(())
}

fn validate_exact_key_count(count: usize) -> Result<(), InternalError> {
    if count > MAX_TYPED_EXACT_KEY_BATCH_ITEMS {
        return Err(InternalError::exact_key_batch_too_many_items(
            count,
            MAX_TYPED_EXACT_KEY_BATCH_ITEMS,
        ));
    }
    Ok(())
}

fn project_distinct_row(
    catalog: &super::super::AcceptedSchemaCatalogContext,
    data_key: &DecodedDataStoreKey,
    raw_row: &crate::db::data::RawRow,
    slots: &[usize],
    budget: &mut HardExecutionBudgetTracker,
) -> Result<Vec<OutputValue>, InternalError> {
    budget
        .charge_periodic(
            DiagnosticExecutionBudgetResource::NestedValueSteps,
            usize_as_u64(slots.len()),
        )
        .map_err(InternalError::from)?;
    let contract = catalog.inspection_plan().row_contract();
    let reader =
        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(raw_row, contract)?;
    reader.validate_primary_key(data_key)?;
    slots
        .iter()
        .map(|slot| {
            output_value_from_runtime(catalog.enum_catalog(), reader.required_cached_value(*slot)?)
                .map_err(|_| InternalError::store_invariant())
        })
        .collect()
}

fn validate_logical_result_bytes(
    entity: &str,
    columns: &[String],
    distinct_rows: &[Option<Vec<OutputValue>>],
    positions: &[u32],
    budget: &mut HardExecutionBudgetTracker,
) -> Result<(), InternalError> {
    let mut total = candid::encode_one((entity, columns))
        .map_err(|_| InternalError::query_executor_invariant())?
        .len();
    let missing_row_bytes = candid::encode_one(&Option::<Vec<OutputValue>>::None)
        .map_err(|_| InternalError::query_executor_invariant())?
        .len();
    let row_bytes = distinct_rows
        .iter()
        .map(|row| {
            if row.is_none() {
                return Ok(missing_row_bytes);
            }
            candid::encode_one(row)
                .map(|bytes| bytes.len())
                .map_err(|_| InternalError::query_executor_invariant())
        })
        .collect::<Result<Vec<_>, _>>()?;
    for position in positions {
        let index =
            usize::try_from(*position).map_err(|_| InternalError::query_executor_invariant())?;
        let bytes = row_bytes
            .get(index)
            .copied()
            .ok_or_else(InternalError::query_executor_invariant)?;
        let exact_charge = checked_add_bytes(
            &mut total,
            bytes,
            MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES,
            InternalError::exact_key_batch_result_bytes_exceeded,
        );
        if let Err(error) = exact_charge {
            let _ = budget.precharge(
                DiagnosticExecutionBudgetResource::ResultBytes,
                usize_as_u64(total),
            );
            return Err(error);
        }
    }
    budget
        .precharge(
            DiagnosticExecutionBudgetResource::ResultBytes,
            usize_as_u64(total),
        )
        .map_err(InternalError::from)
}

fn charge_stored_row(
    row: &RawRow,
    stored_bytes: &mut usize,
    budget: &mut HardExecutionBudgetTracker,
) -> Result<(), InternalError> {
    let exact_charge = checked_add_bytes(
        stored_bytes,
        row.len(),
        MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
        InternalError::exact_key_batch_stored_bytes_exceeded,
    );
    let row_bytes = usize_as_u64(row.len());
    let row_charge = budget.charge_periodic(DiagnosticExecutionBudgetResource::RowsVisited, 1);
    let stored_charge = budget.charge_periodic(
        DiagnosticExecutionBudgetResource::StoredBytesRead,
        row_bytes,
    );
    exact_charge?;
    row_charge.map_err(InternalError::from)?;
    stored_charge.map_err(InternalError::from)?;
    budget
        .charge_periodic(DiagnosticExecutionBudgetResource::DecodedBytes, row_bytes)
        .map_err(InternalError::from)?;
    budget
        .charge_periodic(
            DiagnosticExecutionBudgetResource::MaterializedBytes,
            row_bytes,
        )
        .map_err(InternalError::from)?;
    Ok(())
}

fn load_distinct_raw_rows(
    store: &StoreHandle,
    distinct_keys: &[(DecodedDataStoreKey, RawDataStoreKey)],
    budget: &mut HardExecutionBudgetTracker,
) -> Result<Vec<Option<RawRow>>, InternalError> {
    let mut stored_bytes = 0_usize;
    distinct_keys
        .iter()
        .map(|(_, raw_key)| {
            budget
                .charge_periodic(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 1)
                .map_err(InternalError::from)?;
            let row = store.with_data(|data| data.get(raw_key));
            if let Some(row) = row.as_ref() {
                charge_stored_row(row, &mut stored_bytes, budget)?;
            }
            Ok(row)
        })
        .collect()
}

fn lower_exact_keys(
    entity_tag: crate::types::EntityTag,
    keys: &[PrimaryKeyValue],
    budget: &mut HardExecutionBudgetTracker,
) -> Result<LoweredExactKeys, QueryError> {
    let mut distinct_by_raw = BTreeMap::new();
    let mut distinct_keys = Vec::new();
    let mut positions = Vec::with_capacity(keys.len());
    #[cfg(feature = "diagnostics")]
    let collect_diagnostic_keys = budget.request_diagnostics_enabled();
    #[cfg(feature = "diagnostics")]
    let mut diagnostic_key_hashes = Vec::new();
    let mut input_bytes = 0_usize;
    for primary_key in keys {
        let data_key = DecodedDataStoreKey::new(entity_tag, primary_key);
        let raw_key = data_key.to_raw().map_err(QueryError::execute)?;
        #[cfg(feature = "diagnostics")]
        if collect_diagnostic_keys {
            diagnostic_key_hashes.push(diagnostic_key_hash(raw_key.as_bytes()));
        }
        let exact_charge = checked_add_bytes(
            &mut input_bytes,
            raw_key.as_bytes().len(),
            MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES,
            InternalError::exact_key_batch_input_bytes_exceeded,
        );
        if let Err(error) = exact_charge {
            let _ = budget.precharge(
                DiagnosticExecutionBudgetResource::TemporaryBytes,
                usize_as_u64(input_bytes),
            );
            return Err(QueryError::execute(error));
        }
        let index = if let Some(index) = distinct_by_raw.get(&raw_key) {
            *index
        } else {
            let index = distinct_keys.len();
            distinct_by_raw.insert(raw_key.clone(), index);
            distinct_keys.push((data_key, raw_key));
            index
        };
        positions.push(
            u32::try_from(index)
                .map_err(|_| QueryError::execute(InternalError::query_executor_invariant()))?,
        );
    }
    budget
        .precharge(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            usize_as_u64(input_bytes),
        )
        .map_err(budget_error)?;
    Ok(LoweredExactKeys {
        distinct: distinct_keys,
        positions,
        #[cfg(feature = "diagnostics")]
        diagnostic_key_hashes,
    })
}

#[cfg(feature = "diagnostics")]
fn diagnostic_key_hash(raw_key: &[u8]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(b"icydb/request-diagnostic/exact-key/v1\0");
    hasher.update(raw_key);
    let digest = hasher.finalize();
    let mut hash = [0_u8; 16];
    hash.copy_from_slice(&digest[..16]);
    hash
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one bounded exact-key batch without constructing a dynamic query.
    ///
    /// `None` means the opaque generated binding is stale. The result carries
    /// one decoded projection per distinct key plus the original-position map.
    #[doc(hidden)]
    pub fn execute_public_exact_key_batch_for_typed_binding(
        &self,
        binding: &DynamicTypedEntityBinding,
        keys: &[PrimaryKeyValue],
    ) -> Result<Option<ExactKeyBatchProjectionOutput>, QueryError> {
        let mut budget = HardExecutionBudgetTracker::new_with_request_scope(
            &EXACT_KEY_HARD_BUDGET,
            exact_key_budget_context(binding),
            self.db.request_execution_scope(),
        );
        let result = self.execute_exact_key_batch_with_budget(binding, keys, &mut budget);
        #[cfg(feature = "diagnostics")]
        budget.finish_request_diagnostics();
        result
    }

    fn execute_exact_key_batch_with_budget(
        &self,
        binding: &DynamicTypedEntityBinding,
        keys: &[PrimaryKeyValue],
        budget: &mut HardExecutionBudgetTracker,
    ) -> Result<Option<ExactKeyBatchProjectionOutput>, QueryError> {
        budget
            .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1)
            .map_err(budget_error)?;
        validate_exact_key_count(keys.len()).map_err(QueryError::execute)?;
        budget
            .precharge(
                DiagnosticExecutionBudgetResource::ResultRows,
                usize_as_u64(keys.len()),
            )
            .map_err(budget_error)?;
        let Some(catalog) = self
            .current_typed_entity_binding_catalog(binding)
            .map_err(QueryError::execute)?
        else {
            return Ok(None);
        };

        let lowered = lower_exact_keys(catalog.identity().entity_tag(), keys, budget)?;
        #[cfg(feature = "diagnostics")]
        budget.record_exact_key_hashes(&lowered.diagnostic_key_hashes);
        let distinct_keys = lowered.distinct;
        let positions = lowered.positions;

        let identity = catalog.identity();
        let store = self
            .db
            .recovered_store(identity.store_path())
            .map_err(QueryError::execute)?;
        let raw_rows =
            load_distinct_raw_rows(&store, &distinct_keys, budget).map_err(QueryError::execute)?;

        let schema = catalog.accepted_schema_info();
        let columns = schema
            .field_names_in_slot_order()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let slots = columns
            .iter()
            .map(|column| {
                schema
                    .field_slot_index(column)
                    .ok_or_else(InternalError::query_executor_invariant)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(QueryError::execute)?;
        let distinct_rows = distinct_keys
            .iter()
            .zip(raw_rows.iter())
            .map(|((data_key, _), raw_row)| {
                raw_row
                    .as_ref()
                    .map(|raw_row| {
                        project_distinct_row(&catalog, data_key, raw_row, &slots, budget)
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(QueryError::execute)?;
        let entity = catalog.snapshot().entity_name().to_string();
        validate_logical_result_bytes(&entity, &columns, &distinct_rows, &positions, budget)
            .map_err(QueryError::execute)?;
        budget
            .finish_instruction_watermark()
            .map_err(budget_error)?;

        Ok(Some(ExactKeyBatchProjectionOutput {
            entity,
            columns,
            distinct_rows,
            positions,
        }))
    }

    #[cfg(all(test, feature = "sql", feature = "diagnostics"))]
    pub(in crate::db) fn execute_exact_key_batch_with_hard_budget_for_tests(
        &self,
        binding: &DynamicTypedEntityBinding,
        keys: &[PrimaryKeyValue],
        hard_budget: &HardExecutionBudget,
    ) -> Result<Option<ExactKeyBatchProjectionOutput>, QueryError> {
        let mut budget = HardExecutionBudgetTracker::new_for_tests(
            *hard_budget,
            exact_key_budget_context(binding),
        );
        self.execute_exact_key_batch_with_budget(binding, keys, &mut budget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_byte_bound(
        error: InternalError,
        boundary: icydb_diagnostic_code::RuntimeBoundaryCode,
        actual: usize,
        limit: usize,
    ) {
        assert!(matches!(
            error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary: actual_boundary,
            }) if *actual_boundary == boundary,
        ));
        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::ActualLength,
                    actual as u64,
                ),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::Limit,
                    limit as u64,
                ),
            ],
        );
    }

    #[test]
    fn exact_key_byte_admission_reports_each_bounded_resource() {
        let mut input = MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES;
        let input_error = checked_add_bytes(
            &mut input,
            1,
            MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES,
            InternalError::exact_key_batch_input_bytes_exceeded,
        )
        .expect_err("input key bytes above the cap should reject");
        assert_byte_bound(
            input_error,
            icydb_diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchInputBytesExceeded,
            MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES + 1,
            MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES,
        );

        let mut stored = MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES;
        let stored_error = checked_add_bytes(
            &mut stored,
            1,
            MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
            InternalError::exact_key_batch_stored_bytes_exceeded,
        )
        .expect_err("stored row bytes above the cap should reject");
        assert_byte_bound(
            stored_error,
            icydb_diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchStoredBytesExceeded,
            MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES + 1,
            MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
        );
    }

    #[test]
    fn exact_key_result_bytes_charge_original_duplicate_positions() {
        let row = Some(vec![OutputValue::text(
            "x".repeat(MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES / 2),
        )]);
        let rows = vec![row];
        let mut budget = HardExecutionBudgetTracker::new(
            &EXACT_KEY_HARD_BUDGET,
            HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::PublicRead,
                1,
            ),
        );
        let error = validate_logical_result_bytes(
            "Large",
            &["payload".to_string()],
            &rows,
            &[0, 0],
            &mut budget,
        )
        .expect_err("duplicate logical positions must each charge result bytes");
        assert!(matches!(
            error.diagnostic().detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
                boundary:
                    icydb_diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchResultBytesExceeded,
            }),
        ));
        assert_eq!(
            error.diagnostic_facts()[1],
            (
                icydb_diagnostic_code::DiagnosticFactTag::Limit,
                MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES as u64,
            ),
        );
        assert!(
            budget.observed(DiagnosticExecutionBudgetResource::ResultBytes)
                > MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES as u64,
            "rejected result work remains charged even when the exact-key boundary is returned",
        );
    }

    #[test]
    fn exact_key_lowering_preserves_composite_distinct_positions() {
        let composite = |tenant, local| {
            PrimaryKeyValue::Composite(
                crate::db::CompositePrimaryKeyValue::try_from_components(&[
                    crate::db::PrimaryKeyComponent::Nat64(tenant),
                    crate::db::PrimaryKeyComponent::Nat64(local),
                ])
                .expect("two non-Unit components should form a composite key"),
            )
        };
        let first = composite(7, 11);
        let second = composite(7, 12);
        let mut budget = HardExecutionBudgetTracker::new(
            &EXACT_KEY_HARD_BUDGET,
            HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::PublicRead,
                2,
            ),
        );

        let lowered = lower_exact_keys(
            crate::types::EntityTag::new(9),
            &[first, second, first],
            &mut budget,
        )
        .expect("composite exact keys should lower through the concrete boundary");

        assert_eq!(lowered.positions, vec![0, 1, 0]);
        assert_eq!(lowered.distinct.len(), 2);
        assert_ne!(lowered.distinct[0].1, lowered.distinct[1].1);
    }
}
