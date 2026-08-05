//! Module: db::session::query::exact_key
//! Responsibility: bounded planner-free typed primary-key batches.
//! Does not own: generated row decoding, caller authorization, or request-wide budgets.
//! Boundary: validates one accepted typed binding, charges caller positions,
//! deduplicates canonical keys, and reads each distinct stored row at most once.

use crate::{
    db::{
        DbSession, DynamicTypedEntityBinding, ExactKeyBatchProjectionOutput, PrimaryKeyEncode,
        QueryError,
        data::{DecodedDataStoreKey, StructuralSlotReader},
        schema::output_value_from_runtime,
    },
    error::InternalError,
    traits::CanisterKind,
    value::OutputValue,
};
use std::collections::BTreeMap;

/// Maximum caller positions in one typed exact-key batch.
pub const MAX_TYPED_EXACT_KEY_BATCH_ITEMS: usize = 1_024;
/// Maximum canonical stored-key bytes charged before deduplication.
pub const MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES: usize = 256 * 1_024;
/// Maximum raw stored-row bytes read for distinct keys.
pub const MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES: usize = 4 * 1_024 * 1_024;
/// Maximum encoded logical projection bytes charged by caller position.
pub const MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES: usize = 4 * 1_024 * 1_024;

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
) -> Result<Vec<OutputValue>, InternalError> {
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
        checked_add_bytes(
            &mut total,
            bytes,
            MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES,
            InternalError::exact_key_batch_result_bytes_exceeded,
        )?;
    }
    Ok(())
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one bounded exact-key batch without constructing a dynamic query.
    ///
    /// `None` means the opaque generated binding is stale. The result carries
    /// one decoded projection per distinct key plus the original-position map.
    #[doc(hidden)]
    pub fn execute_public_exact_key_batch_for_typed_binding<K>(
        &self,
        binding: &DynamicTypedEntityBinding,
        keys: &[K],
    ) -> Result<Option<ExactKeyBatchProjectionOutput>, QueryError>
    where
        K: PrimaryKeyEncode,
    {
        validate_exact_key_count(keys.len()).map_err(QueryError::execute)?;
        let Some(catalog) = self
            .current_typed_entity_binding_catalog(binding)
            .map_err(QueryError::execute)?
        else {
            return Ok(None);
        };

        let entity_tag = catalog.identity().entity_tag();
        let mut distinct_by_raw = BTreeMap::new();
        let mut distinct_keys = Vec::new();
        let mut positions = Vec::with_capacity(keys.len());
        let mut input_bytes = 0_usize;
        for key in keys {
            let primary_key = key
                .to_primary_key_value()
                .map_err(InternalError::from)
                .map_err(QueryError::execute)?;
            let data_key = DecodedDataStoreKey::new(entity_tag, &primary_key);
            let raw_key = data_key.to_raw().map_err(QueryError::execute)?;
            checked_add_bytes(
                &mut input_bytes,
                raw_key.as_bytes().len(),
                MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES,
                InternalError::exact_key_batch_input_bytes_exceeded,
            )
            .map_err(QueryError::execute)?;
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

        let identity = catalog.identity();
        let store = self
            .db
            .recovered_store(identity.store_path())
            .map_err(QueryError::execute)?;
        let mut stored_bytes = 0_usize;
        let raw_rows = distinct_keys
            .iter()
            .map(|(_, raw_key)| {
                let row = store.with_data(|data| data.get(raw_key));
                if let Some(row) = row.as_ref() {
                    checked_add_bytes(
                        &mut stored_bytes,
                        row.len(),
                        MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
                        InternalError::exact_key_batch_stored_bytes_exceeded,
                    )?;
                }
                Ok(row)
            })
            .collect::<Result<Vec<_>, InternalError>>()
            .map_err(QueryError::execute)?;

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
                    .map(|raw_row| project_distinct_row(&catalog, data_key, raw_row, &slots))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(QueryError::execute)?;
        let entity = catalog.snapshot().entity_name().to_string();
        validate_logical_result_bytes(&entity, &columns, &distinct_rows, &positions)
            .map_err(QueryError::execute)?;

        Ok(Some(ExactKeyBatchProjectionOutput {
            entity,
            columns,
            distinct_rows,
            positions,
        }))
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
        let row = Some(vec![OutputValue::Text(
            "x".repeat(MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES / 2),
        )]);
        let rows = vec![row];
        let error =
            validate_logical_result_bytes("Large", &["payload".to_string()], &rows, &[0, 0])
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
    }
}
