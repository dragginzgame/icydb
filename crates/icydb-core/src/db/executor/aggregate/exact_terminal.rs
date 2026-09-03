//! Module: executor::aggregate::exact_terminal
//! Responsibility: exact cardinality and indexed numeric aggregate execution.
//! Does not own: generic aggregate terminals or non-count reducers.
//! Boundary: resolves planner-selected exact targets against synchronized store metadata.

use crate::{
    db::{
        Db,
        data::DataStore,
        executor::{
            EntityAuthority,
            budget::{
                charge_current_execution_budget, direct_read_execution_context,
                with_read_execution_budget,
            },
        },
        index::{IndexId, IndexKeyKind, UserIndexPrefixCardinalityKey},
        registry::StoreHandle,
    },
    error::InternalError,
    metrics::EntityMetricsSpan,
    traits::CanisterKind,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource, DiagnosticExecutionLane};
#[cfg(feature = "sql")]
use std::ops::Bound;

const EXACT_COUNT_SHAPE_DOMAIN: u64 = 0x6963_7964_622d_6578;
#[cfg(feature = "sql")]
const EXACT_NUMERIC_AGGREGATE_SHAPE_DOMAIN: u64 = 0x6963_7964_622d_6e75;

#[cfg(feature = "sql")]
use crate::db::{
    executor::{
        aggregate::scalar_terminals::scalar_distinct_conservative_unit_work,
        budget::{
            charge_runtime_value_rows, current_execution_remaining_budget_units,
            try_charge_current_execution_budget_bundle,
        },
    },
    index::{IndexState, IndexStore},
    numeric::{NumericEvalError, average_decimal_terms_checked},
    query::plan::AggregateKind,
};
#[cfg(feature = "sql")]
use crate::{types::Decimal, value::Value};

fn measure_exact_cardinality<T>(run: impl FnOnce() -> T) -> (u64, T) {
    (0, run())
}

#[cfg(feature = "sql")]
struct ExactFirstComponentMetadata<T> {
    value: T,
    examined: u64,
    stop_after: u64,
    local_instructions: u64,
}

#[cfg(feature = "sql")]
fn execute_exact_first_component_metadata<T>(
    authority: &EntityAuthority,
    index_id: IndexId,
    resolve_store: impl FnOnce() -> Result<StoreHandle, InternalError>,
    resolve_stop_after: impl FnOnce() -> Result<Option<u64>, InternalError>,
    read: impl FnOnce(&IndexStore, u64, u64) -> Result<Option<(T, u64, bool)>, InternalError>,
) -> Result<Option<ExactFirstComponentMetadata<T>>, InternalError> {
    if !accepted_index_target_matches(authority, index_id) {
        return Err(InternalError::query_executor_invariant());
    }
    let store = resolve_store()?;
    if !store.with_index(|index| matches!(index.state(), IndexState::Ready)) {
        return Ok(None);
    }
    let Some(stop_after) = resolve_stop_after()? else {
        return Ok(None);
    };
    if stop_after == 0 {
        return Ok(None);
    }

    let data_generation = store.with_data(DataStore::generation);
    let (local_instructions, metadata) = measure_exact_cardinality(|| {
        store.with_index(|index| read(index, data_generation, stop_after))
    });
    let Some((value, examined, complete)) = metadata? else {
        return Ok(None);
    };
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        examined,
    )?;
    if !complete {
        return Ok(None);
    }

    Ok(Some(ExactFirstComponentMetadata {
        value,
        examined,
        stop_after,
        local_instructions,
    }))
}

/// One planner-proved exact-cardinality metadata target.
#[derive(Clone, Copy)]
pub(in crate::db) enum ExactCardinalityTarget<'keys> {
    /// Exact visible cardinality for the accepted entity.
    Entity,
    #[cfg(feature = "sql")]
    /// Exact number of non-empty leading components for one complete user index.
    UserIndexFirstComponentDistinct(IndexId),
    #[cfg(feature = "sql")]
    /// Exact row cardinality inside one first-component user-index interval.
    UserIndexFirstComponentRange {
        index_id: IndexId,
        lower: &'keys Bound<Vec<u8>>,
        upper: &'keys Bound<Vec<u8>>,
    },
    /// Exact visible cardinality summed across one bounded user-index prefix family.
    UserIndexPrefixes(&'keys [UserIndexPrefixCardinalityKey]),
}

impl ExactCardinalityTarget<'_> {
    fn charged_metadata_entries(&self) -> u64 {
        match self {
            Self::Entity => 1,
            #[cfg(feature = "sql")]
            Self::UserIndexFirstComponentDistinct(_) => 0,
            #[cfg(feature = "sql")]
            Self::UserIndexFirstComponentRange { .. } => 0,
            Self::UserIndexPrefixes(keys) => u64::try_from(keys.len()).unwrap_or(u64::MAX),
        }
    }

    const fn charges_result_budget(&self) -> bool {
        match self {
            Self::Entity | Self::UserIndexPrefixes(_) => true,
            #[cfg(feature = "sql")]
            Self::UserIndexFirstComponentDistinct(_)
            | Self::UserIndexFirstComponentRange { .. } => false,
        }
    }
}

/// Execute one fail-closed metadata-only exact cardinality read.
pub(in crate::db) fn execute_exact_cardinality_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    lane: DiagnosticExecutionLane,
    target: ExactCardinalityTarget<'_>,
) -> Result<Option<u64>, InternalError>
where
    C: CanisterKind,
{
    let entity_path = authority.entity_path_handle();
    let _metrics_span = EntityMetricsSpan::new(entity_path.as_ref());
    let context = direct_read_execution_context(&authority, lane, EXACT_COUNT_SHAPE_DOMAIN);
    with_read_execution_budget(db.request_execution_scope(), context, || {
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            target.charged_metadata_entries(),
        )?;
        let store = db.recovered_store(authority.store_path())?;
        let index_prefix_target = !matches!(&target, ExactCardinalityTarget::Entity);
        let (metadata_local_instructions, output) =
            measure_exact_cardinality(|| -> Result<Option<u64>, InternalError> {
                match target {
                    ExactCardinalityTarget::Entity => {
                        Ok(store.exact_entity_count(authority.entity_tag()))
                    }
                    #[cfg(feature = "sql")]
                    ExactCardinalityTarget::UserIndexFirstComponentDistinct(index_id) => {
                        exact_user_index_first_component_cardinality(
                            store, &authority, index_id, None, None,
                        )
                    }
                    #[cfg(feature = "sql")]
                    ExactCardinalityTarget::UserIndexFirstComponentRange {
                        index_id,
                        lower,
                        upper,
                    } => exact_user_index_first_component_cardinality(
                        store,
                        &authority,
                        index_id,
                        Some(lower),
                        Some(upper),
                    ),
                    ExactCardinalityTarget::UserIndexPrefixes(prefix_keys) => {
                        Ok(exact_user_index_prefix_cardinality_sum(store, prefix_keys))
                    }
                }
            });
        let output = output?;
        let Some(output) = output else {
            return Ok(None);
        };
        if target.charges_result_budget() {
            charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultRows, 1)?;
            charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultBytes, 32)?;
        }

        let _ = (index_prefix_target, metadata_local_instructions);

        Ok(Some(output))
    })
}

/// Execute one planner-selected exact indexed `Int32` aggregate.
#[cfg(feature = "sql")]
pub(in crate::db) fn execute_exact_indexed_numeric_aggregate_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    lane: DiagnosticExecutionLane,
    index_id: IndexId,
    output_kinds: &[AggregateKind],
) -> Result<Option<Vec<Value>>, InternalError>
where
    C: CanisterKind,
{
    if output_kinds.is_empty() {
        return Err(InternalError::query_executor_invariant());
    }

    let entity_path = authority.entity_path_handle();
    let _metrics_span = EntityMetricsSpan::new(entity_path.as_ref());
    let context =
        direct_read_execution_context(&authority, lane, EXACT_NUMERIC_AGGREGATE_SHAPE_DOMAIN);
    with_read_execution_budget(db.request_execution_scope(), context, || {
        let Some(metadata) = execute_exact_first_component_metadata(
            &authority,
            index_id,
            || db.recovered_store(authority.store_path()),
            || {
                current_execution_remaining_budget_units(&[(
                    DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                    1,
                )])
                .map(|stop_after| (stop_after != 0).then_some(stop_after))
            },
            |index, data_generation, stop_after| {
                index
                    .exact_first_component_numeric_fold(data_generation, index_id, stop_after)
                    .map(|fold| {
                        fold.map(|(count, sum, examined, complete)| {
                            ((count, sum), examined, complete)
                        })
                    })
            },
        )?
        else {
            return Ok(None);
        };
        let (count, sum) = metadata.value;

        let sum = Decimal::from_i128_with_scale(sum, 0);
        let average = (count != 0)
            .then(|| average_decimal_terms_checked(sum, count))
            .transpose()
            .map_err(NumericEvalError::into_internal_error)?;
        let row = output_kinds
            .iter()
            .map(|kind| match kind {
                AggregateKind::Sum if count == 0 => Ok(Value::Null),
                AggregateKind::Sum => Ok(Value::Decimal(sum)),
                AggregateKind::Avg => Ok(average.map_or(Value::Null, Value::Decimal)),
                _ => Err(InternalError::query_executor_invariant()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        charge_runtime_value_rows(std::slice::from_ref(&row))?;

        let _ = metadata.local_instructions;

        Ok(Some(row))
    })
}

#[cfg(feature = "sql")]
fn exact_user_index_first_component_cardinality(
    store: StoreHandle,
    authority: &EntityAuthority,
    index_id: IndexId,
    lower: Option<&Bound<Vec<u8>>>,
    upper: Option<&Bound<Vec<u8>>>,
) -> Result<Option<u64>, InternalError> {
    let bounds = lower.zip(upper);
    let unbounded = Bound::Unbounded;
    let (lower, upper) = bounds.unwrap_or((&unbounded, &unbounded));
    let Some(metadata) = execute_exact_first_component_metadata(
        authority,
        index_id,
        || Ok(store),
        || {
            let metadata_capacity = current_execution_remaining_budget_units(&[(
                DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                1,
            )])?;
            let stop_after = if bounds.is_some() {
                Some(metadata_capacity)
            } else {
                current_execution_remaining_budget_units(&exact_distinct_per_unit_budget())?
                    .checked_add(1)
                    .map(|semantic_stop_after| semantic_stop_after.min(metadata_capacity))
            };
            Ok(stop_after.filter(|stop_after| *stop_after != 0))
        },
        |index, data_generation, stop_after| {
            index.exact_first_component_range_cardinality(
                data_generation,
                index_id,
                lower,
                upper,
                stop_after,
            )
        },
    )?
    else {
        return Ok(None);
    };
    if bounds.is_none() && metadata.examined == metadata.stop_after {
        return Ok(None);
    }

    let charged = match bounds {
        Some(_) => try_charge_current_execution_budget_bundle(&[
            (DiagnosticExecutionBudgetResource::ResultRows, 1),
            (DiagnosticExecutionBudgetResource::ResultBytes, 32),
        ])?,
        None => try_charge_current_execution_budget_bundle(&exact_distinct_success_budget(
            metadata.examined,
        )?)?,
    };
    if !charged {
        return Ok(None);
    }

    Ok(Some(if bounds.is_some() {
        metadata.value
    } else {
        metadata.examined
    }))
}

#[cfg(feature = "sql")]
fn accepted_index_target_matches(authority: &EntityAuthority, index_id: IndexId) -> bool {
    index_id.entity_tag() == authority.entity_tag()
        && authority.accepted_schema_info().is_some_and(|schema| {
            schema.field_path_indexes().iter().any(|index| {
                IndexId::new_with_generation(
                    authority.entity_tag(),
                    index.ordinal(),
                    index.physical_generation(),
                ) == index_id
            })
        })
}

#[cfg(feature = "sql")]
fn exact_distinct_per_unit_budget() -> [(DiagnosticExecutionBudgetResource, u64); 3] {
    let (state_bytes, nested_steps) = scalar_distinct_conservative_unit_work(&Value::Int64(0));

    [
        (
            DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
            state_bytes,
        ),
        (DiagnosticExecutionBudgetResource::GroupDistinctEntries, 1),
        (
            DiagnosticExecutionBudgetResource::NestedValueSteps,
            nested_steps,
        ),
    ]
}

#[cfg(feature = "sql")]
fn exact_distinct_success_budget(
    count: u64,
) -> Result<[(DiagnosticExecutionBudgetResource, u64); 5], InternalError> {
    let per_unit = exact_distinct_per_unit_budget();
    Ok([
        (
            per_unit[0].0,
            per_unit[0]
                .1
                .checked_mul(count)
                .ok_or_else(InternalError::query_executor_invariant)?,
        ),
        (per_unit[1].0, count),
        (
            per_unit[2].0,
            per_unit[2]
                .1
                .checked_mul(count)
                .ok_or_else(InternalError::query_executor_invariant)?,
        ),
        (DiagnosticExecutionBudgetResource::ResultRows, 1),
        (DiagnosticExecutionBudgetResource::ResultBytes, 32),
    ])
}

fn exact_user_index_prefix_cardinality_sum(
    store: StoreHandle,
    prefix_keys: &[UserIndexPrefixCardinalityKey],
) -> Option<u64> {
    let index_id = common_prefix_cardinality_index_id(prefix_keys)?;
    index_prefix_cardinality_sum(
        store,
        store.with_data(DataStore::generation),
        index_id,
        prefix_keys
            .iter()
            .map(UserIndexPrefixCardinalityKey::prefix_components),
    )
}

fn common_prefix_cardinality_index_id(
    prefix_keys: &[UserIndexPrefixCardinalityKey],
) -> Option<IndexId> {
    let index_id = prefix_keys.first()?.index_id();
    prefix_keys
        .iter()
        .all(|key| key.index_id() == index_id)
        .then_some(index_id)
}

fn index_prefix_cardinality_sum<'a>(
    store: StoreHandle,
    data_generation: u64,
    index_id: IndexId,
    component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
) -> Option<u64> {
    store.exact_user_index_prefix_count_sum(
        data_generation,
        IndexKeyKind::User,
        index_id,
        component_prefixes,
        None,
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        db::index::{IndexId, UserIndexPrefixCardinalityKey},
        types::EntityTag,
    };

    use super::common_prefix_cardinality_index_id;
    #[cfg(feature = "sql")]
    use super::exact_distinct_success_budget;

    #[test]
    fn exact_count_rejects_prefix_keys_from_mixed_index_generations() {
        let entity_tag = EntityTag::new(0xCA7D);
        let current_index = IndexId::new_with_generation(entity_tag, 2, 7);
        let next_index = IndexId::new_with_generation(entity_tag, 2, 8);
        let current =
            UserIndexPrefixCardinalityKey::new(current_index, vec![b"collection-a".to_vec()]);
        let same_generation =
            UserIndexPrefixCardinalityKey::new(current_index, vec![b"collection-b".to_vec()]);
        let next_generation =
            UserIndexPrefixCardinalityKey::new(next_index, vec![b"collection-c".to_vec()]);

        assert_eq!(
            common_prefix_cardinality_index_id(&[current.clone(), same_generation]),
            Some(current_index),
        );
        assert_eq!(
            common_prefix_cardinality_index_id(&[current, next_generation]),
            None,
        );
    }

    #[test]
    #[cfg(feature = "sql")]
    fn exact_distinct_budget_overflow_is_an_invariant_failure() {
        assert!(exact_distinct_success_budget(u64::MAX).is_err());
    }
}
