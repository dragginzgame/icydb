//! Module: executor::aggregate::count_terminal
//! Responsibility: scalar COUNT terminal preflight and cardinality-backed execution.
//! Does not own: generic scalar terminal dispatch or non-count aggregate reducers.
//! Boundary: COUNT-specific helpers consumed by aggregate terminal orchestration.

use crate::{
    db::{
        access::{ExecutionPathPayload, LoweredAccess, lower_access},
        data::{DataStore, DecodedDataStoreKey, StoreVisit},
        executor::{
            EntityAuthority, LoweredIndexPrefixCardinalityKey, PreparedAggregatePlan,
            aggregate::{
                AccessPlannedQuery, PageSpec, PreparedAggregateStreamingInputs,
                PreparedScalarTerminalPreflight, ScalarAggregateOutput,
            },
            exact_count_cardinality_prefixes_for_plan,
            pipeline::contracts::LoadExecutor,
            plan_metrics::{record_plan_metrics, record_rows_scanned_for_path},
            validate_executor_plan_for_authority,
        },
        index::IndexKeyKind,
        query::builder::aggregate::{ScalarTerminalBoundaryOutput, ScalarTerminalBoundaryRequest},
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::EntityTag,
    value::Value,
};
use std::ops::Bound;

// Execute prepared COUNT through store-cardinality fast-path semantics.
pub(super) fn execute_count_primary_key_cardinality_terminal_request(
    prepared: PreparedAggregateStreamingInputs<'_>,
) -> Result<ScalarAggregateOutput, InternalError> {
    let lowered_access = prepared.lowered_access()?;
    let (count, rows_scanned) = aggregate_count_from_pk_cardinality_with_store(
        &prepared.logical_plan,
        &lowered_access,
        prepared.authority.entity_tag(),
        prepared.store,
    )?;
    record_rows_scanned_for_path(prepared.authority.entity_path(), rows_scanned);

    Ok(ScalarAggregateOutput::Count(count))
}

// Prepare early scalar terminal work that must run before aggregate streaming
// setup. Preflights are fail-open so missing runtime metadata falls back to the
// normal aggregate terminal boundary without losing correctness.
pub(super) fn try_prepare_scalar_terminal_preflight<'plan>(
    plan: &'plan PreparedAggregatePlan,
    request: &ScalarTerminalBoundaryRequest,
) -> Option<PreparedScalarTerminalPreflight<'plan>> {
    match request {
        ScalarTerminalBoundaryRequest::Count => try_prepare_index_prefix_cardinality_preflight(
            plan,
            |authority, logical_plan, prefixes| {
                PreparedScalarTerminalPreflight::CountIndexPrefixCardinality {
                    authority,
                    logical_plan,
                    prefixes,
                }
            },
        ),
        ScalarTerminalBoundaryRequest::Exists => try_prepare_index_prefix_cardinality_preflight(
            plan,
            |authority, logical_plan, prefixes| {
                PreparedScalarTerminalPreflight::ExistsIndexPrefixCardinality {
                    authority,
                    logical_plan,
                    prefixes,
                }
            },
        ),
        ScalarTerminalBoundaryRequest::IdTerminal { .. }
        | ScalarTerminalBoundaryRequest::IdBySlot { .. }
        | ScalarTerminalBoundaryRequest::NthBySlot { .. }
        | ScalarTerminalBoundaryRequest::MedianBySlot { .. }
        | ScalarTerminalBoundaryRequest::MinMaxBySlot { .. } => None,
    }
}

fn try_prepare_index_prefix_cardinality_preflight<'plan>(
    plan: &'plan PreparedAggregatePlan,
    build: impl FnOnce(
        EntityAuthority,
        &'plan AccessPlannedQuery,
        Vec<LoweredIndexPrefixCardinalityKey>,
    ) -> PreparedScalarTerminalPreflight<'plan>,
) -> Option<PreparedScalarTerminalPreflight<'plan>> {
    let authority = plan.authority();
    let logical_plan = plan.logical_plan();
    let Ok(lowered_access) = lower_access(authority.entity_tag(), &logical_plan.access) else {
        return None;
    };
    let Ok(index_prefix_specs) = plan.index_prefix_specs() else {
        return None;
    };
    let prefixes = exact_count_cardinality_prefixes_for_plan(
        logical_plan,
        &lowered_access,
        index_prefix_specs,
    )?;

    Some(build(authority, logical_plan, prefixes))
}

pub(super) fn execute_scalar_terminal_preflight<E>(
    executor: &LoadExecutor<E>,
    preflight: PreparedScalarTerminalPreflight<'_>,
) -> Result<Option<ScalarTerminalBoundaryOutput>, InternalError>
where
    E: EntityKind + EntityValue,
{
    match preflight {
        PreparedScalarTerminalPreflight::CountIndexPrefixCardinality {
            authority,
            logical_plan,
            prefixes,
        } => execute_count_index_prefix_cardinality_preflight(
            executor,
            authority,
            logical_plan,
            &prefixes,
        ),
        PreparedScalarTerminalPreflight::ExistsIndexPrefixCardinality {
            authority,
            logical_plan,
            prefixes,
        } => execute_exists_index_prefix_cardinality_preflight(
            executor,
            authority,
            logical_plan,
            &prefixes,
        ),
    }
}

fn execute_count_index_prefix_cardinality_preflight<E>(
    executor: &LoadExecutor<E>,
    authority: EntityAuthority,
    logical_plan: &AccessPlannedQuery,
    prefixes: &[LoweredIndexPrefixCardinalityKey],
) -> Result<Option<ScalarTerminalBoundaryOutput>, InternalError>
where
    E: EntityKind + EntityValue,
{
    validate_executor_plan_for_authority(&authority, logical_plan)?;
    let store = executor.db.recovered_store(authority.store_path())?;
    let Some(count) =
        count_index_prefix_cardinality(store, logical_plan.scalar_plan().page.as_ref(), prefixes)
    else {
        return Ok(None);
    };

    record_plan_metrics(authority.entity_path(), logical_plan);
    record_index_prefix_cardinality_terminal(authority.entity_path());

    Ok(Some(ScalarTerminalBoundaryOutput::Count(count)))
}

fn execute_exists_index_prefix_cardinality_preflight<E>(
    executor: &LoadExecutor<E>,
    authority: EntityAuthority,
    logical_plan: &AccessPlannedQuery,
    prefixes: &[LoweredIndexPrefixCardinalityKey],
) -> Result<Option<ScalarTerminalBoundaryOutput>, InternalError>
where
    E: EntityKind + EntityValue,
{
    validate_executor_plan_for_authority(&authority, logical_plan)?;
    let store = executor.db.recovered_store(authority.store_path())?;
    let Some(exists) =
        exists_index_prefix_cardinality(store, logical_plan.scalar_plan().page.as_ref(), prefixes)
    else {
        return Ok(None);
    };

    record_plan_metrics(authority.entity_path(), logical_plan);
    record_index_prefix_cardinality_terminal(authority.entity_path());

    Ok(Some(ScalarTerminalBoundaryOutput::Exists(exists)))
}

fn count_index_prefix_cardinality(
    store: StoreHandle,
    page: Option<&PageSpec>,
    prefixes: &[LoweredIndexPrefixCardinalityKey],
) -> Option<u32> {
    let data_generation = store.with_data(DataStore::generation);
    let available_rows = store.with_index(|store| {
        let mut total = 0_u64;
        for prefix in prefixes {
            let count = store.exact_prefix_cardinality(
                data_generation,
                IndexKeyKind::User,
                prefix.index_id(),
                prefix.prefix_components(),
            )?;
            total = total.saturating_add(count);
        }
        Some(total)
    });
    let available_rows = available_rows?;
    let available_rows = usize::try_from(available_rows).unwrap_or(usize::MAX);
    let count_window = CountWindowResult::from_candidate_rows(page, available_rows);

    Some(count_window.count())
}

fn exists_index_prefix_cardinality(
    store: StoreHandle,
    page: Option<&PageSpec>,
    prefixes: &[LoweredIndexPrefixCardinalityKey],
) -> Option<bool> {
    let Some(required_candidate_rows) = exists_window_required_candidate_rows(page) else {
        return Some(false);
    };
    let data_generation = store.with_data(DataStore::generation);

    store.with_index(|store| {
        let mut available_rows = 0_u64;
        for prefix in prefixes {
            let count = store.exact_prefix_cardinality(
                data_generation,
                IndexKeyKind::User,
                prefix.index_id(),
                prefix.prefix_components(),
            )?;
            available_rows = available_rows.saturating_add(count);
            if available_rows >= required_candidate_rows {
                return Some(true);
            }
        }

        Some(false)
    })
}

fn exists_window_required_candidate_rows(page: Option<&PageSpec>) -> Option<u64> {
    match page {
        Some(PageSpec { limit: Some(0), .. }) => None,
        Some(page) => Some(u64::from(page.offset).saturating_add(1)),
        None => Some(1),
    }
}

fn record_index_prefix_cardinality_terminal(entity_path: &'static str) {
    record_rows_scanned_for_path(entity_path, 0);
    #[cfg(all(feature = "diagnostics", feature = "sql"))]
    super::scalar_terminals::record_index_prefix_cardinality_terminal_attribution();
}

// Resolve COUNT for PK full-scan/key-range shapes from store cardinality while
// preserving canonical page-window and scan-accounting semantics.
fn aggregate_count_from_pk_cardinality_with_store(
    logical_plan: &AccessPlannedQuery,
    lowered_access: &LoweredAccess<'_, Value>,
    entity_tag: EntityTag,
    store: StoreHandle,
) -> Result<(u32, usize), InternalError> {
    // Phase 1: snapshot pagination + access payload before resolving store cardinality.
    let page = logical_plan.scalar_plan().page.as_ref();
    let Some(path) = lowered_access.executable().as_path() else {
        return Err(InternalError::query_executor_invariant());
    };

    // Phase 2: read candidate-row cardinality directly from primary storage.
    let available_rows = match path {
        ExecutionPathPayload::FullScan => store.with_data(|data| {
            let mut count = 0usize;
            let _: Result<(), InternalError> = data.visit_entity(entity_tag, |_raw_key, _row| {
                count = count.saturating_add(1);
                Ok(StoreVisit::Continue)
            });
            count
        }),
        ExecutionPathPayload::KeyRange { start, end } => {
            let start_raw =
                DecodedDataStoreKey::try_from_structural_key(entity_tag, start)?.to_raw()?;
            let end_raw =
                DecodedDataStoreKey::try_from_structural_key(entity_tag, end)?.to_raw()?;

            store.with_data(|data| {
                let mut count = 0usize;
                let _: Result<(), InternalError> = data.visit_range(
                    (Bound::Included(start_raw), Bound::Included(end_raw)),
                    |_raw_key, _row| {
                        count = count.saturating_add(1);
                        Ok(StoreVisit::Continue)
                    },
                );
                count
            })
        }
        _ => {
            return Err(InternalError::query_executor_invariant());
        }
    };

    // Phase 3: apply canonical COUNT window semantics and emit scan metrics.
    let count_window = CountWindowResult::from_candidate_rows(page, available_rows);

    Ok((count_window.count(), count_window.rows_scanned()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CountWindowResult {
    count: u32,
    rows_scanned: usize,
}

impl CountWindowResult {
    // Map one candidate cardinality and optional page contract to canonical
    // COUNT result and scan accounting semantics.
    fn from_candidate_rows(page: Option<&PageSpec>, available_rows: usize) -> Self {
        let Some(page) = page else {
            return Self::new(usize_to_u32_saturating(available_rows), available_rows);
        };
        let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);

        match page.limit {
            Some(0) => Self::new(0, 0),
            Some(limit) => {
                let limit = usize::try_from(limit).unwrap_or(usize::MAX);
                let rows_scanned = available_rows.min(offset.saturating_add(limit));
                let count = available_rows.saturating_sub(offset).min(limit);

                Self::new(usize_to_u32_saturating(count), rows_scanned)
            }
            None => {
                let count = available_rows.saturating_sub(offset);
                Self::new(usize_to_u32_saturating(count), available_rows)
            }
        }
    }

    const fn new(count: u32, rows_scanned: usize) -> Self {
        Self {
            count,
            rows_scanned,
        }
    }

    const fn count(self) -> u32 {
        self.count
    }

    const fn rows_scanned(self) -> usize {
        self.rows_scanned
    }
}

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
