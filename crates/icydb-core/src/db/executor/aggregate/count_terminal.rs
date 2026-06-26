//! Module: executor::aggregate::count_terminal
//! Responsibility: scalar COUNT terminal preflight and cardinality-backed execution.
//! Does not own: generic scalar terminal dispatch or non-count aggregate reducers.
//! Boundary: COUNT-specific helpers consumed by aggregate terminal orchestration.

use crate::{
    db::{
        access::{ExecutionPathPayload, LoweredAccess},
        data::{DataStore, DecodedDataStoreKey, RawDataStoreKey, StoreVisit},
        executor::{
            EntityAuthority, LoweredIndexPrefixCardinalityPlan, PreparedAggregatePlan,
            aggregate::{
                AccessPlannedQuery, PageSpec, PreparedAggregateStreamingInputs,
                PreparedScalarTerminalPreflight, ScalarAggregateOutput,
            },
            exact_count_cardinality_prefixes_for_plan,
            pipeline::contracts::LoadExecutor,
            plan_metrics::{record_plan_metrics, record_rows_scanned_for_path},
            planning::route::index_multi_lookup_prefix_cardinality_preflight_shape_supported,
            validate_executor_plan_for_authority,
        },
        index::{IndexId, IndexKeyKind},
        query::builder::aggregate::{ScalarTerminalBoundaryOutput, ScalarTerminalBoundaryRequest},
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::EntityTag,
    value::Value,
};
use std::ops::Bound;

#[cfg(feature = "sql")]
use crate::db::access::LoweredIndexPrefixCardinalitySpec;

#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::measure_local_instruction_delta as measure_count_terminal_phase;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndexPrefixCardinalityTerminal {
    Count,
    Exists,
}

impl IndexPrefixCardinalityTerminal {
    fn for_request(
        request: &ScalarTerminalBoundaryRequest,
        logical_plan: &AccessPlannedQuery,
    ) -> Option<Self> {
        match request {
            ScalarTerminalBoundaryRequest::Count => Some(Self::Count),
            ScalarTerminalBoundaryRequest::Exists
                if exists_index_prefix_cardinality_preflight_supported(logical_plan) =>
            {
                Some(Self::Exists)
            }
            ScalarTerminalBoundaryRequest::Exists
            | ScalarTerminalBoundaryRequest::IdTerminal { .. }
            | ScalarTerminalBoundaryRequest::IdBySlot { .. }
            | ScalarTerminalBoundaryRequest::NthBySlot { .. }
            | ScalarTerminalBoundaryRequest::MedianBySlot { .. }
            | ScalarTerminalBoundaryRequest::MinMaxBySlot { .. } => None,
        }
    }

    const fn allow_ordered_plan(self) -> bool {
        matches!(self, Self::Count)
    }

    const fn into_preflight<'plan>(
        self,
        authority: EntityAuthority,
        logical_plan: &'plan AccessPlannedQuery,
        prefixes: LoweredIndexPrefixCardinalityPlan<'plan>,
    ) -> PreparedScalarTerminalPreflight<'plan> {
        match self {
            Self::Count => PreparedScalarTerminalPreflight::CountIndexPrefixCardinality {
                authority,
                logical_plan,
                prefixes,
            },
            Self::Exists => PreparedScalarTerminalPreflight::ExistsIndexPrefixCardinality {
                authority,
                logical_plan,
                prefixes,
            },
        }
    }

    fn output_for_plan(
        self,
        store: StoreHandle,
        page: Option<&PageSpec>,
        prefixes: LoweredIndexPrefixCardinalityPlan<'_>,
    ) -> Option<ScalarTerminalBoundaryOutput> {
        match self {
            Self::Count => count_index_prefix_cardinality(store, page, prefixes)
                .map(ScalarTerminalBoundaryOutput::Count),
            Self::Exists => exists_index_prefix_cardinality(store, page, prefixes)
                .map(ScalarTerminalBoundaryOutput::Exists),
        }
    }
}

#[cfg(feature = "diagnostics")]
fn measure_index_prefix_cardinality_preflight<T>(run: impl FnOnce() -> T) -> (u64, T) {
    measure_count_terminal_phase(run)
}

#[cfg(not(feature = "diagnostics"))]
fn measure_index_prefix_cardinality_preflight<T>(run: impl FnOnce() -> T) -> (u64, T) {
    (0, run())
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    #[cfg(feature = "sql")]
    pub(in crate::db) fn execute_direct_count_index_prefix_cardinality_request(
        &self,
        authority: EntityAuthority,
        page: Option<&PageSpec>,
        prefixes: &[LoweredIndexPrefixCardinalitySpec],
    ) -> Result<Option<ScalarTerminalBoundaryOutput>, InternalError> {
        let store = self.db.recovered_store(authority.store_path())?;
        Ok(execute_measured_index_prefix_cardinality_terminal(
            authority.entity_path(),
            || {},
            || {
                count_index_prefix_cardinality_specs(store, page, prefixes)
                    .map(ScalarTerminalBoundaryOutput::Count)
            },
        ))
    }
}

fn execute_measured_index_prefix_cardinality_terminal(
    entity_path: &'static str,
    record_output_context: impl FnOnce(),
    resolve: impl FnOnce() -> Option<ScalarTerminalBoundaryOutput>,
) -> Option<ScalarTerminalBoundaryOutput> {
    let (metadata_local_instructions, output) = measure_index_prefix_cardinality_preflight(resolve);
    let output = output?;

    record_output_context();
    record_index_prefix_cardinality_terminal(entity_path, metadata_local_instructions);

    Some(output)
}

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
    let terminal = IndexPrefixCardinalityTerminal::for_request(request, plan.logical_plan())?;

    try_prepare_index_prefix_cardinality_preflight(plan, terminal)
}

fn exists_index_prefix_cardinality_preflight_supported(logical_plan: &AccessPlannedQuery) -> bool {
    logical_plan
        .access_shape_facts()
        .single_path_facts()
        .is_some_and(|shape_facts| {
            index_multi_lookup_prefix_cardinality_preflight_shape_supported(&shape_facts)
        })
}

fn try_prepare_index_prefix_cardinality_preflight(
    plan: &PreparedAggregatePlan,
    terminal: IndexPrefixCardinalityTerminal,
) -> Option<PreparedScalarTerminalPreflight<'_>> {
    let authority = plan.authority();
    let logical_plan = plan.logical_plan();
    let Ok(index_prefix_specs) = plan.index_prefix_specs() else {
        return None;
    };
    let prefixes = exact_count_cardinality_prefixes_for_plan(
        authority.entity_tag(),
        logical_plan,
        index_prefix_specs,
        terminal.allow_ordered_plan(),
    )?;

    Some(terminal.into_preflight(authority, logical_plan, prefixes))
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
        } => execute_index_prefix_cardinality_preflight(
            executor,
            IndexPrefixCardinalityTerminal::Count,
            authority,
            logical_plan,
            prefixes,
        ),
        PreparedScalarTerminalPreflight::ExistsIndexPrefixCardinality {
            authority,
            logical_plan,
            prefixes,
        } => execute_index_prefix_cardinality_preflight(
            executor,
            IndexPrefixCardinalityTerminal::Exists,
            authority,
            logical_plan,
            prefixes,
        ),
    }
}

fn execute_index_prefix_cardinality_preflight<E>(
    executor: &LoadExecutor<E>,
    terminal: IndexPrefixCardinalityTerminal,
    authority: EntityAuthority,
    logical_plan: &AccessPlannedQuery,
    prefixes: LoweredIndexPrefixCardinalityPlan<'_>,
) -> Result<Option<ScalarTerminalBoundaryOutput>, InternalError>
where
    E: EntityKind + EntityValue,
{
    validate_executor_plan_for_authority(&authority, logical_plan)?;
    let store = executor.db.recovered_store(authority.store_path())?;
    Ok(execute_measured_index_prefix_cardinality_terminal(
        authority.entity_path(),
        || record_plan_metrics(authority.entity_path(), logical_plan),
        || terminal.output_for_plan(store, logical_plan.scalar_plan().page.as_ref(), prefixes),
    ))
}

fn count_index_prefix_cardinality(
    store: StoreHandle,
    page: Option<&PageSpec>,
    prefixes: LoweredIndexPrefixCardinalityPlan<'_>,
) -> Option<u32> {
    count_index_prefix_cardinality_from_sum(page, |required_candidate_rows| {
        index_prefix_cardinality_sum_for_plan(store, prefixes, required_candidate_rows)
    })
}

#[cfg(feature = "sql")]
fn count_index_prefix_cardinality_specs(
    store: StoreHandle,
    page: Option<&PageSpec>,
    prefixes: &[LoweredIndexPrefixCardinalitySpec],
) -> Option<u32> {
    count_index_prefix_cardinality_from_sum(page, |required_candidate_rows| {
        index_prefix_cardinality_sum_for_specs(store, prefixes, required_candidate_rows)
    })
}

fn count_index_prefix_cardinality_from_sum(
    page: Option<&PageSpec>,
    sum: impl FnOnce(Option<u64>) -> Option<u64>,
) -> Option<u32> {
    let candidate_window = CardinalityCandidateWindow::for_count(page);
    if candidate_window.is_empty() {
        return Some(0);
    }

    let available_rows = sum(candidate_window.stop_after())?;
    let available_rows = usize::try_from(available_rows).unwrap_or(usize::MAX);
    let count_window = CountWindowResult::from_candidate_rows(page, available_rows);

    Some(count_window.count())
}

#[cfg(feature = "sql")]
fn index_prefix_cardinality_sum_for_specs(
    store: StoreHandle,
    prefixes: &[LoweredIndexPrefixCardinalitySpec],
    stop_after: Option<u64>,
) -> Option<u64> {
    let index_id = common_prefix_cardinality_index_id(prefixes)?;
    index_prefix_cardinality_sum(
        store,
        store.with_data(DataStore::generation),
        index_id,
        prefixes
            .iter()
            .map(LoweredIndexPrefixCardinalitySpec::prefix_components),
        stop_after,
    )
}

#[cfg(feature = "sql")]
fn common_prefix_cardinality_index_id(
    prefixes: &[LoweredIndexPrefixCardinalitySpec],
) -> Option<crate::db::index::IndexId> {
    let index_id = prefixes.first()?.index_id();
    prefixes
        .iter()
        .all(|spec| spec.index_id() == index_id)
        .then_some(index_id)
}

fn exists_index_prefix_cardinality(
    store: StoreHandle,
    page: Option<&PageSpec>,
    prefixes: LoweredIndexPrefixCardinalityPlan<'_>,
) -> Option<bool> {
    let Some(required_candidate_rows) = CardinalityCandidateWindow::for_exists(page).bounded_rows()
    else {
        return Some(false);
    };

    let available_rows =
        index_prefix_cardinality_sum_for_plan(store, prefixes, Some(required_candidate_rows))?;

    Some(available_rows >= required_candidate_rows)
}

fn index_prefix_cardinality_sum_for_plan(
    store: StoreHandle,
    prefixes: LoweredIndexPrefixCardinalityPlan<'_>,
    stop_after: Option<u64>,
) -> Option<u64> {
    let prefix_len = prefixes.prefix_len();
    if prefixes
        .specs()
        .iter()
        .any(|spec| spec.prefix_components().len() < prefix_len)
    {
        return None;
    }

    index_prefix_cardinality_sum(
        store,
        store.with_data(DataStore::generation),
        prefixes.index_id(),
        prefixes
            .specs()
            .iter()
            .map(|spec| &spec.prefix_components()[..prefix_len]),
        stop_after,
    )
}

fn index_prefix_cardinality_sum<'a>(
    store: StoreHandle,
    data_generation: u64,
    index_id: IndexId,
    component_prefixes: impl IntoIterator<Item = &'a [Vec<u8>]>,
    stop_after: Option<u64>,
) -> Option<u64> {
    store.with_index(|store| {
        store.exact_prefix_cardinality_sum(
            data_generation,
            IndexKeyKind::User,
            index_id,
            component_prefixes,
            stop_after,
        )
    })
}

fn record_index_prefix_cardinality_terminal(
    entity_path: &'static str,
    base_row_local_instructions: u64,
) {
    record_rows_scanned_for_path(entity_path, 0);
    #[cfg(not(feature = "diagnostics"))]
    let _ = base_row_local_instructions;
    #[cfg(feature = "diagnostics")]
    super::terminal_attribution::record_index_prefix_cardinality_terminal_attribution(
        base_row_local_instructions,
    );
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

    // Phase 2: read or scan only the candidate-row cardinality needed by COUNT.
    let candidate_rows = match path {
        ExecutionPathPayload::FullScan => count_full_entity_candidate_rows(store, entity_tag, page),
        ExecutionPathPayload::KeyRange { start, end } => {
            let start_raw =
                DecodedDataStoreKey::try_from_structural_key(entity_tag, start)?.to_raw()?;
            let end_raw =
                DecodedDataStoreKey::try_from_structural_key(entity_tag, end)?.to_raw()?;

            count_data_range_candidate_rows(store, start_raw, end_raw, page)
        }
        _ => {
            return Err(InternalError::query_executor_invariant());
        }
    };

    // Phase 3: apply canonical COUNT window semantics and emit scan metrics.
    let count_window = CountWindowResult::from_candidate_rows(page, candidate_rows.available);

    Ok((count_window.count(), candidate_rows.scanned))
}

fn count_full_entity_candidate_rows(
    store: StoreHandle,
    entity_tag: EntityTag,
    page: Option<&PageSpec>,
) -> CountCandidateRows {
    if let Some(available) = store.with_data(|data| data.exact_entity_count(entity_tag)) {
        return CountCandidateRows::metadata(available);
    }

    store.with_data(|data| {
        let mut count = 0usize;
        let scan_limit = count_scan_limit(page);
        if scan_limit == Some(0) {
            return CountCandidateRows::scanned(0);
        }

        let _: Result<(), InternalError> = data.visit_entity(entity_tag, |_raw_key, _row| {
            count = count.saturating_add(1);
            Ok(count_store_visit(count, scan_limit))
        });

        CountCandidateRows::scanned(count)
    })
}

fn count_data_range_candidate_rows(
    store: StoreHandle,
    start_raw: RawDataStoreKey,
    end_raw: RawDataStoreKey,
    page: Option<&PageSpec>,
) -> CountCandidateRows {
    store.with_data(|data| {
        let mut count = 0usize;
        let scan_limit = count_scan_limit(page);
        if scan_limit == Some(0) {
            return CountCandidateRows::scanned(0);
        }

        let _: Result<(), InternalError> = data.visit_range(
            (Bound::Included(start_raw), Bound::Included(end_raw)),
            |_raw_key, _row| {
                count = count.saturating_add(1);
                Ok(count_store_visit(count, scan_limit))
            },
        );

        CountCandidateRows::scanned(count)
    })
}

fn count_scan_limit(page: Option<&PageSpec>) -> Option<usize> {
    CardinalityCandidateWindow::for_count(page).scan_limit()
}

fn count_store_visit(count: usize, scan_limit: Option<usize>) -> StoreVisit {
    if scan_limit.is_some_and(|limit| count >= limit) {
        StoreVisit::Stop
    } else {
        StoreVisit::Continue
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CountCandidateRows {
    available: usize,
    scanned: usize,
}

impl CountCandidateRows {
    fn metadata(available: u64) -> Self {
        Self {
            available: usize::try_from(available).unwrap_or(usize::MAX),
            scanned: 0,
        }
    }

    const fn scanned(count: usize) -> Self {
        Self {
            available: count,
            scanned: count,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CardinalityCandidateWindow {
    Empty,
    Bounded(u64),
    Unbounded,
}

impl CardinalityCandidateWindow {
    fn for_count(page: Option<&PageSpec>) -> Self {
        match page {
            Some(PageSpec { limit: Some(0), .. }) => Self::Empty,
            Some(page) => page.limit.map_or(Self::Unbounded, |limit| {
                Self::Bounded(page_window_candidate_rows(page, limit))
            }),
            None => Self::Unbounded,
        }
    }

    fn for_exists(page: Option<&PageSpec>) -> Self {
        match page {
            Some(PageSpec { limit: Some(0), .. }) => Self::Empty,
            Some(page) => Self::Bounded(page_window_candidate_rows(page, 1)),
            None => Self::Bounded(1),
        }
    }

    const fn is_empty(self) -> bool {
        matches!(self, Self::Empty)
    }

    const fn stop_after(self) -> Option<u64> {
        match self {
            Self::Empty => Some(0),
            Self::Bounded(rows) => Some(rows),
            Self::Unbounded => None,
        }
    }

    const fn bounded_rows(self) -> Option<u64> {
        match self {
            Self::Bounded(rows) => Some(rows),
            Self::Empty | Self::Unbounded => None,
        }
    }

    fn scan_limit(self) -> Option<usize> {
        self.stop_after()
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))
    }
}

fn page_window_candidate_rows(page: &PageSpec, rows_after_offset: u32) -> u64 {
    u64::from(page.offset).saturating_add(u64::from(rows_after_offset))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CountWindowResult {
    count: u32,
}

impl CountWindowResult {
    // Map one candidate cardinality and optional page contract to canonical
    // COUNT result and scan accounting semantics.
    fn from_candidate_rows(page: Option<&PageSpec>, available_rows: usize) -> Self {
        let Some(page) = page else {
            return Self::new(usize_to_u32_saturating(available_rows));
        };
        let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);

        match page.limit {
            Some(0) => Self::new(0),
            Some(limit) => {
                let limit = usize::try_from(limit).unwrap_or(usize::MAX);
                let count = available_rows.saturating_sub(offset).min(limit);

                Self::new(usize_to_u32_saturating(count))
            }
            None => {
                let count = available_rows.saturating_sub(offset);
                Self::new(usize_to_u32_saturating(count))
            }
        }
    }

    const fn new(count: u32) -> Self {
        Self { count }
    }

    const fn count(self) -> u32 {
        self.count
    }
}

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
