//! Module: executor::terminal::page::coordinator
//! Responsibility: admit and account for bounded work in production scalar pages.
//! Does not own: cursor authentication, source-revision proofs, or public page DTOs.
//! Boundary: preflight scan/output units and retain their exact page-work receipts.

use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        executor::budget::{
            ExecutionBudgetUsage, RUNTIME_VALUE_NODE_OVERHEAD_BYTES,
            charge_current_execution_budget, current_execution_budget_usage, resource_index,
            runtime_value_work,
        },
        index::IndexKey,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::cell::RefCell;

const RESOURCE_COUNT: usize = DiagnosticExecutionBudgetResource::ALL.len();
const PAGE_RESOURCE_NOT_OWNED: u64 = u64::MAX;
const MAX_SCALAR_ROW_NESTED_STEPS: u64 = 256;
// Stable protocol identities frozen from the original v1 operational
// envelopes. Limit tuning within either profile must not invalidate progress.
const TRUSTED_SCALAR_PAGE_PROFILE_IDENTITY: u64 = 0x1fcc_206d_e0a3_9aa5;
const PUBLIC_SCALAR_PAGE_PROFILE_IDENTITY: u64 = 0xdb4e_b896_81d8_92bd;
const DEFAULT_PAGE_LIMITS: [u64; RESOURCE_COUNT] = [
    PAGE_RESOURCE_NOT_OWNED, // query executions belong to the hard request/execution scope
    PAGE_RESOURCE_NOT_OWNED, // planning steps happen outside physical page traversal
    PAGE_RESOURCE_NOT_OWNED, // plan compilations happen outside physical page traversal
    10_000,                  // key/index entries visited
    10_000,                  // rows visited
    16 * 1_024 * 1_024,      // stored bytes read
    2_000_000,               // predicate/expression steps
    2_000_000,               // nested value steps
    16 * 1_024 * 1_024,      // decoded bytes
    16 * 1_024 * 1_024,      // materialized bytes
    10_000,                  // sort entries
    1_000_000,               // sort comparisons
    16 * 1_024 * 1_024,      // sort temporary bytes
    10_000,                  // group/distinct entries
    16 * 1_024 * 1_024,      // group/distinct state bytes
    100_000,                 // cursor and lookahead steps
    16 * 1_024 * 1_024,      // other temporary bytes
    1_024,                   // output rows per page
    8 * 1_024 * 1_024,       // output bytes per page
    PAGE_RESOURCE_NOT_OWNED, // instruction watermark remains a hard-budget concern
];

/// Finite successful-progress limits for one scalar page.
///
/// An absent limit means the resource is not page-owned and therefore cannot
/// be charged by a page unit. Every physical page resource has a finite limit
/// below the enclosing hard execution budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct PageWorkEnvelope {
    limits: [u64; RESOURCE_COUNT],
    identity: u64,
    profile_identity: u64,
}

impl PageWorkEnvelope {
    /// Return the maintained scalar-page envelope.
    #[must_use]
    pub(in crate::db) const fn default_scalar() -> Self {
        Self::new(DEFAULT_PAGE_LIMITS, TRUSTED_SCALAR_PAGE_PROFILE_IDENTITY)
    }

    /// Return the maintained public scalar-page envelope.
    #[must_use]
    pub(in crate::db) const fn public_scalar() -> Self {
        let mut limits = DEFAULT_PAGE_LIMITS;
        limits[resource_index(DiagnosticExecutionBudgetResource::ResultRows)] = 100;
        Self::new(limits, PUBLIC_SCALAR_PAGE_PROFILE_IDENTITY)
    }

    /// Return the exact identity reported by this page's work receipt.
    #[must_use]
    pub(in crate::db) const fn identity(self) -> u64 {
        self.identity
    }

    /// Return the stable execution-profile identity authenticated by cursors.
    ///
    /// Operational limits may change between calls within one profile without
    /// changing query results or invalidating monotonic progress.
    #[must_use]
    pub(in crate::db) const fn profile_identity(self) -> u64 {
        self.profile_identity
    }

    /// Return one resource limit when that resource belongs to page progress.
    #[must_use]
    pub(in crate::db) const fn limit(
        self,
        resource: DiagnosticExecutionBudgetResource,
    ) -> Option<u64> {
        let limit = self.limits[resource_index(resource)];
        if limit == PAGE_RESOURCE_NOT_OWNED {
            None
        } else {
            Some(limit)
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn with_limit_for_tests(
        mut self,
        resource: DiagnosticExecutionBudgetResource,
        limit: u64,
    ) -> Self {
        self.limits[resource_index(resource)] = limit;
        self.identity = page_envelope_identity(&self.limits);
        self
    }

    const fn new(limits: [u64; RESOURCE_COUNT], profile_identity: u64) -> Self {
        Self {
            identity: page_envelope_identity(&limits),
            limits,
            profile_identity,
        }
    }
}

/// Work known before one physical unit is consumed.
///
/// Routes must preflight the complete indivisible inspection or emission unit
/// before performing its charged work. This prevents an envelope stop from
/// repeatedly redoing half of one unit without advancing the physical anchor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct PageWork {
    amounts: [u64; RESOURCE_COUNT],
}

impl PageWork {
    /// Construct an empty physical-work demand.
    #[must_use]
    pub(in crate::db::executor) const fn empty() -> Self {
        Self {
            amounts: [0; RESOURCE_COUNT],
        }
    }

    /// Construct one named resource demand.
    #[must_use]
    pub(in crate::db::executor) const fn one(
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Self {
        let mut work = Self::empty();
        work.amounts[resource_index(resource)] = amount;
        work
    }

    /// Saturating composition for work known to belong to one atomic unit.
    #[must_use]
    pub(in crate::db::executor) const fn merge(mut self, other: Self) -> Self {
        let mut index = 0;
        while index < RESOURCE_COUNT {
            self.amounts[index] = self.amounts[index].saturating_add(other.amounts[index]);
            index += 1;
        }
        self
    }

    #[must_use]
    pub(in crate::db::executor) const fn amount(
        self,
        resource: DiagnosticExecutionBudgetResource,
    ) -> u64 {
        self.amounts[resource_index(resource)]
    }

    fn from_execution_delta(
        before: ExecutionBudgetUsage,
        after: ExecutionBudgetUsage,
    ) -> Result<Self, InternalError> {
        let mut work = Self::empty();
        for resource in DiagnosticExecutionBudgetResource::ALL {
            let before = before.observed(resource);
            let after = after.observed(resource);
            let Some(delta) = after.checked_sub(before) else {
                return Err(InternalError::query_executor_invariant());
            };
            work.amounts[resource_index(resource)] = delta;
        }

        Ok(work)
    }
}

/// Work actually admitted for one successful scalar page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct PageWorkReceipt {
    envelope_identity: u64,
    observed: PageWork,
}

impl PageWorkReceipt {
    /// Return admitted work for one page resource.
    #[must_use]
    pub(in crate::db) const fn observed(self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.observed.amount(resource)
    }
}

enum PageWorkAdmission {
    Admitted,
    EnvelopeFull,
}

struct PageWorkTracker {
    envelope: PageWorkEnvelope,
    observed: PageWork,
}

impl PageWorkTracker {
    const fn new(envelope: PageWorkEnvelope) -> Self {
        Self {
            envelope,
            observed: PageWork::empty(),
        }
    }

    fn from_receipt(
        envelope: PageWorkEnvelope,
        receipt: Option<PageWorkReceipt>,
    ) -> Result<Self, InternalError> {
        let observed = match receipt {
            Some(receipt) if receipt.envelope_identity == envelope.identity() => receipt.observed,
            Some(_) => return Err(InternalError::query_executor_invariant()),
            None => PageWork::empty(),
        };
        for resource in DiagnosticExecutionBudgetResource::ALL {
            if let Some(limit) = envelope.limit(resource)
                && observed.amount(resource) > limit
            {
                return Err(InternalError::query_executor_invariant());
            }
        }

        Ok(Self { envelope, observed })
    }

    fn admit(&mut self, work: PageWork) -> Result<PageWorkAdmission, InternalError> {
        let mut resource_index_in_all = 0;
        while resource_index_in_all < RESOURCE_COUNT {
            let resource = DiagnosticExecutionBudgetResource::ALL[resource_index_in_all];
            let amount = work.amount(resource);
            if amount != 0 {
                let Some(limit) = self.envelope.limit(resource) else {
                    return Err(InternalError::query_executor_invariant());
                };
                if amount > limit {
                    return Err(InternalError::page_unit_too_large(resource, limit, amount));
                }
                if self.observed.amount(resource).saturating_add(amount) > limit {
                    return Ok(PageWorkAdmission::EnvelopeFull);
                }
            }
            resource_index_in_all += 1;
        }

        for resource in DiagnosticExecutionBudgetResource::ALL {
            charge_current_execution_budget(resource, work.amount(resource))?;
        }
        self.observed = self.observed.merge(work);
        Ok(PageWorkAdmission::Admitted)
    }

    fn preflight(&self, work: PageWork) -> Result<PageWorkAdmission, InternalError> {
        let mut resource_index_in_all = 0;
        while resource_index_in_all < RESOURCE_COUNT {
            let resource = DiagnosticExecutionBudgetResource::ALL[resource_index_in_all];
            let amount = work.amount(resource);
            if amount != 0 {
                let Some(limit) = self.envelope.limit(resource) else {
                    return Err(InternalError::query_executor_invariant());
                };
                if amount > limit {
                    return Err(InternalError::page_unit_too_large(resource, limit, amount));
                }
                if self.observed.amount(resource).saturating_add(amount) > limit {
                    return Ok(PageWorkAdmission::EnvelopeFull);
                }
            }
            resource_index_in_all += 1;
        }

        Ok(PageWorkAdmission::Admitted)
    }

    fn commit_observed(
        &mut self,
        reservation: PageWork,
        observed: PageWork,
    ) -> Result<(), InternalError> {
        let mut page_observed = PageWork::empty();
        for resource in DiagnosticExecutionBudgetResource::ALL {
            let amount = observed.amount(resource);
            if self.envelope.limit(resource).is_none() {
                continue;
            }
            if amount > reservation.amount(resource) {
                return Err(InternalError::query_executor_invariant());
            }
            page_observed.amounts[resource_index(resource)] = amount;
        }
        self.observed = self.observed.merge(page_observed);

        Ok(())
    }

    const fn receipt(&self) -> PageWorkReceipt {
        PageWorkReceipt {
            envelope_identity: self.envelope.identity(),
            observed: self.observed,
        }
    }
}

struct ProductionScalarPageUnit {
    reservation: PageWork,
    before: ExecutionBudgetUsage,
}

struct ProductionScalarPageWork {
    tracker: PageWorkTracker,
    unit: Option<ProductionScalarPageUnit>,
    envelope_stopped: bool,
}

/// Page-local output owner resumed from the physical scan receipt.
///
/// Projection invokes this once per completed output row. Each row is one
/// bounded framed unit: its exact nested/value backing is measured before it
/// is admitted, and the first row that cannot fit remains unreturned.
pub(in crate::db::executor) struct ProductionScalarOutputWork {
    tracker: PageWorkTracker,
    envelope_stopped: bool,
}

impl ProductionScalarOutputWork {
    /// Continue one page envelope from its optional physical-scan receipt.
    pub(in crate::db::executor) fn new(
        envelope: PageWorkEnvelope,
        scan_receipt: Option<PageWorkReceipt>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            tracker: PageWorkTracker::from_receipt(envelope, scan_receipt)?,
            envelope_stopped: false,
        })
    }

    /// Admit one complete projected output row before it crosses the executor
    /// response boundary. `false` means successful page exhaustion; an
    /// individually oversized row remains a typed terminal failure.
    pub(in crate::db::executor) fn admit_row(
        &mut self,
        row: &[Value],
    ) -> Result<bool, InternalError> {
        if self.envelope_stopped {
            return Ok(false);
        }

        let work = scalar_output_row_work(row);
        match self.tracker.preflight(work) {
            Ok(PageWorkAdmission::Admitted) => {
                if !matches!(self.tracker.admit(work)?, PageWorkAdmission::Admitted) {
                    return Err(InternalError::query_executor_invariant());
                }
                Ok(true)
            }
            Ok(PageWorkAdmission::EnvelopeFull) => {
                charge_discarded_output_materialization(work)?;
                self.envelope_stopped = true;
                Ok(false)
            }
            Err(error) => {
                charge_discarded_output_materialization(work)?;
                Err(error)
            }
        }
    }

    /// Return whether output, rather than physical scanning, filled the page.
    #[must_use]
    pub(in crate::db::executor) const fn envelope_stopped(&self) -> bool {
        self.envelope_stopped
    }

    /// Return the combined physical-scan and output-emission receipt.
    #[must_use]
    pub(in crate::db::executor) const fn receipt(&self) -> PageWorkReceipt {
        self.tracker.receipt()
    }
}

fn scalar_output_row_work(row: &[Value]) -> PageWork {
    let (bytes, nested_steps) = row.iter().fold((0_u64, 0_u64), |total, value| {
        let work = runtime_value_work(value);
        (
            total.0.saturating_add(work.0),
            total.1.saturating_add(work.1),
        )
    });

    PageWork::one(DiagnosticExecutionBudgetResource::ResultRows, 1)
        .merge(PageWork::one(
            DiagnosticExecutionBudgetResource::NestedValueSteps,
            nested_steps,
        ))
        .merge(PageWork::one(
            DiagnosticExecutionBudgetResource::MaterializedBytes,
            bytes,
        ))
        .merge(PageWork::one(
            DiagnosticExecutionBudgetResource::ResultBytes,
            bytes,
        ))
}

// Projection has already completed one bounded row when exact output size is
// known. If that row cannot enter this page, retain its completed transient
// materialization cost in the hard request budget without claiming output.
fn charge_discarded_output_materialization(work: PageWork) -> Result<(), InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        work.amount(DiagnosticExecutionBudgetResource::NestedValueSteps),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::MaterializedBytes,
        work.amount(DiagnosticExecutionBudgetResource::MaterializedBytes),
    )
}

impl ProductionScalarPageWork {
    const fn new(envelope: PageWorkEnvelope) -> Self {
        Self {
            tracker: PageWorkTracker::new(envelope),
            unit: None,
            envelope_stopped: false,
        }
    }
}

std::thread_local! {
    static ACTIVE_PRODUCTION_SCALAR_PAGE_WORK: RefCell<Option<ProductionScalarPageWork>> =
        const { RefCell::new(None) };
}

/// Result of one production scalar scan governed by a page-local envelope.
pub(in crate::db::executor) struct ProductionScalarPageWorkResult<T> {
    pub(in crate::db::executor) value: T,
    pub(in crate::db::executor) envelope_stopped: bool,
    pub(in crate::db::executor) receipt: PageWorkReceipt,
}

/// Install one synchronous page-work owner around the production scalar scan.
///
/// Nested owners are forbidden: one cursor page has exactly one tracker, and
/// every physical candidate beneath it contributes to that same page-local
/// receipt while the request's hard tracker remains cumulative.
pub(in crate::db::executor) fn with_production_scalar_page_work<T>(
    envelope: PageWorkEnvelope,
    run: impl FnOnce() -> Result<T, InternalError>,
) -> Result<ProductionScalarPageWorkResult<T>, InternalError> {
    ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        let mut active = active
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        if active.is_some() {
            return Err(InternalError::query_executor_invariant());
        }
        *active = Some(ProductionScalarPageWork::new(envelope));
        Ok(())
    })?;

    let result = run();
    let work = ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        active
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?
            .take()
            .ok_or_else(InternalError::query_executor_invariant)
    })?;
    let value = result?;
    if work.unit.is_some() {
        return Err(InternalError::query_executor_invariant());
    }

    Ok(ProductionScalarPageWorkResult {
        value,
        envelope_stopped: work.envelope_stopped,
        receipt: work.tracker.receipt(),
    })
}

/// Return whether the current scalar execution requires a finite physical
/// access bound before its next candidate may be consumed.
pub(in crate::db::executor) fn production_scalar_page_work_is_active() -> Result<bool, InternalError>
{
    ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        active
            .try_borrow()
            .map_err(|_| InternalError::query_executor_invariant())
            .map(|active| active.is_some())
    })
}

/// Return the finite key/index-entry ceiling owned by the active scalar page.
///
/// Physical leaves use this immutable ceiling when they are constructed so an
/// otherwise valid small page never inherits a larger default refill unit.
pub(in crate::db::executor) fn production_scalar_page_access_entry_limit()
-> Result<Option<usize>, InternalError> {
    ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        let active = active
            .try_borrow()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let Some(work) = active.as_ref() else {
            return Ok(None);
        };
        let limit = work
            .tracker
            .envelope
            .limit(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited)
            .ok_or_else(InternalError::query_executor_invariant)?;

        Ok(Some(usize::try_from(limit).unwrap_or(usize::MAX)))
    })
}

/// Preflight one complete scalar candidate inspection before the key stream
/// or row store is touched. `access_entry_bound` is route-owned and must bound
/// every physical access entry one `next_key` call can visit.
pub(in crate::db::executor) fn begin_production_scalar_page_unit(
    access_entry_bound: usize,
) -> Result<bool, InternalError> {
    let active = ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        active
            .try_borrow()
            .map_err(|_| InternalError::query_executor_invariant())
            .map(|active| active.is_some())
    })?;
    if !active {
        return Ok(true);
    }

    let reservation = scalar_candidate_inspection_reservation(access_entry_bound)?;
    let complete_unit_reservation = reservation.merge(scalar_output_row_reservation()?);
    let before = current_execution_budget_usage()?;
    ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        let mut active = active
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let work = active
            .as_mut()
            .ok_or_else(InternalError::query_executor_invariant)?;
        if work.unit.is_some() {
            return Err(InternalError::query_executor_invariant());
        }
        match work.tracker.preflight(complete_unit_reservation)? {
            PageWorkAdmission::Admitted => {
                work.unit = Some(ProductionScalarPageUnit {
                    reservation,
                    before,
                });
                Ok(true)
            }
            PageWorkAdmission::EnvelopeFull => {
                work.envelope_stopped = true;
                Ok(false)
            }
        }
    })
}

/// Commit the exact hard-budget delta for one successfully completed scalar
/// candidate. Unused conservative reservation is released immediately.
pub(in crate::db::executor) fn finish_production_scalar_page_unit() -> Result<(), InternalError> {
    let active = ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        active
            .try_borrow()
            .map_err(|_| InternalError::query_executor_invariant())
            .map(|active| active.is_some())
    })?;
    if !active {
        return Ok(());
    }

    let after = current_execution_budget_usage()?;
    ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        let mut active = active
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let work = active
            .as_mut()
            .ok_or_else(InternalError::query_executor_invariant)?;
        let unit = work
            .unit
            .take()
            .ok_or_else(InternalError::query_executor_invariant)?;
        let observed = PageWork::from_execution_delta(unit.before, after)?;
        work.tracker.commit_observed(unit.reservation, observed)
    })
}

fn scalar_candidate_inspection_reservation(
    access_entry_bound: usize,
) -> Result<PageWork, InternalError> {
    let access_entries = u64::try_from(access_entry_bound).unwrap_or(u64::MAX);
    let index_bytes = IndexKey::MAX_STORED_SIZE_BYTES
        .checked_mul(access_entries)
        .ok_or_else(InternalError::query_executor_invariant)?;
    let row_bytes = u64::from(MAX_ROW_BYTES);
    let access_and_row_bytes = index_bytes
        .checked_add(row_bytes)
        .ok_or_else(InternalError::query_executor_invariant)?;
    let predicate_steps = access_entries
        .checked_add(1)
        .ok_or_else(InternalError::query_executor_invariant)?;
    let cursor_steps = access_entries
        .checked_mul(3)
        .and_then(|steps| steps.checked_add(1))
        .ok_or_else(InternalError::query_executor_invariant)?;

    Ok(PageWork::one(
        DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        access_entries,
    )
    .merge(PageWork::one(
        DiagnosticExecutionBudgetResource::RowsVisited,
        1,
    ))
    .merge(PageWork::one(
        DiagnosticExecutionBudgetResource::StoredBytesRead,
        access_and_row_bytes,
    ))
    .merge(PageWork::one(
        DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        predicate_steps,
    ))
    .merge(PageWork::one(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        MAX_SCALAR_ROW_NESTED_STEPS,
    ))
    .merge(PageWork::one(
        DiagnosticExecutionBudgetResource::DecodedBytes,
        access_and_row_bytes,
    ))
    .merge(PageWork::one(
        DiagnosticExecutionBudgetResource::MaterializedBytes,
        row_bytes,
    ))
    .merge(PageWork::one(
        DiagnosticExecutionBudgetResource::CursorSteps,
        cursor_steps,
    ))
    .merge(PageWork::one(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        index_bytes,
    )))
}

fn scalar_output_row_reservation() -> Result<PageWork, InternalError> {
    let maximum_output_bytes = u64::from(MAX_ROW_BYTES)
        .checked_add(
            MAX_SCALAR_ROW_NESTED_STEPS
                .checked_mul(RUNTIME_VALUE_NODE_OVERHEAD_BYTES)
                .ok_or_else(InternalError::query_executor_invariant)?,
        )
        .ok_or_else(InternalError::query_executor_invariant)?;

    Ok(PageWork::one(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        MAX_SCALAR_ROW_NESTED_STEPS,
    )
    .merge(PageWork::one(
        DiagnosticExecutionBudgetResource::MaterializedBytes,
        maximum_output_bytes,
    )))
}

const fn page_envelope_identity(limits: &[u64; RESOURCE_COUNT]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    let mut index = 0;
    while index < RESOURCE_COUNT {
        let encoded = if limits[index] == PAGE_RESOURCE_NOT_OWNED {
            0
        } else {
            limits[index].rotate_left(1) | 1
        };
        hash ^= encoded ^ (index as u64).rotate_left(32);
        hash = hash.wrapping_mul(FNV_PRIME);
        index += 1;
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        QueryError,
        executor::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            read_hard_budget_limit_for_tests, with_query_execution_budget_for_tests,
        },
    };
    use icydb_diagnostic_code::{
        DiagnosticDetail, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
        RuntimeBoundaryCode,
    };

    const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);
    const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0x2210_0007_0000_0001,
    );

    #[test]
    fn production_units_reserve_one_output_row_before_cumulative_row_bytes_cross_the_envelope() {
        const OBSERVED_ROW_BYTES: u64 = 3 * 1_024 * 1_024;
        let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
        let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
            with_production_scalar_page_work(PageWorkEnvelope::default_scalar(), || {
                let mut completed = 0_u64;
                while begin_production_scalar_page_unit(1)? {
                    charge_test_production_candidate(OBSERVED_ROW_BYTES)?;
                    finish_production_scalar_page_unit()?;
                    completed = completed.saturating_add(1);
                }
                Ok(completed)
            })
            .map_err(QueryError::execute)
        })
        .expect("the page envelope should stop successfully");

        assert_eq!(result.value, 3);
        assert!(result.envelope_stopped);
        assert_eq!(
            result
                .receipt
                .observed(DiagnosticExecutionBudgetResource::StoredBytesRead),
            3 * OBSERVED_ROW_BYTES,
        );
        assert_eq!(
            result
                .receipt
                .observed(DiagnosticExecutionBudgetResource::PredicateExpressionSteps),
            3,
        );
    }

    #[test]
    fn active_page_publishes_its_finite_access_entry_limit() {
        assert_eq!(
            production_scalar_page_access_entry_limit()
                .expect("inactive page authority should be readable"),
            None,
        );
        let envelope = PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 4);
        let result = with_production_scalar_page_work(envelope, || {
            production_scalar_page_access_entry_limit()
        })
        .expect("active page authority should publish its entry limit");

        assert_eq!(result.value, Some(4));
        assert!(!result.envelope_stopped);
    }

    #[test]
    fn production_units_stop_before_cumulative_expression_work_can_cross_the_envelope() {
        let envelope = PageWorkEnvelope::default_scalar().with_limit_for_tests(
            DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
            5,
        );
        let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
        let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
            with_production_scalar_page_work(envelope, || {
                let mut completed = 0_u64;
                while begin_production_scalar_page_unit(1)? {
                    charge_test_production_candidate(1)?;
                    finish_production_scalar_page_unit()?;
                    completed = completed.saturating_add(1);
                }
                Ok(completed)
            })
            .map_err(QueryError::execute)
        })
        .expect("expression work should stop as successful page progress");

        assert_eq!(result.value, 4);
        assert!(result.envelope_stopped);
        assert_eq!(
            result
                .receipt
                .observed(DiagnosticExecutionBudgetResource::PredicateExpressionSteps),
            4,
        );
    }

    #[test]
    fn production_unit_hard_failure_returns_no_successful_page() {
        const OBSERVED_ROW_BYTES: u64 = 3 * 1_024 * 1_024;
        let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(
                DiagnosticExecutionBudgetResource::StoredBytesRead,
                2 * 1_024 * 1_024,
            );
        let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
            with_production_scalar_page_work(PageWorkEnvelope::default_scalar(), || {
                if !begin_production_scalar_page_unit(1)? {
                    return Err(InternalError::query_executor_invariant());
                }
                charge_test_production_candidate(OBSERVED_ROW_BYTES)?;
                finish_production_scalar_page_unit()
            })
            .map_err(QueryError::execute)
        });
        let Err(error) = result else {
            panic!("hard exhaustion must discard page progress")
        };

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
    }

    #[test]
    fn production_unit_rejects_a_route_pull_larger_than_an_empty_page() {
        let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
        let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
            with_production_scalar_page_work(PageWorkEnvelope::default_scalar(), || {
                let _ = begin_production_scalar_page_unit(1_000)?;
                Ok(())
            })
            .map_err(QueryError::execute)
        });
        let Err(error) = result else {
            panic!("an unfit route pull must fail before storage work")
        };

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::PageUnitTooLarge,
            })
        ));
    }

    #[test]
    fn production_output_stops_before_crossing_result_bytes_and_charges_discarded_work() {
        let envelope = PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultBytes, 50);
        let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
        let (receipt, usage) =
            with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
                (|| {
                    let mut output = ProductionScalarOutputWork::new(envelope, None)?;
                    assert!(output.admit_row(&[Value::Text("a".to_string())])?);
                    assert!(!output.admit_row(&[Value::Text("b".to_string())])?);
                    assert!(output.envelope_stopped());
                    Ok::<_, InternalError>((output.receipt(), current_execution_budget_usage()?))
                })()
                .map_err(QueryError::execute)
            })
            .expect("output bytes should end the page without a hard failure");

        assert_eq!(
            receipt.observed(DiagnosticExecutionBudgetResource::ResultRows),
            1,
        );
        assert_eq!(
            receipt.observed(DiagnosticExecutionBudgetResource::ResultBytes),
            33,
        );
        assert_eq!(
            usage.observed(DiagnosticExecutionBudgetResource::ResultBytes),
            33,
            "the unreturned row must not be claimed as result output",
        );
        assert_eq!(
            usage.observed(DiagnosticExecutionBudgetResource::MaterializedBytes),
            66,
            "both the returned row and bounded discarded row were materialized",
        );
    }

    #[test]
    fn production_output_continues_the_same_envelope_as_candidate_scanning() {
        const OBSERVED_ROW_BYTES: u64 = 3 * 1_024 * 1_024;
        let envelope = PageWorkEnvelope::default_scalar();
        let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
        let receipt = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
            (|| {
                let scan = with_production_scalar_page_work(envelope, || {
                    let mut completed = 0_u64;
                    while begin_production_scalar_page_unit(1)? {
                        charge_test_production_candidate(OBSERVED_ROW_BYTES)?;
                        finish_production_scalar_page_unit()?;
                        completed = completed.saturating_add(1);
                    }
                    Ok(completed)
                })?;
                assert_eq!(scan.value, 3);

                let mut output = ProductionScalarOutputWork::new(envelope, Some(scan.receipt))?;
                assert!(output.admit_row(&[Value::Blob(vec![0; MAX_ROW_BYTES as usize])])?);
                Ok::<_, InternalError>(output.receipt())
            })()
            .map_err(QueryError::execute)
        })
        .expect("scan and output should share one cumulative page envelope");

        assert_eq!(
            receipt.observed(DiagnosticExecutionBudgetResource::MaterializedBytes),
            3 * OBSERVED_ROW_BYTES + u64::from(MAX_ROW_BYTES) + RUNTIME_VALUE_NODE_OVERHEAD_BYTES,
        );
        assert_eq!(
            receipt.observed(DiagnosticExecutionBudgetResource::ResultRows),
            1,
        );
    }

    #[test]
    fn production_output_rejects_one_row_that_cannot_fit_an_empty_page() {
        let envelope = PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultBytes, 32);
        let hard_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
        let result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
            (|| {
                let mut output = ProductionScalarOutputWork::new(envelope, None)?;
                output.admit_row(&[Value::Text("a".to_string())])
            })()
            .map_err(QueryError::execute)
        });
        let Err(error) = result else {
            panic!("one oversized output row must fail instead of returning a stalled page")
        };

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::PageUnitTooLarge,
            })
        ));
    }

    #[test]
    fn default_page_limits_are_strictly_inside_the_read_hard_budget() {
        let envelope = PageWorkEnvelope::default_scalar();
        for resource in DiagnosticExecutionBudgetResource::ALL {
            if let Some(page_limit) = envelope.limit(resource) {
                assert!(
                    page_limit < read_hard_budget_limit_for_tests(resource),
                    "page resource {resource:?} must preserve hard-budget headroom",
                );
            }
        }
        assert_eq!(
            PageWorkEnvelope::default_scalar().identity(),
            PageWorkEnvelope::default_scalar().identity(),
        );
        assert_ne!(
            envelope.identity(),
            envelope
                .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultRows, 7)
                .identity(),
        );
    }

    fn charge_test_production_candidate(row_bytes: u64) -> Result<(), InternalError> {
        for (resource, amount) in [
            (DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 1),
            (DiagnosticExecutionBudgetResource::RowsVisited, 1),
            (
                DiagnosticExecutionBudgetResource::StoredBytesRead,
                row_bytes,
            ),
            (
                DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
                1,
            ),
            (DiagnosticExecutionBudgetResource::NestedValueSteps, 1),
            (DiagnosticExecutionBudgetResource::DecodedBytes, row_bytes),
            (
                DiagnosticExecutionBudgetResource::MaterializedBytes,
                row_bytes,
            ),
            (DiagnosticExecutionBudgetResource::CursorSteps, 1),
        ] {
            charge_current_execution_budget(resource, amount)?;
        }

        Ok(())
    }
}
