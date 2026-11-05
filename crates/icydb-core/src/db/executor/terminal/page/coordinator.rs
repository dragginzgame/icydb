//! Module: executor::terminal::page::coordinator
//! Responsibility: admit and account for bounded work in production scalar pages.
//! Does not own: cursor authentication, source-revision proofs, or public page DTOs.
//! Boundary: preflight scan/output units and retain their exact page-work receipts.

use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        index::IndexKey,
        resource::budget::{
            ExecutionBudgetUsage, RUNTIME_VALUE_NODE_OVERHEAD_BYTES,
            charge_current_execution_budget_ordered, current_execution_budget_usage,
            resource_index, runtime_value_work,
        },
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::cell::RefCell;

const RESOURCE_COUNT: usize = DiagnosticExecutionBudgetResource::ALL.len();
const PAGE_RESOURCE_NOT_OWNED: u64 = u64::MAX;
const MAX_SCALAR_ROW_NESTED_STEPS: u64 = 256;
// This reservation depends only on compile-time caps, not on the candidate.
// Overflow in a changed cap must fail compilation rather than each row pull.
const SCALAR_OUTPUT_ROW_RESERVATION: PageWork = PageWork::one(
    DiagnosticExecutionBudgetResource::NestedValueSteps,
    MAX_SCALAR_ROW_NESTED_STEPS,
)
.merge(PageWork::one(
    DiagnosticExecutionBudgetResource::MaterializedBytes,
    MAX_ROW_BYTES as u64 + MAX_SCALAR_ROW_NESTED_STEPS * RUNTIME_VALUE_NODE_OVERHEAD_BYTES,
));
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

    // The output owner preflights once, immediately before charging. No callback
    // or tracker mutation intervenes between admission and these ordered charges.
    fn charge_admitted(&mut self, work: PageWork) -> Result<(), InternalError> {
        charge_current_execution_budget_ordered(
            DiagnosticExecutionBudgetResource::ALL
                .into_iter()
                .map(|resource| (resource, work.amount(resource))),
        )?;
        self.observed = self.observed.merge(work);
        Ok(())
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
                self.tracker.charge_admitted(work)?;
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
    charge_current_execution_budget_ordered([
        (
            DiagnosticExecutionBudgetResource::NestedValueSteps,
            work.amount(DiagnosticExecutionBudgetResource::NestedValueSteps),
        ),
        (
            DiagnosticExecutionBudgetResource::MaterializedBytes,
            work.amount(DiagnosticExecutionBudgetResource::MaterializedBytes),
        ),
    ])
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
    ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        let mut active = active
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let Some(work) = active.as_mut() else {
            return Ok(true);
        };
        let reservation = scalar_candidate_inspection_reservation(access_entry_bound)?;
        let complete_unit_reservation = reservation.merge(SCALAR_OUTPUT_ROW_RESERVATION);
        let before = current_execution_budget_usage()?;
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
    ACTIVE_PRODUCTION_SCALAR_PAGE_WORK.with(|active| {
        let mut active = active
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let Some(work) = active.as_mut() else {
            return Ok(());
        };
        let after = current_execution_budget_usage()?;
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

    // Every resource occurs once: populate its demand directly instead of
    // composing whole resource vectors for these independent amounts.
    let mut work = PageWork::empty();
    for (resource, amount) in [
        (
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            access_entries,
        ),
        (DiagnosticExecutionBudgetResource::RowsVisited, 1),
        (
            DiagnosticExecutionBudgetResource::StoredBytesRead,
            access_and_row_bytes,
        ),
        (
            DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
            predicate_steps,
        ),
        (
            DiagnosticExecutionBudgetResource::NestedValueSteps,
            MAX_SCALAR_ROW_NESTED_STEPS,
        ),
        (
            DiagnosticExecutionBudgetResource::DecodedBytes,
            access_and_row_bytes,
        ),
        (
            DiagnosticExecutionBudgetResource::MaterializedBytes,
            row_bytes,
        ),
        (DiagnosticExecutionBudgetResource::CursorSteps, cursor_steps),
        (
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            index_bytes,
        ),
    ] {
        work.amounts[resource_index(resource)] = amount;
    }

    Ok(work)
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
mod tests;
