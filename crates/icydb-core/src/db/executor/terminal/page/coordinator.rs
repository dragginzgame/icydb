//! Module: executor::terminal::page::coordinator
//! Responsibility: bounded scalar-page progress independent of hard execution failure.
//! Does not own: cursor authentication, source-revision proofs, or public page DTOs.
//! Boundary: consumes preflighted physical units and returns rows plus exact internal progress.

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
const TRUSTED_SCALAR_PAGE_PROFILE_IDENTITY: u64 = 0x0481_7ce8_139a_63f6;
const PUBLIC_SCALAR_PAGE_PROFILE_IDENTITY: u64 = 0x9dbc_64c1_e664_c15e;
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
    100_000,                 // bounded diagnostics aggregation
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

    #[must_use]
    const fn with_one_result_row(mut self) -> Self {
        self.amounts[resource_index(DiagnosticExecutionBudgetResource::ResultRows)] = 1;
        self
    }
}

/// Immutable query-window identity carried by internal scalar progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarPageWindow {
    initial_offset: u64,
    total_limit: Option<u64>,
}

impl ScalarPageWindow {
    /// Construct the complete traversal window. `total_limit` is not a
    /// per-page limit and the initial offset is consumed only once.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        initial_offset: u64,
        total_limit: Option<u64>,
    ) -> Self {
        Self {
            initial_offset,
            total_limit,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarPageContract {
    envelope_profile_identity: u64,
    window: ScalarPageWindow,
}

impl ScalarPageContract {
    const fn new(envelope: PageWorkEnvelope, window: ScalarPageWindow) -> Self {
        Self {
            envelope_profile_identity: envelope.profile_identity(),
            window,
        }
    }

    /// Return the immutable execution-profile identity for cursor binding.
    #[must_use]
    pub(in crate::db::executor) const fn envelope_profile_identity(self) -> u64 {
        self.envelope_profile_identity
    }

    /// Return the immutable total traversal window.
    #[must_use]
    pub(in crate::db::executor) const fn window(self) -> ScalarPageWindow {
        self.window
    }
}

/// Internal continuation state before Patch 8 authenticates and encodes it.
///
/// Logical and physical boundaries stay separate. An unconsumed lookahead key
/// is never also installed as the consumed physical anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarPageProgress<LogicalBoundary, PhysicalAnchor> {
    contract: ScalarPageContract,
    last_emitted_logical: Option<LogicalBoundary>,
    last_consumed_physical: Option<PhysicalAnchor>,
    unconsumed_lookahead: Option<PhysicalAnchor>,
    matching_rows_skipped: u64,
    rows_emitted: u64,
}

impl<LogicalBoundary, PhysicalAnchor> ScalarPageProgress<LogicalBoundary, PhysicalAnchor> {
    const fn initial(contract: ScalarPageContract) -> Self {
        Self {
            contract,
            last_emitted_logical: None,
            last_consumed_physical: None,
            unconsumed_lookahead: None,
            matching_rows_skipped: 0,
            rows_emitted: 0,
        }
    }

    fn consume_filtered(&mut self, physical: PhysicalAnchor) {
        self.last_consumed_physical = Some(physical);
        self.unconsumed_lookahead = None;
    }

    fn consume_skipped_match(&mut self, physical: PhysicalAnchor) {
        self.consume_filtered(physical);
        self.matching_rows_skipped = self.matching_rows_skipped.saturating_add(1);
    }

    fn consume_emitted_match(&mut self, logical: LogicalBoundary, physical: PhysicalAnchor) {
        self.last_emitted_logical = Some(logical);
        self.last_consumed_physical = Some(physical);
        self.unconsumed_lookahead = None;
        self.rows_emitted = self.rows_emitted.saturating_add(1);
    }

    /// Borrow the last logical boundary actually emitted to the caller.
    #[must_use]
    pub(in crate::db::executor) const fn last_emitted_logical(&self) -> Option<&LogicalBoundary> {
        self.last_emitted_logical.as_ref()
    }

    /// Borrow the last physical entry fully consumed by traversal.
    #[must_use]
    pub(in crate::db::executor) const fn last_consumed_physical(&self) -> Option<&PhysicalAnchor> {
        self.last_consumed_physical.as_ref()
    }

    /// Borrow a matching lookahead entry that must be re-read on resume.
    #[must_use]
    pub(in crate::db::executor) const fn unconsumed_lookahead(&self) -> Option<&PhysicalAnchor> {
        self.unconsumed_lookahead.as_ref()
    }

    /// Return the number of matching rows consumed by the initial offset.
    #[must_use]
    pub(in crate::db::executor) const fn matching_rows_skipped(&self) -> u64 {
        self.matching_rows_skipped
    }

    /// Return the number of rows emitted against the total query limit.
    #[must_use]
    pub(in crate::db::executor) const fn rows_emitted(&self) -> u64 {
        self.rows_emitted
    }

    /// Return the immutable contract carried across pages.
    #[must_use]
    pub(in crate::db::executor) const fn contract(&self) -> ScalarPageContract {
        self.contract
    }
}

/// One preflightable physical traversal unit.
pub(in crate::db::executor) struct ScalarPageUnit<Row, LogicalBoundary, PhysicalAnchor> {
    physical: PhysicalAnchor,
    inspection_work: PageWork,
    outcome: ScalarPageUnitOutcome<Row, LogicalBoundary>,
}

enum ScalarPageUnitOutcome<Row, LogicalBoundary> {
    Filtered,
    Matching {
        row: Row,
        logical: LogicalBoundary,
        emission_work: PageWork,
    },
}

impl<Row, LogicalBoundary, PhysicalAnchor> ScalarPageUnit<Row, LogicalBoundary, PhysicalAnchor> {
    /// Construct one examined physical entry rejected before output.
    #[must_use]
    pub(in crate::db::executor) const fn filtered(
        physical: PhysicalAnchor,
        inspection_work: PageWork,
    ) -> Self {
        Self {
            physical,
            inspection_work,
            outcome: ScalarPageUnitOutcome::Filtered,
        }
    }

    /// Construct one matching entry with separate inspection and emission work.
    /// Result-row cardinality is coordinator-owned and need not be supplied in
    /// `emission_work`.
    #[must_use]
    pub(in crate::db::executor) const fn matching(
        physical: PhysicalAnchor,
        inspection_work: PageWork,
        row: Row,
        logical: LogicalBoundary,
        emission_work: PageWork,
    ) -> Self {
        Self {
            physical,
            inspection_work,
            outcome: ScalarPageUnitOutcome::Matching {
                row,
                logical,
                emission_work,
            },
        }
    }
}

/// Work actually admitted for one successful scalar page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct PageWorkReceipt {
    envelope_identity: u64,
    observed: PageWork,
}

impl PageWorkReceipt {
    /// Return the envelope identity under which this work was admitted.
    #[must_use]
    pub(in crate::db) const fn envelope_identity(self) -> u64 {
        self.envelope_identity
    }

    /// Return admitted work for one page resource.
    #[must_use]
    pub(in crate::db) const fn observed(self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.observed.amount(resource)
    }
}

/// Internal bounded scalar-page result. A non-null continuation means only
/// that physical exhaustion was not proved.
pub(in crate::db::executor) struct BoundedScalarPage<Row, LogicalBoundary, PhysicalAnchor> {
    rows: Vec<Row>,
    continuation: Option<ScalarPageProgress<LogicalBoundary, PhysicalAnchor>>,
    work: PageWorkReceipt,
}

impl<Row, LogicalBoundary, PhysicalAnchor> BoundedScalarPage<Row, LogicalBoundary, PhysicalAnchor> {
    /// Split the internal page into rows, optional progress, and charged work.
    pub(in crate::db::executor) fn into_parts(
        self,
    ) -> (
        Vec<Row>,
        Option<ScalarPageProgress<LogicalBoundary, PhysicalAnchor>>,
        PageWorkReceipt,
    ) {
        (self.rows, self.continuation, self.work)
    }
}

/// A resume request changed an immutable page/query contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarPageContractMismatch {
    expected: ScalarPageContract,
    actual: ScalarPageContract,
}

impl ScalarPageContractMismatch {
    /// Return the immutable contract carried by the supplied progress.
    #[must_use]
    pub(in crate::db::executor) const fn expected(self) -> ScalarPageContract {
        self.expected
    }

    /// Return the immutable contract requested for the resumed execution.
    #[must_use]
    pub(in crate::db::executor) const fn actual(self) -> ScalarPageContract {
        self.actual
    }
}

/// Internal coordinator failure. Hard-budget and oversize failures retain
/// their normal typed `InternalError`; progress-identity mismatch remains a
/// distinct input for Patch 8 cursor diagnostics.
#[derive(Debug)]
pub(in crate::db::executor) enum ScalarPageCoordinatorError {
    Execution(InternalError),
    ContractMismatch(ScalarPageContractMismatch),
}

impl From<InternalError> for ScalarPageCoordinatorError {
    fn from(error: InternalError) -> Self {
        Self::Execution(error)
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

    const fn output_window_full(&self) -> bool {
        let resource = DiagnosticExecutionBudgetResource::ResultRows;
        match self.envelope.limit(resource) {
            Some(limit) => self.observed.amount(resource) >= limit,
            None => false,
        }
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

/// Coordinate one bounded scalar page over route-owned physical units.
///
/// Unit work is envelope-admitted before it is charged to the enclosing hard
/// budget. Envelope exhaustion is successful resumable progress; hard-budget
/// exhaustion returns an error and discards the partial page.
pub(in crate::db::executor) fn coordinate_scalar_page<Row, LogicalBoundary, PhysicalAnchor, Units>(
    envelope: PageWorkEnvelope,
    window: ScalarPageWindow,
    resume: Option<ScalarPageProgress<LogicalBoundary, PhysicalAnchor>>,
    units: Units,
) -> Result<BoundedScalarPage<Row, LogicalBoundary, PhysicalAnchor>, ScalarPageCoordinatorError>
where
    Units: IntoIterator<Item = ScalarPageUnit<Row, LogicalBoundary, PhysicalAnchor>>,
{
    let contract = ScalarPageContract::new(envelope, window);
    let mut progress = match resume {
        Some(progress) if progress.contract == contract => progress,
        Some(progress) => {
            return Err(ScalarPageCoordinatorError::ContractMismatch(
                ScalarPageContractMismatch {
                    expected: progress.contract,
                    actual: contract,
                },
            ));
        }
        None => ScalarPageProgress::initial(contract),
    };
    let mut tracker = PageWorkTracker::new(envelope);
    let mut rows = Vec::new();

    if total_limit_reached(window, &progress) {
        return Ok(completed_page(rows, tracker));
    }

    for unit in units {
        let ScalarPageUnit {
            physical,
            inspection_work,
            outcome,
        } = unit;
        match outcome {
            ScalarPageUnitOutcome::Filtered => {
                if matches!(
                    tracker.admit(inspection_work)?,
                    PageWorkAdmission::EnvelopeFull
                ) {
                    return Ok(progress_page(rows, progress, tracker));
                }
                progress.consume_filtered(physical);
            }
            ScalarPageUnitOutcome::Matching {
                row: _,
                logical: _,
                emission_work: _,
            } if progress.matching_rows_skipped < window.initial_offset => {
                if matches!(
                    tracker.admit(inspection_work)?,
                    PageWorkAdmission::EnvelopeFull
                ) {
                    return Ok(progress_page(rows, progress, tracker));
                }
                progress.consume_skipped_match(physical);
            }
            ScalarPageUnitOutcome::Matching { .. } if tracker.output_window_full() => {
                let lookahead_work = inspection_work.merge(PageWork::one(
                    DiagnosticExecutionBudgetResource::CursorSteps,
                    1,
                ));
                if matches!(
                    tracker.admit(lookahead_work)?,
                    PageWorkAdmission::EnvelopeFull
                ) {
                    return Ok(progress_page(rows, progress, tracker));
                }
                progress.unconsumed_lookahead = Some(physical);
                return Ok(progress_page(rows, progress, tracker));
            }
            ScalarPageUnitOutcome::Matching {
                row,
                logical,
                emission_work,
            } => {
                let complete_work = inspection_work.merge(emission_work).with_one_result_row();
                if matches!(
                    tracker.admit(complete_work)?,
                    PageWorkAdmission::EnvelopeFull
                ) {
                    return Ok(progress_page(rows, progress, tracker));
                }
                progress.consume_emitted_match(logical, physical);
                rows.push(row);
                if total_limit_reached(window, &progress) {
                    return Ok(completed_page(rows, tracker));
                }
            }
        }
    }

    Ok(completed_page(rows, tracker))
}

const fn total_limit_reached<LogicalBoundary, PhysicalAnchor>(
    window: ScalarPageWindow,
    progress: &ScalarPageProgress<LogicalBoundary, PhysicalAnchor>,
) -> bool {
    match window.total_limit {
        Some(limit) => progress.rows_emitted >= limit,
        None => false,
    }
}

const fn progress_page<Row, LogicalBoundary, PhysicalAnchor>(
    rows: Vec<Row>,
    progress: ScalarPageProgress<LogicalBoundary, PhysicalAnchor>,
    tracker: PageWorkTracker,
) -> BoundedScalarPage<Row, LogicalBoundary, PhysicalAnchor> {
    BoundedScalarPage {
        rows,
        continuation: Some(progress),
        work: tracker.receipt(),
    }
}

const fn completed_page<Row, LogicalBoundary, PhysicalAnchor>(
    rows: Vec<Row>,
    tracker: PageWorkTracker,
) -> BoundedScalarPage<Row, LogicalBoundary, PhysicalAnchor> {
    BoundedScalarPage {
        rows,
        continuation: None,
        work: tracker.receipt(),
    }
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
        DiagnosticFactTag, RuntimeBoundaryCode,
    };

    const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);
    const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0x2210_0007_0000_0001,
    );

    fn inspected_entry_work() -> PageWork {
        PageWork::one(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 1)
    }

    fn emitted_row_work(bytes: u64) -> PageWork {
        PageWork::one(DiagnosticExecutionBudgetResource::ResultBytes, bytes)
    }

    fn filtered(physical: u64) -> ScalarPageUnit<u64, u64, u64> {
        ScalarPageUnit::filtered(physical, inspected_entry_work())
    }

    fn matching(physical: u64) -> ScalarPageUnit<u64, u64, u64> {
        ScalarPageUnit::matching(
            physical,
            inspected_entry_work(),
            physical,
            physical,
            emitted_row_work(8),
        )
    }

    #[test]
    fn selective_scan_returns_empty_page_after_advancing_physical_progress() {
        let envelope = PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 2);
        let page = coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(0, None),
            None,
            [filtered(1), filtered(2), filtered(3)],
        )
        .expect("page envelope should return progress instead of failure");
        let (rows, continuation, work) = page.into_parts();
        let progress = continuation.expect("physical traversal is not exhausted");

        assert!(rows.is_empty());
        assert_eq!(progress.last_consumed_physical(), Some(&2));
        assert_eq!(progress.unconsumed_lookahead(), None);
        assert_eq!(progress.last_emitted_logical(), None);
        assert_eq!(progress.matching_rows_skipped(), 0);
        assert_eq!(progress.rows_emitted(), 0);
        assert_eq!(
            progress.contract().envelope_profile_identity(),
            envelope.profile_identity(),
        );
        assert_eq!(progress.contract().window(), ScalarPageWindow::new(0, None),);
        assert_eq!(
            work.observed(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited),
            2,
        );
    }

    #[test]
    fn exact_full_page_consumes_nonmatching_tail_until_exhaustion_is_proved() {
        let envelope = PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultRows, 2)
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 4);
        let first = coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(0, None),
            None,
            [
                matching(1),
                matching(2),
                filtered(3),
                filtered(4),
                filtered(5),
            ],
        )
        .expect("first bounded page should succeed");
        let (rows, continuation, _) = first.into_parts();
        let progress = continuation.expect("the long tail is not yet exhausted");

        assert_eq!(rows, vec![1, 2]);
        assert_eq!(progress.last_consumed_physical(), Some(&4));

        let second = coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(0, None),
            Some(progress),
            [filtered(5), filtered(6)],
        )
        .expect("tail progress page should succeed");
        let (rows, continuation, _) = second.into_parts();

        assert!(rows.is_empty());
        assert!(continuation.is_none(), "the tail is now proven exhausted");
    }

    #[test]
    fn ten_thousand_units_is_progress_not_false_exhaustion() {
        let envelope = PageWorkEnvelope::default_scalar().with_limit_for_tests(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            10_000,
        );
        let first = coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(0, None),
            None,
            (1_u64..=10_001).map(filtered),
        )
        .expect("the work envelope should return resumable progress");
        let (rows, continuation, _) = first.into_parts();
        let progress = continuation.expect("10,000 examined units do not prove exhaustion");
        assert!(rows.is_empty());
        assert_eq!(progress.last_consumed_physical(), Some(&10_000));

        let second = coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(0, None),
            Some(progress),
            [filtered(10_001)],
        )
        .expect("the final progress page should prove exhaustion");
        let (rows, continuation, _) = second.into_parts();
        assert!(rows.is_empty());
        assert!(continuation.is_none());
    }

    #[test]
    fn lookahead_match_is_not_consumed_or_skipped_on_resume() {
        let envelope = PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultRows, 1);
        let first = coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(0, None),
            None,
            [matching(1), filtered(2), matching(3)],
        )
        .expect("lookahead page should succeed");
        let (rows, continuation, _) = first.into_parts();
        let progress = continuation.expect("lookahead proves another match exists");

        assert_eq!(rows, vec![1]);
        assert_eq!(progress.last_emitted_logical(), Some(&1));
        assert_eq!(progress.last_consumed_physical(), Some(&2));
        assert_eq!(progress.unconsumed_lookahead(), Some(&3));
        assert_eq!(progress.rows_emitted(), 1);

        let second = coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(0, None),
            Some(progress),
            [matching(3)],
        )
        .expect("resume should re-read the lookahead match");
        let (rows, continuation, _) = second.into_parts();

        assert_eq!(rows, vec![3]);
        assert!(continuation.is_none());
    }

    #[test]
    fn page_envelope_returns_progress_while_hard_budget_returns_no_page() {
        let envelope = PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 1);
        let page = coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(0, None),
            None,
            [filtered(1), filtered(2)],
        )
        .expect("page envelope should be successful progress");
        assert!(page.into_parts().1.is_some());

        let hard_budget = HardExecutionBudget::uniform_for_tests(100, TEST_HEADROOM)
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 1);
        let hard_result = with_query_execution_budget_for_tests(hard_budget, TEST_CONTEXT, || {
            coordinate_scalar_page(
                PageWorkEnvelope::default_scalar(),
                ScalarPageWindow::new(0, None),
                None,
                [filtered(1), filtered(2)],
            )
            .map_err(coordinator_error_as_query_error)
        });
        let Err(error) = hard_result else {
            panic!("hard-budget exhaustion must not return a partial page")
        };

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
    }

    #[test]
    fn first_indivisible_unit_above_envelope_returns_terminal_typed_error() {
        let envelope = PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::StoredBytesRead, 4);
        let oversized = ScalarPageUnit::matching(
            1_u64,
            PageWork::one(DiagnosticExecutionBudgetResource::StoredBytesRead, 5),
            1_u64,
            1_u64,
            PageWork::empty(),
        );
        let error = match coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(0, None),
            None,
            [oversized],
        ) {
            Ok(_) => panic!("an unfit first unit must not return an unchanged cursor"),
            Err(ScalarPageCoordinatorError::Execution(error)) => error,
            Err(ScalarPageCoordinatorError::ContractMismatch(_)) => {
                panic!("the initial contract is valid")
            }
        };

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::PageUnitTooLarge,
            })
        ));
        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (DiagnosticFactTag::BudgetResource, 6),
                (DiagnosticFactTag::Limit, 4),
                (DiagnosticFactTag::Actual, 5),
            ],
        );
    }

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
    fn resume_accepts_changed_envelope_but_rejects_window_or_profile() {
        assert_eq!(
            PageWorkEnvelope::default_scalar().identity(),
            PageWorkEnvelope::default_scalar().profile_identity(),
        );
        assert_eq!(
            PageWorkEnvelope::public_scalar().identity(),
            PageWorkEnvelope::public_scalar().profile_identity(),
        );
        let envelope = PageWorkEnvelope::default_scalar()
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 1);
        let first = coordinate_scalar_page(
            envelope,
            ScalarPageWindow::new(2, Some(5)),
            None,
            [filtered(1), filtered(2)],
        )
        .expect("initial progress page should succeed");
        let progress = first
            .into_parts()
            .1
            .expect("the second physical entry remains");

        let changed_envelope =
            envelope.with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultRows, 7);
        assert_ne!(changed_envelope.identity(), envelope.identity());
        assert_eq!(
            changed_envelope.profile_identity(),
            envelope.profile_identity(),
        );
        coordinate_scalar_page::<u64, _, _, _>(
            changed_envelope,
            ScalarPageWindow::new(2, Some(5)),
            Some(progress.clone()),
            std::iter::empty(),
        )
        .expect("operational limits within one profile may change on resume");

        for (changed_envelope, changed_window) in [
            (envelope, ScalarPageWindow::new(3, Some(5))),
            (envelope, ScalarPageWindow::new(2, Some(6))),
            (
                PageWorkEnvelope::public_scalar(),
                ScalarPageWindow::new(2, Some(5)),
            ),
        ] {
            let result = coordinate_scalar_page::<u64, _, _, _>(
                changed_envelope,
                changed_window,
                Some(progress.clone()),
                std::iter::empty(),
            );
            match result {
                Err(ScalarPageCoordinatorError::ContractMismatch(mismatch)) => {
                    assert_ne!(mismatch.expected(), mismatch.actual());
                }
                Err(ScalarPageCoordinatorError::Execution(_)) => {
                    panic!("contract drift should remain distinct from execution failure")
                }
                Ok(_) => panic!("contract drift must reject resume"),
            }
        }
    }

    #[test]
    fn offset_is_consumed_once_and_total_limit_ends_the_traversal() {
        let page = coordinate_scalar_page(
            PageWorkEnvelope::default_scalar(),
            ScalarPageWindow::new(1, Some(2)),
            None,
            [matching(1), matching(2), matching(3), matching(4)],
        )
        .expect("bounded total window should succeed");
        let (rows, continuation, work) = page.into_parts();

        assert_eq!(rows, vec![2, 3]);
        assert!(continuation.is_none());
        assert_eq!(
            work.observed(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited),
            3,
        );
        assert_eq!(
            work.observed(DiagnosticExecutionBudgetResource::ResultRows),
            2,
        );
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
        assert_eq!(
            PageWorkReceipt {
                envelope_identity: envelope.identity(),
                observed: PageWork::empty(),
            }
            .envelope_identity(),
            envelope.identity(),
        );
    }

    fn coordinator_error_as_query_error(error: ScalarPageCoordinatorError) -> QueryError {
        match error {
            ScalarPageCoordinatorError::Execution(error) => QueryError::execute(error),
            ScalarPageCoordinatorError::ContractMismatch(_) => {
                QueryError::execute(InternalError::query_executor_invariant())
            }
        }
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
