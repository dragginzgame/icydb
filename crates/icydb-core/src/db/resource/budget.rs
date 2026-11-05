//! Module: db::resource::budget
//! Responsibility: finite hard limits and monotonic accounting for database work.
//! Does not own: request-root propagation or paging progress.
//! Boundary: charges one named resource before or during bounded work and returns typed exhaustion.

// The injected test budget variant must remain in the borrowed/owned budget enum and its dispatch.

#[cfg(test)]
mod maintenance_tests;
#[cfg(test)]
mod ordered_charge_tests;

use std::{
    cell::{Cell, RefCell},
    iter,
};

#[cfg(test)]
use crate::db::QueryError;
use crate::db::resource::request::RequestExecutionScope;
use crate::{error::InternalError, value::Value};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};

const RESOURCE_COUNT: usize = DiagnosticExecutionBudgetResource::ALL.len();
const INSTRUCTION_WATERMARK_CHARGE_INTERVAL: u16 = 64;
const INSTRUCTION_WATERMARK_LARGE_CHARGE: u64 = 1_024 * 1_024;

const READ_FAILURE_HEADROOM: HardExecutionFailureHeadroom =
    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1_024);
pub(in crate::db) const MUTATION_EXECUTION_INSTRUCTION_LIMIT: u64 = 30_000_000_000;
pub(in crate::db) const MUTATION_EXECUTION_INSTRUCTION_FAILURE_RESERVE: u64 = 5_000_000_000;
const MUTATION_FAILURE_HEADROOM: HardExecutionFailureHeadroom =
    HardExecutionFailureHeadroom::new(MUTATION_EXECUTION_INSTRUCTION_FAILURE_RESERVE, 64 * 1_024);
pub(in crate::db::resource) const READ_HARD_BUDGET: HardExecutionBudget = HardExecutionBudget::new(
    [
        1,                   // query executions
        2_000_000,           // planning steps
        64,                  // plan compilations
        250_000,             // key/index entries visited
        250_000,             // rows visited
        128 * 1_024 * 1_024, // stored bytes read
        16_000_000,          // predicate/expression steps
        16_000_000,          // nested value steps
        128 * 1_024 * 1_024, // decoded bytes
        128 * 1_024 * 1_024, // materialized bytes
        250_000,             // sort entries
        32_000_000,          // sort comparisons
        128 * 1_024 * 1_024, // sort temporary bytes
        100_000,             // group/distinct entries
        128 * 1_024 * 1_024, // group/distinct state bytes
        1_000_000,           // cursor steps
        128 * 1_024 * 1_024, // temporary bytes
        100_000,             // result rows
        64 * 1_024 * 1_024,  // result bytes
        4_500_000_000,       // instruction units
    ],
    READ_FAILURE_HEADROOM,
);
static MUTATION_HARD_BUDGET: HardExecutionBudget = READ_HARD_BUDGET
    .with_limit(
        DiagnosticExecutionBudgetResource::InstructionUnits,
        MUTATION_EXECUTION_INSTRUCTION_LIMIT,
    )
    .with_failure_headroom(MUTATION_FAILURE_HEADROOM);
pub(in crate::db) const MUTATION_EXECUTION_BUDGET_POLICY_IDENTITY: u32 =
    hard_execution_budget_policy_identity(&MUTATION_HARD_BUDGET);

const fn hard_execution_budget_policy_identity(budget: &HardExecutionBudget) -> u32 {
    let mut identity = 0x811c_9dc5_u32;
    let mut index = 0;
    while index < RESOURCE_COUNT {
        identity = fold_execution_budget_policy_u64(identity, budget.limits[index]);
        index += 1;
    }
    identity =
        fold_execution_budget_policy_u64(identity, budget.failure_headroom.instruction_units);
    fold_execution_budget_policy_u64(identity, budget.failure_headroom.response_bytes)
}

const fn fold_execution_budget_policy_u64(mut identity: u32, value: u64) -> u32 {
    let bytes = value.to_le_bytes();
    identity ^= u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    identity = identity.wrapping_mul(0x0100_0193);
    identity ^= u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    identity = identity.wrapping_mul(0x0100_0193);

    identity
}

std::thread_local! {
    static ACTIVE_EXECUTION_BUDGET: RefCell<Option<HardExecutionBudgetTracker>> =
        const { RefCell::new(None) };
}

/// Reserved capacity needed to construct and encode a typed budget failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct HardExecutionFailureHeadroom {
    instruction_units: u64,
    response_bytes: u64,
}

impl HardExecutionFailureHeadroom {
    /// Construct one explicit failure-reserve contract.
    #[must_use]
    pub(in crate::db) const fn new(instruction_units: u64, response_bytes: u64) -> Self {
        Self {
            instruction_units,
            response_bytes,
        }
    }

    const fn is_reserved(self) -> bool {
        self.instruction_units != 0 && self.response_bytes != 0
    }
}

/// Finite per-resource ceilings for one hard execution scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct HardExecutionBudget {
    limits: [u64; RESOURCE_COUNT],
    failure_headroom: HardExecutionFailureHeadroom,
}

impl HardExecutionBudget {
    /// Construct one closed finite budget profile.
    #[must_use]
    pub(in crate::db) const fn new(
        limits: [u64; RESOURCE_COUNT],
        failure_headroom: HardExecutionFailureHeadroom,
    ) -> Self {
        Self {
            limits,
            failure_headroom,
        }
    }

    /// Derive a fixed profile by changing one resource ceiling.
    #[must_use]
    pub(in crate::db::resource) const fn with_limit(
        mut self,
        resource: DiagnosticExecutionBudgetResource,
        limit: u64,
    ) -> Self {
        self.limits[resource_index(resource)] = limit;
        self
    }

    const fn with_failure_headroom(mut self, headroom: HardExecutionFailureHeadroom) -> Self {
        self.failure_headroom = headroom;
        self
    }

    /// Return the absolute ceiling for one maintained resource.
    #[must_use]
    pub(in crate::db) const fn limit(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.limits[resource_index(resource)]
    }

    /// Preflight the combined cost of repeated resources without charging work.
    pub(in crate::db) fn can_charge_budget_bundle(
        &self,
        charges: &[(DiagnosticExecutionBudgetResource, u64)],
        observed: impl Fn(DiagnosticExecutionBudgetResource) -> u64,
    ) -> bool {
        let Some(totals) = budget_bundle_totals(charges) else {
            return false;
        };
        charges.iter().all(|(resource, _amount)| {
            observed(*resource)
                .checked_add(totals[resource_index(*resource)])
                .is_some_and(|next| next <= self.limit(*resource))
        })
    }

    /// Count complete units using the combined per-resource cost at both owners.
    pub(in crate::db) fn remaining_budget_units(
        &self,
        per_unit: &[(DiagnosticExecutionBudgetResource, u64)],
        observed: impl Fn(DiagnosticExecutionBudgetResource) -> u64,
    ) -> u64 {
        let Some(totals) = budget_bundle_totals(per_unit) else {
            return 0;
        };
        DiagnosticExecutionBudgetResource::ALL
            .into_iter()
            .zip(totals)
            .filter(|(_resource, amount)| *amount != 0)
            .map(|(resource, amount)| {
                self.limit(resource).saturating_sub(observed(resource)) / amount
            })
            .min()
            .unwrap_or(u64::MAX)
    }
}

/// Both budget owners use checked totals: overflow cannot fit even a maximum
/// ceiling. The closed resource set needs only fixed-size scratch accounting.
fn budget_bundle_totals(
    charges: &[(DiagnosticExecutionBudgetResource, u64)],
) -> Option<[u64; RESOURCE_COUNT]> {
    let mut totals = [0_u64; RESOURCE_COUNT];
    for (resource, amount) in charges {
        let total = &mut totals[resource_index(*resource)];
        *total = total.checked_add(*amount)?;
    }

    Some(totals)
}

/// Immutable attribution attached to one hard-budget counter set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct HardExecutionContext {
    scope: DiagnosticExecutionBudgetScope,
    lane: DiagnosticExecutionLane,
    normalized_shape_fingerprint_prefix: u64,
}

impl HardExecutionContext {
    /// Construct attribution for one counter owner and literal-free query shape.
    #[must_use]
    pub(in crate::db) const fn new(
        scope: DiagnosticExecutionBudgetScope,
        lane: DiagnosticExecutionLane,
        normalized_shape_fingerprint_prefix: u64,
    ) -> Self {
        Self {
            scope,
            lane,
            normalized_shape_fingerprint_prefix,
        }
    }

    /// Reattribute the same lane and normalized shape to another counter owner.
    #[must_use]
    pub(in crate::db) const fn with_scope(self, scope: DiagnosticExecutionBudgetScope) -> Self {
        Self { scope, ..self }
    }

    /// Preserve the caller's lane when entering a request preparation segment.
    #[must_use]
    pub(in crate::db) const fn lane(self) -> DiagnosticExecutionLane {
        self.lane
    }
}

/// Typed hard-budget exhaustion with no query literals or row payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutionBudgetExceeded {
    resource: DiagnosticExecutionBudgetResource,
    limit: u64,
    observed: u64,
    context: HardExecutionContext,
}

impl ExecutionBudgetExceeded {
    #[must_use]
    pub(in crate::db) const fn new(
        resource: DiagnosticExecutionBudgetResource,
        limit: u64,
        observed: u64,
        context: HardExecutionContext,
    ) -> Self {
        Self {
            resource,
            limit,
            observed,
            context,
        }
    }

    /// Return the resource whose ceiling rejected work.
    #[must_use]
    pub(in crate::db) const fn resource(self) -> DiagnosticExecutionBudgetResource {
        self.resource
    }

    /// Return the configured hard ceiling.
    #[must_use]
    pub(in crate::db) const fn limit(self) -> u64 {
        self.limit
    }

    /// Return the attempted cumulative usage, saturated on arithmetic overflow.
    #[must_use]
    pub(in crate::db) const fn observed(self) -> u64 {
        self.observed
    }

    /// Return the counter owner that rejected work.
    #[must_use]
    pub(in crate::db) const fn scope(self) -> DiagnosticExecutionBudgetScope {
        self.context.scope
    }

    /// Return the execution lane whose work was charged.
    #[must_use]
    pub(in crate::db) const fn lane(self) -> DiagnosticExecutionLane {
        self.context.lane
    }

    /// Return the bounded literal-free query-shape fingerprint prefix.
    #[must_use]
    pub(in crate::db) const fn normalized_shape_fingerprint_prefix(self) -> u64 {
        self.context.normalized_shape_fingerprint_prefix
    }
}

impl From<ExecutionBudgetExceeded> for InternalError {
    fn from(exhausted: ExecutionBudgetExceeded) -> Self {
        Self::execution_budget_exceeded(
            exhausted.resource(),
            exhausted.limit(),
            exhausted.observed(),
            exhausted.scope(),
            exhausted.lane(),
            exhausted.normalized_shape_fingerprint_prefix(),
        )
    }
}

enum HardExecutionBudgetAuthority {
    Static(&'static HardExecutionBudget),
    #[cfg(test)]
    TestOwned(Box<HardExecutionBudget>),
}

impl HardExecutionBudgetAuthority {
    const fn budget(&self) -> &HardExecutionBudget {
        match self {
            Self::Static(budget) => budget,
            #[cfg(test)]
            Self::TestOwned(budget) => budget,
        }
    }
}

/// Monotonic usage counters for one hard execution budget.
pub(in crate::db) struct HardExecutionBudgetTracker {
    budget: HardExecutionBudgetAuthority,
    context: HardExecutionContext,
    request_scope: Option<RequestExecutionScope>,
    observed: [u64; RESOURCE_COUNT],
    last_instruction_counter: Option<u64>,
    charges_since_instruction_watermark: u16,
}

impl HardExecutionBudgetTracker {
    /// Start one counter set at zero usage.
    #[must_use]
    pub(in crate::db) fn new(
        budget: &'static HardExecutionBudget,
        context: HardExecutionContext,
    ) -> Self {
        debug_assert!(budget.failure_headroom.is_reserved());
        Self {
            budget: HardExecutionBudgetAuthority::Static(budget),
            context,
            request_scope: None,
            observed: [0; RESOURCE_COUNT],
            last_instruction_counter: None,
            charges_since_instruction_watermark: 0,
        }
    }

    /// Start one execution counter set attached to an aggregate request scope.
    #[must_use]
    pub(in crate::db) fn new_with_request_scope(
        budget: &'static HardExecutionBudget,
        context: HardExecutionContext,
        request_scope: &RequestExecutionScope,
    ) -> Self {
        let mut tracker = Self::new(budget, context);
        tracker.request_scope = Some(request_scope.clone());
        tracker
    }

    /// Charge work whose bounded amount is known before it starts.
    pub(in crate::db) fn precharge(
        &mut self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        self.charge_raw(resource, amount)
    }

    /// Charge one bounded increment at a maintained loop boundary.
    pub(in crate::db) fn charge_periodic(
        &mut self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        self.charge(resource, amount)
    }

    /// Sample the current IC instruction watermark at a bounded operator seam.
    pub(in crate::db) fn check_instruction_watermark(
        &mut self,
    ) -> Result<(), ExecutionBudgetExceeded> {
        // Keep physical per-operation accounting separate from the message
        // safety ceiling. Earlier application/maintenance work consumes the
        // same IC message even when this tracker has just been constructed.
        self.record_instruction_watermark(crate::runtime::local_instruction_counter())?;
        if self.context.lane == DiagnosticExecutionLane::Mutation {
            let resource = DiagnosticExecutionBudgetResource::InstructionUnits;
            let limit = self.budget.budget().limit(resource);
            let current = crate::runtime::message_instruction_counter();
            if current > limit {
                return Err(ExecutionBudgetExceeded::new(
                    resource,
                    limit,
                    current,
                    self.context,
                ));
            }
        }
        Ok(())
    }

    // Keep counter-delta arithmetic testable without substituting native time
    // for IC instruction measurements.
    fn record_instruction_watermark(
        &mut self,
        current: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        let delta = self
            .last_instruction_counter
            .map_or(0, |previous| current.saturating_sub(previous));
        self.last_instruction_counter = Some(current);
        self.charges_since_instruction_watermark = 0;
        self.charge_raw(DiagnosticExecutionBudgetResource::InstructionUnits, delta)
    }

    /// Finish instruction accounting after the execution entry opened a watermark.
    pub(in crate::db) fn finish_instruction_watermark(
        &mut self,
    ) -> Result<(), ExecutionBudgetExceeded> {
        if self.last_instruction_counter.is_none() {
            return Ok(());
        }

        self.check_instruction_watermark()
    }

    fn remaining_budget_units(&self, per_unit: &[(DiagnosticExecutionBudgetResource, u64)]) -> u64 {
        let execution_remaining = self
            .budget
            .budget()
            .remaining_budget_units(per_unit, |resource| self.observed[resource_index(resource)]);
        let request_remaining = self
            .request_scope
            .as_ref()
            .map_or(u64::MAX, |scope| scope.remaining_budget_units(per_unit));

        execution_remaining.min(request_remaining)
    }

    fn can_charge_budget_bundle(
        &self,
        charges: &[(DiagnosticExecutionBudgetResource, u64)],
    ) -> bool {
        self.budget
            .budget()
            .can_charge_budget_bundle(charges, |resource| self.observed[resource_index(resource)])
            && self
                .request_scope
                .as_ref()
                .is_none_or(|scope| scope.can_charge_budget_bundle(charges))
    }

    fn try_charge_budget_bundle(
        &mut self,
        charges: &[(DiagnosticExecutionBudgetResource, u64)],
    ) -> Result<bool, ExecutionBudgetExceeded> {
        if !self.can_charge_budget_bundle(charges) {
            return Ok(false);
        }

        // Sample instructions before publishing any semantic charge. The
        // second preflight accounts for that real instruction work.
        self.check_instruction_watermark()?;
        if !self.can_charge_budget_bundle(charges) {
            return Ok(false);
        }

        if let Some(scope) = self.request_scope.as_ref()
            && !scope.try_commit_budget_bundle(charges)
        {
            return Ok(false);
        }
        for (resource, amount) in charges {
            let observed = &mut self.observed[resource_index(*resource)];
            *observed = observed.saturating_add(*amount);
        }
        self.charges_since_instruction_watermark = self
            .charges_since_instruction_watermark
            .saturating_add(u16::try_from(charges.len()).unwrap_or(u16::MAX));

        Ok(true)
    }

    fn charge(
        &mut self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        if !matches!(
            resource,
            DiagnosticExecutionBudgetResource::InstructionUnits
        ) {
            self.charges_since_instruction_watermark =
                self.charges_since_instruction_watermark.saturating_add(1);
            if self.charges_since_instruction_watermark >= INSTRUCTION_WATERMARK_CHARGE_INTERVAL
                || amount >= INSTRUCTION_WATERMARK_LARGE_CHARGE
            {
                self.check_instruction_watermark()?;
            }
        }
        self.charge_raw(resource, amount)
    }

    fn charge_raw(
        &mut self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), ExecutionBudgetExceeded> {
        let index = resource_index(resource);
        let current = self.observed[index];
        let (observed, overflowed) = current.overflowing_add(amount);
        let observed = if overflowed { u64::MAX } else { observed };
        self.observed[index] = observed;
        let limit = self.budget.budget().limit(resource);
        let execution_result = if overflowed || observed > limit {
            Err(ExecutionBudgetExceeded::new(
                resource,
                limit,
                observed,
                self.context,
            ))
        } else {
            Ok(())
        };
        let request_result = self
            .request_scope
            .as_ref()
            .map_or(Ok(()), |scope| scope.charge(self.context, resource, amount));
        execution_result?;
        request_result
    }
}

/// Run one prepared read under a finite per-execution hard budget.
pub(in crate::db) fn with_read_execution_budget<T>(
    request_scope: &RequestExecutionScope,
    context: HardExecutionContext,
    run: impl FnOnce() -> Result<T, InternalError>,
) -> Result<T, InternalError> {
    let mut tracker = HardExecutionBudgetTracker::new_with_request_scope(
        &READ_HARD_BUDGET,
        context,
        request_scope,
    );
    // The first periodic sample must include work performed before that seam,
    // and automatic finish must account for reads too short to sample in a loop.
    tracker.check_instruction_watermark()?;
    with_execution_budget(
        tracker,
        run,
        std::convert::identity,
        ExecutionBudgetFinish::Automatic,
    )
}

/// Run one durable mutation advance under its fixed engine-owned allocation.
///
/// Mutation execution samples explicitly before every publication boundary so
/// deterministic exhaustion can leave the budget scope and persist one
/// progress-only terminal transition from the reserved failure headroom.
pub(in crate::db) fn with_mutation_execution_budget<T, E>(
    context: HardExecutionContext,
    run: impl FnOnce() -> Result<T, E>,
    map_internal: fn(InternalError) -> E,
) -> Result<T, E> {
    let mut tracker = HardExecutionBudgetTracker::new(&MUTATION_HARD_BUDGET, context);
    tracker
        .check_instruction_watermark()
        .map_err(InternalError::from)
        .map_err(map_internal)?;
    with_execution_budget(tracker, run, map_internal, ExecutionBudgetFinish::Explicit)
}

/// Charge one maintained physical resource in the innermost active execution.
///
/// Optional execution instrumentation uses this helper. Compiler construction
/// instead requires the strict `ExecutionConstructionBudget` adapter below.
pub(in crate::db) fn charge_current_execution_budget(
    resource: DiagnosticExecutionBudgetResource,
    amount: u64,
) -> Result<(), InternalError> {
    charge_current_execution_budget_typed(resource, amount).map_err(InternalError::from)
}

/// Existing active-budget failure cases retained by expression-layer carriers.
/// This adds no budget scope or policy; invariant failures stay distinct from
/// the exact exhaustion facts already owned by `ExecutionBudgetExceeded`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionBudgetChargeError {
    Invariant,
    Exceeded(ExecutionBudgetExceeded),
}

impl From<ExecutionBudgetChargeError> for InternalError {
    fn from(error: ExecutionBudgetChargeError) -> Self {
        match error {
            ExecutionBudgetChargeError::Invariant => Self::query_executor_invariant(),
            ExecutionBudgetChargeError::Exceeded(error) => error.into(),
        }
    }
}

/// Charge the same active execution while retaining typed exhaustion for
/// expression errors that must be cloneable without flattening numeric facts.
pub(in crate::db) fn charge_current_execution_budget_typed(
    resource: DiagnosticExecutionBudgetResource,
    amount: u64,
) -> Result<(), ExecutionBudgetChargeError> {
    if amount == 0 {
        return Ok(());
    }
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let mut budget = budget
            .try_borrow_mut()
            .map_err(|_| ExecutionBudgetChargeError::Invariant)?;
        let Some(budget) = budget.as_mut() else {
            return Ok(());
        };
        budget
            .charge_periodic(resource, amount)
            .map_err(ExecutionBudgetChargeError::Exceeded)
    })
}

/// Explicit, volatile authority for one maintenance page or complete staged
/// schema operation. It shares the mutation policy, not its thread-local tracker.
/// Callers retain one instance across all rows/indexes and publish only after
/// `run` succeeds. Existing soft page limits remain separate yield decisions.
pub(in crate::db) struct MaintenanceConstructionBudget {
    tracker: RefCell<HardExecutionBudgetTracker>,
    exhausted: Cell<Option<ExecutionBudgetExceeded>>,
}

impl MaintenanceConstructionBudget {
    /// Reuse the engine's mutation ceilings without introducing another profile.
    pub(in crate::db) fn new() -> Self {
        Self {
            exhausted: Cell::new(None),
            tracker: RefCell::new(HardExecutionBudgetTracker::new(
                &MUTATION_HARD_BUDGET,
                HardExecutionContext::new(
                    DiagnosticExecutionBudgetScope::Execution,
                    DiagnosticExecutionLane::Mutation,
                    0,
                ),
            )),
        }
    }

    /// Account a construction segment, including failed work, without resetting
    /// counters. Complete staged builders may call this for successive rows.
    /// A hard failure is an error, never a successful empty page or retry signal.
    pub(in crate::db) fn run<T, E>(
        &self,
        run: impl FnOnce(&Self) -> Result<T, E>,
        map_error: fn(InternalError) -> E,
    ) -> Result<T, E> {
        self.with_tracker(HardExecutionBudgetTracker::check_instruction_watermark)
            .map_err(map_error)?;
        let result = run(self);
        self.with_tracker(HardExecutionBudgetTracker::check_instruction_watermark)
            .map_err(map_error)?;
        result
    }

    // Retain the first exhaustion without retaining payloads. Even if an inner
    // caller swallows a charge failure, later segments/finish cannot publish.
    fn with_tracker(
        &self,
        charge: impl FnOnce(&mut HardExecutionBudgetTracker) -> Result<(), ExecutionBudgetExceeded>,
    ) -> Result<(), InternalError> {
        if let Some(error) = self.exhausted.get() {
            return Err(error.into());
        }
        let mut tracker = self
            .tracker
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        charge(&mut tracker).map_err(|error| {
            self.exhausted.set(Some(error));
            InternalError::from(error)
        })
    }
}

impl crate::db::resource::construction::ConstructionBudget for MaintenanceConstructionBudget {
    fn charge(
        &self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), InternalError> {
        self.with_tracker(|tracker| tracker.charge_periodic(resource, amount))
    }
}

/// Borrow the active execution's counters without starting another instruction interval.
/// Missing execution authority is an invariant error, never an unmetered fallback.
pub(in crate::db) struct ExecutionConstructionBudget;

impl crate::db::resource::construction::ConstructionBudget for ExecutionConstructionBudget {
    fn charge(
        &self,
        resource: DiagnosticExecutionBudgetResource,
        amount: u64,
    ) -> Result<(), InternalError> {
        ACTIVE_EXECUTION_BUDGET.with(|budget| {
            let mut budget = budget
                .try_borrow_mut()
                .map_err(|_| InternalError::query_executor_invariant())?;
            let budget = budget
                .as_mut()
                .ok_or_else(InternalError::query_executor_invariant)?;
            budget
                .charge_periodic(resource, amount)
                .map_err(InternalError::from)
        })
    }
}

/// Return complete equal-cost units remaining in both the active execution
/// and its owning request scope without mutating either authority.
pub(in crate::db) fn current_execution_remaining_budget_units(
    per_unit: &[(DiagnosticExecutionBudgetResource, u64)],
) -> Result<u64, InternalError> {
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let budget = budget
            .try_borrow()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let budget = budget
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)?;

        Ok(budget.remaining_budget_units(per_unit))
    })
}

/// Atomically charge one semantic bundle in the active execution and request.
/// `false` means the complete bundle does not fit and neither scope changed.
pub(in crate::db) fn try_charge_current_execution_budget_bundle(
    charges: &[(DiagnosticExecutionBudgetResource, u64)],
) -> Result<bool, InternalError> {
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let mut budget = budget
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let budget = budget
            .as_mut()
            .ok_or_else(InternalError::query_executor_invariant)?;

        budget
            .try_charge_budget_bundle(charges)
            .map_err(InternalError::from)
    })
}

/// Charge maintained physical resources in order through one active-budget lookup.
///
/// Earlier and attempted charges remain accounted when one fails; later charges
/// are skipped, matching sequential calls to [`charge_current_execution_budget`].
/// Empty and zero-only sequences do not borrow the active budget.
pub(in crate::db) fn charge_current_execution_budget_ordered(
    charges: impl IntoIterator<Item = (DiagnosticExecutionBudgetResource, u64)>,
) -> Result<(), InternalError> {
    let mut charges = charges.into_iter().filter(|(_, amount)| *amount != 0);
    let Some(first) = charges.next() else {
        return Ok(());
    };
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let mut budget = budget
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let Some(budget) = budget.as_mut() else {
            return Ok(());
        };

        for (resource, amount) in iter::once(first).chain(charges) {
            budget
                .charge_periodic(resource, amount)
                .map_err(InternalError::from)?;
        }
        Ok(())
    })
}

/// Charge row decoding before accessing slots in scalar or grouped execution.
/// Decoded bytes precede nested steps so exhaustion retains the same partial
/// accounting and diagnostic resource in both readers.
pub(in crate::db) fn charge_decoded_row(
    row_bytes: usize,
    nested_steps: usize,
) -> Result<(), InternalError> {
    charge_current_execution_budget_ordered([
        (
            DiagnosticExecutionBudgetResource::DecodedBytes,
            usize_as_u64(row_bytes),
        ),
        (
            DiagnosticExecutionBudgetResource::NestedValueSteps,
            usize_as_u64(nested_steps),
        ),
    ])
}

/// Charge the stored and materialized bytes for one owned data-row payload.
pub(in crate::db) fn charge_materialized_data_row(row_bytes: usize) -> Result<(), InternalError> {
    let row_bytes = usize_as_u64(row_bytes);
    charge_current_execution_budget_ordered([
        (
            DiagnosticExecutionBudgetResource::StoredBytesRead,
            row_bytes,
        ),
        (
            DiagnosticExecutionBudgetResource::MaterializedBytes,
            row_bytes,
        ),
    ])
}

/// Build one typed budget failure from a stricter operator-local hard limit.
///
/// Grouped planning can impose a lower retained-state ceiling than the root
/// request budget. The failure still belongs to the active execution and must
/// preserve its scope, lane, and normalized shape rather than degrading to an
/// unclassified internal error.
pub(in crate::db) fn current_execution_budget_exceeded(
    resource: DiagnosticExecutionBudgetResource,
    limit: u64,
    observed: u64,
) -> InternalError {
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let Ok(budget) = budget.try_borrow() else {
            return InternalError::query_executor_invariant();
        };
        let Some(budget) = budget.as_ref() else {
            return InternalError::query_executor_invariant();
        };

        ExecutionBudgetExceeded::new(resource, limit, observed, budget.context).into()
    })
}

/// Monotonic hard-budget counters at one maintained execution boundary.
///
/// Page admission compares two snapshots only after it has preflighted the
/// complete bounded physical unit. The hard budget remains authoritative and
/// continues to retain every charge, including work from a failed unit.
#[derive(Clone, Copy)]
pub(in crate::db) struct ExecutionBudgetUsage {
    observed: [u64; RESOURCE_COUNT],
}

impl ExecutionBudgetUsage {
    /// Return one cumulative resource observation.
    #[must_use]
    pub(in crate::db) const fn observed(self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.observed[resource_index(resource)]
    }
}

/// Snapshot the innermost execution's cumulative counters.
///
/// Scalar page execution always runs below a hard read budget. Requiring that
/// budget here keeps page-local accounting from becoming a second, optional
/// source of physical-work truth.
pub(in crate::db) fn current_execution_budget_usage() -> Result<ExecutionBudgetUsage, InternalError>
{
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let budget = budget
            .try_borrow()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let budget = budget
            .as_ref()
            .ok_or_else(InternalError::query_executor_invariant)?;

        Ok(ExecutionBudgetUsage {
            observed: budget.observed,
        })
    })
}

/// Precharge one full in-memory sort whose entry count is already known.
pub(in crate::db) fn charge_sort_work<R>(entries: usize) -> Result<(), InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortEntries,
        usize_as_u64(entries),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortComparisons,
        sort_comparison_allowance(entries),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortTemporaryBytes,
        usize_as_u64(entries.saturating_mul(std::mem::size_of::<R>())),
    )
}

/// Return the existing full-sort comparison allowance for an admitted count.
/// Construction and execution share this policy while retaining their own counters.
pub(in crate::db) fn sort_comparison_allowance(entries: usize) -> u64 {
    if entries <= 1 {
        return 0;
    }
    usize_as_u64(entries).saturating_mul(u64::from((entries - 1).bit_width()))
}

/// Precharge one qualifying grouped heap candidate before retention or comparison.
pub(in crate::db) fn charge_grouped_top_k_candidate<R>(
    retained_count: usize,
    selection_bound: usize,
    retained_backing_bytes: u64,
) -> Result<(), InternalError> {
    let comparisons = if retained_count == 0 {
        0
    } else if retained_count < selection_bound {
        1
    } else {
        retained_count.saturating_add(1)
    };
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::SortEntries, 1)?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortComparisons,
        u64::try_from(comparisons).unwrap_or(u64::MAX),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortTemporaryBytes,
        u64::try_from(std::mem::size_of::<R>())
            .unwrap_or(u64::MAX)
            .saturating_add(retained_backing_bytes),
    )
}

/// Sample only the instruction watermark for the innermost active execution.
pub(in crate::db) fn finish_current_execution_instruction_watermark() -> Result<(), InternalError> {
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let mut budget = budget
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let Some(budget) = budget.as_mut() else {
            return Ok(());
        };

        budget
            .finish_instruction_watermark()
            .map_err(InternalError::from)
    })
}

/// Charge one fully materialized runtime-value result before response shaping.
pub(in crate::db) fn charge_runtime_value_rows(rows: &[Vec<Value>]) -> Result<(), InternalError> {
    let (bytes, nested_steps) = rows.iter().flatten().fold((0_u64, 0_u64), |total, value| {
        let value_work = runtime_value_work(value);
        (
            total.0.saturating_add(value_work.0),
            total.1.saturating_add(value_work.1),
        )
    });

    charge_runtime_result_work(rows.len(), bytes, nested_steps)
}

/// Charge materialized result totals in the same order for scalar and grouped output.
pub(in crate::db) fn charge_runtime_result_work(
    row_count: usize,
    bytes: u64,
    nested_steps: u64,
) -> Result<(), InternalError> {
    charge_current_execution_budget_ordered([
        (
            DiagnosticExecutionBudgetResource::ResultRows,
            usize_as_u64(row_count),
        ),
        (
            DiagnosticExecutionBudgetResource::NestedValueSteps,
            nested_steps,
        ),
    ])?;
    charge_current_execution_budget_ordered([
        (DiagnosticExecutionBudgetResource::MaterializedBytes, bytes),
        (DiagnosticExecutionBudgetResource::ResultBytes, bytes),
    ])
}

pub(in crate::db) const RUNTIME_VALUE_NODE_OVERHEAD_BYTES: u64 = 32;

pub(in crate::db) fn runtime_value_work(value: &Value) -> (u64, u64) {
    const VALUE_OVERHEAD: u64 = RUNTIME_VALUE_NODE_OVERHEAD_BYTES;
    match value {
        Value::Blob(value) => (VALUE_OVERHEAD.saturating_add(usize_as_u64(value.len())), 1),
        Value::Text(value) => (VALUE_OVERHEAD.saturating_add(usize_as_u64(value.len())), 1),
        Value::Principal(value) => (
            VALUE_OVERHEAD.saturating_add(usize_as_u64(value.as_slice().len())),
            1,
        ),
        Value::List(values) => values.iter().fold((VALUE_OVERHEAD, 1_u64), |total, value| {
            let value_work = runtime_value_work(value);
            (
                total.0.saturating_add(value_work.0),
                total.1.saturating_add(value_work.1),
            )
        }),
        Value::Map(entries) => {
            entries
                .iter()
                .fold((VALUE_OVERHEAD, 1_u64), |total, (key, value)| {
                    let key_work = runtime_value_work(key);
                    let value_work = runtime_value_work(value);
                    (
                        total
                            .0
                            .saturating_add(key_work.0)
                            .saturating_add(value_work.0),
                        total
                            .1
                            .saturating_add(key_work.1)
                            .saturating_add(value_work.1),
                    )
                })
        }
        Value::Enum(value) => value.payload().map_or((VALUE_OVERHEAD, 1), |payload| {
            let payload_work = runtime_value_work(payload);
            (
                VALUE_OVERHEAD.saturating_add(payload_work.0),
                1_u64.saturating_add(payload_work.1),
            )
        }),
        Value::Account(_)
        | Value::Bool(_)
        | Value::Date(_)
        | Value::Decimal(_)
        | Value::Duration(_)
        | Value::Float32(_)
        | Value::Float64(_)
        | Value::Int64(_)
        | Value::Int128(_)
        | Value::Null
        | Value::Subaccount(_)
        | Value::Timestamp(_)
        | Value::Nat64(_)
        | Value::Nat128(_)
        | Value::Ulid(_)
        | Value::Unit => (VALUE_OVERHEAD, 1),
        Value::U256(_) => (VALUE_OVERHEAD, 32),
    }
}

#[derive(Clone, Copy)]
enum ExecutionBudgetFinish {
    Automatic,
    Explicit,
}

fn with_execution_budget<T, E>(
    mut tracker: HardExecutionBudgetTracker,
    run: impl FnOnce() -> Result<T, E>,
    map_internal: fn(InternalError) -> E,
    finish: ExecutionBudgetFinish,
) -> Result<T, E> {
    tracker
        .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1)
        .map_err(InternalError::from)
        .map_err(map_internal)?;
    let installed = ACTIVE_EXECUTION_BUDGET
        .with(|budget| {
            let mut budget = budget
                .try_borrow_mut()
                .map_err(|_| InternalError::query_executor_invariant())?;
            if budget.is_some() {
                return Ok(false);
            }
            *budget = Some(tracker);
            Ok::<bool, InternalError>(true)
        })
        .map_err(map_internal)?;
    if !installed {
        return run();
    }

    let result = run();
    let final_budget_result = match finish {
        ExecutionBudgetFinish::Automatic => finish_current_execution_instruction_watermark(),
        ExecutionBudgetFinish::Explicit => Ok(()),
    };
    let removed = ACTIVE_EXECUTION_BUDGET.with(|budget| {
        budget
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?
            .take()
            .ok_or_else(InternalError::query_executor_invariant)
    });
    let removed = removed.map_err(map_internal)?;

    let _ = removed;
    final_budget_result.map_err(map_internal)?;

    result
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(in crate::db) const fn resource_index(resource: DiagnosticExecutionBudgetResource) -> usize {
    match resource {
        DiagnosticExecutionBudgetResource::QueryExecutions => 0,
        DiagnosticExecutionBudgetResource::PlanningSteps => 1,
        DiagnosticExecutionBudgetResource::PlanCompilations => 2,
        DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited => 3,
        DiagnosticExecutionBudgetResource::RowsVisited => 4,
        DiagnosticExecutionBudgetResource::StoredBytesRead => 5,
        DiagnosticExecutionBudgetResource::PredicateExpressionSteps => 6,
        DiagnosticExecutionBudgetResource::NestedValueSteps => 7,
        DiagnosticExecutionBudgetResource::DecodedBytes => 8,
        DiagnosticExecutionBudgetResource::MaterializedBytes => 9,
        DiagnosticExecutionBudgetResource::SortEntries => 10,
        DiagnosticExecutionBudgetResource::SortComparisons => 11,
        DiagnosticExecutionBudgetResource::SortTemporaryBytes => 12,
        DiagnosticExecutionBudgetResource::GroupDistinctEntries => 13,
        DiagnosticExecutionBudgetResource::GroupDistinctStateBytes => 14,
        DiagnosticExecutionBudgetResource::CursorSteps => 15,
        DiagnosticExecutionBudgetResource::TemporaryBytes => 16,
        DiagnosticExecutionBudgetResource::ResultRows => 17,
        DiagnosticExecutionBudgetResource::ResultBytes => 18,
        DiagnosticExecutionBudgetResource::InstructionUnits => 19,
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db) use helper_tests::read_hard_budget_limit_for_tests;
#[cfg(test)]
pub(in crate::db) use helper_tests::with_execution_budget_for_tests;
#[cfg(test)]
pub(in crate::db) use helper_tests::with_query_execution_budget_for_tests;
