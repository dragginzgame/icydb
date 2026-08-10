//! Module: db::executor::budget
//! Responsibility: finite hard limits and monotonic accounting for database work.
//! Does not own: request-root propagation or paging progress.
//! Boundary: charges one named resource before or during bounded work and returns typed exhaustion.

use std::cell::RefCell;

#[cfg(test)]
use crate::db::QueryError;
#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::RequestDiagnosticResourceUsage;
use crate::db::executor::{EntityAuthority, RuntimeGroupedRow, SharedPreparedExecutionPlan};
use crate::db::session::RequestExecutionScope;
use crate::{error::InternalError, value::Value};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};

const RESOURCE_COUNT: usize = DiagnosticExecutionBudgetResource::ALL.len();
const INSTRUCTION_WATERMARK_CHARGE_INTERVAL: u16 = 64;
const INSTRUCTION_WATERMARK_LARGE_CHARGE: u64 = 1_024 * 1_024;

const READ_FAILURE_HEADROOM: HardExecutionFailureHeadroom =
    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1_024);
static READ_HARD_BUDGET: HardExecutionBudget = HardExecutionBudget::new(
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
        1_000_000,           // diagnostic steps
        100_000,             // result rows
        64 * 1_024 * 1_024,  // result bytes
        4_500_000_000,       // instruction units
    ],
    READ_FAILURE_HEADROOM,
);

#[cfg(test)]
pub(in crate::db::executor) const fn read_hard_budget_limit_for_tests(
    resource: DiagnosticExecutionBudgetResource,
) -> u64 {
    READ_HARD_BUDGET.limit(resource)
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

    /// Return the instruction reserve excluded from ordinary charged work.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn instruction_units(self) -> u64 {
        self.instruction_units
    }

    /// Return the response-byte reserve excluded from ordinary result work.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn response_bytes(self) -> u64 {
        self.response_bytes
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

    /// Return the absolute ceiling for one maintained resource.
    #[must_use]
    pub(in crate::db) const fn limit(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.limits[resource_index(resource)]
    }

    /// Return the reserve retained for typed failure construction.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn failure_headroom(&self) -> HardExecutionFailureHeadroom {
        self.failure_headroom
    }

    /// Construct a uniform test-only profile without exposing limit minting publicly.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn uniform_for_tests(
        limit: u64,
        failure_headroom: HardExecutionFailureHeadroom,
    ) -> Self {
        Self::new([limit; RESOURCE_COUNT], failure_headroom)
    }

    /// Replace one resource ceiling in a test-only injected profile.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn with_limit_for_tests(
        mut self,
        resource: DiagnosticExecutionBudgetResource,
        limit: u64,
    ) -> Self {
        self.limits[resource_index(resource)] = limit;
        self
    }
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

    #[cfg(feature = "diagnostics")]
    #[must_use]
    pub(in crate::db) const fn normalized_shape_fingerprint_prefix(self) -> u64 {
        self.normalized_shape_fingerprint_prefix
    }
}

/// Build literal-free attribution for one prepared read shape.
pub(in crate::db::executor) fn prepared_read_execution_context(
    plan: &SharedPreparedExecutionPlan,
    lane: DiagnosticExecutionLane,
) -> HardExecutionContext {
    HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        lane,
        plan.execution_shape_fingerprint_prefix(),
    )
}

/// Derive one literal-free shape prefix while immutable prepared residents are built.
pub(in crate::db::executor) fn read_shape_fingerprint_prefix(
    authority: &EntityAuthority,
    logical: &crate::db::query::plan::AccessPlannedQuery,
) -> u64 {
    let fingerprint = authority.accepted_schema_fingerprint();
    let mut prefix = u64::from_be_bytes([
        fingerprint[0],
        fingerprint[1],
        fingerprint[2],
        fingerprint[3],
        fingerprint[4],
        fingerprint[5],
        fingerprint[6],
        fingerprint[7],
    ]) ^ authority.entity_tag().value().rotate_left(17);
    let scalar = logical.scalar_plan();
    prefix ^= u64::from(logical.has_residual_filter_predicate()).rotate_left(7);
    prefix ^= u64::from(scalar.distinct).rotate_left(11);
    prefix ^=
        usize_as_u64(scalar.order.as_ref().map_or(0, |order| order.fields.len())).rotate_left(23);
    prefix ^= usize_as_u64(
        logical
            .scalar_projection_plan()
            .map_or(0, <[crate::db::query::plan::expr::CompiledExpr]>::len),
    )
    .rotate_left(31);
    prefix ^= usize_as_u64(logical.grouped_aggregate_execution_specs().map_or(
        0,
        <[crate::db::query::plan::GroupedAggregateExecutionSpec]>::len,
    ))
    .rotate_left(41);

    prefix
}

/// Build bounded attribution for a direct read terminal without a full plan.
pub(in crate::db) const fn direct_read_execution_context(
    authority: &EntityAuthority,
    lane: DiagnosticExecutionLane,
    shape_domain: u64,
) -> HardExecutionContext {
    let fingerprint = authority.accepted_schema_fingerprint();
    let prefix = u64::from_be_bytes([
        fingerprint[0],
        fingerprint[1],
        fingerprint[2],
        fingerprint[3],
        fingerprint[4],
        fingerprint[5],
        fingerprint[6],
        fingerprint[7],
    ]) ^ authority.entity_tag().value().rotate_left(17)
        ^ shape_domain;

    HardExecutionContext::new(DiagnosticExecutionBudgetScope::Execution, lane, prefix)
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

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn new_for_tests(
        budget: HardExecutionBudget,
        context: HardExecutionContext,
    ) -> Self {
        debug_assert!(budget.failure_headroom.is_reserved());
        Self {
            budget: HardExecutionBudgetAuthority::TestOwned(Box::new(budget)),
            context,
            request_scope: None,
            observed: [0; RESOURCE_COUNT],
            last_instruction_counter: None,
            charges_since_instruction_watermark: 0,
        }
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

    /// Return the retained cumulative usage, including rejected work attempts.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn observed(&self, resource: DiagnosticExecutionBudgetResource) -> u64 {
        self.observed[resource_index(resource)]
    }

    /// Sample the current IC instruction watermark at a bounded operator seam.
    pub(in crate::db) fn check_instruction_watermark(
        &mut self,
    ) -> Result<(), ExecutionBudgetExceeded> {
        let current = local_instruction_counter();
        let delta = self
            .last_instruction_counter
            .map_or(0, |previous| current.saturating_sub(previous));
        self.last_instruction_counter = Some(current);
        self.charges_since_instruction_watermark = 0;
        self.charge_raw(DiagnosticExecutionBudgetResource::InstructionUnits, delta)
    }

    /// Finish instruction accounting only after a maintained loop opened a watermark.
    pub(in crate::db) fn finish_instruction_watermark(
        &mut self,
    ) -> Result<(), ExecutionBudgetExceeded> {
        if self.last_instruction_counter.is_none() {
            return Ok(());
        }

        self.check_instruction_watermark()
    }

    /// Publish one bounded request diagnostic observation after execution.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn finish_request_diagnostics(&self) {
        let Some(scope) = self.request_scope.as_ref() else {
            return;
        };
        scope.record_execution(
            self.context,
            RequestDiagnosticResourceUsage {
                keys_visited: self.observed
                    [resource_index(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited)],
                rows_visited: self.observed
                    [resource_index(DiagnosticExecutionBudgetResource::RowsVisited)],
                rows_returned: self.observed
                    [resource_index(DiagnosticExecutionBudgetResource::ResultRows)],
                stored_bytes_read: self.observed
                    [resource_index(DiagnosticExecutionBudgetResource::StoredBytesRead)],
                decoded_bytes: self.observed
                    [resource_index(DiagnosticExecutionBudgetResource::DecodedBytes)],
                materialized_bytes: self.observed
                    [resource_index(DiagnosticExecutionBudgetResource::MaterializedBytes)],
                result_bytes: self.observed
                    [resource_index(DiagnosticExecutionBudgetResource::ResultBytes)],
            },
        );
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn request_diagnostics_enabled(&self) -> bool {
        self.request_scope
            .as_ref()
            .is_some_and(RequestExecutionScope::diagnostics_enabled)
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn record_exact_key_hashes(&self, hashes: &[[u8; 16]]) {
        if let Some(scope) = self.request_scope.as_ref() {
            scope.record_exact_key_hashes(self.context, hashes);
        }
    }

    /// Return the profile's failure-construction reserve.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn failure_headroom(&self) -> HardExecutionFailureHeadroom {
        self.budget.budget().failure_headroom()
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
pub(in crate::db::executor) fn with_read_execution_budget<T>(
    request_scope: &RequestExecutionScope,
    context: HardExecutionContext,
    run: impl FnOnce() -> Result<T, InternalError>,
) -> Result<T, InternalError> {
    with_execution_budget(
        HardExecutionBudgetTracker::new_with_request_scope(
            &READ_HARD_BUDGET,
            context,
            request_scope,
        ),
        run,
        std::convert::identity,
    )
}

/// Charge one maintained physical resource in the innermost active execution.
pub(in crate::db::executor) fn charge_current_execution_budget(
    resource: DiagnosticExecutionBudgetResource,
    amount: u64,
) -> Result<(), InternalError> {
    if amount == 0 {
        return Ok(());
    }
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let mut budget = budget
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let Some(budget) = budget.as_mut() else {
            return Ok(());
        };

        budget
            .charge_periodic(resource, amount)
            .map_err(InternalError::from)
    })
}

/// Charge two maintained physical resources through one active-budget lookup.
///
/// The charges retain their declared order: if the first charge exhausts its
/// budget, the second is not applied, matching two sequential calls to
/// [`charge_current_execution_budget`].
pub(in crate::db::executor) fn charge_current_execution_budget_pair(
    first: (DiagnosticExecutionBudgetResource, u64),
    second: (DiagnosticExecutionBudgetResource, u64),
) -> Result<(), InternalError> {
    if first.1 == 0 && second.1 == 0 {
        return Ok(());
    }
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let mut budget = budget
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?;
        let Some(budget) = budget.as_mut() else {
            return Ok(());
        };

        if first.1 != 0 {
            budget
                .charge_periodic(first.0, first.1)
                .map_err(InternalError::from)?;
        }
        if second.1 != 0 {
            budget
                .charge_periodic(second.0, second.1)
                .map_err(InternalError::from)?;
        }
        Ok(())
    })
}

/// Build one typed budget failure from a stricter operator-local hard limit.
///
/// Grouped planning can impose a lower retained-state ceiling than the root
/// request budget. The failure still belongs to the active execution and must
/// preserve its scope, lane, and normalized shape rather than degrading to an
/// unclassified internal error.
pub(in crate::db::executor) fn current_execution_budget_exceeded(
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
pub(in crate::db::executor) struct ExecutionBudgetUsage {
    observed: [u64; RESOURCE_COUNT],
}

impl ExecutionBudgetUsage {
    /// Return one cumulative resource observation.
    #[must_use]
    pub(in crate::db::executor) const fn observed(
        self,
        resource: DiagnosticExecutionBudgetResource,
    ) -> u64 {
        self.observed[resource_index(resource)]
    }
}

/// Snapshot the innermost execution's cumulative counters.
///
/// Scalar page execution always runs below a hard read budget. Requiring that
/// budget here keeps page-local accounting from becoming a second, optional
/// source of physical-work truth.
pub(in crate::db::executor) fn current_execution_budget_usage()
-> Result<ExecutionBudgetUsage, InternalError> {
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
pub(in crate::db::executor) fn charge_sort_work<R>(entries: usize) -> Result<(), InternalError> {
    let comparisons_per_entry = if entries <= 1 {
        0
    } else {
        usize::BITS.saturating_sub(entries.saturating_sub(1).leading_zeros())
    };
    let comparisons = entries.saturating_mul(comparisons_per_entry as usize);
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortEntries,
        usize_as_u64(entries),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortComparisons,
        usize_as_u64(comparisons),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortTemporaryBytes,
        usize_as_u64(entries.saturating_mul(std::mem::size_of::<R>())),
    )
}

/// Sample only the instruction watermark for the innermost active execution.
pub(in crate::db::executor) fn finish_current_execution_instruction_watermark()
-> Result<(), InternalError> {
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

/// Admit one optional diagnostics update, suppressing detail after its finite allowance.
#[must_use]
pub(in crate::db::executor) fn admit_current_execution_diagnostic_step() -> bool {
    ACTIVE_EXECUTION_BUDGET.with(|budget| {
        let Ok(mut budget) = budget.try_borrow_mut() else {
            return false;
        };
        let Some(budget) = budget.as_mut() else {
            return true;
        };

        budget
            .charge_periodic(DiagnosticExecutionBudgetResource::DiagnosticSteps, 1)
            .is_ok()
    })
}

/// Charge one fully materialized runtime-value result before response shaping.
pub(in crate::db::executor) fn charge_runtime_value_rows(
    rows: &[Vec<Value>],
) -> Result<(), InternalError> {
    let (bytes, nested_steps) = rows.iter().flatten().fold((0_u64, 0_u64), |total, value| {
        let value_work = runtime_value_work(value);
        (
            total.0.saturating_add(value_work.0),
            total.1.saturating_add(value_work.1),
        )
    });
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::ResultRows,
        usize_as_u64(rows.len()),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        nested_steps,
    )?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::MaterializedBytes, bytes)?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultBytes, bytes)
}

/// Charge grouped runtime rows before cursor and public DTO finalization.
pub(in crate::db::executor) fn charge_runtime_grouped_rows(
    rows: &[RuntimeGroupedRow],
) -> Result<(), InternalError> {
    let (bytes, nested_steps) = rows
        .iter()
        .flat_map(|row| row.group_key().iter().chain(row.aggregate_values()))
        .fold((0_u64, 0_u64), |total, value| {
            let value_work = runtime_value_work(value);
            (
                total.0.saturating_add(value_work.0),
                total.1.saturating_add(value_work.1),
            )
        });
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::ResultRows,
        usize_as_u64(rows.len()),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        nested_steps,
    )?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::MaterializedBytes, bytes)?;
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultBytes, bytes)
}

pub(in crate::db::executor) const RUNTIME_VALUE_NODE_OVERHEAD_BYTES: u64 = 32;

pub(in crate::db::executor) fn runtime_value_work(value: &Value) -> (u64, u64) {
    const VALUE_OVERHEAD: u64 = RUNTIME_VALUE_NODE_OVERHEAD_BYTES;
    match value {
        Value::Blob(value) => (VALUE_OVERHEAD.saturating_add(usize_as_u64(value.len())), 1),
        Value::Text(value) => (VALUE_OVERHEAD.saturating_add(usize_as_u64(value.len())), 1),
        Value::IntBig(value) => (
            VALUE_OVERHEAD.saturating_add(usize_as_u64(value.to_leb128().len())),
            1,
        ),
        Value::NatBig(value) => (
            VALUE_OVERHEAD.saturating_add(usize_as_u64(value.to_leb128().len())),
            1,
        ),
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
    }
}

fn with_execution_budget<T, E>(
    mut tracker: HardExecutionBudgetTracker,
    run: impl FnOnce() -> Result<T, E>,
    map_internal: fn(InternalError) -> E,
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
    let final_budget_result = finish_current_execution_instruction_watermark();
    let removed = ACTIVE_EXECUTION_BUDGET.with(|budget| {
        budget
            .try_borrow_mut()
            .map_err(|_| InternalError::query_executor_invariant())?
            .take()
            .ok_or_else(InternalError::query_executor_invariant)
    });
    let removed = removed.map_err(map_internal)?;
    #[cfg(feature = "diagnostics")]
    removed.finish_request_diagnostics();
    #[cfg(not(feature = "diagnostics"))]
    let _ = removed;
    final_budget_result.map_err(map_internal)?;

    result
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(target_arch = "wasm32")]
fn local_instruction_counter() -> u64 {
    crate::runtime::performance_counter(1)
}

#[cfg(not(target_arch = "wasm32"))]
const fn local_instruction_counter() -> u64 {
    0
}

#[cfg(test)]
pub(in crate::db) fn with_query_execution_budget_for_tests<T>(
    budget: HardExecutionBudget,
    context: HardExecutionContext,
    run: impl FnOnce() -> Result<T, QueryError>,
) -> Result<T, QueryError> {
    with_execution_budget(
        HardExecutionBudgetTracker::new_for_tests(budget, context),
        run,
        QueryError::execute,
    )
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
        DiagnosticExecutionBudgetResource::DiagnosticSteps => 17,
        DiagnosticExecutionBudgetResource::ResultRows => 18,
        DiagnosticExecutionBudgetResource::ResultBytes => 19,
        DiagnosticExecutionBudgetResource::InstructionUnits => 20,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::RequestExecutionRoot;
    use icydb_diagnostic_code::{DiagnosticDetail, DiagnosticFactTag, RuntimeBoundaryCode};

    const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);
    const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::PublicRead,
        0x0102_0304_0506_0708,
    );
    static PAIR_FIRST_FAILURE_BUDGET: HardExecutionBudget =
        HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 0);

    #[test]
    fn every_resource_charges_monotonically_and_retains_rejected_work() {
        let budget = HardExecutionBudget::new([1; RESOURCE_COUNT], TEST_HEADROOM);
        for resource in DiagnosticExecutionBudgetResource::ALL {
            let mut tracker = HardExecutionBudgetTracker::new_for_tests(budget, TEST_CONTEXT);
            tracker
                .precharge(resource, 1)
                .expect("work at the hard ceiling should be admitted");
            let exhausted = tracker
                .charge_periodic(resource, 1)
                .expect_err("work above the hard ceiling should reject");

            assert_eq!(exhausted.resource(), resource);
            assert_eq!(exhausted.limit(), 1);
            assert_eq!(exhausted.observed(), 2);
            assert_eq!(tracker.observed(resource), 2);
        }
    }

    #[test]
    fn arithmetic_overflow_is_exhaustion_and_never_refunds_usage() {
        let budget = HardExecutionBudget::new([u64::MAX; RESOURCE_COUNT], TEST_HEADROOM);
        let resource = DiagnosticExecutionBudgetResource::PlanningSteps;
        let mut tracker = HardExecutionBudgetTracker::new_for_tests(budget, TEST_CONTEXT);
        tracker
            .precharge(resource, u64::MAX)
            .expect("the representable ceiling should be admitted");
        let exhausted = tracker
            .charge_periodic(resource, 1)
            .expect_err("counter overflow must reject");

        assert_eq!(exhausted.observed(), u64::MAX);
        assert_eq!(tracker.observed(resource), u64::MAX);
    }

    #[test]
    fn exhaustion_maps_to_complete_typed_diagnostic_facts() {
        let budget = HardExecutionBudget::new([0; RESOURCE_COUNT], TEST_HEADROOM);
        let mut tracker = HardExecutionBudgetTracker::new_for_tests(budget, TEST_CONTEXT);
        let exhausted = tracker
            .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1)
            .expect_err("zero query allowance should reject");
        let error = InternalError::from(exhausted);

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (DiagnosticFactTag::BudgetResource, 1),
                (DiagnosticFactTag::Limit, 0),
                (DiagnosticFactTag::Actual, 1),
                (DiagnosticFactTag::ExecutionBudgetScope, 1),
                (DiagnosticFactTag::ExecutionLane, 1),
                (
                    DiagnosticFactTag::QueryShapeFingerprintPrefix,
                    0x0102_0304_0506_0708,
                ),
            ],
        );
        assert_eq!(tracker.failure_headroom(), TEST_HEADROOM);
        assert_eq!(TEST_HEADROOM.instruction_units(), 500);
        assert_eq!(TEST_HEADROOM.response_bytes(), 256);
    }

    #[test]
    fn diagnostic_exhaustion_suppresses_optional_detail_without_failing_execution() {
        let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::DiagnosticSteps, 0);
        let result = with_execution_budget(
            HardExecutionBudgetTracker::new_for_tests(budget, TEST_CONTEXT),
            || {
                assert!(!admit_current_execution_diagnostic_step());
                charge_current_execution_budget(DiagnosticExecutionBudgetResource::ResultRows, 1)?;
                Ok::<_, InternalError>(())
            },
            std::convert::identity,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn paired_budget_charges_preserve_sequential_failure_order() {
        let root = RequestExecutionRoot::new_for_tests(HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            TEST_HEADROOM,
        ));
        let error = with_execution_budget(
            HardExecutionBudgetTracker::new_with_request_scope(
                &PAIR_FIRST_FAILURE_BUDGET,
                TEST_CONTEXT,
                &root.scope(),
            ),
            || {
                charge_current_execution_budget_pair(
                    (DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited, 1),
                    (DiagnosticExecutionBudgetResource::CursorSteps, 1),
                )
            },
            std::convert::identity,
        )
        .expect_err("the first paired charge should retain its ordinary hard limit");

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert_eq!(
            root.observed(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited),
            1,
        );
        assert_eq!(
            root.observed(DiagnosticExecutionBudgetResource::CursorSteps),
            0,
            "the second charge must not run after the first fails",
        );
    }

    #[test]
    fn derived_execution_trackers_cannot_reset_the_shared_request_scope() {
        let request_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::QueryExecutions, 2);
        let root = RequestExecutionRoot::new_for_tests(request_budget);
        let scope = root.scope();

        for _ in 0..2 {
            HardExecutionBudgetTracker::new_with_request_scope(
                &READ_HARD_BUDGET,
                TEST_CONTEXT,
                &scope,
            )
            .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1)
            .expect("work at the aggregate request ceiling should admit");
        }
        let exhausted = HardExecutionBudgetTracker::new_with_request_scope(
            &READ_HARD_BUDGET,
            TEST_CONTEXT,
            &root.scope(),
        )
        .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1)
        .expect_err("a fresh derived scope handle must not reset request counters");

        assert_eq!(exhausted.scope(), DiagnosticExecutionBudgetScope::Request);
        assert_eq!(exhausted.observed(), 3);
        assert_eq!(
            root.observed(DiagnosticExecutionBudgetResource::QueryExecutions),
            3,
        );
    }

    #[test]
    fn failures_retries_and_nested_executions_remain_charged() {
        let request_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
        let root = RequestExecutionRoot::new_for_tests(request_budget);
        let scope = root.scope();
        let failed = with_execution_budget(
            HardExecutionBudgetTracker::new_with_request_scope(
                &READ_HARD_BUDGET,
                TEST_CONTEXT,
                &scope,
            ),
            || Err::<(), _>(InternalError::query_executor_invariant()),
            std::convert::identity,
        );
        assert!(failed.is_err());

        let retried = with_execution_budget(
            HardExecutionBudgetTracker::new_with_request_scope(
                &READ_HARD_BUDGET,
                TEST_CONTEXT,
                &scope,
            ),
            || {
                with_execution_budget(
                    HardExecutionBudgetTracker::new_with_request_scope(
                        &READ_HARD_BUDGET,
                        TEST_CONTEXT,
                        &scope,
                    ),
                    || Ok::<_, InternalError>(()),
                    std::convert::identity,
                )
            },
            std::convert::identity,
        );
        assert!(retried.is_ok());
        assert_eq!(
            root.observed(DiagnosticExecutionBudgetResource::QueryExecutions),
            3,
            "the failed attempt, retry, and nested execution all stay charged",
        );
    }

    #[test]
    fn planning_and_compilation_charges_share_the_request_scope() {
        let request_budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::PlanningSteps, 2)
            .with_limit_for_tests(DiagnosticExecutionBudgetResource::PlanCompilations, 1);
        let root = RequestExecutionRoot::new_for_tests(request_budget);
        let first_scope = root.scope();
        first_scope
            .charge(
                TEST_CONTEXT,
                DiagnosticExecutionBudgetResource::PlanningSteps,
                1,
            )
            .expect("the first planning operation should be admitted");
        first_scope
            .charge(
                TEST_CONTEXT,
                DiagnosticExecutionBudgetResource::PlanCompilations,
                1,
            )
            .expect("the first compilation should be admitted");

        let second_scope = root.scope();
        second_scope
            .charge(
                TEST_CONTEXT,
                DiagnosticExecutionBudgetResource::PlanningSteps,
                1,
            )
            .expect("planning at the aggregate ceiling should be admitted");
        let exhausted = second_scope
            .charge(
                TEST_CONTEXT,
                DiagnosticExecutionBudgetResource::PlanCompilations,
                1,
            )
            .expect_err("a derived scope must retain the earlier compilation charge");

        assert_eq!(exhausted.scope(), DiagnosticExecutionBudgetScope::Request);
        assert_eq!(exhausted.limit(), 1);
        assert_eq!(exhausted.observed(), 2);
        assert_eq!(
            root.observed(DiagnosticExecutionBudgetResource::PlanningSteps),
            2,
        );
        assert_eq!(
            root.observed(DiagnosticExecutionBudgetResource::PlanCompilations),
            2,
        );
    }

    #[test]
    fn toko_shaped_n_plus_one_work_fails_at_the_aggregate_request_boundary() {
        let root = RequestExecutionRoot::__new_runtime_root();
        let scope = root.scope();
        let mut rejected = None;
        for _ in 0..257 {
            let charge = HardExecutionBudgetTracker::new_with_request_scope(
                &READ_HARD_BUDGET,
                HardExecutionContext::new(
                    DiagnosticExecutionBudgetScope::Execution,
                    DiagnosticExecutionLane::TrustedRead,
                    0x746f_6b6f_2d6e_2b31,
                ),
                &scope,
            )
            .precharge(DiagnosticExecutionBudgetResource::QueryExecutions, 1);
            if let Err(exhausted) = charge {
                rejected = Some(exhausted);
                break;
            }
        }
        let exhausted = rejected.expect("the 257th individually bounded query should reject");

        assert_eq!(exhausted.scope(), DiagnosticExecutionBudgetScope::Request);
        assert_eq!(exhausted.lane(), DiagnosticExecutionLane::TrustedRead);
        assert_eq!(exhausted.limit(), 256);
        assert_eq!(exhausted.observed(), 257);
    }
}
