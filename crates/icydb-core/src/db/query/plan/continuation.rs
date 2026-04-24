//! Module: query::plan::continuation
//! Responsibility: planner-owned continuation contracts and grouped/scalar resume windows.
//! Does not own: cursor token decoding internals or executor-side re-derivation policy.
//! Boundary: emits immutable continuation semantics consumed by runtime layers.

use crate::{
    db::{
        access::{AccessPlan, lower_executable_access_plan},
        cursor::{ContinuationSignature, CursorPlanError, GroupedPlannedCursor, PlannedCursor},
        query::plan::{
            AccessPlannedQuery, ExecutionOrderContract, ExecutionShapeSignature,
            GroupedCursorPolicyViolation, grouped_cursor_policy_violation,
        },
    },
    value::Value,
};

///
/// PlannedContinuationContract
///
/// Immutable planner-owned continuation semantic contract.
/// Runtime layers consume this contract and must not re-derive continuation
/// shape, window, or grouped/scalar compatibility semantics independently.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PlannedContinuationContract {
    pub(in crate::db) shape_signature: ExecutionShapeSignature,
    pub(in crate::db) boundary_arity: usize,
    pub(in crate::db) window_size: usize,
    pub(in crate::db) order_contract: ExecutionOrderContract,
    page_limit: Option<usize>,
    access: AccessPlan<Value>,
    grouped_cursor_policy_violation: Option<GroupedCursorPolicyViolation>,
}

///
/// GroupedContinuationWindow
///
/// Planner-contract grouped continuation paging window.
/// Carries grouped page limit/offset/window progression semantics derived once
/// from immutable continuation contract state plus validated grouped cursor state.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct GroupedContinuationWindow {
    limit: Option<usize>,
    initial_offset_for_page: usize,
    selection_bound: Option<usize>,
    resume_initial_offset: u32,
    resume_boundary: Option<Value>,
}

impl GroupedContinuationWindow {
    // Construct one immutable grouped continuation paging window.
    const fn new(
        limit: Option<usize>,
        initial_offset_for_page: usize,
        selection_bound: Option<usize>,
        resume_initial_offset: u32,
        resume_boundary: Option<Value>,
    ) -> Self {
        Self {
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        }
    }

    /// Decompose grouped continuation window fields into grouped-fold tuple order.
    #[must_use]
    pub(in crate::db) fn into_parts(
        self,
    ) -> (Option<usize>, usize, Option<usize>, u32, Option<Value>) {
        (
            self.limit,
            self.initial_offset_for_page,
            self.selection_bound,
            self.resume_initial_offset,
            self.resume_boundary,
        )
    }
}

///
/// GroupedWindowProjection
///
/// Internal grouped paging-window projection assembled from one planned
/// continuation contract plus validated grouped cursor state.
/// This keeps grouped resume arithmetic in one local phase instead of spreading
/// it across the outward `GroupedContinuationWindow` construction path.
///

struct GroupedWindowProjection {
    limit: Option<usize>,
    initial_offset_for_page: usize,
    selection_bound: Option<usize>,
    resume_initial_offset: u32,
    resume_boundary: Option<Value>,
}

impl GroupedWindowProjection {
    // Project grouped window arithmetic from planner-owned continuation state.
    fn from_contract_and_cursor(
        contract: &PlannedContinuationContract,
        cursor: &GroupedPlannedCursor,
    ) -> Self {
        let resume_initial_offset = if cursor.is_empty() {
            contract.effective_offset(false)
        } else {
            cursor.initial_offset()
        };
        let initial_offset_for_page = if cursor.is_empty() {
            contract.window_size()
        } else {
            0
        };
        let selection_bound = contract.page_limit().and_then(|limit| {
            limit
                .checked_add(initial_offset_for_page)
                .and_then(|count| count.checked_add(1))
        });
        let resume_boundary = cursor
            .last_group_key()
            .map(|last_group_key| Value::List(last_group_key.to_vec()));

        Self {
            limit: contract.page_limit(),
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        }
    }

    // Finalize the outward grouped paging-window DTO.
    fn into_window(self) -> GroupedContinuationWindow {
        GroupedContinuationWindow::new(
            self.limit,
            self.initial_offset_for_page,
            self.selection_bound,
            self.resume_initial_offset,
            self.resume_boundary,
        )
    }
}

///
/// GroupedCursorAction
///
/// Internal grouped continuation action discriminator.
/// Keeps grouped-plan requirement messages and cursor-policy gating shared
/// across grouped cursor preparation, grouped cursor revalidation, and grouped
/// paging-window projection so those entrypoints do not drift independently.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedCursorAction {
    Prepare,
    Revalidate,
    PagingWindow,
}

impl GroupedCursorAction {
    // Return the canonical grouped-plan requirement text for one grouped cursor action.
    const fn grouped_plan_required_message(self) -> &'static str {
        match self {
            Self::Prepare => "grouped cursor preparation requires grouped logical plans",
            Self::Revalidate => "grouped cursor revalidation requires grouped logical plans",
            Self::PagingWindow => "grouped paging window requires grouped logical plans",
        }
    }
}

/// Derive the effective offset under cursor-window semantics.
///
/// Offset applies only for initial requests. Once a continuation cursor is
/// present, offset must be treated as `0` to avoid double-skipping rows.
#[must_use]
pub(in crate::db) const fn effective_offset_for_cursor_window(
    window_size: u32,
    cursor_present: bool,
) -> u32 {
    if cursor_present { 0 } else { window_size }
}

///
/// ScalarAccessWindowPlan
///
/// Planner-owned scalar access-window DTO.
/// Carries effective offset and optional page limit so downstream route/runtime
/// layers consume one canonical window projection contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarAccessWindowPlan {
    effective_offset: u32,
    limit: Option<u32>,
}

impl ScalarAccessWindowPlan {
    /// Construct one planner-projected scalar access-window plan.
    #[must_use]
    pub(in crate::db) const fn new(effective_offset: u32, limit: Option<u32>) -> Self {
        Self {
            effective_offset,
            limit,
        }
    }

    /// Return optional page limit.
    #[must_use]
    pub(in crate::db) const fn limit(self) -> Option<u32> {
        self.limit
    }

    /// Return optional keep-count horizon (`effective_offset + limit`).
    #[must_use]
    pub(in crate::db) fn keep_count(self) -> Option<usize> {
        let limit = self.limit?;
        let offset = usize::try_from(self.effective_offset).unwrap_or(usize::MAX);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);

        Some(offset.saturating_add(limit))
    }

    /// Return effective offset projected into access-window lower-bound form.
    #[must_use]
    pub(in crate::db) fn lower_bound(self) -> usize {
        usize::try_from(self.effective_offset).unwrap_or(usize::MAX)
    }

    /// Return bounded fetch count including one continuation lookahead row.
    ///
    /// This projection keeps lookahead arithmetic planner-owned so route/runtime
    /// layers consume one explicit fetch contract without policy branching.
    #[must_use]
    pub(in crate::db) fn fetch_count(self) -> Option<usize> {
        let keep_count = self.keep_count();
        if self.limit.is_none() {
            return keep_count;
        }
        if self.limit == Some(0) {
            return Some(0);
        }

        keep_count.map(|fetch| fetch.saturating_add(1))
    }
}

impl PlannedContinuationContract {
    #[must_use]
    pub(in crate::db) const fn new(
        shape_signature: ExecutionShapeSignature,
        boundary_arity: usize,
        window_size: usize,
        order_contract: ExecutionOrderContract,
        page_limit: Option<usize>,
        access: AccessPlan<Value>,
        grouped_cursor_policy_violation: Option<GroupedCursorPolicyViolation>,
    ) -> Self {
        Self {
            shape_signature,
            boundary_arity,
            window_size,
            order_contract,
            page_limit,
            access,
            grouped_cursor_policy_violation,
        }
    }

    #[must_use]
    pub(in crate::db) const fn is_grouped(&self) -> bool {
        self.order_contract.is_grouped()
    }

    /// Borrow the immutable execution-order contract projected by planning.
    #[must_use]
    pub(in crate::db) const fn order_contract(&self) -> &ExecutionOrderContract {
        &self.order_contract
    }

    /// Borrow planner-projected scalar page limit under this continuation contract.
    #[must_use]
    pub(in crate::db) const fn page_limit(&self) -> Option<usize> {
        self.page_limit
    }

    /// Borrow planner-projected window size (`offset`) under this contract.
    #[must_use]
    pub(in crate::db) const fn window_size(&self) -> usize {
        self.window_size
    }

    /// Borrow planner-projected access plan used for continuation consistency checks.
    #[must_use]
    pub(in crate::db) const fn access_plan(&self) -> &AccessPlan<Value> {
        &self.access
    }

    /// Borrow grouped cursor-policy violation, if continuation is disallowed for grouped shape.
    #[must_use]
    pub(in crate::db) const fn grouped_cursor_policy_violation(
        &self,
    ) -> Option<GroupedCursorPolicyViolation> {
        self.grouped_cursor_policy_violation
    }

    #[must_use]
    pub(in crate::db) const fn continuation_signature(&self) -> ContinuationSignature {
        self.shape_signature.continuation_signature()
    }

    #[must_use]
    pub(in crate::db) const fn boundary_arity(&self) -> usize {
        self.boundary_arity
    }

    /// Return expected initial offset encoded in continuation tokens.
    #[must_use]
    pub(in crate::db) fn expected_initial_offset(&self) -> u32 {
        u32::try_from(self.window_size()).unwrap_or(u32::MAX)
    }

    /// Return effective offset for this request under cursor-window semantics.
    ///
    /// Offset is consumed only for initial requests; continuation requests resume
    /// from cursor boundary and therefore use offset `0`.
    #[must_use]
    pub(in crate::db) fn effective_offset(&self, cursor_present: bool) -> u32 {
        effective_offset_for_cursor_window(self.expected_initial_offset(), cursor_present)
    }

    /// Validate scalar cursor bytes against this immutable continuation contract.
    pub(in crate::db) fn prepare_scalar_cursor(
        &self,
        entity_path: &'static str,
        entity_tag: crate::types::EntityTag,
        entity_model: &crate::model::entity::EntityModel,
        bytes: Option<&[u8]>,
    ) -> Result<PlannedCursor, CursorPlanError> {
        if self.is_grouped() {
            return Err(CursorPlanError::continuation_cursor_invariant(
                "grouped plans require grouped cursor preparation",
            ));
        }

        crate::db::cursor::prepare_cursor(
            lower_executable_access_plan(self.access_plan())
                .as_path()
                .cloned(),
            entity_path,
            entity_tag,
            entity_model,
            self.order_contract.order_spec(),
            self.order_contract.direction(),
            self.continuation_signature(),
            self.expected_initial_offset(),
            bytes,
        )
    }

    /// Validate grouped cursor bytes against this immutable continuation contract.
    #[cfg(test)]
    pub(in crate::db) fn prepare_grouped_cursor(
        &self,
        entity_path: &'static str,
        bytes: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, CursorPlanError> {
        self.validate_grouped_cursor_contract(GroupedCursorAction::Prepare, bytes.is_some())?;

        crate::db::cursor::prepare_grouped_cursor(
            entity_path,
            self.order_contract.order_spec(),
            self.order_contract.direction(),
            self.continuation_signature(),
            self.expected_initial_offset(),
            bytes,
        )
    }

    /// Validate one already-decoded grouped cursor token against this immutable continuation contract.
    pub(in crate::db) fn prepare_grouped_cursor_token(
        &self,
        entity_path: &'static str,
        cursor: Option<crate::db::cursor::GroupedContinuationToken>,
    ) -> Result<GroupedPlannedCursor, CursorPlanError> {
        self.validate_grouped_cursor_contract(GroupedCursorAction::Prepare, cursor.is_some())?;

        crate::db::cursor::prepare_grouped_cursor_token(
            entity_path,
            self.order_contract.order_spec(),
            self.order_contract.direction(),
            self.continuation_signature(),
            self.expected_initial_offset(),
            cursor,
        )
    }

    /// Revalidate scalar cursor state against this immutable continuation contract.
    pub(in crate::db) fn revalidate_scalar_cursor(
        &self,
        entity_tag: crate::types::EntityTag,
        entity_model: &crate::model::entity::EntityModel,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, CursorPlanError> {
        if self.is_grouped() {
            return Err(CursorPlanError::continuation_cursor_invariant(
                "grouped plans require grouped cursor revalidation",
            ));
        }

        crate::db::cursor::revalidate_cursor(
            lower_executable_access_plan(self.access_plan())
                .as_path()
                .cloned(),
            entity_tag,
            entity_model,
            self.order_contract.order_spec(),
            self.order_contract.direction(),
            self.expected_initial_offset(),
            cursor,
        )
    }

    /// Revalidate grouped cursor state against this immutable continuation contract.
    pub(in crate::db) fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, CursorPlanError> {
        self.validate_grouped_cursor_contract(GroupedCursorAction::Revalidate, !cursor.is_empty())?;

        crate::db::cursor::revalidate_grouped_cursor(self.expected_initial_offset(), cursor)
    }

    /// Derive grouped paging contracts from validated grouped cursor state.
    pub(in crate::db) fn project_grouped_paging_window(
        &self,
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedContinuationWindow, CursorPlanError> {
        self.validate_grouped_cursor_contract(
            GroupedCursorAction::PagingWindow,
            !cursor.is_empty(),
        )?;

        Ok(GroupedWindowProjection::from_contract_and_cursor(self, cursor).into_window())
    }

    // Enforce grouped continuation ownership once for all grouped cursor entrypoints.
    fn validate_grouped_cursor_contract(
        &self,
        action: GroupedCursorAction,
        cursor_applied: bool,
    ) -> Result<(), CursorPlanError> {
        if !self.is_grouped() {
            return Err(CursorPlanError::continuation_cursor_invariant(
                action.grouped_plan_required_message(),
            ));
        }

        self.validate_grouped_cursor_policy_if_applied(cursor_applied)
    }

    // Apply grouped cursor-policy violations only when continuation is actually reused.
    fn validate_grouped_cursor_policy_if_applied(
        &self,
        cursor_applied: bool,
    ) -> Result<(), CursorPlanError> {
        if !cursor_applied {
            return Ok(());
        }

        self.validate_grouped_cursor_policy()
    }

    fn validate_grouped_cursor_policy(&self) -> Result<(), CursorPlanError> {
        if let Some(violation) = self.grouped_cursor_policy_violation() {
            return Err(violation.into_cursor_plan_error());
        }

        Ok(())
    }
}

impl AccessPlannedQuery {
    /// Project planner-owned scalar access-window contract.
    #[must_use]
    pub(in crate::db) fn scalar_access_window_plan(
        &self,
        cursor_present: bool,
    ) -> ScalarAccessWindowPlan {
        let page_window = PlannedPageWindow::from_query(self);
        let effective_offset =
            effective_offset_for_cursor_window(page_window.offset_u32(), cursor_present);

        ScalarAccessWindowPlan::new(effective_offset, page_window.limit_u32())
    }

    /// Build one immutable continuation contract from planner-owned semantics.
    #[must_use]
    pub(in crate::db) fn planned_continuation_contract(
        &self,
        entity_path: &'static str,
    ) -> Option<PlannedContinuationContract> {
        if !self.scalar_plan().mode.is_load() {
            return None;
        }

        let page_window = PlannedPageWindow::from_query(self);
        let shape_signature = self.execution_shape_signature(entity_path);
        let boundary_arity = self.grouped_plan().map_or_else(
            || {
                self.scalar_plan()
                    .order
                    .as_ref()
                    .map_or(0, |order| order.fields.len())
            },
            |grouped| grouped.group.group_fields.len(),
        );
        let is_grouped = self.grouped_plan().is_some();
        let order_contract =
            ExecutionOrderContract::from_plan(is_grouped, self.scalar_plan().order.as_ref());
        let access = self.access.clone();
        let grouped_cursor_policy_violation = self
            .grouped_plan()
            .and_then(|grouped| grouped_cursor_policy_violation(grouped, true));

        Some(PlannedContinuationContract::new(
            shape_signature,
            boundary_arity,
            page_window.offset_usize(),
            order_contract,
            page_window.limit_usize(),
            access,
            grouped_cursor_policy_violation,
        ))
    }
}

// Freeze one planner-owned page-window view so scalar access-window planning
// and continuation-contract assembly do not each re-derive page offset/limit
// conversions from the same logical page spec.
struct PlannedPageWindow {
    offset: u32,
    limit: Option<u32>,
}

impl PlannedPageWindow {
    // Project the logical page window from one access-planned query.
    fn from_query(plan: &AccessPlannedQuery) -> Self {
        let page = plan.scalar_plan().page.as_ref();

        Self {
            offset: page.map_or(0, |page| page.offset),
            limit: page.and_then(|page| page.limit),
        }
    }

    // Borrow the canonical u32 page offset used by cursor-window semantics.
    const fn offset_u32(&self) -> u32 {
        self.offset
    }

    // Borrow the canonical u32 page limit used by scalar access-window planning.
    const fn limit_u32(&self) -> Option<u32> {
        self.limit
    }

    // Project the page offset into continuation-contract usize form.
    fn offset_usize(&self) -> usize {
        usize::try_from(self.offset).unwrap_or(usize::MAX)
    }

    // Project the page limit into continuation-contract usize form.
    fn limit_usize(&self) -> Option<usize> {
        self.limit
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))
    }
}
