use crate::{
    db::{
        access::AccessPlan,
        cursor::{
            ContinuationSignature, CursorPlanError, CursorValidationOutcome, GroupedPlannedCursor,
            PlannedCursor,
        },
        query::plan::{
            AccessPlannedQuery, ExecutionOrderContract, ExecutionShapeSignature,
            GroupedCursorPolicyViolation, grouped_cursor_policy_violation_for_continuation,
        },
    },
    error::InternalError,
    traits::{EntityKind, FieldValue},
    value::Value,
};

///
/// ContinuationContract
///
/// Immutable planner-owned continuation semantic contract.
/// Runtime layers consume this contract and must not re-derive continuation
/// shape, window, or grouped/scalar compatibility semantics independently.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ContinuationContract<K> {
    pub(in crate::db) shape_signature: ExecutionShapeSignature,
    pub(in crate::db) boundary_arity: usize,
    pub(in crate::db) window_size: usize,
    pub(in crate::db) order_contract: ExecutionOrderContract,
    page_limit: Option<usize>,
    access: AccessPlan<K>,
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
/// AccessWindowLookaheadPolicy
///
/// Planner-owned lookahead policy contract for scalar access-window projection.
/// Runtime layers select one policy and consume planner-projected fetch counts
/// without reconstructing lookahead arithmetic locally.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AccessWindowLookaheadPolicy {
    None,
    ExtraRow,
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

    /// Return effective offset under cursor-window semantics.
    #[must_use]
    pub(in crate::db) const fn effective_offset(self) -> u32 {
        self.effective_offset
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

    /// Return one fetch-count projection under one planner-owned lookahead policy.
    #[must_use]
    pub(in crate::db) fn fetch_count_for(
        self,
        lookahead_policy: AccessWindowLookaheadPolicy,
    ) -> Option<usize> {
        let keep_count = self.keep_count();
        if !matches!(lookahead_policy, AccessWindowLookaheadPolicy::ExtraRow)
            || self.limit.is_none()
        {
            return keep_count;
        }
        if self.limit == Some(0) {
            return Some(0);
        }

        keep_count.map(|fetch| fetch.saturating_add(1))
    }
}

impl<K: FieldValue + Clone> ContinuationContract<K> {
    #[must_use]
    pub(in crate::db) const fn new(
        shape_signature: ExecutionShapeSignature,
        boundary_arity: usize,
        window_size: usize,
        order_contract: ExecutionOrderContract,
        page_limit: Option<usize>,
        access: AccessPlan<K>,
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

    /// Borrow planner-projected access plan used for continuation compatibility checks.
    #[must_use]
    pub(in crate::db) const fn access_plan(&self) -> &AccessPlan<K> {
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

    /// Validate optional cursor bytes against this immutable continuation contract.
    pub(in crate::db) fn validate_cursor_bytes<E: EntityKind<Key = K>>(
        &self,
        bytes: Option<&[u8]>,
    ) -> Result<CursorValidationOutcome, CursorPlanError> {
        if self.is_grouped() && bytes.is_some() {
            self.validate_grouped_cursor_policy()?;
        }

        let access = if self.is_grouped() {
            None
        } else {
            self.access_plan().resolve_strategy().as_path().cloned()
        };

        crate::db::cursor::validate_cursor_compatibility::<E>(
            &self.order_contract,
            access,
            self.continuation_signature(),
            self.expected_initial_offset(),
            bytes,
        )
    }

    /// Validate scalar cursor bytes against this immutable continuation contract.
    pub(in crate::db) fn prepare_scalar_cursor<E: EntityKind<Key = K>>(
        &self,
        bytes: Option<&[u8]>,
    ) -> Result<PlannedCursor, CursorPlanError> {
        if self.is_grouped() {
            return Err(cursor_invariant_error(
                "grouped plans require grouped cursor preparation",
            ));
        }

        match self.validate_cursor_bytes::<E>(bytes)? {
            CursorValidationOutcome::Scalar(cursor) => Ok(*cursor),
            CursorValidationOutcome::Grouped(_) => Err(cursor_invariant_error(
                "grouped plans require grouped cursor preparation",
            )),
        }
    }

    /// Validate grouped cursor bytes against this immutable continuation contract.
    pub(in crate::db) fn prepare_grouped_cursor<E: EntityKind<Key = K>>(
        &self,
        bytes: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, CursorPlanError> {
        if !self.is_grouped() {
            return Err(cursor_invariant_error(
                "grouped cursor preparation requires grouped logical plans",
            ));
        }

        match self.validate_cursor_bytes::<E>(bytes)? {
            CursorValidationOutcome::Grouped(cursor) => Ok(cursor),
            CursorValidationOutcome::Scalar(_) => Err(cursor_invariant_error(
                "grouped cursor preparation requires grouped logical plans",
            )),
        }
    }

    /// Revalidate scalar cursor state against this immutable continuation contract.
    pub(in crate::db) fn revalidate_scalar_cursor<E: EntityKind<Key = K>>(
        &self,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, CursorPlanError> {
        if self.is_grouped() {
            return Err(cursor_invariant_error(
                "grouped plans require grouped cursor revalidation",
            ));
        }

        crate::db::cursor::revalidate_cursor::<E>(
            self.access_plan().resolve_strategy().as_path().cloned(),
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
        if !self.is_grouped() {
            return Err(cursor_invariant_error(
                "grouped cursor revalidation requires grouped logical plans",
            ));
        }

        if !cursor.is_empty() {
            self.validate_grouped_cursor_policy()?;
        }

        crate::db::cursor::revalidate_grouped_cursor(self.expected_initial_offset(), cursor)
    }

    /// Derive grouped paging contracts from validated grouped cursor state.
    pub(in crate::db) fn grouped_paging_window(
        &self,
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedContinuationWindow, CursorPlanError> {
        if !self.is_grouped() {
            return Err(cursor_invariant_error(
                "grouped paging window requires grouped logical plans",
            ));
        }

        if !cursor.is_empty() {
            self.validate_grouped_cursor_policy()?;
        }

        let resume_initial_offset = if cursor.is_empty() {
            self.effective_offset(false)
        } else {
            cursor.initial_offset()
        };
        let initial_offset_for_page = if cursor.is_empty() {
            self.window_size()
        } else {
            0
        };
        let selection_bound = self.page_limit().and_then(|limit| {
            limit
                .checked_add(initial_offset_for_page)
                .and_then(|count| count.checked_add(1))
        });
        let resume_boundary = cursor
            .last_group_key()
            .map(|last_group_key| Value::List(last_group_key.to_vec()));

        Ok(GroupedContinuationWindow::new(
            self.page_limit(),
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        ))
    }

    fn validate_grouped_cursor_policy(&self) -> Result<(), CursorPlanError> {
        if let Some(violation) = self.grouped_cursor_policy_violation() {
            return Err(CursorPlanError::continuation_cursor_invariant(
                InternalError::executor_invariant_message(violation.invariant_message()),
            ));
        }

        Ok(())
    }
}

impl<K: FieldValue + Clone> AccessPlannedQuery<K> {
    /// Project planner-owned scalar access-window contract.
    #[must_use]
    pub(in crate::db) fn scalar_access_window_plan(
        &self,
        cursor_present: bool,
    ) -> ScalarAccessWindowPlan {
        let page = self.scalar_plan().page.as_ref();
        let offset = page.map_or(0, |page| page.offset);
        let limit = page.and_then(|page| page.limit);
        let effective_offset = effective_offset_for_cursor_window(offset, cursor_present);

        ScalarAccessWindowPlan::new(effective_offset, limit)
    }

    /// Build one immutable continuation contract from planner-owned semantics.
    #[must_use]
    pub(in crate::db) fn continuation_contract(
        &self,
        entity_path: &'static str,
    ) -> Option<ContinuationContract<K>> {
        if !self.scalar_plan().mode.is_load() {
            return None;
        }

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
        let window_size = self
            .scalar_plan()
            .page
            .as_ref()
            .map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX));
        let page_limit = self
            .scalar_plan()
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
        let is_grouped = self.grouped_plan().is_some();
        let order_contract =
            ExecutionOrderContract::from_plan(is_grouped, self.scalar_plan().order.as_ref());
        let access = self.access.clone();
        let grouped_cursor_policy_violation = self
            .grouped_plan()
            .and_then(|grouped| grouped_cursor_policy_violation_for_continuation(grouped, true));

        Some(ContinuationContract::new(
            shape_signature,
            boundary_arity,
            window_size,
            order_contract,
            page_limit,
            access,
            grouped_cursor_policy_violation,
        ))
    }
}

fn cursor_invariant_error(message: impl Into<String>) -> CursorPlanError {
    CursorPlanError::continuation_cursor_invariant(InternalError::executor_invariant_message(
        message,
    ))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{AccessWindowLookaheadPolicy, ScalarAccessWindowPlan};

    #[test]
    fn scalar_access_window_fetch_count_unbounded_remains_unbounded() {
        let window = ScalarAccessWindowPlan::new(3, None);

        assert_eq!(
            window.fetch_count_for(AccessWindowLookaheadPolicy::None),
            None
        );
        assert_eq!(
            window.fetch_count_for(AccessWindowLookaheadPolicy::ExtraRow),
            None
        );
    }

    #[test]
    fn scalar_access_window_fetch_count_bounded_adds_lookahead_row() {
        let window = ScalarAccessWindowPlan::new(3, Some(2));

        assert_eq!(
            window.fetch_count_for(AccessWindowLookaheadPolicy::None),
            Some(5)
        );
        assert_eq!(
            window.fetch_count_for(AccessWindowLookaheadPolicy::ExtraRow),
            Some(6)
        );
    }

    #[test]
    fn scalar_access_window_fetch_count_limit_zero_projects_zero_lookahead() {
        let window = ScalarAccessWindowPlan::new(4, Some(0));

        assert_eq!(
            window.fetch_count_for(AccessWindowLookaheadPolicy::None),
            Some(4)
        );
        assert_eq!(
            window.fetch_count_for(AccessWindowLookaheadPolicy::ExtraRow),
            Some(0)
        );
    }
}
