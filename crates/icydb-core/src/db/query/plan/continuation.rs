use crate::{
    db::{
        access::{AccessPlan, lower_executable_access_plan},
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
    pub shape_signature: ExecutionShapeSignature,
    pub boundary_arity: usize,
    pub window_size: usize,
    pub order_contract: ExecutionOrderContract,
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

    /// Return whether cursor continuation is supported for this contract.
    #[must_use]
    pub(in crate::db) const fn supports_cursor(&self) -> bool {
        self.order_contract.supports_cursor()
    }

    #[must_use]
    pub(in crate::db) const fn continuation_signature(&self) -> ContinuationSignature {
        self.shape_signature.continuation_signature()
    }

    #[must_use]
    pub(in crate::db) const fn boundary_arity(&self) -> usize {
        self.boundary_arity
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
            let executable_access = lower_executable_access_plan(&self.access);
            executable_access.as_path().cloned()
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

        let executable_access = lower_executable_access_plan(&self.access);

        crate::db::cursor::revalidate_cursor::<E>(
            executable_access.as_path().cloned(),
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
            self.expected_initial_offset()
        } else {
            cursor.initial_offset()
        };
        let initial_offset_for_page = if cursor.is_empty() {
            self.window_size
        } else {
            0
        };
        let selection_bound = self.page_limit.and_then(|limit| {
            limit
                .checked_add(initial_offset_for_page)
                .and_then(|count| count.checked_add(1))
        });
        let resume_boundary = cursor
            .last_group_key()
            .map(|last_group_key| Value::List(last_group_key.to_vec()));

        Ok(GroupedContinuationWindow::new(
            self.page_limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        ))
    }

    fn expected_initial_offset(&self) -> u32 {
        u32::try_from(self.window_size).unwrap_or(u32::MAX)
    }

    fn validate_grouped_cursor_policy(&self) -> Result<(), CursorPlanError> {
        if let Some(violation) = self.grouped_cursor_policy_violation {
            return Err(CursorPlanError::continuation_cursor_invariant(
                InternalError::executor_invariant_message(violation.invariant_message()),
            ));
        }

        Ok(())
    }
}

impl<K: FieldValue + Clone> AccessPlannedQuery<K> {
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
