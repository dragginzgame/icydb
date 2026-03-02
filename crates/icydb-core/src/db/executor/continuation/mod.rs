use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorBoundary, CursorPlanError, GroupedContinuationToken,
            GroupedPlannedCursor, PlannedCursor, range_token_from_cursor_anchor,
        },
        direction::Direction,
        executor::{ExecutorPlanError, RangeToken, traversal::derive_primary_scan_direction},
        query::plan::{
            AccessPlannedQuery, GroupedCursorPolicyViolation,
            grouped_cursor_policy_violation_for_continuation, lower_executable_access_path,
        },
    },
    error::InternalError,
    traits::{EntityKind, FieldValue},
    value::Value,
};

///
/// ContinuationEngine
///
/// Executor-owned continuation protocol facade.
/// Centralizes cursor decode/revalidation entrypoints and grouped pagination
/// token/window contracts so executor load paths consume one boundary.
///

pub(in crate::db::executor) struct ContinuationEngine;

impl ContinuationEngine {
    /// Validate and decode one scalar cursor through the canonical cursor protocol.
    pub(in crate::db::executor) fn prepare_scalar_cursor_for_plan<E>(
        plan: &AccessPlannedQuery<E::Key>,
        continuation_signature: ContinuationSignature,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError>
    where
        E: EntityKind,
        E::Key: FieldValue,
    {
        if plan.grouped_plan().is_some() {
            return Err(ExecutorPlanError::from(
                CursorPlanError::continuation_cursor_invariant(
                    InternalError::executor_invariant_message(
                        "grouped plans require grouped cursor preparation",
                    ),
                ),
            ));
        }

        let direction = derive_primary_scan_direction(plan.scalar_plan().order.as_ref());
        crate::db::cursor::prepare_cursor::<E>(
            plan.access.as_path().map(lower_executable_access_path),
            plan.scalar_plan().order.as_ref(),
            direction,
            continuation_signature,
            Self::initial_page_offset(plan),
            cursor,
        )
        .map_err(ExecutorPlanError::from)
    }

    /// Revalidate one executor-provided scalar cursor through the canonical cursor spine.
    pub(in crate::db::executor) fn revalidate_scalar_cursor_for_plan<E>(
        plan: &AccessPlannedQuery<E::Key>,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError>
    where
        E: EntityKind,
        E::Key: FieldValue,
    {
        if plan.grouped_plan().is_some() {
            return Err(invariant(
                "grouped plans require grouped cursor revalidation",
            ));
        }

        let direction = derive_primary_scan_direction(plan.scalar_plan().order.as_ref());
        crate::db::cursor::revalidate_cursor::<E>(
            plan.access.as_path().map(lower_executable_access_path),
            plan.scalar_plan().order.as_ref(),
            direction,
            Self::initial_page_offset(plan),
            cursor,
        )
        .map_err(InternalError::from_cursor_plan_error)
    }

    /// Derive scalar runtime cursor/access bindings from one validated cursor.
    #[must_use]
    pub(in crate::db::executor) fn scalar_runtime(
        cursor: PlannedCursor,
    ) -> ScalarContinuationRuntime {
        ScalarContinuationRuntime::new(cursor)
    }

    /// Validate and decode one grouped cursor through grouped cursor protocol checks.
    pub(in crate::db::executor) fn prepare_grouped_cursor_for_plan<K>(
        entity_path: &'static str,
        plan: &AccessPlannedQuery<K>,
        continuation_signature: ContinuationSignature,
        cursor: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        if plan.grouped_plan().is_none() {
            return Err(ExecutorPlanError::from(
                CursorPlanError::continuation_cursor_invariant(
                    InternalError::executor_invariant_message(
                        "grouped cursor preparation requires grouped logical plans",
                    ),
                ),
            ));
        }
        if let Some(message) = Self::grouped_cursor_policy_violation_message(plan, cursor.is_some())
        {
            return Err(ExecutorPlanError::from(
                CursorPlanError::continuation_cursor_invariant(message),
            ));
        }

        crate::db::cursor::prepare_grouped_cursor(
            entity_path,
            plan.scalar_plan().order.as_ref(),
            continuation_signature,
            Self::initial_page_offset(plan),
            cursor,
        )
        .map_err(ExecutorPlanError::from)
    }

    /// Revalidate one grouped cursor against grouped continuation invariants.
    pub(in crate::db::executor) fn revalidate_grouped_cursor_for_plan<K>(
        plan: &AccessPlannedQuery<K>,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        if plan.grouped_plan().is_none() {
            return Err(invariant(
                "grouped cursor revalidation requires grouped logical plans",
            ));
        }
        if let Some(message) =
            Self::grouped_cursor_policy_violation_message(plan, !cursor.is_empty())
        {
            return Err(InternalError::from_cursor_plan_error(
                CursorPlanError::continuation_cursor_invariant(message),
            ));
        }

        crate::db::cursor::revalidate_grouped_cursor(Self::initial_page_offset(plan), cursor)
            .map_err(InternalError::from_cursor_plan_error)
    }

    /// Derive grouped pagination contracts from one grouped plan + grouped cursor state.
    #[must_use]
    pub(in crate::db::executor) fn grouped_paging_contract<K>(
        plan: &AccessPlannedQuery<K>,
        cursor: &GroupedPlannedCursor,
    ) -> GroupedContinuationPaging {
        let initial_offset = plan
            .scalar_plan()
            .page
            .as_ref()
            .map_or(0, |page| page.offset);
        let resume_initial_offset = if cursor.is_empty() {
            initial_offset
        } else {
            cursor.initial_offset()
        };
        let resume_boundary = cursor
            .last_group_key()
            .map(|last_group_key| Value::List(last_group_key.to_vec()));
        let apply_initial_offset = cursor.is_empty();
        let limit = plan
            .scalar_plan()
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .and_then(|limit| usize::try_from(limit).ok());
        let initial_offset_for_page = if apply_initial_offset {
            usize::try_from(initial_offset).unwrap_or(usize::MAX)
        } else {
            0
        };
        let selection_bound = limit.and_then(|limit| {
            limit
                .checked_add(initial_offset_for_page)
                .and_then(|count| count.checked_add(1))
        });

        GroupedContinuationPaging::new(
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        )
    }

    /// Build one grouped continuation token for grouped page finalization.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_next_cursor_token(
        continuation_signature: ContinuationSignature,
        last_group_key: Vec<Value>,
        resume_initial_offset: u32,
    ) -> GroupedContinuationToken {
        GroupedContinuationToken::new_with_direction(
            continuation_signature,
            last_group_key,
            Direction::Asc,
            resume_initial_offset,
        )
    }

    // Derive initial page offset used for continuation compatibility checks.
    const fn initial_page_offset<K>(plan: &AccessPlannedQuery<K>) -> u32 {
        match plan.scalar_plan().page.as_ref() {
            Some(page) => page.offset,
            None => 0,
        }
    }

    // Return grouped cursor policy violation text for one grouped cursor shape.
    fn grouped_cursor_policy_violation_message<K>(
        plan: &AccessPlannedQuery<K>,
        cursor_present: bool,
    ) -> Option<&'static str> {
        plan.grouped_plan()
            .and_then(|grouped| {
                grouped_cursor_policy_violation_for_continuation(grouped, cursor_present)
            })
            .map(GroupedCursorPolicyViolation::invariant_message)
    }
}

///
/// ScalarContinuationRuntime
///
/// Normalized scalar continuation runtime state.
/// Carries the validated cursor plus pre-derived boundary and index-range anchor
/// bindings so load/route code does not decode cursor internals directly.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarContinuationRuntime {
    cursor_boundary: Option<CursorBoundary>,
    index_range_token: Option<RangeToken>,
}

impl ScalarContinuationRuntime {
    /// Build one scalar runtime cursor binding bundle from one planned cursor.
    #[must_use]
    pub(in crate::db::executor) fn new(cursor: PlannedCursor) -> Self {
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_token = cursor
            .index_range_anchor()
            .map(range_token_from_cursor_anchor);

        Self {
            cursor_boundary,
            index_range_token,
        }
    }

    /// Borrow optional scalar cursor boundary.
    #[must_use]
    pub(in crate::db::executor) const fn cursor_boundary(&self) -> Option<&CursorBoundary> {
        self.cursor_boundary.as_ref()
    }

    /// Borrow optional index-range continuation anchor token.
    #[must_use]
    pub(in crate::db::executor) const fn index_range_token(&self) -> Option<&RangeToken> {
        self.index_range_token.as_ref()
    }
}

///
/// GroupedContinuationPaging
///
/// Canonical grouped paging contract derived from grouped plan + cursor state.
/// Keeps grouped resume boundary, offset semantics, and bounded selection shape
/// centralized outside grouped fold mechanics.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedContinuationPaging {
    limit: Option<usize>,
    initial_offset_for_page: usize,
    selection_bound: Option<usize>,
    resume_initial_offset: u32,
    resume_boundary: Option<Value>,
}

impl GroupedContinuationPaging {
    /// Construct one grouped continuation paging contract.
    #[must_use]
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

    /// Decompose grouped paging fields into load-fold friendly tuple order.
    #[must_use]
    pub(in crate::db::executor) fn into_parts(
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

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
