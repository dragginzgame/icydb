//! Module: executor::planning::continuation::scalar
//! Responsibility: scalar continuation planning/runtime bindings for access resume behavior.
//! Does not own: cursor token encoding policy or planner semantic ownership.
//! Boundary: consumes validated continuation contracts and computes scalar resume inputs.

use crate::{
    db::{
        access::{AccessPathKind, IndexShapeDetails},
        cursor::{
            CursorBoundary, CursorBoundarySlot,
            effective_keep_count_for_limit as continuation_keep_count_for_limit,
            effective_page_offset_for_window as continuation_page_offset_for_window,
        },
        data::primary_key_value_from_structural_value,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, ContinuationMode, LoweredIndexPrefixSpec,
            LoweredIndexRangeSpec, LoweredKey, RouteContinuationPlan,
            budget::ExecutionConstructionBudget, planning::route::LoadOrderRouteMode,
            route::access_order_satisfied_by_route_mode,
        },
        index::IndexKey,
        query::{
            construction::ConstructionBudget,
            plan::{AccessPlannedQuery, ContinuationPolicy, DeterministicSecondaryIndexOrderMatch},
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    value::Value,
};
use std::{ops::Bound, rc::Rc};

///
/// ScalarContinuationContext
///
/// Normalized scalar continuation runtime state.
/// Carries the validated cursor plus pre-derived boundary and index-range anchor
/// bindings so load/route code does not decode cursor internals directly.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarContinuationContext {
    cursor_boundary: Option<Rc<CursorBoundary>>,
    physical_primary_key_boundary: Option<Rc<CursorBoundary>>,
}

impl ScalarContinuationContext {
    /// Construct one empty scalar continuation runtime for initial executions.
    #[must_use]
    pub(in crate::db) const fn initial() -> Self {
        Self {
            cursor_boundary: None,
            physical_primary_key_boundary: None,
        }
    }

    /// Construct one runtime continuation after its authenticated token and
    /// immutable page contract have been validated by the session boundary.
    #[must_use]
    pub(in crate::db) fn resumed(cursor_boundary: CursorBoundary) -> Self {
        Self {
            cursor_boundary: Some(Rc::new(cursor_boundary)),
            physical_primary_key_boundary: None,
        }
    }

    /// Construct one resumed runtime with authenticated physical primary-key progress.
    ///
    /// `cursor_boundary` remains the last row actually emitted. The physical
    /// boundary may be later when residual predicates rejected additional
    /// candidates before the page work envelope stopped.
    #[must_use]
    pub(in crate::db) fn resumed_with_primary_progress(
        cursor_boundary: Option<CursorBoundary>,
        physical_primary_key_boundary: CursorBoundary,
    ) -> Self {
        Self {
            cursor_boundary: cursor_boundary.map(Rc::new),
            physical_primary_key_boundary: Some(Rc::new(physical_primary_key_boundary)),
        }
    }

    /// Borrow optional scalar cursor boundary.
    #[must_use]
    pub(in crate::db::executor) fn cursor_boundary(&self) -> Option<&CursorBoundary> {
        self.cursor_boundary.as_deref()
    }

    /// Return whether this scalar continuation has logical or physical progress.
    #[must_use]
    pub(in crate::db) const fn has_progress(&self) -> bool {
        self.cursor_boundary.is_some() || self.physical_primary_key_boundary.is_some()
    }

    /// Return whether a final-order scan cap applies after consumed progress.
    /// Initial scans need no resume anchor. Resumed primary-key and proven
    /// secondary-index orders apply their boundary before output caps.
    #[must_use]
    pub(in crate::db::executor) fn can_bound_ordered_scan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> bool {
        !self.has_progress()
            || scalar_order_is_primary_key_only(plan)
            || scalar_secondary_index_order(plan).is_some()
    }

    /// Encode authenticated logical progress with the accepted index owner.
    /// The returned key is local to execution; token and plan formats stay unchanged.
    pub(in crate::db::executor) fn secondary_index_resume_anchor(
        &self,
        plan: &AccessPlannedQuery,
        schema: &SchemaInfo,
        prefixes: &[LoweredIndexPrefixSpec],
        ranges: &[LoweredIndexRangeSpec],
    ) -> Result<Option<LoweredKey>, InternalError> {
        let Some(boundary) = self.cursor_boundary() else {
            return Ok(None);
        };
        let Some((index, prefix_len)) = scalar_secondary_index_order(plan) else {
            return Ok(None);
        };
        let primary_len = plan.primary_key_names()?.len();
        let order = plan
            .planner_route_profile()
            .secondary_order_contract()
            .ok_or_else(InternalError::query_executor_invariant)?;
        if boundary.slots.len() != order.non_primary_key_terms().len() + primary_len {
            return Err(InternalError::query_executor_invariant());
        }
        let budget: &dyn ConstructionBudget = &ExecutionConstructionBudget;
        let mut values = budget.vec_with_capacity(boundary.slots.len())?;
        for slot in &boundary.slots {
            let CursorBoundarySlot::Present(value) = slot else {
                return Err(InternalError::query_executor_invariant());
            };
            values.push(value);
        }
        let primary_values = values
            .get(values.len().saturating_sub(primary_len)..)
            .ok_or_else(InternalError::query_executor_invariant)?;
        let primary_key = match primary_values {
            [value] => primary_key_value_from_structural_value(value)?,
            _ => primary_key_value_from_structural_value(&Value::List(
                budget.copy_slice(primary_values, |value| budget.copy_value(value))?,
            ))?,
        };
        let start = secondary_index_template(prefixes, ranges)?;
        if start.component_count() != index.key_arity() {
            return Err(InternalError::query_executor_invariant());
        }
        Ok(Some(start.raw_resume_anchor_with_accepted_suffix(
            schema,
            index.name(),
            prefix_len,
            &values,
            &primary_key,
            budget,
        )?))
    }

    /// Derive route continuation mode from scalar continuation context shape.
    #[must_use]
    pub(in crate::db::executor) const fn route_continuation_mode(&self) -> ContinuationMode {
        if self.has_progress() {
            ContinuationMode::CursorBoundary
        } else {
            ContinuationMode::Initial
        }
    }

    /// Derive one route continuation plan from scalar runtime state and planner policy.
    ///
    /// This keeps continuation/window derivation in continuation authority so
    /// route planning consumes one pre-derived continuation contract.
    #[must_use]
    pub(in crate::db::executor) fn route_continuation_plan(
        &self,
        plan: &AccessPlannedQuery,
        continuation_policy: ContinuationPolicy,
    ) -> RouteContinuationPlan {
        RouteContinuationPlan::from_scalar_access_window_plan(
            self.route_continuation_mode(),
            continuation_policy,
            plan.scalar_access_window_plan(self.has_progress()),
        )
    }

    /// Build access-stream continuation input for routed stream resolution.
    #[must_use]
    pub(in crate::db::executor) fn access_scan_input<'a>(
        &'a self,
        direction: Direction,
        plan: &AccessPlannedQuery,
        secondary_index_anchor: Option<&'a LoweredKey>,
    ) -> AccessScanContinuationInput<'a> {
        let primary_key_ordered = scalar_order_is_primary_key_only(plan);
        AccessScanContinuationInput::with_primary_key_boundary(
            secondary_index_anchor,
            direction,
            primary_key_ordered
                .then_some(
                    self.physical_primary_key_boundary
                        .as_deref()
                        .or_else(|| self.cursor_boundary()),
                )
                .flatten(),
        )
    }

    /// Assert scalar route-continuation invariants against this runtime context.
    ///
    /// Keeps scalar continuation protocol sanity checks centralized in
    /// continuation runtime so load entrypoints consume one invariant boundary.
    pub(in crate::db::executor) fn debug_assert_route_continuation_invariants(
        &self,
        plan: &AccessPlannedQuery,
        route_continuation: RouteContinuationPlan,
    ) {
        debug_assert!(
            route_continuation.strict_advance_required_when_applied(),
            "route invariant: continuation executions must enforce strict advancement policy",
        );
        debug_assert_eq!(
            route_continuation.effective_offset(),
            continuation_page_offset_for_window(plan, self.has_progress()),
            "route window effective offset must match logical plan offset semantics",
        );
    }

    /// Derive effective keep count (`offset + limit`) under this continuation context.
    #[must_use]
    pub(in crate::db::executor) fn keep_count_for_limit_window(
        &self,
        plan: &AccessPlannedQuery,
        limit: u32,
    ) -> usize {
        continuation_keep_count_for_limit(plan, self.has_progress(), limit)
    }

    /// Validate load scan-budget hint preconditions under this continuation context.
    ///
    /// Bounded load scan hints are only valid for non-continuation executions on
    /// streaming-safe access shapes where access order is already final.
    pub(in crate::db::executor) fn validate_load_scan_budget_hint(
        &self,
        scan_budget_hint: Option<usize>,
        load_order_route_mode: LoadOrderRouteMode,
    ) -> Result<(), InternalError> {
        if scan_budget_hint.is_some() && self.has_progress() {
            return Err(InternalError::query_executor_invariant());
        }
        if scan_budget_hint.is_some() && !load_order_route_mode.allows_streaming_load() {
            return Err(InternalError::query_executor_invariant());
        }

        Ok(())
    }
}

// Bind primary progress only when the complete canonical order is the PK tuple.
fn scalar_order_is_primary_key_only(plan: &AccessPlannedQuery) -> bool {
    let Ok(primary_key_names) = plan.primary_key_names() else {
        return false;
    };

    plan.scalar_plan().order.as_ref().is_some_and(|order| {
        order
            .primary_key_only_direction_fields(primary_key_names)
            .is_some()
    })
}

// Reuse the planner's accepted final-order proof, narrowing it to physical
// leaves whose raw-key order is retained. PK-merged branch sets are separate.
fn scalar_secondary_index_order(plan: &AccessPlannedQuery) -> Option<(IndexShapeDetails, usize)> {
    if !access_order_satisfied_by_route_mode(plan) {
        return None;
    }
    let facts = plan.access_shape_facts();
    if !matches!(
        facts.single_path_facts()?.kind(),
        AccessPathKind::IndexPrefix | AccessPathKind::IndexRange | AccessPathKind::IndexMultiLookup
    ) {
        return None;
    }
    let index = facts
        .single_path_index_prefix_details()
        .or_else(|| facts.single_path_index_range_details())?;
    let contract = plan.planner_route_profile().secondary_order_contract()?;
    if contract.non_primary_key_terms().is_empty() {
        return None;
    }
    let prefix_len = match contract.classify_index_key_items(index.key_items(), index.slot_arity())
    {
        DeterministicSecondaryIndexOrderMatch::Full => 0,
        DeterministicSecondaryIndexOrderMatch::Suffix => index.slot_arity(),
        DeterministicSecondaryIndexOrderMatch::None => return None,
    };
    Some((index, prefix_len))
}

// Lowering already owns the physical generation and key kind; never rebuild
// either from generated schema models or cursor values.
fn secondary_index_template(
    prefixes: &[LoweredIndexPrefixSpec],
    ranges: &[LoweredIndexRangeSpec],
) -> Result<IndexKey, InternalError> {
    let (lower, upper) = if let Some(spec) = prefixes.first() {
        spec.raw_bounds(&ExecutionConstructionBudget)?
    } else if let [spec] = ranges {
        (spec.lower(), spec.upper())
    } else {
        return Err(InternalError::query_executor_invariant());
    };
    let raw = match lower {
        Bound::Included(key) | Bound::Excluded(key) => key,
        Bound::Unbounded => match upper {
            Bound::Included(key) | Bound::Excluded(key) => key,
            Bound::Unbounded => return Err(InternalError::query_executor_invariant()),
        },
    };
    IndexKey::try_from_raw(raw).map_err(|_| InternalError::query_executor_invariant())
}
