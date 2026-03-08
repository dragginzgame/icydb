//! Module: access::lowering
//! Responsibility: lower validated semantic access specs into raw index-key bounds.
//! Does not own: access-shape validation or executor scan implementation.
//! Boundary: planner emits lowered contracts consumed directly by executor.

use crate::{
    db::{
        access::{
            AccessExecutionMode, AccessPathDispatch, AccessPlan, AccessPlanDispatch,
            ExecutableAccessPath, ExecutableAccessPlan, ExecutionBounds, ExecutionDistinctMode,
            ExecutionOrdering, ExecutionPathPayload, dispatch_access_plan,
        },
        direction::Direction,
        index::{
            EncodedValue, IndexRangeBoundEncodeError, RawIndexKey,
            raw_bounds_for_semantic_index_component_range, raw_keys_for_encoded_prefix,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use std::ops::Bound;

#[cfg(test)]
use crate::db::access::{AccessPath, dispatch::dispatch_access_path};

pub(in crate::db) const LOWERED_INDEX_RANGE_SPEC_INVALID: &str =
    "validated index-range plan could not be lowered to raw bounds";
pub(in crate::db) const LOWERED_INDEX_PREFIX_SPEC_INVALID: &str =
    "validated index-prefix plan could not be lowered to raw bounds";
const LOWERED_INDEX_PREFIX_VALUE_NOT_INDEXABLE: &str =
    "validated index-prefix value is not indexable";

pub(in crate::db) type LoweredKey = RawIndexKey;

/// Lower one structural `AccessPlan` into its normalized executable contract.
#[must_use]
pub(in crate::db) fn lower_executable_access_plan<K>(
    access: &AccessPlan<K>,
) -> ExecutableAccessPlan<'_, K> {
    match dispatch_access_plan(access) {
        AccessPlanDispatch::Path(path) => {
            ExecutableAccessPlan::for_path(lower_executable_path_dispatch(path))
        }
        AccessPlanDispatch::Union(children) => {
            ExecutableAccessPlan::union(children.iter().map(lower_executable_access_plan).collect())
        }
        AccessPlanDispatch::Intersection(children) => ExecutableAccessPlan::intersection(
            children.iter().map(lower_executable_access_plan).collect(),
        ),
    }
}

/// Lower one structural `AccessPath` into its normalized executable contract.
#[must_use]
#[cfg(test)]
pub(in crate::db) const fn lower_executable_access_path<K>(
    path: &AccessPath<K>,
) -> ExecutableAccessPath<'_, K> {
    lower_executable_path_dispatch(dispatch_access_path(path))
}

// Lower one access-path dispatch payload into executable path contracts.
const fn lower_executable_path_dispatch<K>(
    path: AccessPathDispatch<'_, K>,
) -> ExecutableAccessPath<'_, K> {
    match path {
        AccessPathDispatch::ByKey(key) => ExecutableAccessPath::new(
            AccessExecutionMode::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::Unbounded,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::ByKey(key),
        ),
        AccessPathDispatch::ByKeys(keys) => ExecutableAccessPath::new(
            AccessExecutionMode::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::Unbounded,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::ByKeys(keys),
        ),
        AccessPathDispatch::KeyRange { start, end } => ExecutableAccessPath::new(
            AccessExecutionMode::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::PrimaryKeyRange,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::KeyRange { start, end },
        ),
        AccessPathDispatch::IndexPrefix { index, values } => ExecutableAccessPath::new(
            AccessExecutionMode::OrderedIndexScan,
            ExecutionOrdering::ByIndex(Direction::Asc),
            ExecutionBounds::IndexPrefix {
                index,
                prefix_len: values.len(),
            },
            ExecutionDistinctMode::PreOrdered,
            true,
            ExecutionPathPayload::IndexPrefix,
        ),
        AccessPathDispatch::IndexMultiLookup { index, values } => ExecutableAccessPath::new(
            AccessExecutionMode::Composite,
            ExecutionOrdering::Natural,
            ExecutionBounds::IndexPrefix {
                index,
                prefix_len: 1,
            },
            ExecutionDistinctMode::RequiresMaterialization,
            true,
            ExecutionPathPayload::IndexMultiLookup {
                value_count: values.len(),
            },
        ),
        AccessPathDispatch::IndexRange { spec } => {
            let index = *spec.index();
            let prefix_len = spec.prefix_values().len();

            ExecutableAccessPath::new(
                AccessExecutionMode::IndexRange,
                ExecutionOrdering::ByIndex(Direction::Asc),
                ExecutionBounds::IndexRange { index, prefix_len },
                ExecutionDistinctMode::PreOrdered,
                true,
                ExecutionPathPayload::IndexRange {
                    prefix_values: spec.prefix_values(),
                    lower: spec.lower(),
                    upper: spec.upper(),
                },
            )
        }
        AccessPathDispatch::FullScan => ExecutableAccessPath::new(
            AccessExecutionMode::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::Unbounded,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::FullScan,
        ),
    }
}

///
/// LoweredIndexNotIndexableReasonScope
///
/// Access-lowering scope for stable "not indexable" reason wording.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoweredIndexNotIndexableReasonScope {
    ValidatedSpec,
    CursorContinuationAnchor,
}

///
/// LoweredIndexPrefixSpec
///
/// Lowered index-prefix contract with fully materialized byte bounds.
/// Executor runtime consumes this directly and does not perform encoding.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct LoweredIndexPrefixSpec {
    index: IndexModel,
    lower: Bound<LoweredKey>,
    upper: Bound<LoweredKey>,
}

impl LoweredIndexPrefixSpec {
    #[must_use]
    pub(in crate::db) const fn new(
        index: IndexModel,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
    ) -> Self {
        Self {
            index,
            lower,
            upper,
        }
    }

    #[must_use]
    pub(in crate::db) const fn index(&self) -> &IndexModel {
        &self.index
    }

    #[must_use]
    pub(in crate::db) const fn lower(&self) -> &Bound<LoweredKey> {
        &self.lower
    }

    #[must_use]
    pub(in crate::db) const fn upper(&self) -> &Bound<LoweredKey> {
        &self.upper
    }
}

///
/// LoweredIndexRangeSpec
///
/// Lowered index-range contract with fully materialized byte bounds.
/// Executor runtime consumes this directly and does not perform encoding.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct LoweredIndexRangeSpec {
    index: IndexModel,
    lower: Bound<LoweredKey>,
    upper: Bound<LoweredKey>,
}

impl LoweredIndexRangeSpec {
    #[must_use]
    pub(in crate::db) const fn new(
        index: IndexModel,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
    ) -> Self {
        Self {
            index,
            lower,
            upper,
        }
    }

    #[must_use]
    pub(in crate::db) const fn index(&self) -> &IndexModel {
        &self.index
    }

    #[must_use]
    pub(in crate::db) const fn lower(&self) -> &Bound<LoweredKey> {
        &self.lower
    }

    #[must_use]
    pub(in crate::db) const fn upper(&self) -> &Bound<LoweredKey> {
        &self.upper
    }
}

// Lower semantic index-prefix access into byte bounds at lowering time.
pub(in crate::db) fn lower_index_prefix_specs<E: EntityKind>(
    access_plan: &AccessPlan<E::Key>,
) -> Result<Vec<LoweredIndexPrefixSpec>, InternalError> {
    // Phase 1: collect semantic prefix specs from access-plan tree.
    let mut specs = Vec::new();
    collect_index_prefix_specs::<E>(access_plan, &mut specs)?;

    Ok(specs)
}

// Lower semantic index-range access into byte bounds at lowering time.
pub(in crate::db) fn lower_index_range_specs<E: EntityKind>(
    access_plan: &AccessPlan<E::Key>,
) -> Result<Vec<LoweredIndexRangeSpec>, InternalError> {
    // Phase 1: collect semantic range specs from access-plan tree.
    let mut specs = Vec::new();
    collect_index_range_specs::<E>(access_plan, &mut specs)?;

    Ok(specs)
}

// Lower one semantic range envelope into byte bounds with stable reason mapping.
fn lower_index_range_bounds_for_scope<E: EntityKind>(
    index: &IndexModel,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
    scope: LoweredIndexNotIndexableReasonScope,
) -> Result<(Bound<LoweredKey>, Bound<LoweredKey>), &'static str> {
    raw_bounds_for_semantic_index_component_range::<E>(index, prefix, lower, upper)
        .map_err(|err| map_index_range_not_indexable_reason(scope, err))
}

// Lower one semantic range envelope for cursor-anchor containment checks.
pub(in crate::db) fn lower_cursor_anchor_index_range_bounds<E: EntityKind>(
    index: &IndexModel,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<(Bound<LoweredKey>, Bound<LoweredKey>), &'static str> {
    lower_index_range_bounds_for_scope::<E>(
        index,
        prefix,
        lower,
        upper,
        LoweredIndexNotIndexableReasonScope::CursorContinuationAnchor,
    )
}

// Collect index-prefix specs in deterministic depth-first traversal order.
fn collect_index_prefix_specs<E: EntityKind>(
    access: &AccessPlan<E::Key>,
    specs: &mut Vec<LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    match dispatch_access_plan(access) {
        AccessPlanDispatch::Path(path) => {
            match path {
                AccessPathDispatch::IndexPrefix { index, values } => {
                    lower_index_prefix_values_for_specs::<E>(index, values, specs)?;
                }
                AccessPathDispatch::IndexMultiLookup { index, values } => {
                    for value in values {
                        lower_index_prefix_values_for_specs::<E>(
                            index,
                            std::slice::from_ref(value),
                            specs,
                        )?;
                    }
                }
                AccessPathDispatch::ByKey(_)
                | AccessPathDispatch::ByKeys(_)
                | AccessPathDispatch::KeyRange { .. }
                | AccessPathDispatch::IndexRange { .. }
                | AccessPathDispatch::FullScan => {}
            }

            Ok(())
        }
        AccessPlanDispatch::Union(children) | AccessPlanDispatch::Intersection(children) => {
            for child in children {
                collect_index_prefix_specs::<E>(child, specs)?;
            }

            Ok(())
        }
    }
}

fn lower_index_prefix_values_for_specs<E: EntityKind>(
    index: IndexModel,
    values: &[Value],
    specs: &mut Vec<LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    let prefix_components = EncodedValue::try_encode_all(values).map_err(|_| {
        crate::db::error::executor_invariant(LOWERED_INDEX_PREFIX_VALUE_NOT_INDEXABLE)
    })?;
    let (lower, upper) = raw_keys_for_encoded_prefix::<E>(&index, prefix_components.as_slice());
    specs.push(LoweredIndexPrefixSpec::new(
        index,
        Bound::Included(lower),
        Bound::Included(upper),
    ));

    Ok(())
}

// Collect index-range specs in deterministic depth-first traversal order.
fn collect_index_range_specs<E: EntityKind>(
    access: &AccessPlan<E::Key>,
    specs: &mut Vec<LoweredIndexRangeSpec>,
) -> Result<(), InternalError> {
    match dispatch_access_plan(access) {
        AccessPlanDispatch::Path(path) => {
            if let AccessPathDispatch::IndexRange { spec } = path {
                debug_assert_eq!(
                    spec.field_slots().len(),
                    spec.prefix_values().len().saturating_add(1),
                    "semantic range field-slot arity must remain prefix_len + range slot",
                );
                let (lower, upper) = lower_index_range_bounds_for_scope::<E>(
                    spec.index(),
                    spec.prefix_values(),
                    spec.lower(),
                    spec.upper(),
                    LoweredIndexNotIndexableReasonScope::ValidatedSpec,
                )
                .map_err(InternalError::query_executor_invariant)?;
                specs.push(LoweredIndexRangeSpec::new(*spec.index(), lower, upper));
            }

            Ok(())
        }
        AccessPlanDispatch::Union(children) | AccessPlanDispatch::Intersection(children) => {
            for child in children {
                collect_index_range_specs::<E>(child, specs)?;
            }

            Ok(())
        }
    }
}

const fn map_bound_encode_error(
    err: IndexRangeBoundEncodeError,
    prefix_reason: &'static str,
    lower_reason: &'static str,
    upper_reason: &'static str,
) -> &'static str {
    match err {
        IndexRangeBoundEncodeError::Prefix => prefix_reason,
        IndexRangeBoundEncodeError::Lower => lower_reason,
        IndexRangeBoundEncodeError::Upper => upper_reason,
    }
}

const fn map_index_range_not_indexable_reason(
    scope: LoweredIndexNotIndexableReasonScope,
    err: IndexRangeBoundEncodeError,
) -> &'static str {
    match scope {
        LoweredIndexNotIndexableReasonScope::ValidatedSpec => map_bound_encode_error(
            err,
            "validated index-range prefix is not indexable",
            "validated index-range lower bound is not indexable",
            "validated index-range upper bound is not indexable",
        ),
        LoweredIndexNotIndexableReasonScope::CursorContinuationAnchor => map_bound_encode_error(
            err,
            "index-range continuation anchor prefix is not indexable",
            "index-range cursor lower continuation bound is not indexable",
            "index-range cursor upper continuation bound is not indexable",
        ),
    }
}
