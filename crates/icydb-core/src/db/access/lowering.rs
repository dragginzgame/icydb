//! Module: access::lowering
//! Responsibility: lower validated semantic access specs into raw index-key bounds.
//! Does not own: access-shape validation or executor scan implementation.
//! Boundary: planner emits lowered contracts consumed directly by executor.

use crate::{
    db::{
        access::{
            AccessPathDispatch, AccessPathExecutionKind, AccessPlan, AccessPlanDispatch,
            ExecutableAccessPath, ExecutableAccessPlan, ExecutionBounds, ExecutionDistinctMode,
            ExecutionOrdering, ExecutionPathPayload, dispatch_access_plan,
        },
        direction::Direction,
        index::{
            EncodedValue, IndexId, IndexRangeBoundEncodeError, RawIndexKey,
            raw_bounds_for_semantic_index_component_range, raw_keys_for_encoded_prefix,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    types::EntityTag,
    value::Value,
};
use std::ops::Bound;

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

// Lower one access-path dispatch payload into executable path contracts.
const fn lower_executable_path_dispatch<K>(
    path: AccessPathDispatch<'_, K>,
) -> ExecutableAccessPath<'_, K> {
    match path {
        AccessPathDispatch::ByKey(key) => ExecutableAccessPath::new(
            AccessPathExecutionKind::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::Unbounded,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::ByKey(key),
        ),
        AccessPathDispatch::ByKeys(keys) => ExecutableAccessPath::new(
            AccessPathExecutionKind::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::Unbounded,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::ByKeys(keys),
        ),
        AccessPathDispatch::KeyRange { start, end } => ExecutableAccessPath::new(
            AccessPathExecutionKind::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::PrimaryKeyRange,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::KeyRange { start, end },
        ),
        AccessPathDispatch::IndexPrefix { index, values } => ExecutableAccessPath::new(
            AccessPathExecutionKind::OrderedIndexScan,
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
            AccessPathExecutionKind::Composite,
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
                AccessPathExecutionKind::IndexRange,
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
            AccessPathExecutionKind::FullScan,
            ExecutionOrdering::Natural,
            ExecutionBounds::Unbounded,
            ExecutionDistinctMode::None,
            false,
            ExecutionPathPayload::FullScan,
        ),
    }
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
    const INVALID_REASON: &str = "validated index-prefix plan could not be lowered to raw bounds";

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

    /// Return the canonical lowered-prefix invalidation reason shared by
    /// planner/executor boundary checks.
    #[must_use]
    pub(in crate::db) const fn invalid_reason() -> &'static str {
        Self::INVALID_REASON
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
    const INVALID_REASON: &str = "validated index-range plan could not be lowered to raw bounds";

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

    /// Return the canonical lowered-range invalidation reason shared by
    /// planner/executor boundary checks.
    #[must_use]
    pub(in crate::db) const fn invalid_reason() -> &'static str {
        Self::INVALID_REASON
    }

    // Build the canonical lowering-time invariant for validated range specs
    // that still fail raw bound encoding.
    fn validated_spec_not_indexable(err: IndexRangeBoundEncodeError) -> InternalError {
        InternalError::query_executor_invariant(err.validated_spec_not_indexable_reason())
    }
}

// Lower semantic index-prefix access into byte bounds at lowering time.
pub(in crate::db) fn lower_index_prefix_specs<K>(
    entity_tag: EntityTag,
    access_plan: &AccessPlan<K>,
) -> Result<Vec<LoweredIndexPrefixSpec>, InternalError> {
    // Phase 1: collect semantic prefix specs from access-plan tree.
    let mut specs = Vec::new();
    collect_index_prefix_specs(entity_tag, access_plan, &mut specs)?;

    Ok(specs)
}

// Lower semantic index-range access into byte bounds at lowering time.
pub(in crate::db) fn lower_index_range_specs<K>(
    entity_tag: EntityTag,
    access_plan: &AccessPlan<K>,
) -> Result<Vec<LoweredIndexRangeSpec>, InternalError> {
    // Phase 1: collect semantic range specs from access-plan tree.
    let mut specs = Vec::new();
    collect_index_range_specs(entity_tag, access_plan, &mut specs)?;

    Ok(specs)
}

// Lower one semantic range envelope into byte bounds with stable reason mapping.
fn lower_index_range_bounds_for_scope(
    entity_tag: EntityTag,
    index: &IndexModel,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<(Bound<LoweredKey>, Bound<LoweredKey>), InternalError> {
    let index_id = IndexId::new(entity_tag, index.ordinal());

    raw_bounds_for_semantic_index_component_range(&index_id, index, prefix, lower, upper)
        .map_err(LoweredIndexRangeSpec::validated_spec_not_indexable)
}

// Collect index-prefix specs in deterministic depth-first traversal order.
fn collect_index_prefix_specs<K>(
    entity_tag: EntityTag,
    access: &AccessPlan<K>,
    specs: &mut Vec<LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    match dispatch_access_plan(access) {
        AccessPlanDispatch::Path(path) => {
            match path {
                AccessPathDispatch::IndexPrefix { index, values } => {
                    lower_index_prefix_values_for_specs(entity_tag, index, values, specs)?;
                }
                AccessPathDispatch::IndexMultiLookup { index, values } => {
                    for value in values {
                        lower_index_prefix_values_for_specs(
                            entity_tag,
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
                collect_index_prefix_specs(entity_tag, child, specs)?;
            }

            Ok(())
        }
    }
}

fn lower_index_prefix_values_for_specs(
    entity_tag: EntityTag,
    index: IndexModel,
    values: &[Value],
    specs: &mut Vec<LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    let prefix_components = EncodedValue::try_encode_all(values).map_err(|_| {
        InternalError::query_executor_invariant("validated index-prefix value is not indexable")
    })?;
    let index_id = IndexId::new(entity_tag, index.ordinal());
    let (lower, upper) =
        raw_keys_for_encoded_prefix(&index_id, &index, prefix_components.as_slice());
    specs.push(LoweredIndexPrefixSpec::new(
        index,
        Bound::Included(lower),
        Bound::Included(upper),
    ));

    Ok(())
}

// Collect index-range specs in deterministic depth-first traversal order.
fn collect_index_range_specs<K>(
    entity_tag: EntityTag,
    access: &AccessPlan<K>,
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
                let (lower, upper) = lower_index_range_bounds_for_scope(
                    entity_tag,
                    spec.index(),
                    spec.prefix_values(),
                    spec.lower(),
                    spec.upper(),
                )?;
                specs.push(LoweredIndexRangeSpec::new(*spec.index(), lower, upper));
            }

            Ok(())
        }
        AccessPlanDispatch::Union(children) | AccessPlanDispatch::Intersection(children) => {
            for child in children {
                collect_index_range_specs(entity_tag, child, specs)?;
            }

            Ok(())
        }
    }
}
