use crate::{
    db::{
        access::{AccessPath, AccessPlan, AccessPlannedQuery},
        index::{
            EncodedValue, IndexRangeNotIndexableReasonScope, RawIndexKey,
            map_index_range_not_indexable_reason, raw_bounds_for_semantic_index_component_range,
            raw_keys_for_encoded_prefix,
        },
    },
    error::InternalError,
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use std::ops::Bound;

pub(in crate::db) const LOWERED_INDEX_RANGE_SPEC_INVALID: &str =
    "validated index-range plan could not be lowered to raw bounds";
pub(in crate::db) const LOWERED_INDEX_PREFIX_SPEC_INVALID: &str =
    "validated index-prefix plan could not be lowered to raw bounds";
const LOWERED_INDEX_PREFIX_VALUE_NOT_INDEXABLE: &str =
    "validated index-prefix value is not indexable";

pub(in crate::db) type LoweredKey = RawIndexKey;

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
    plan: &AccessPlannedQuery<E::Key>,
) -> Result<Vec<LoweredIndexPrefixSpec>, InternalError> {
    let mut specs = Vec::new();
    collect_index_prefix_specs::<E>(&plan.access, &mut specs)?;

    Ok(specs)
}

// Lower semantic index-range access into byte bounds at lowering time.
pub(in crate::db) fn lower_index_range_specs<E: EntityKind>(
    plan: &AccessPlannedQuery<E::Key>,
) -> Result<Vec<LoweredIndexRangeSpec>, InternalError> {
    let mut specs = Vec::new();
    collect_index_range_specs::<E>(&plan.access, &mut specs)?;

    Ok(specs)
}

// Lower one semantic range envelope into byte bounds with stable reason mapping.
pub(in crate::db) fn lower_index_range_bounds_for_scope<E: EntityKind>(
    index: &IndexModel,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
    scope: IndexRangeNotIndexableReasonScope,
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
        IndexRangeNotIndexableReasonScope::CursorContinuationAnchor,
    )
}

// Collect index-prefix specs in deterministic depth-first traversal order.
fn collect_index_prefix_specs<E: EntityKind>(
    access: &AccessPlan<E::Key>,
    specs: &mut Vec<LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    match access {
        AccessPlan::Path(path) => {
            if let AccessPath::IndexPrefix { index, values } = path.as_ref() {
                let prefix_components = EncodedValue::try_encode_all(values).map_err(|_| {
                    InternalError::query_executor_invariant(
                        LOWERED_INDEX_PREFIX_VALUE_NOT_INDEXABLE,
                    )
                })?;
                let (lower, upper) =
                    raw_keys_for_encoded_prefix::<E>(index, prefix_components.as_slice());
                specs.push(LoweredIndexPrefixSpec::new(
                    *index,
                    Bound::Included(lower),
                    Bound::Included(upper),
                ));
            }

            Ok(())
        }
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            for child in children {
                collect_index_prefix_specs::<E>(child, specs)?;
            }

            Ok(())
        }
    }
}

// Collect index-range specs in deterministic depth-first traversal order.
fn collect_index_range_specs<E: EntityKind>(
    access: &AccessPlan<E::Key>,
    specs: &mut Vec<LoweredIndexRangeSpec>,
) -> Result<(), InternalError> {
    match access {
        AccessPlan::Path(path) => {
            if let AccessPath::IndexRange { spec } = path.as_ref() {
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
                    IndexRangeNotIndexableReasonScope::ValidatedSpec,
                )
                .map_err(InternalError::query_executor_invariant)?;
                specs.push(LoweredIndexRangeSpec::new(*spec.index(), lower, upper));
            }

            Ok(())
        }
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            for child in children {
                collect_index_range_specs::<E>(child, specs)?;
            }

            Ok(())
        }
    }
}
