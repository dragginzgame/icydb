use crate::{
    db::{
        index::{
            EncodedValue, IndexRangeNotIndexableReasonScope, RawIndexKey,
            map_index_range_not_indexable_reason, raw_keys_for_encoded_prefix,
        },
        query::plan::{AccessPath, AccessPlan, AccessPlannedQuery},
    },
    error::InternalError,
    model::index::IndexModel,
    traits::EntityKind,
};
use std::ops::Bound;

use crate::db::query::plan::lowering::index_bounds::raw_bounds_for_semantic_index_range_spec;

pub(in crate::db::query::plan) const INDEX_RANGE_SPEC_INVALID: &str =
    "validated index-range plan could not be lowered to raw bounds";
pub(in crate::db::query::plan) const INDEX_PREFIX_SPEC_INVALID: &str =
    "validated index-prefix plan could not be lowered to raw bounds";
const INDEX_PREFIX_SPEC_VALUE_NOT_INDEXABLE: &str = "validated index-prefix value is not indexable";

///
/// IndexPrefixSpec
///
/// Executor-lowered index-prefix contract with fully materialized raw-key bounds.
/// This keeps runtime prefix traversal mechanical and free of `Value` encoding.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexPrefixSpec {
    index: IndexModel,
    lower: Bound<RawIndexKey>,
    upper: Bound<RawIndexKey>,
}

impl IndexPrefixSpec {
    #[must_use]
    pub(in crate::db) const fn new(
        index: IndexModel,
        lower: Bound<RawIndexKey>,
        upper: Bound<RawIndexKey>,
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
    pub(in crate::db) const fn lower(&self) -> &Bound<RawIndexKey> {
        &self.lower
    }

    #[must_use]
    pub(in crate::db) const fn upper(&self) -> &Bound<RawIndexKey> {
        &self.upper
    }
}

///
/// IndexRangeSpec
///
/// Executor-lowered index-range contract with fully materialized raw-key bounds.
/// This keeps runtime traversal mechanical and free of `Value` decoding/encoding.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexRangeSpec {
    index: IndexModel,
    lower: Bound<RawIndexKey>,
    upper: Bound<RawIndexKey>,
}

impl IndexRangeSpec {
    #[must_use]
    pub(in crate::db) const fn new(
        index: IndexModel,
        lower: Bound<RawIndexKey>,
        upper: Bound<RawIndexKey>,
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
    pub(in crate::db) const fn lower(&self) -> &Bound<RawIndexKey> {
        &self.lower
    }

    #[must_use]
    pub(in crate::db) const fn upper(&self) -> &Bound<RawIndexKey> {
        &self.upper
    }
}

// Lower semantic index-prefix access into raw-key bounds once at plan materialization.
pub(in crate::db::query::plan) fn build_index_prefix_specs<E: EntityKind>(
    plan: &AccessPlannedQuery<E::Key>,
) -> Result<Vec<IndexPrefixSpec>, InternalError> {
    let mut specs = Vec::new();
    collect_index_prefix_specs::<E>(&plan.access, &mut specs)?;

    Ok(specs)
}

// Lower semantic index-range access into raw-key bounds once at plan materialization.
pub(in crate::db::query::plan) fn build_index_range_specs<E: EntityKind>(
    plan: &AccessPlannedQuery<E::Key>,
) -> Result<Vec<IndexRangeSpec>, InternalError> {
    let mut specs = Vec::new();
    collect_index_range_specs::<E>(&plan.access, &mut specs)?;

    Ok(specs)
}

// Collect index-prefix specs in deterministic depth-first traversal order.
fn collect_index_prefix_specs<E: EntityKind>(
    access: &AccessPlan<E::Key>,
    specs: &mut Vec<IndexPrefixSpec>,
) -> Result<(), InternalError> {
    match access {
        AccessPlan::Path(path) => {
            if let AccessPath::IndexPrefix { index, values } = path.as_ref() {
                let encoded_prefix = EncodedValue::try_encode_all(values).map_err(|_| {
                    InternalError::query_executor_invariant(INDEX_PREFIX_SPEC_VALUE_NOT_INDEXABLE)
                })?;
                let (lower, upper) =
                    raw_keys_for_encoded_prefix::<E>(index, encoded_prefix.as_slice());
                specs.push(IndexPrefixSpec::new(
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
    specs: &mut Vec<IndexRangeSpec>,
) -> Result<(), InternalError> {
    match access {
        AccessPlan::Path(path) => {
            if let AccessPath::IndexRange { spec } = path.as_ref() {
                debug_assert_eq!(
                    spec.field_slots().len(),
                    spec.prefix_values().len().saturating_add(1),
                    "semantic range field-slot arity must remain prefix_len + range slot",
                );
                let (lower, upper) =
                    raw_bounds_for_semantic_index_range_spec::<E>(spec).map_err(|err| {
                        InternalError::query_executor_invariant(
                            map_index_range_not_indexable_reason(
                                IndexRangeNotIndexableReasonScope::ValidatedSpec,
                                err,
                            ),
                        )
                    })?;
                specs.push(IndexRangeSpec::new(*spec.index(), lower, upper));
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
