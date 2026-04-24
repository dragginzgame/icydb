//! Module: access::lowering
//! Responsibility: lower validated semantic access specs into raw index-key bounds.
//! Does not own: access-shape validation or executor scan implementation.
//! Boundary: planner emits lowered contracts consumed directly by executor.

use crate::{
    db::{
        access::{
            AccessPathDispatch, AccessPlan, ExecutableAccessPath, ExecutableAccessPlan,
            ExecutionPathPayload, dispatch_access_path,
        },
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

///
/// IndexSpecCollection
///
/// Optional raw-index output sink used while lowering an access tree.
/// Full access preparation collects index specs; executable-only lowering
/// disables collection and keeps the same structural traversal.
///

enum IndexSpecCollection<'a> {
    Disabled,
    Raw {
        entity_tag: EntityTag,
        index_prefix_specs: &'a mut Vec<LoweredIndexPrefixSpec>,
        index_range_specs: &'a mut Vec<LoweredIndexRangeSpec>,
    },
}

impl IndexSpecCollection<'_> {
    fn collect_path<K>(
        &mut self,
        path: &AccessPathDispatch<'_, K>,
    ) -> Result<(), LoweredAccessError> {
        match self {
            Self::Disabled => Ok(()),
            Self::Raw {
                entity_tag,
                index_prefix_specs,
                index_range_specs,
            } => {
                lower_index_specs_for_path(*entity_tag, path, index_prefix_specs, index_range_specs)
            }
        }
    }
}

///
/// LoweredAccess
///
/// Bundled lowering result for one access tree.
/// Carries the executable tree and all index-bound specs from one traversal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct LoweredAccess<'a, K> {
    executable: ExecutableAccessPlan<'a, K>,
    index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    index_range_specs: Vec<LoweredIndexRangeSpec>,
}

impl<'a, K> LoweredAccess<'a, K> {
    #[must_use]
    pub(in crate::db) const fn index_prefix_specs(&self) -> &[LoweredIndexPrefixSpec] {
        self.index_prefix_specs.as_slice()
    }

    #[must_use]
    pub(in crate::db) const fn index_range_specs(&self) -> &[LoweredIndexRangeSpec] {
        self.index_range_specs.as_slice()
    }

    #[must_use]
    pub(in crate::db) fn into_parts(
        self,
    ) -> (
        ExecutableAccessPlan<'a, K>,
        Vec<LoweredIndexPrefixSpec>,
        Vec<LoweredIndexRangeSpec>,
    ) {
        (
            self.executable,
            self.index_prefix_specs,
            self.index_range_specs,
        )
    }
}

///
/// LoweredAccessError
///
/// Failure category for bundled access lowering.
/// Keeps prefix/range invalidation distinguishable while sharing traversal.
///

#[derive(Debug)]
pub(in crate::db) enum LoweredAccessError {
    IndexPrefix(InternalError),
    IndexRange(InternalError),
}

impl LoweredAccessError {
    #[must_use]
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::IndexPrefix(err) | Self::IndexRange(err) => err,
        }
    }
}

/// Lower one structural access plan into executable and raw index-bound specs.
pub(in crate::db) fn lower_access<K>(
    entity_tag: EntityTag,
    access: &AccessPlan<K>,
) -> Result<LoweredAccess<'_, K>, LoweredAccessError> {
    let mut index_prefix_specs = Vec::new();
    let mut index_range_specs = Vec::new();
    let executable = {
        let mut index_spec_collection = IndexSpecCollection::Raw {
            entity_tag,
            index_prefix_specs: &mut index_prefix_specs,
            index_range_specs: &mut index_range_specs,
        };
        lower_access_node(access, &mut index_spec_collection)?
    };

    Ok(LoweredAccess {
        executable,
        index_prefix_specs,
        index_range_specs,
    })
}

/// Lower one structural `AccessPlan` into its normalized executable contract.
#[must_use]
pub(in crate::db) fn lower_executable_access_plan<K>(
    access: &AccessPlan<K>,
) -> ExecutableAccessPlan<'_, K> {
    let mut index_spec_collection = IndexSpecCollection::Disabled;
    lower_access_node(access, &mut index_spec_collection)
        .expect("executable-only access lowering cannot collect raw index specs")
}

// Lower one access-path dispatch payload into executable path contracts.
const fn lower_executable_path_dispatch<K>(
    path: AccessPathDispatch<'_, K>,
) -> ExecutableAccessPath<'_, K> {
    match path {
        AccessPathDispatch::ByKey(key) => {
            ExecutableAccessPath::new(ExecutionPathPayload::ByKey(key))
        }
        AccessPathDispatch::ByKeys(keys) => {
            ExecutableAccessPath::new(ExecutionPathPayload::ByKeys(keys))
        }
        AccessPathDispatch::KeyRange { start, end } => {
            ExecutableAccessPath::new(ExecutionPathPayload::KeyRange { start, end })
        }
        AccessPathDispatch::IndexPrefix { index, values } => {
            ExecutableAccessPath::new(ExecutionPathPayload::IndexPrefix {
                index,
                prefix_len: values.len(),
            })
        }
        AccessPathDispatch::IndexMultiLookup { index, values } => {
            ExecutableAccessPath::new(ExecutionPathPayload::IndexMultiLookup {
                index,
                value_count: values.len(),
            })
        }
        AccessPathDispatch::IndexRange { spec } => {
            let index = *spec.index();
            let prefix_len = spec.prefix_values().len();

            ExecutableAccessPath::new(ExecutionPathPayload::IndexRange {
                index,
                prefix_len,
                prefix_values: spec.prefix_values(),
                lower: spec.lower(),
                upper: spec.upper(),
            })
        }
        AccessPathDispatch::FullScan => ExecutableAccessPath::new(ExecutionPathPayload::FullScan),
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

// Lower one access node and collect raw index-bound specs in the same
// deterministic depth-first traversal.
fn lower_access_node<'a, K>(
    access: &'a AccessPlan<K>,
    index_spec_collection: &mut IndexSpecCollection<'_>,
) -> Result<ExecutableAccessPlan<'a, K>, LoweredAccessError> {
    match access {
        AccessPlan::Path(path) => {
            let path = dispatch_access_path(path.as_ref());
            index_spec_collection.collect_path(&path)?;

            Ok(ExecutableAccessPlan::for_path(
                lower_executable_path_dispatch(path),
            ))
        }
        AccessPlan::Union(children) => {
            let mut lowered_children = Vec::with_capacity(children.len());
            for child in children {
                lowered_children.push(lower_access_node(child, index_spec_collection)?);
            }

            Ok(ExecutableAccessPlan::union(lowered_children))
        }
        AccessPlan::Intersection(children) => {
            let mut lowered_children = Vec::with_capacity(children.len());
            for child in children {
                lowered_children.push(lower_access_node(child, index_spec_collection)?);
            }

            Ok(ExecutableAccessPlan::intersection(lowered_children))
        }
    }
}

fn lower_index_specs_for_path<K>(
    entity_tag: EntityTag,
    path: &AccessPathDispatch<'_, K>,
    index_prefix_specs: &mut Vec<LoweredIndexPrefixSpec>,
    index_range_specs: &mut Vec<LoweredIndexRangeSpec>,
) -> Result<(), LoweredAccessError> {
    match path {
        AccessPathDispatch::IndexPrefix { index, values } => {
            lower_index_prefix_values_for_specs(entity_tag, *index, values, index_prefix_specs)
                .map_err(LoweredAccessError::IndexPrefix)?;
        }
        AccessPathDispatch::IndexMultiLookup { index, values } => {
            for value in *values {
                lower_index_prefix_values_for_specs(
                    entity_tag,
                    *index,
                    std::slice::from_ref(value),
                    index_prefix_specs,
                )
                .map_err(LoweredAccessError::IndexPrefix)?;
            }
        }
        AccessPathDispatch::IndexRange { spec } => {
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
            )
            .map_err(LoweredAccessError::IndexRange)?;
            index_range_specs.push(LoweredIndexRangeSpec::new(*spec.index(), lower, upper));
        }
        AccessPathDispatch::ByKey(_)
        | AccessPathDispatch::ByKeys(_)
        | AccessPathDispatch::KeyRange { .. }
        | AccessPathDispatch::FullScan => {}
    }

    Ok(())
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
