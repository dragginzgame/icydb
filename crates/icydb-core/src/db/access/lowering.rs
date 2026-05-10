//! Module: access::lowering
//! Responsibility: lower validated semantic access specs into raw index-key bounds.
//! Does not own: access-shape validation or executor scan implementation.
//! Boundary: planner emits lowered contracts consumed directly by executor.

use crate::{
    db::{
        access::{AccessPath, AccessPlan, ExecutableAccessPlan},
        index::{
            IndexBoundsSpec, IndexId, IndexRangeBoundEncodeError, RawIndexKey,
            build_index_bounds_for_arity,
        },
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
use std::ops::Bound;

pub(in crate::db) type LoweredKey = RawIndexKey;

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
    pub(in crate::db) const fn executable(&self) -> &ExecutableAccessPlan<'a, K> {
        &self.executable
    }

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
    let executable = lower_access_node(
        entity_tag,
        access,
        &mut index_prefix_specs,
        &mut index_range_specs,
    )?;

    Ok(LoweredAccess {
        executable,
        index_prefix_specs,
        index_range_specs,
    })
}

///
/// LoweredIndexScanContract
///
/// Reduced index facts carried after raw bounds have been materialized.
/// Physical executor scans only need these facts for diagnostics and raw-entry
/// membership validation; they must not reopen generated key-shape authority.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct LoweredIndexScanContract {
    name: String,
    store_path: String,
    unique: bool,
}

impl LoweredIndexScanContract {
    #[must_use]
    fn from_access_contract(index: crate::db::access::SemanticIndexAccessContract) -> Self {
        Self {
            name: index.name().to_string(),
            store_path: index.store_path().to_string(),
            unique: index.is_unique(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
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
    scan_contract: LoweredIndexScanContract,
    lower: Bound<LoweredKey>,
    upper: Bound<LoweredKey>,
}

impl LoweredIndexPrefixSpec {
    const INVALID_REASON: &str = "validated index-prefix plan could not be lowered to raw bounds";

    #[must_use]
    pub(in crate::db) fn new(
        index: crate::db::access::SemanticIndexAccessContract,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
    ) -> Self {
        Self {
            scan_contract: LoweredIndexScanContract::from_access_contract(index),
            lower,
            upper,
        }
    }

    #[must_use]
    pub(in crate::db) fn scan_contract(&self) -> LoweredIndexScanContract {
        self.scan_contract.clone()
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
    scan_contract: LoweredIndexScanContract,
    lower: Bound<LoweredKey>,
    upper: Bound<LoweredKey>,
}

impl LoweredIndexRangeSpec {
    const INVALID_REASON: &str = "validated index-range plan could not be lowered to raw bounds";

    #[must_use]
    pub(in crate::db) fn new(
        index: crate::db::access::SemanticIndexAccessContract,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
    ) -> Self {
        Self {
            scan_contract: LoweredIndexScanContract::from_access_contract(index),
            lower,
            upper,
        }
    }

    #[must_use]
    pub(in crate::db) fn scan_contract(&self) -> LoweredIndexScanContract {
        self.scan_contract.clone()
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
    index: crate::db::access::SemanticIndexAccessContract,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<(Bound<LoweredKey>, Bound<LoweredKey>), InternalError> {
    let index_id = IndexId::new(entity_tag, index.ordinal());

    build_index_bounds_for_arity(
        &index_id,
        index.key_arity(),
        IndexBoundsSpec::component_range(prefix, lower, upper),
    )
    .map_err(LoweredIndexRangeSpec::validated_spec_not_indexable)
}

// Lower one access node and collect raw index-bound specs in the same
// deterministic depth-first traversal.
fn lower_access_node<'a, K>(
    entity_tag: EntityTag,
    access: &'a AccessPlan<K>,
    index_prefix_specs: &mut Vec<LoweredIndexPrefixSpec>,
    index_range_specs: &mut Vec<LoweredIndexRangeSpec>,
) -> Result<ExecutableAccessPlan<'a, K>, LoweredAccessError> {
    match access {
        AccessPlan::Path(path) => {
            let path = path.as_ref();
            lower_index_specs_for_path(entity_tag, path, index_prefix_specs, index_range_specs)?;

            Ok(ExecutableAccessPlan::from_access_path(path))
        }
        AccessPlan::Union(children) => {
            let mut lowered_children = Vec::with_capacity(children.len());
            for child in children {
                lowered_children.push(lower_access_node(
                    entity_tag,
                    child,
                    index_prefix_specs,
                    index_range_specs,
                )?);
            }

            Ok(ExecutableAccessPlan::union(lowered_children))
        }
        AccessPlan::Intersection(children) => {
            let mut lowered_children = Vec::with_capacity(children.len());
            for child in children {
                lowered_children.push(lower_access_node(
                    entity_tag,
                    child,
                    index_prefix_specs,
                    index_range_specs,
                )?);
            }

            Ok(ExecutableAccessPlan::intersection(lowered_children))
        }
    }
}

fn lower_index_specs_for_path<K>(
    entity_tag: EntityTag,
    path: &AccessPath<K>,
    index_prefix_specs: &mut Vec<LoweredIndexPrefixSpec>,
    index_range_specs: &mut Vec<LoweredIndexRangeSpec>,
) -> Result<(), LoweredAccessError> {
    match path {
        AccessPath::IndexPrefix { index, values } => {
            lower_index_prefix_values_for_specs(
                entity_tag,
                index.clone(),
                values,
                index_prefix_specs,
            )
            .map_err(LoweredAccessError::IndexPrefix)?;
        }
        AccessPath::IndexMultiLookup { index, values } => {
            for value in values {
                lower_index_prefix_values_for_specs(
                    entity_tag,
                    index.clone(),
                    std::slice::from_ref(value),
                    index_prefix_specs,
                )
                .map_err(LoweredAccessError::IndexPrefix)?;
            }
        }
        AccessPath::IndexRange { spec } => {
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
            index_range_specs.push(LoweredIndexRangeSpec::new(spec.index(), lower, upper));
        }
        AccessPath::ByKey(_)
        | AccessPath::ByKeys(_)
        | AccessPath::KeyRange { .. }
        | AccessPath::FullScan => {}
    }

    Ok(())
}

fn lower_index_prefix_values_for_specs(
    entity_tag: EntityTag,
    index: crate::db::access::SemanticIndexAccessContract,
    values: &[Value],
    specs: &mut Vec<LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    let index_id = IndexId::new(entity_tag, index.ordinal());
    let (lower, upper) = build_index_bounds_for_arity(
        &index_id,
        index.key_arity(),
        IndexBoundsSpec::Prefix { values },
    )
    .map_err(|_| {
        InternalError::query_executor_invariant("validated index-prefix value is not indexable")
    })?;
    specs.push(LoweredIndexPrefixSpec::new(index, lower, upper));

    Ok(())
}
