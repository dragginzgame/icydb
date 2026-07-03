//! Module: access::lowering
//! Responsibility: lower validated semantic access specs into raw index-key bounds.
//! Does not own: access-shape validation or executor scan implementation.
//! Boundary: planner emits lowered contracts consumed directly by executor.

use crate::{
    db::{
        access::{AccessPath, AccessPlan, ExecutableAccessPlan},
        index::{
            EncodedValue, IndexBoundsSpec, IndexId, IndexKeyKind, IndexRangeBoundEncodeError,
            RawIndexStoreKey, build_index_bounds_lowering_for_arity,
            build_index_prefix_bounds_for_encoded_components,
            raw_keys_for_component_prefix_with_kind,
        },
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
#[cfg(test)]
use std::cell::Cell;
use std::{ops::Bound, sync::Arc, sync::OnceLock};

#[cfg(test)]
thread_local! {
    static DEFERRED_INDEX_PREFIX_RAW_BOUND_MATERIALIZATION_COUNT: Cell<u64> =
        const { Cell::new(0) };
}

#[cfg(test)]
fn record_deferred_index_prefix_raw_bound_materialization() {
    DEFERRED_INDEX_PREFIX_RAW_BOUND_MATERIALIZATION_COUNT.with(|count| {
        count.set(count.get().saturating_add(1));
    });
}

#[cfg(not(test))]
const fn record_deferred_index_prefix_raw_bound_materialization() {}

#[cfg(test)]
pub(in crate::db) fn current_deferred_index_prefix_raw_bound_materialization_count_for_tests() -> u64
{
    DEFERRED_INDEX_PREFIX_RAW_BOUND_MATERIALIZATION_COUNT.with(Cell::get)
}

pub(in crate::db) type LoweredKey = RawIndexStoreKey;

type LoweredIndexRangeEnvelope = (Bound<LoweredKey>, Bound<LoweredKey>, Vec<Vec<u8>>);

const DEFERRED_MULTI_LOOKUP_PREFIX_BOUND_MIN_VALUES: usize = 32;

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
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn index_prefix_specs(&self) -> &[LoweredIndexPrefixSpec] {
        self.index_prefix_specs.as_slice()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn index_range_specs(&self) -> &[LoweredIndexRangeSpec] {
        self.index_range_specs.as_slice()
    }

    #[must_use]
    pub(in crate::db) fn into_executable_and_index_specs(
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
    IndexPrefix,
    IndexRange,
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
    name: Arc<str>,
    store_path: Arc<str>,
}

impl LoweredIndexScanContract {
    #[must_use]
    fn from_access_contract(index: crate::db::access::SemanticIndexAccessContract) -> Self {
        Self {
            name: Arc::from(index.name()),
            store_path: Arc::from(index.store_path()),
        }
    }

    #[must_use]
    pub(in crate::db) fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub(in crate::db) fn store_path(&self) -> &str {
        &self.store_path
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
    raw_bounds: LoweredIndexPrefixRawBounds,
    prefix_components: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeferredIndexPrefixRawBoundsSource {
    index_id: IndexId,
    key_kind: IndexKeyKind,
    key_arity: usize,
}

#[derive(Debug)]
enum LoweredIndexPrefixRawBounds {
    Materialized {
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
    },
    DeferredComponentPrefix {
        source: DeferredIndexPrefixRawBoundsSource,
        raw_bounds: OnceLock<(Bound<LoweredKey>, Bound<LoweredKey>)>,
    },
}

impl Clone for LoweredIndexPrefixRawBounds {
    fn clone(&self) -> Self {
        match self {
            Self::Materialized { lower, upper } => Self::Materialized {
                lower: lower.clone(),
                upper: upper.clone(),
            },
            Self::DeferredComponentPrefix { source, raw_bounds } => {
                let cloned_raw_bounds = OnceLock::new();
                if let Some(bounds) = raw_bounds.get() {
                    let _ = cloned_raw_bounds.set(bounds.clone());
                }

                Self::DeferredComponentPrefix {
                    source: *source,
                    raw_bounds: cloned_raw_bounds,
                }
            }
        }
    }
}

impl Eq for LoweredIndexPrefixRawBounds {}

impl PartialEq for LoweredIndexPrefixRawBounds {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Materialized {
                    lower: left_lower,
                    upper: left_upper,
                },
                Self::Materialized {
                    lower: right_lower,
                    upper: right_upper,
                },
            ) => left_lower == right_lower && left_upper == right_upper,
            (
                Self::DeferredComponentPrefix {
                    source: left_source,
                    ..
                },
                Self::DeferredComponentPrefix {
                    source: right_source,
                    ..
                },
            ) => left_source == right_source,
            _ => false,
        }
    }
}

impl LoweredIndexPrefixRawBounds {
    const fn materialized(lower: Bound<LoweredKey>, upper: Bound<LoweredKey>) -> Self {
        Self::Materialized { lower, upper }
    }

    const fn deferred_component_prefix(
        index_id: IndexId,
        key_kind: IndexKeyKind,
        key_arity: usize,
    ) -> Self {
        Self::DeferredComponentPrefix {
            source: DeferredIndexPrefixRawBoundsSource {
                index_id,
                key_kind,
                key_arity,
            },
            raw_bounds: OnceLock::new(),
        }
    }

    fn raw_bounds(
        &self,
        prefix_components: &[Vec<u8>],
    ) -> Result<(&Bound<LoweredKey>, &Bound<LoweredKey>), InternalError> {
        match self {
            Self::Materialized { lower, upper } => Ok((lower, upper)),
            Self::DeferredComponentPrefix { source, raw_bounds } => {
                if let Some(bounds) = raw_bounds.get() {
                    return Ok((&bounds.0, &bounds.1));
                }

                let (lower, upper) = raw_keys_for_component_prefix_with_kind(
                    &source.index_id,
                    source.key_kind,
                    source.key_arity,
                    prefix_components,
                )
                .map_err(validated_spec_not_indexable)?;
                record_deferred_index_prefix_raw_bound_materialization();
                let _ = raw_bounds.set((Bound::Included(lower), Bound::Included(upper)));
                raw_bounds
                    .get()
                    .map(|bounds| (&bounds.0, &bounds.1))
                    .ok_or_else(InternalError::query_executor_invariant)
            }
        }
    }

    const fn deferred_source(&self) -> Option<DeferredIndexPrefixRawBoundsSource> {
        match self {
            Self::Materialized { .. } => None,
            Self::DeferredComponentPrefix { source, .. } => Some(*source),
        }
    }
}

impl LoweredIndexPrefixSpec {
    #[must_use]
    fn new(
        index: crate::db::access::SemanticIndexAccessContract,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
        prefix_components: Vec<Vec<u8>>,
    ) -> Self {
        Self::from_scan_contract(
            LoweredIndexScanContract::from_access_contract(index),
            lower,
            upper,
            prefix_components,
        )
    }

    #[must_use]
    const fn from_scan_contract(
        scan_contract: LoweredIndexScanContract,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
        prefix_components: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            scan_contract,
            raw_bounds: LoweredIndexPrefixRawBounds::materialized(lower, upper),
            prefix_components,
        }
    }

    #[must_use]
    const fn from_deferred_component_prefix(
        scan_contract: LoweredIndexScanContract,
        index_id: IndexId,
        key_kind: IndexKeyKind,
        key_arity: usize,
        prefix_components: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            scan_contract,
            raw_bounds: LoweredIndexPrefixRawBounds::deferred_component_prefix(
                index_id, key_kind, key_arity,
            ),
            prefix_components,
        }
    }

    pub(in crate::db) fn from_raw_component_prefix(
        entity_tag: EntityTag,
        index: crate::db::access::SemanticIndexAccessContract,
        key_kind: IndexKeyKind,
        prefix_components: Vec<Vec<u8>>,
    ) -> Result<Self, InternalError> {
        if prefix_components.is_empty() || prefix_components.len() > index.key_arity() {
            return Err(InternalError::query_executor_invariant());
        }

        let index_id = IndexId::new(entity_tag, index.ordinal());
        let (lower, upper) = raw_keys_for_component_prefix_with_kind(
            &index_id,
            key_kind,
            index.key_arity(),
            prefix_components.as_slice(),
        )
        .map_err(validated_spec_not_indexable)?;

        Ok(Self::new(
            index,
            Bound::Included(lower),
            Bound::Excluded(upper),
            prefix_components,
        ))
    }

    #[must_use]
    pub(in crate::db) fn scan_contract(&self) -> LoweredIndexScanContract {
        self.scan_contract.clone()
    }

    pub(in crate::db) fn raw_bounds(
        &self,
    ) -> Result<(&Bound<LoweredKey>, &Bound<LoweredKey>), InternalError> {
        self.raw_bounds.raw_bounds(self.prefix_components())
    }

    pub(in crate::db) fn lower(&self) -> Result<&Bound<LoweredKey>, InternalError> {
        self.raw_bounds().map(|bounds| bounds.0)
    }

    #[cfg(test)]
    pub(in crate::db) fn upper(&self) -> Result<&Bound<LoweredKey>, InternalError> {
        self.raw_bounds().map(|bounds| bounds.1)
    }

    #[must_use]
    pub(in crate::db) const fn prefix_components(&self) -> &[Vec<u8>] {
        self.prefix_components.as_slice()
    }

    #[must_use]
    pub(in crate::db) const fn deferred_cardinality_source(
        &self,
    ) -> Option<(IndexId, IndexKeyKind)> {
        match self.raw_bounds.deferred_source() {
            Some(source) => Some((source.index_id, source.key_kind)),
            None => None,
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn has_deferred_raw_bounds_for_tests(&self) -> bool {
        matches!(
            &self.raw_bounds,
            LoweredIndexPrefixRawBounds::DeferredComponentPrefix { .. }
        )
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn deferred_raw_bounds_materialized_for_tests(&self) -> bool {
        match &self.raw_bounds {
            LoweredIndexPrefixRawBounds::Materialized { .. } => true,
            LoweredIndexPrefixRawBounds::DeferredComponentPrefix { raw_bounds, .. } => {
                raw_bounds.get().is_some()
            }
        }
    }
}

///
/// LoweredIndexPrefixCardinalitySpec
///
/// Exact index-prefix metadata key for count-only paths.
/// It intentionally carries only the already-encoded prefix components needed
/// by index cardinality metadata, not scan bounds or executor traversal state.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sql")]
pub(in crate::db) struct LoweredIndexPrefixCardinalitySpec {
    index_id: IndexId,
    prefix_components: Vec<Vec<u8>>,
}

#[cfg(feature = "sql")]
impl LoweredIndexPrefixCardinalitySpec {
    #[must_use]
    pub(in crate::db) const fn new(index_id: IndexId, prefix_components: Vec<Vec<u8>>) -> Self {
        Self {
            index_id,
            prefix_components,
        }
    }

    #[must_use]
    pub(in crate::db) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    #[must_use]
    pub(in crate::db) const fn prefix_components(&self) -> &[Vec<u8>] {
        self.prefix_components.as_slice()
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
    prefix_components: Vec<Vec<u8>>,
}

impl LoweredIndexRangeSpec {
    #[must_use]
    fn new(
        index: crate::db::access::SemanticIndexAccessContract,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
        prefix_components: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            scan_contract: LoweredIndexScanContract::from_access_contract(index),
            lower,
            upper,
            prefix_components,
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

    #[must_use]
    pub(in crate::db) const fn prefix_components(&self) -> &[Vec<u8>] {
        self.prefix_components.as_slice()
    }
}

// Build the canonical lowering-time invariant for validated index specs that
// still fail raw bound encoding.
fn validated_spec_not_indexable(_err: IndexRangeBoundEncodeError) -> InternalError {
    InternalError::query_executor_invariant()
}

// Lower one semantic range envelope into byte bounds with stable reason mapping.
fn lower_index_range_bounds_for_scope(
    entity_tag: EntityTag,
    index: crate::db::access::SemanticIndexAccessContract,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<LoweredIndexRangeEnvelope, InternalError> {
    let index_id = IndexId::new(entity_tag, index.ordinal());

    let lowering = build_index_bounds_lowering_for_arity(
        &index_id,
        index.key_arity(),
        IndexBoundsSpec::component_range(prefix, lower, upper),
    )
    .map_err(validated_spec_not_indexable)?;

    Ok(lowering.into_bounds_and_prefix_components())
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
            .map_err(|_err| LoweredAccessError::IndexPrefix)?;
        }
        AccessPath::IndexMultiLookup { index, values } => {
            lower_single_component_index_prefix_values_for_specs(
                entity_tag,
                index.clone(),
                values,
                index_prefix_specs,
            )
            .map_err(|_err| LoweredAccessError::IndexPrefix)?;
        }
        AccessPath::IndexBranchSet { spec } => {
            for branch_value in spec.branch_values() {
                let values = spec.branch_prefix_values(branch_value);
                lower_index_prefix_values_for_specs(
                    entity_tag,
                    spec.index(),
                    values.as_slice(),
                    index_prefix_specs,
                )
                .map_err(|_err| LoweredAccessError::IndexPrefix)?;
            }
        }
        AccessPath::IndexRange { spec } => {
            debug_assert_eq!(
                spec.field_slots().len(),
                spec.prefix_values().len().saturating_add(1),
                "semantic range field-slot arity must remain prefix_len + range slot",
            );
            let (lower, upper, prefix_components) = lower_index_range_bounds_for_scope(
                entity_tag,
                spec.index(),
                spec.prefix_values(),
                spec.lower(),
                spec.upper(),
            )
            .map_err(|_err| LoweredAccessError::IndexRange)?;
            index_range_specs.push(LoweredIndexRangeSpec::new(
                spec.index(),
                lower,
                upper,
                prefix_components,
            ));
        }
        AccessPath::ByKey(_)
        | AccessPath::ByKeys(_)
        | AccessPath::KeyRange { .. }
        | AccessPath::FullScan => {}
    }

    Ok(())
}

#[cfg(feature = "sql")]
pub(in crate::db) fn lower_exact_index_prefix_cardinality_specs_for_prefix_access(
    entity_tag: EntityTag,
    access: &crate::db::query::plan::CountCardinalityPrefixAccess<'_>,
) -> Result<Vec<LoweredIndexPrefixCardinalitySpec>, LoweredAccessError> {
    let values = access.values();
    if values.is_empty() {
        return Err(LoweredAccessError::IndexPrefix);
    }

    match values {
        crate::db::query::plan::CountCardinalityPrefixValues::One(value) => {
            lower_single_component_index_prefix_cardinality_specs(
                entity_tag,
                access.index().ordinal(),
                std::slice::from_ref(value),
            )
            .map_err(|_err| LoweredAccessError::IndexPrefix)
        }
        crate::db::query::plan::CountCardinalityPrefixValues::Many(values) => {
            lower_single_component_index_prefix_cardinality_specs(
                entity_tag,
                access.index().ordinal(),
                values,
            )
            .map_err(|_err| LoweredAccessError::IndexPrefix)
        }
    }
}

#[cfg(feature = "sql")]
fn lower_single_component_index_prefix_cardinality_specs(
    entity_tag: EntityTag,
    index_ordinal: u16,
    values: &[Value],
) -> Result<Vec<LoweredIndexPrefixCardinalitySpec>, InternalError> {
    if values.is_empty() {
        return Err(InternalError::query_executor_invariant());
    }

    let index_id = IndexId::new(entity_tag, index_ordinal);
    let mut specs = Vec::with_capacity(values.len());
    for value in values {
        let component = EncodedValue::try_from_ref(value)
            .map_err(|_| InternalError::query_executor_invariant())?
            .into_bytes();
        specs.push(LoweredIndexPrefixCardinalitySpec::new(
            index_id,
            vec![component],
        ));
    }

    Ok(specs)
}

fn lower_index_prefix_values_for_specs(
    entity_tag: EntityTag,
    index: crate::db::access::SemanticIndexAccessContract,
    values: &[Value],
    specs: &mut Vec<LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    let encoded_values = EncodedValue::try_encode_all(values)
        .map_err(|_| InternalError::query_executor_invariant())?;
    let scan_contract = LoweredIndexScanContract::from_access_contract(index.clone());

    push_lowered_index_prefix_spec_from_encoded_components(
        entity_tag,
        &index,
        scan_contract,
        &encoded_values,
        specs,
    )
}

fn push_lowered_index_prefix_spec_from_encoded_components(
    entity_tag: EntityTag,
    index: &crate::db::access::SemanticIndexAccessContract,
    scan_contract: LoweredIndexScanContract,
    encoded_values: &[EncodedValue],
    specs: &mut Vec<LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    let index_id = IndexId::new(entity_tag, index.ordinal());
    let (lower, upper) = build_index_prefix_bounds_for_encoded_components(
        &index_id,
        IndexKeyKind::User,
        index.key_arity(),
        encoded_values,
    )
    .map_err(validated_spec_not_indexable)?;
    let prefix_components = encoded_values
        .iter()
        .map(|encoded| encoded.encoded().to_vec())
        .collect();
    specs.push(LoweredIndexPrefixSpec::from_scan_contract(
        scan_contract,
        lower,
        upper,
        prefix_components,
    ));

    Ok(())
}

fn push_lowered_index_prefix_spec_from_single_encoded_component(
    entity_tag: EntityTag,
    index: &crate::db::access::SemanticIndexAccessContract,
    scan_contract: LoweredIndexScanContract,
    encoded_value: EncodedValue,
    specs: &mut Vec<LoweredIndexPrefixSpec>,
    defer_raw_bounds: bool,
) -> Result<(), InternalError> {
    let index_id = IndexId::new(entity_tag, index.ordinal());
    if !defer_raw_bounds {
        let (lower, upper) = build_index_prefix_bounds_for_encoded_components(
            &index_id,
            IndexKeyKind::User,
            index.key_arity(),
            std::slice::from_ref(&encoded_value),
        )
        .map_err(validated_spec_not_indexable)?;
        specs.push(LoweredIndexPrefixSpec::from_scan_contract(
            scan_contract,
            lower,
            upper,
            vec![encoded_value.into_bytes()],
        ));

        return Ok(());
    }

    specs.push(LoweredIndexPrefixSpec::from_deferred_component_prefix(
        scan_contract,
        index_id,
        IndexKeyKind::User,
        index.key_arity(),
        vec![encoded_value.into_bytes()],
    ));

    Ok(())
}

fn lower_single_component_index_prefix_values_for_specs(
    entity_tag: EntityTag,
    index: crate::db::access::SemanticIndexAccessContract,
    values: &[Value],
    specs: &mut Vec<LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    let scan_contract = LoweredIndexScanContract::from_access_contract(index.clone());
    let defer_raw_bounds = values.len() >= DEFERRED_MULTI_LOOKUP_PREFIX_BOUND_MIN_VALUES;

    specs.reserve(values.len());
    for value in values {
        let encoded = EncodedValue::try_from_ref(value)
            .map_err(|_| InternalError::query_executor_invariant())?;
        push_lowered_index_prefix_spec_from_single_encoded_component(
            entity_tag,
            &index,
            scan_contract.clone(),
            encoded,
            specs,
            defer_raw_bounds,
        )?;
    }

    Ok(())
}
