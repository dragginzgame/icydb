//! Module: access::lowering
//! Responsibility: lower validated semantic access specs into raw index-key bounds.
//! Does not own: access-shape validation or executor scan implementation.
//! Boundary: planner emits lowered contracts consumed directly by executor.

#[cfg(test)]
mod ownership_tests;

use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        index::{
            EncodedValue, IndexId, IndexKeyKind, IndexRangeBoundEncodeError, RawIndexStoreKey,
            build_index_component_range_with_encoded_prefix,
            build_index_prefix_bounds_for_encoded_components,
            encode_accepted_index_literal_component, raw_keys_for_component_prefix_with_kind,
        },
        query::construction::ConstructionBudget,
        schema::SchemaInfo,
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::{ops::Bound, sync::Arc, sync::OnceLock};

use crate::db::index::UserIndexPrefixCardinalityKey;

const fn record_deferred_index_prefix_raw_bound_materialization() {}

pub(in crate::db) type LoweredKey = RawIndexStoreKey;

type LoweredIndexRangeEnvelope = (Bound<LoweredKey>, Bound<LoweredKey>, Vec<Vec<u8>>);

const DEFERRED_MULTI_LOOKUP_PREFIX_BOUND_MIN_VALUES: usize = 32;

///
/// LoweredIndexSpecs
///
/// Index-bound specs collected from one access tree in depth-first order.
/// Execution owns its tree projection; preparation only retains these specs.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct LoweredIndexSpecs {
    index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    index_range_specs: Vec<LoweredIndexRangeSpec>,
}

impl LoweredIndexSpecs {
    #[must_use]
    pub(in crate::db) fn into_index_specs(
        self,
    ) -> (Vec<LoweredIndexPrefixSpec>, Vec<LoweredIndexRangeSpec>) {
        (self.index_prefix_specs, self.index_range_specs)
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
    Construction(InternalError),
}

impl LoweredAccessError {
    /// Convert access-lowering failure at the prepared-plan boundary without
    /// misclassifying index encoding failure as cursor state corruption.
    #[must_use]
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::IndexPrefix | Self::IndexRange => InternalError::index_invariant(),
            Self::Construction(error) => error,
        }
    }
}

/// Lower an access plan using accepted index contracts for enum equality
/// components.
pub(in crate::db) fn lower_access_with_schema_info<K>(
    entity_tag: EntityTag,
    access: &AccessPlan<K>,
    schema_info: &SchemaInfo,
    budget: &dyn ConstructionBudget,
) -> Result<LoweredIndexSpecs, LoweredAccessError> {
    let mut index_prefix_specs = Vec::new();
    let mut index_range_specs = Vec::new();
    lower_access_node(
        entity_tag,
        access,
        schema_info,
        &mut index_prefix_specs,
        &mut index_range_specs,
        budget,
    )?;

    Ok(LoweredIndexSpecs {
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

        let index_id =
            IndexId::new_with_generation(entity_tag, index.ordinal(), index.physical_generation());
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

    #[must_use]
    pub(in crate::db) const fn prefix_components(&self) -> &[Vec<u8>] {
        self.prefix_components.as_slice()
    }

    /// Consume this preparation and transfer its encoded prefix components.
    ///
    /// Advisory planner consumers use this after charging the complete
    /// transient lowering footprint, avoiding a second component buffer.
    #[must_use]
    pub(in crate::db) fn into_prefix_components(self) -> Vec<Vec<u8>> {
        self.prefix_components
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
    schema_info: &SchemaInfo,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
    budget: &dyn ConstructionBudget,
) -> Result<LoweredIndexRangeEnvelope, LoweredAccessError> {
    let index_id =
        IndexId::new_with_generation(entity_tag, index.ordinal(), index.physical_generation());

    let encoded_prefix = encode_index_prefix_values(schema_info, &index, prefix.iter(), budget)
        .map_err(|error| match error {
            LoweredAccessError::IndexPrefix => LoweredAccessError::IndexRange,
            error => error,
        })?;
    let lowering = build_index_component_range_with_encoded_prefix(
        &index_id,
        index.key_arity(),
        encoded_prefix,
        lower,
        upper,
    )
    .map_err(|_| LoweredAccessError::IndexRange)?;

    Ok(lowering.into_bounds_and_prefix_components())
}

// Lower one access node and collect raw index-bound specs in the same
// deterministic depth-first traversal.
fn lower_access_node<K>(
    entity_tag: EntityTag,
    access: &AccessPlan<K>,
    schema_info: &SchemaInfo,
    index_prefix_specs: &mut Vec<LoweredIndexPrefixSpec>,
    index_range_specs: &mut Vec<LoweredIndexRangeSpec>,
    budget: &dyn ConstructionBudget,
) -> Result<(), LoweredAccessError> {
    // Charge before descending or encoding a leaf. This traversal does not
    // construct an executable tree; byte encoding owns separate pending work.
    budget
        .charge(Resource::PredicateExpressionSteps, 1)
        .map_err(LoweredAccessError::Construction)?;
    match access {
        AccessPlan::Path(path) => {
            let path = path.as_ref();
            // The shared shape owner supplies exact output counts without
            // visiting operands. Admit outer backing before any leaf encoding;
            // scalar bytes and raw-bound construction retain their own owners.
            let shape = path.shape_facts();
            budget
                .reserve_vec(index_prefix_specs, shape.index_prefix_spec_count())
                .map_err(LoweredAccessError::Construction)?;
            budget
                .reserve_vec(
                    index_range_specs,
                    usize::from(shape.consumes_index_range_spec()),
                )
                .map_err(LoweredAccessError::Construction)?;
            lower_index_specs_for_path(
                entity_tag,
                path,
                schema_info,
                index_prefix_specs,
                index_range_specs,
                budget,
            )
        }
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            for child in children {
                lower_access_node(
                    entity_tag,
                    child,
                    schema_info,
                    index_prefix_specs,
                    index_range_specs,
                    budget,
                )?;
            }

            Ok(())
        }
    }
}

fn lower_index_specs_for_path<K>(
    entity_tag: EntityTag,
    path: &AccessPath<K>,
    schema_info: &SchemaInfo,
    index_prefix_specs: &mut Vec<LoweredIndexPrefixSpec>,
    index_range_specs: &mut Vec<LoweredIndexRangeSpec>,
    budget: &dyn ConstructionBudget,
) -> Result<(), LoweredAccessError> {
    match path {
        AccessPath::IndexPrefix { index, values } => {
            lower_index_prefix_values_for_specs(
                entity_tag,
                index.clone(),
                values.iter(),
                schema_info,
                index_prefix_specs,
                budget,
            )?;
        }
        AccessPath::IndexMultiLookup { index, values } => {
            lower_single_component_index_prefix_values_for_specs(
                entity_tag,
                index.clone(),
                values,
                schema_info,
                index_prefix_specs,
                budget,
            )?;
        }
        AccessPath::IndexBranchSet { spec } => {
            for branch_value in spec.branch_values() {
                lower_index_prefix_values_for_specs(
                    entity_tag,
                    spec.index(),
                    spec.branch_prefix_values(branch_value),
                    schema_info,
                    index_prefix_specs,
                    budget,
                )?;
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
                schema_info,
                spec.prefix_values(),
                spec.lower(),
                spec.upper(),
                budget,
            )?;
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

pub(in crate::db) fn lower_exact_user_index_prefix_cardinality_keys_for_prefix_access(
    entity_tag: EntityTag,
    access: &crate::db::query::plan::CountCardinalityPrefixAccess<'_>,
    schema_info: &SchemaInfo,
    budget: &dyn ConstructionBudget,
) -> Result<Vec<UserIndexPrefixCardinalityKey>, LoweredAccessError> {
    let values = access.values();
    if values.is_empty() {
        return Err(LoweredAccessError::IndexPrefix);
    }

    match values {
        crate::db::query::plan::CountCardinalityPrefixValues::One(value) => {
            lower_single_component_user_index_prefix_cardinality_keys(
                entity_tag,
                access.index().clone(),
                std::slice::from_ref(*value),
                schema_info,
                budget,
            )
        }
        crate::db::query::plan::CountCardinalityPrefixValues::Many(values) => {
            lower_single_component_user_index_prefix_cardinality_keys(
                entity_tag,
                access.index().clone(),
                values,
                schema_info,
                budget,
            )
        }
        crate::db::query::plan::CountCardinalityPrefixValues::ExactPrefixes(prefixes) => {
            lower_user_index_prefix_cardinality_keys(
                entity_tag,
                access.index().clone(),
                prefixes,
                schema_info,
                budget,
            )
        }
    }
}

fn lower_user_index_prefix_cardinality_keys(
    entity_tag: EntityTag,
    index: crate::db::access::SemanticIndexAccessContract,
    prefixes: &[Vec<Value>],
    schema_info: &SchemaInfo,
    budget: &dyn ConstructionBudget,
) -> Result<Vec<UserIndexPrefixCardinalityKey>, LoweredAccessError> {
    // Every component is encoded against the already selected accepted index;
    // this boundary derives lookup keys and never reconstructs index authority.
    if prefixes.is_empty() {
        return Err(LoweredAccessError::IndexPrefix);
    }

    let index_id =
        IndexId::new_with_generation(entity_tag, index.ordinal(), index.physical_generation());
    let mut keys = budget
        .vec_with_capacity(prefixes.len())
        .map_err(LoweredAccessError::Construction)?;
    for prefix in prefixes {
        if prefix.is_empty() {
            return Err(LoweredAccessError::IndexPrefix);
        }
        // Transfer encoder-owned payloads; retained accounting observes their
        // actual capacity instead of paying for a second, tight byte copy.
        let components = encode_index_prefix_values(schema_info, &index, prefix.iter(), budget)?
            .into_iter()
            .map(EncodedValue::into_bytes)
            .collect();
        keys.push(UserIndexPrefixCardinalityKey::new(index_id, components));
    }

    Ok(keys)
}

fn lower_single_component_user_index_prefix_cardinality_keys(
    entity_tag: EntityTag,
    index: crate::db::access::SemanticIndexAccessContract,
    values: &[Value],
    schema_info: &SchemaInfo,
    budget: &dyn ConstructionBudget,
) -> Result<Vec<UserIndexPrefixCardinalityKey>, LoweredAccessError> {
    if values.is_empty() {
        return Err(LoweredAccessError::IndexPrefix);
    }

    let index_id =
        IndexId::new_with_generation(entity_tag, index.ordinal(), index.physical_generation());
    let mut keys = budget
        .vec_with_capacity(values.len())
        .map_err(LoweredAccessError::Construction)?;
    for value in values {
        let components =
            encode_index_prefix_values(schema_info, &index, std::iter::once(value), budget)?
                .into_iter()
                .map(EncodedValue::into_bytes)
                .collect();
        keys.push(UserIndexPrefixCardinalityKey::new(index_id, components));
    }

    Ok(keys)
}

fn lower_index_prefix_values_for_specs<'a>(
    entity_tag: EntityTag,
    index: crate::db::access::SemanticIndexAccessContract,
    values: impl Iterator<Item = &'a Value>,
    schema_info: &SchemaInfo,
    specs: &mut Vec<LoweredIndexPrefixSpec>,
    budget: &dyn ConstructionBudget,
) -> Result<(), LoweredAccessError> {
    let encoded_values = encode_index_prefix_values(schema_info, &index, values, budget)?;
    let scan_contract = LoweredIndexScanContract::from_access_contract(index.clone());

    push_lowered_index_prefix_spec_from_encoded_components(
        entity_tag,
        &index,
        scan_contract,
        encoded_values,
        specs,
        false,
    )
    .map_err(|_| LoweredAccessError::IndexPrefix)
}

fn push_lowered_index_prefix_spec_from_encoded_components(
    entity_tag: EntityTag,
    index: &crate::db::access::SemanticIndexAccessContract,
    scan_contract: LoweredIndexScanContract,
    encoded_values: Vec<EncodedValue>,
    specs: &mut Vec<LoweredIndexPrefixSpec>,
    defer_raw_bounds: bool,
) -> Result<(), InternalError> {
    let index_id =
        IndexId::new_with_generation(entity_tag, index.ordinal(), index.physical_generation());
    let raw_bounds = if defer_raw_bounds {
        None
    } else {
        Some(
            build_index_prefix_bounds_for_encoded_components(
                &index_id,
                IndexKeyKind::User,
                index.key_arity(),
                &encoded_values,
            )
            .map_err(validated_spec_not_indexable)?,
        )
    };
    // Raw bounds only borrow the encoded values. Keep those allocations in
    // the final spec instead of copying and discarding every component.
    let prefix_components = encoded_values
        .into_iter()
        .map(EncodedValue::into_bytes)
        .collect();
    specs.push(match raw_bounds {
        Some((lower, upper)) => LoweredIndexPrefixSpec::from_scan_contract(
            scan_contract,
            lower,
            upper,
            prefix_components,
        ),
        None => LoweredIndexPrefixSpec::from_deferred_component_prefix(
            scan_contract,
            index_id,
            IndexKeyKind::User,
            index.key_arity(),
            prefix_components,
        ),
    });

    Ok(())
}

fn lower_single_component_index_prefix_values_for_specs(
    entity_tag: EntityTag,
    index: crate::db::access::SemanticIndexAccessContract,
    values: &[Value],
    schema_info: &SchemaInfo,
    specs: &mut Vec<LoweredIndexPrefixSpec>,
    budget: &dyn ConstructionBudget,
) -> Result<(), LoweredAccessError> {
    let scan_contract = LoweredIndexScanContract::from_access_contract(index.clone());
    let defer_raw_bounds = values.len() >= DEFERRED_MULTI_LOOKUP_PREFIX_BOUND_MIN_VALUES;

    for value in values {
        let encoded =
            encode_index_prefix_values(schema_info, &index, std::iter::once(value), budget)?;
        push_lowered_index_prefix_spec_from_encoded_components(
            entity_tag,
            &index,
            scan_contract.clone(),
            encoded,
            specs,
            defer_raw_bounds,
        )
        .map_err(|_| LoweredAccessError::IndexPrefix)?;
    }

    Ok(())
}

fn encode_index_prefix_values<'a>(
    schema_info: &SchemaInfo,
    index: &crate::db::access::SemanticIndexAccessContract,
    values: impl Iterator<Item = &'a Value>,
    budget: &dyn ConstructionBudget,
) -> Result<Vec<EncodedValue>, LoweredAccessError> {
    // Slice, singleton and borrowed branch iterators all provide their exact
    // lower size hint. Reserve before encoding; growth remains charged even if
    // a future iterator cannot provide its full size up front.
    let mut encoded = budget
        .vec_with_capacity(values.size_hint().0)
        .map_err(LoweredAccessError::Construction)?;
    for (component_index, value) in values.enumerate() {
        budget
            .charge(Resource::PredicateExpressionSteps, 1)
            .map_err(LoweredAccessError::Construction)?;
        budget
            .reserve_vec(&mut encoded, 1)
            .map_err(LoweredAccessError::Construction)?;
        encoded.push(
            encode_index_component(schema_info, index, component_index, value)
                .map_err(|_| LoweredAccessError::IndexPrefix)?,
        );
    }

    Ok(encoded)
}

fn encode_index_component(
    schema_info: &SchemaInfo,
    index: &crate::db::access::SemanticIndexAccessContract,
    component_index: usize,
    value: &Value,
) -> Result<EncodedValue, InternalError> {
    let bytes =
        encode_accepted_index_literal_component(schema_info, index.name(), component_index, value)?
            .ok_or_else(InternalError::query_executor_invariant)?;

    Ok(EncodedValue::from_canonical_bytes(bytes))
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_copy!(DeferredIndexPrefixRawBoundsSource);

#[cfg(test)]
mod retention_tests {
    use super::{
        EntityTag, IndexId, IndexKeyKind, LoweredIndexPrefixRawBounds, LoweredIndexPrefixSpec,
        LoweredIndexScanContract,
    };
    use crate::{MAX_INDEX_FIELDS, db::index::IndexKey, retained::RetainedBytes};
    use std::sync::Arc;

    #[test]
    fn retained_deferred_bounds_reserve_encoder_capacity_before_materialization() {
        for arity in 1..=MAX_INDEX_FIELDS {
            for prefix_len in 0..=arity {
                let prefix_components = vec![vec![1_u8; 8]; prefix_len];
                let expected =
                    IndexKey::raw_prefix_bounds_retained_capacity(arity, &prefix_components);
                let spec = LoweredIndexPrefixSpec {
                    scan_contract: LoweredIndexScanContract {
                        name: Arc::from("index"),
                        store_path: Arc::from("store"),
                    },
                    raw_bounds: LoweredIndexPrefixRawBounds::deferred_component_prefix(
                        IndexId::new(EntityTag::new(254), 1),
                        IndexKeyKind::User,
                        arity,
                    ),
                    prefix_components,
                };
                let before =
                    RetainedBytes::measure(&spec, usize::MAX).expect("reserved bound capacity");
                let (lower, upper) = spec.raw_bounds().expect("valid prefix");
                let mut materialized = RetainedBytes::new(usize::MAX);
                materialized.visit(lower).expect("known lower allocation");
                materialized.visit(upper).expect("known upper allocation");
                assert_eq!(materialized.total(), expected);
                assert_eq!(RetainedBytes::measure(&spec, usize::MAX), Some(before));
            }
        }
    }
}
impl crate::retained::Retained for LoweredIndexPrefixSpec {
    fn visit_retained(&self, bytes: &mut crate::retained::RetainedBytes) -> Option<()> {
        let Self {
            scan_contract,
            raw_bounds,
            prefix_components,
        } = self;
        bytes.visit(scan_contract)?;
        bytes.visit(prefix_components)?;
        match raw_bounds {
            LoweredIndexPrefixRawBounds::Materialized { lower, upper } => {
                bytes.visit(lower)?;
                bytes.visit(upper)
            }
            LoweredIndexPrefixRawBounds::DeferredComponentPrefix { source, raw_bounds } => {
                // Borrowed bounds must remain retained once materialized. Reserve
                // their encoder-owned capacity before admitting the shared plan.
                if let Some(bounds) = raw_bounds.get() {
                    return bytes.visit(bounds);
                }
                bytes.add(
                    crate::db::index::IndexKey::raw_prefix_bounds_retained_capacity(
                        source.key_arity,
                        prefix_components,
                    ),
                )
            }
        }
    }
}
crate::retained::retained_fields!(LoweredIndexRangeSpec {
Self{scan_contract,lower,upper,prefix_components} => [scan_contract,lower,upper,prefix_components],
});
crate::retained::retained_fields!(LoweredIndexScanContract {
Self{name,store_path} => [name,store_path],
});
