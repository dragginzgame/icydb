//! Module: query::plan::covering
//! Responsibility: planner covering-projection eligibility and order-contract derivation.
//! Does not own: runtime projection materialization or executor ordering enforcement.
//! Boundary: exposes planner-only covering contracts for index-backed paths.

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::db::{
    access::AccessPlan,
    direction::Direction,
    predicate::IndexPredicateCapability,
    query::plan::{
        AccessPlannedQuery, DeterministicSecondaryIndexOrderMatch, FieldSlot, OrderDirection,
        OrderSpec, expr::ProjectionSpec, index_order_terms,
    },
};
use crate::{
    model::{
        field::FieldModel,
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
    value::Value,
};

///
/// CoveringProjectionOrder
///
/// Planner-owned covering projection order contract.
/// Index order means projected component order is preserved from index traversal.
/// Primary-key order means runtime must reorder by primary key after projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CoveringProjectionOrder {
    IndexOrder(Direction),
    PrimaryKeyOrder(Direction),
}

///
/// CoveringProjectionContext
///
/// Planner-owned covering projection context contract.
/// Captures projection component position, bound-prefix arity, and output-order
/// interpretation for one index-backed access shape.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CoveringProjectionContext {
    pub(in crate::db) component_index: usize,
    pub(in crate::db) prefix_len: usize,
    pub(in crate::db) order_contract: CoveringProjectionOrder,
}

///
/// CoveringReadFieldSource
///
/// Planner-owned covering-read source contract for one scalar output field.
/// Pure covering-read fast paths admit only index components, primary-key
/// output, and prefix-bound constants.
/// `RowField` is reserved for SQL-side hybrid direct projection plans that
/// still need sparse row reads for uncovered fields, but it is not admitted by
/// executor covering-read fast paths.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum CoveringReadFieldSource {
    IndexComponent { component_index: usize },
    PrimaryKey,
    Constant(Value),
    RowField,
}

///
/// CoveringReadField
///
/// One planner-owned scalar covering-read output field.
/// Output order stays canonical projection order while `source` records how
/// runtime can satisfy the value without row-materialized reads.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CoveringReadField {
    pub(in crate::db) field_slot: FieldSlot,
    pub(in crate::db) source: CoveringReadFieldSource,
}

///
/// CoveringReadPlan
///
/// Planner-owned scalar covering-read contract.
/// This stays route-local and phase-1 conservative: only direct field
/// projections over index-backed scalar reads can produce this plan.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CoveringReadPlan {
    pub(in crate::db) fields: Vec<CoveringReadField>,
    pub(in crate::db) prefix_len: usize,
    pub(in crate::db) order_contract: CoveringProjectionOrder,
}

///
/// CoveringExistingRowMode
///
/// Planner-owned row-presence contract for one covering-read execution shape.
/// `RequiresRowPresenceCheck` keeps the current fail-closed semantics explicit:
/// the route is covering-backed, but execution must still confirm that the row
/// exists in row storage before it can emit output.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CoveringExistingRowMode {
    ProvenByPlanner,
    RequiresRowPresenceCheck,
}

impl CoveringExistingRowMode {
    /// Return whether execution still owes an authoritative row-presence probe
    /// before it may emit covering output.
    #[must_use]
    pub(in crate::db) const fn requires_row_presence_check(self) -> bool {
        matches!(self, Self::RequiresRowPresenceCheck)
    }
}

///
/// CoveringReadExecutionPlan
///
/// Execution-grade planner-owned covering-read contract.
/// This promotes the older projection-only covering plan into a route payload
/// that also carries explicit existing-row semantics for execution/runtime.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CoveringReadExecutionPlan {
    pub(in crate::db) fields: Vec<CoveringReadField>,
    pub(in crate::db) prefix_len: usize,
    pub(in crate::db) order_contract: CoveringProjectionOrder,
    pub(in crate::db) existing_row_mode: CoveringExistingRowMode,
}

/// Return whether one plan's residual predicate stays compatible with the
/// strict covering-read and covering-existing-rows admission rules.
#[must_use]
pub(in crate::db) fn covering_strict_predicate_compatible(
    plan: &AccessPlannedQuery,
    predicate_index_capability: Option<IndexPredicateCapability>,
) -> bool {
    !plan.has_residual_filter_expr()
        && (!plan.has_residual_filter_predicate()
            || predicate_index_capability == Some(IndexPredicateCapability::FullyIndexable))
}

/// Return one stable explain reason code for the current scalar load
/// covering-read admission outcome.
#[must_use]
pub(in crate::db) fn covering_read_reason_code_for_load_plan(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
    covering_read_selected: bool,
) -> &'static str {
    if covering_read_selected {
        return "cover_read_route";
    }
    if plan.scalar_plan().order.is_some() {
        return "order_mat";
    }
    let index_shape_supported =
        plan.access.as_index_prefix_path().is_some() || plan.access.as_index_range_path().is_some();
    if !index_shape_supported {
        return "access_not_cov";
    }
    if (plan.has_residual_filter_expr() || plan.has_residual_filter_predicate())
        && !strict_predicate_compatible
    {
        return "pred_not_strict";
    }
    if plan.scalar_plan().distinct {
        return "distinct_mat";
    }

    "proj_not_cov"
}

/// Return whether one scalar aggregate terminal can remain index-only using
/// existing-row semantics under the current planner + predicate-compile
/// contracts.
#[must_use]
pub(in crate::db) fn index_covering_existing_rows_terminal_eligible(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> bool {
    if plan.scalar_plan().order.is_some() {
        return false;
    }

    let index_shape_supported =
        plan.access.as_index_prefix_path().is_some() || plan.access.as_index_range_path().is_some();
    if !index_shape_supported {
        return false;
    }
    if plan.scalar_plan().predicate.is_none() {
        return true;
    }

    strict_predicate_compatible
}

/// Derive one planner-owned scalar covering-read plan from generated field-table
/// authority plus the frozen projection contract on the plan.
#[must_use]
pub(in crate::db) fn covering_read_plan_from_fields(
    fields: &[FieldModel],
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
    strict_predicate_compatible: bool,
) -> Option<CoveringReadPlan> {
    covering_index_projection_plan_from_fields(
        fields,
        plan,
        primary_key_name,
        strict_predicate_compatible,
        CoveringProjectionFieldSourcePolicy::StrictCovering,
        false,
    )
}

/// Derive one planner-owned hybrid direct-field projection plan for SQL
/// projection consumers that can mix covering fields with sparse row-backed
/// fields over the same index-backed access path.
///
/// This helper stays intentionally narrower than the executor covering-read
/// fast path:
/// - direct-field projections only
/// - index-backed access only
/// - no grouped plans
/// - no residual predicate
/// - at least one row-backed projected field
/// - projected fields may still be primary-key, constant, or index-backed
///   alongside those row-backed fields
#[must_use]
pub(in crate::db) fn covering_hybrid_projection_plan_from_fields(
    fields: &[FieldModel],
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
) -> Option<CoveringReadPlan> {
    covering_index_projection_plan_from_fields(
        fields,
        plan,
        primary_key_name,
        false,
        CoveringProjectionFieldSourcePolicy::HybridRowFallback,
        true,
    )
}

/// Derive one execution-grade scalar covering-read plan from generated field-table
/// authority plus the planner-owned projection contract.
#[must_use]
pub(in crate::db) fn covering_read_execution_plan_from_fields(
    fields: &[FieldModel],
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
    strict_predicate_compatible: bool,
) -> Option<CoveringReadExecutionPlan> {
    // Phase 1: secondary covering routes now inherit planner-owned authority
    // directly. Once a secondary index-backed covering shape is admitted at
    // planning time, execution may trust that visible index path without a
    // separate executor-side authority resolver.
    if let Some(covering) =
        covering_read_plan_from_fields(fields, plan, primary_key_name, strict_predicate_compatible)
    {
        return Some(covering_read_execution_plan(
            covering,
            CoveringExistingRowMode::ProvenByPlanner,
        ));
    }

    // Phase 2: admit only authoritative primary-store traversal shapes as the
    // first planner-proven existing-row cohort. These keys come from the row
    // store itself, so they do not inherit secondary-index stale-entry risk.
    let (covering, existing_row_mode) =
        primary_store_covering_plan_from_fields(fields, plan, primary_key_name)?;

    Some(covering_read_execution_plan(covering, existing_row_mode))
}

/// Derive one covering projection context from one access shape + scalar order
/// contract and target field.
#[must_use]
pub(in crate::db) fn covering_index_projection_context<K>(
    access: &AccessPlan<K>,
    order: Option<&OrderSpec>,
    target_field: &str,
    primary_key_name: &'static str,
) -> Option<CoveringProjectionContext> {
    let metadata = covering_access_metadata(access)?;
    let order_terms = metadata.order_terms();
    let component_index = metadata
        .coverable_component_fields
        .iter()
        .position(|field| field.is_some_and(|field| field == target_field))?;
    let order_contract = covering_projection_order_contract(
        order,
        order_terms.as_slice(),
        metadata.prefix_len,
        primary_key_name,
        metadata.path_kind_is_range,
    )?;

    Some(CoveringProjectionContext {
        component_index,
        prefix_len: metadata.prefix_len,
        order_contract,
    })
}

/// Resolve one constant projection value when access shape binds the target
/// field through index-prefix equality components.
#[must_use]
pub(in crate::db) fn constant_covering_projection_value_from_access<K>(
    access: &AccessPlan<K>,
    target_field: &str,
) -> Option<Value> {
    let metadata = covering_access_metadata(access)?;

    constant_covering_projection_value_from_prefix(
        metadata.coverable_component_fields.as_slice(),
        metadata.prefix_values,
        target_field,
    )
}

/// Return whether adjacent dedupe is safe for one covering projection context.
///
/// Safety contract:
/// - output order remains index traversal order (no primary-key reorder),
/// - target field is the first unbound index component.
#[must_use]
pub(in crate::db) const fn covering_index_adjacent_distinct_eligible(
    context: CoveringProjectionContext,
) -> bool {
    matches!(
        context.order_contract,
        CoveringProjectionOrder::IndexOrder(_)
    ) && context.component_index == context.prefix_len
}

// Resolve one covering projection order contract from scalar ORDER BY shape.
fn covering_projection_order_contract(
    order: Option<&OrderSpec>,
    index_order_terms: &[&str],
    prefix_len: usize,
    primary_key_name: &'static str,
    path_kind_is_range: bool,
) -> Option<CoveringProjectionOrder> {
    let Some(order) = order else {
        return Some(CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc));
    };
    if let Some(direction) = order.primary_key_only_direction(primary_key_name) {
        let direction = match direction {
            OrderDirection::Asc => Direction::Asc,
            OrderDirection::Desc => Direction::Desc,
        };

        return Some(CoveringProjectionOrder::PrimaryKeyOrder(direction));
    }

    let order_contract = order.deterministic_secondary_order_contract(primary_key_name)?;
    let direction = match order_contract.direction() {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    };
    match order_contract.classify_index_match(index_order_terms, prefix_len) {
        DeterministicSecondaryIndexOrderMatch::Suffix => {
            Some(CoveringProjectionOrder::IndexOrder(direction))
        }
        DeterministicSecondaryIndexOrderMatch::Full if !path_kind_is_range => {
            Some(CoveringProjectionOrder::IndexOrder(direction))
        }
        DeterministicSecondaryIndexOrderMatch::Full
        | DeterministicSecondaryIndexOrderMatch::None => None,
    }
}

// Freeze one execution-grade covering-read plan from one planner-owned
// projection plan plus its row-presence contract.
fn covering_read_execution_plan(
    covering: CoveringReadPlan,
    existing_row_mode: CoveringExistingRowMode,
) -> CoveringReadExecutionPlan {
    CoveringReadExecutionPlan {
        fields: covering.fields,
        prefix_len: covering.prefix_len,
        order_contract: covering.order_contract,
        existing_row_mode,
    }
}

// Resolve one planner-owned covering projection plan plus row-presence mode
// for primary-key-only projection over primary-store access shapes.
//
// This helper now admits two explicit cohorts:
// - authoritative primary-store traversal (`FullScan` / `KeyRange`), which is
//   planner-proven because emitted keys come from the row store itself
// - exact primary-key lookup (`ByKey` / `ByKeys`), which still requires row
//   presence checks because the access payload names keys rather than proving
//   their existence
fn primary_store_covering_plan_from_fields(
    fields: &[FieldModel],
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
) -> Option<(CoveringReadPlan, CoveringExistingRowMode)> {
    // Phase 1: keep primary-store covering admission narrow and explicit.
    if plan.grouped_plan().is_some()
        || !plan.scalar_plan().mode.is_load()
        || plan.scalar_plan().distinct
        || plan.has_residual_filter_expr()
        || plan.has_residual_filter_predicate()
    {
        return None;
    }
    let access_facts = primary_store_covering_access_facts(&plan.access)?;

    // Phase 2: require a direct-field projection that can be satisfied by the
    // authoritative primary key alone under one PK-order contract.
    let order_contract = covering_projection_order_contract(
        plan.scalar_plan().order.as_ref(),
        &[],
        0,
        primary_key_name,
        false,
    )?;
    let source_context = CoveringProjectionSourceContext {
        coverable_component_fields: &[],
        prefix_values: &[],
        primary_key_name,
        source_policy: CoveringProjectionFieldSourcePolicy::StrictCovering,
    };
    let fields = covering_projection_fields_from_projection(
        fields,
        plan.frozen_projection_spec(),
        source_context,
    )?;
    if fields.is_empty() {
        return None;
    }
    if fields
        .iter()
        .any(|field| !matches!(field.source, CoveringReadFieldSource::PrimaryKey))
    {
        return None;
    }
    if !access_facts.supports_order_contract(order_contract) {
        return None;
    }

    Some((
        CoveringReadPlan {
            fields,
            prefix_len: 0,
            order_contract,
        },
        access_facts.existing_row_mode,
    ))
}

// Resolve one constant projection value from index-prefix component bindings.
fn constant_covering_projection_value_from_prefix(
    coverable_component_fields: &[Option<&'static str>],
    prefix_values: &[Value],
    target_field: &str,
) -> Option<Value> {
    coverable_component_fields
        .iter()
        .zip(prefix_values.iter())
        .find_map(|(field, value)| {
            field
                .is_some_and(|field| field == target_field)
                .then(|| value.clone())
        })
}

///
/// CoveringAccessMetadata
///
/// Shared planner covering-access metadata for index-backed access shapes.
/// This keeps prefix/range covering bookkeeping under one authority instead of
/// re-deriving the same index-field and prefix metadata in each helper.
///

struct CoveringAccessMetadata<'a> {
    order_terms: Vec<String>,
    coverable_component_fields: Vec<Option<&'static str>>,
    prefix_values: &'a [Value],
    prefix_len: usize,
    path_kind_is_range: bool,
}

// Project one immutable covering-access metadata bundle from one access shape.
fn covering_access_metadata<K>(access: &AccessPlan<K>) -> Option<CoveringAccessMetadata<'_>> {
    if let Some((index, values)) = access.as_index_prefix_path() {
        return Some(CoveringAccessMetadata {
            order_terms: index_order_terms(index),
            coverable_component_fields: coverable_component_fields_for_index(index),
            prefix_values: values,
            prefix_len: values.len(),
            path_kind_is_range: false,
        });
    }
    if let Some(spec) = access.as_index_range_path() {
        return Some(CoveringAccessMetadata {
            order_terms: index_order_terms(spec.index()),
            coverable_component_fields: coverable_component_fields_for_index(spec.index()),
            prefix_values: spec.prefix_values(),
            prefix_len: spec.prefix_values().len(),
            path_kind_is_range: true,
        });
    }

    None
}

impl CoveringAccessMetadata<'_> {
    // Borrow the canonical order terms as string slices so planner-owned order
    // matching uses expression-aware key-item text instead of raw field names.
    fn order_terms(&self) -> Vec<&str> {
        self.order_terms.iter().map(String::as_str).collect()
    }
}

// Freeze the shared index-backed covering plan contract once so the pure and
// hybrid covering planners do not each restate the same access/order setup.
fn prepare_covering_index_projection_plan<'a>(
    plan: &'a AccessPlannedQuery,
    primary_key_name: &'static str,
    residual_filter_predicate_supported: bool,
) -> Option<(CoveringAccessMetadata<'a>, CoveringProjectionOrder)> {
    if plan.grouped_plan().is_some() || !plan.scalar_plan().mode.is_load() {
        return None;
    }
    if plan.has_residual_filter_expr() {
        return None;
    }
    if plan.has_residual_filter_predicate() && !residual_filter_predicate_supported {
        return None;
    }

    let metadata = covering_access_metadata(&plan.access)?;
    let order_terms = metadata.order_terms();
    let order_contract = covering_projection_order_contract(
        plan.scalar_plan().order.as_ref(),
        order_terms.as_slice(),
        metadata.prefix_len,
        primary_key_name,
        metadata.path_kind_is_range,
    )?;

    Some((metadata, order_contract))
}

// Derive one index-backed covering plan from the shared access/order contract
// plus one field-source resolver for the requested covering surface.
fn covering_index_projection_plan_from_fields(
    fields: &[FieldModel],
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
    residual_filter_predicate_supported: bool,
    source_policy: CoveringProjectionFieldSourcePolicy,
    require_row_field: bool,
) -> Option<CoveringReadPlan> {
    // Phase 1: reject unsupported plan shapes and freeze the shared
    // index-backed covering contract once for the whole projection.
    let (metadata, order_contract) = prepare_covering_index_projection_plan(
        plan,
        primary_key_name,
        residual_filter_predicate_supported,
    )?;

    // Phase 2: derive the requested covering field surface in canonical
    // projection order and keep hybrid admission fail-closed on at least one
    // explicit row-backed field when requested.
    let source_context = CoveringProjectionSourceContext {
        coverable_component_fields: metadata.coverable_component_fields.as_slice(),
        prefix_values: metadata.prefix_values,
        primary_key_name,
        source_policy,
    };
    let fields = covering_projection_fields_from_projection(
        fields,
        plan.frozen_projection_spec(),
        source_context,
    )?;
    if fields.is_empty() {
        return None;
    }
    if require_row_field
        && !fields
            .iter()
            .any(|field| matches!(field.source, CoveringReadFieldSource::RowField))
    {
        return None;
    }

    Some(CoveringReadPlan {
        fields,
        prefix_len: metadata.prefix_len,
        order_contract,
    })
}

///
/// PrimaryStoreCoveringAccessFacts
///
/// Shared planner-owned primary-store covering access facts.
/// This keeps row-presence classification and PK-order preservation policy on
/// one path-owned authority instead of re-reading the same primary-store
/// cohorts through separate helpers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PrimaryStoreCoveringAccessFacts {
    existing_row_mode: CoveringExistingRowMode,
    descending_pk_order_supported: bool,
}

impl PrimaryStoreCoveringAccessFacts {
    // Return whether this primary-store covering cohort can preserve the
    // requested PK-only output order contract.
    const fn supports_order_contract(self, order_contract: CoveringProjectionOrder) -> bool {
        match order_contract {
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc) => true,
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc) => {
                self.descending_pk_order_supported
            }
            CoveringProjectionOrder::IndexOrder(_) => false,
        }
    }
}

// Classify one shared primary-store covering access fact bundle from the
// current access shape.
fn primary_store_covering_access_facts<K>(
    access: &AccessPlan<K>,
) -> Option<PrimaryStoreCoveringAccessFacts> {
    let path = access.as_path()?;

    // Authoritative scans already preserve planner-owned PK order through
    // route direction and runtime reorder behavior, so they stay fully proven.
    if path.is_primary_store_authoritative_scan() {
        return Some(PrimaryStoreCoveringAccessFacts {
            existing_row_mode: CoveringExistingRowMode::ProvenByPlanner,
            descending_pk_order_supported: true,
        });
    }

    if !path.is_primary_key_lookup() {
        return None;
    }

    // Exact key lookup names one concrete row candidate, so descending PK
    // order is singleton-safe even though execution still owes a row check.
    if path.is_by_key() {
        return Some(PrimaryStoreCoveringAccessFacts {
            existing_row_mode: CoveringExistingRowMode::RequiresRowPresenceCheck,
            descending_pk_order_supported: true,
        });
    }

    // Multi-key lookup still resolves keys in canonical ascending PK order, so
    // phase 1 keeps descending PK output fail-closed here.
    Some(PrimaryStoreCoveringAccessFacts {
        existing_row_mode: CoveringExistingRowMode::RequiresRowPresenceCheck,
        descending_pk_order_supported: false,
    })
}

///
/// CoveringProjectionFieldSourcePolicy
///
/// Shared planner-owned covering field-source policy.
/// This keeps the only real divergence between pure covering and hybrid
/// covering explicit at the source-classification seam.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoveringProjectionFieldSourcePolicy {
    StrictCovering,
    HybridRowFallback,
}

///
/// CoveringProjectionSourceContext
///
/// Shared planner-owned covering source context for one projection walk.
/// This keeps component-field ownership, prefix-bound constants, and the
/// current source policy together so per-field classification does not need to
/// re-enter the broader access-shape helper stack.
///

#[derive(Clone, Copy)]
struct CoveringProjectionSourceContext<'a> {
    coverable_component_fields: &'a [Option<&'static str>],
    prefix_values: &'a [Value],
    primary_key_name: &'static str,
    source_policy: CoveringProjectionFieldSourcePolicy,
}

// Assemble one projected covering field list while leaving source selection on
// one explicit policy surface so pure and hybrid covering plans share the same
// projection-walk and field-slot resolution contract.
fn covering_projection_fields_from_projection(
    fields: &[FieldModel],
    projection: &ProjectionSpec,
    source_context: CoveringProjectionSourceContext<'_>,
) -> Option<Vec<CoveringReadField>> {
    let mut projection_fields = Vec::with_capacity(projection.len());

    for projection_field in projection.fields() {
        let field_name = projection_field.direct_field_name()?;
        let field_slot = resolve_covering_field_slot(fields, field_name)?;
        let source = covering_projection_field_source(field_name, source_context)?;
        projection_fields.push(CoveringReadField { field_slot, source });
    }

    Some(projection_fields)
}

// Resolve one covering field against generated field-table authority without
// reopening the wider semantic entity model.
fn resolve_covering_field_slot(fields: &[FieldModel], field_name: &str) -> Option<FieldSlot> {
    let (index, field) = fields
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == field_name)?;

    Some(FieldSlot {
        index,
        field: field.name().to_string(),
        kind: Some(field.kind()),
    })
}

// Resolve one projected covering field source under the requested planner
// policy. Pure covering stays fail-closed on uncovered fields, while hybrid
// covering explicitly falls back to sparse row reads.
fn covering_projection_field_source(
    field_name: &str,
    source_context: CoveringProjectionSourceContext<'_>,
) -> Option<CoveringReadFieldSource> {
    if field_name == source_context.primary_key_name {
        return Some(CoveringReadFieldSource::PrimaryKey);
    }
    if let Some(value) = constant_covering_projection_value_from_prefix(
        source_context.coverable_component_fields,
        source_context.prefix_values,
        field_name,
    ) {
        return Some(CoveringReadFieldSource::Constant(value));
    }

    source_context
        .coverable_component_fields
        .iter()
        .position(|field| field.is_some_and(|field| field == field_name))
        .map(|component_index| CoveringReadFieldSource::IndexComponent { component_index })
        .or_else(|| {
            matches!(
                source_context.source_policy,
                CoveringProjectionFieldSourcePolicy::HybridRowFallback
            )
            .then_some(CoveringReadFieldSource::RowField)
        })
}

// Project one component-field layout that preserves only directly recoverable
// raw entity fields. Expression key items intentionally map to `None` here so
// covering reads do not claim the original field can be reconstructed from the
// derived component bytes.
fn coverable_component_fields_for_index(index: &IndexModel) -> Vec<Option<&'static str>> {
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => fields.iter().copied().map(Some).collect(),
        IndexKeyItemsRef::Items(items) => items
            .iter()
            .map(|item| match item {
                IndexKeyItem::Field(field) => Some(*field),
                IndexKeyItem::Expression(_) => None,
            })
            .collect(),
    }
}
