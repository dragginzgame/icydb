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
    query::plan::{
        AccessPlannedQuery, FieldSlot, OrderDirection, OrderSpec,
        expr::{ProjectionSpec, projection_field_direct_field_name},
    },
};
use crate::{model::entity::EntityModel, value::Value};

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
/// Phase 1 stays intentionally narrow:
/// index components, primary-key output, and prefix-bound constants only.
///

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum CoveringReadFieldSource {
    IndexComponent { component_index: usize },
    PrimaryKey,
    Constant(Value),
}

///
/// CoveringReadField
///
/// One planner-owned scalar covering-read output field.
/// Output order stays canonical projection order while `source` records how
/// runtime can satisfy the value without row-materialized reads.
///

#[cfg_attr(not(test), allow(dead_code))]
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

#[cfg_attr(not(test), allow(dead_code))]
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

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CoveringExistingRowMode {
    ProvenByPlanner,
    RequiresRowPresenceCheck,
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

/// Derive one planner-owned scalar covering-read plan for direct field
/// projections over index-backed scalar load shapes.
#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) fn covering_read_plan(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
    strict_predicate_compatible: bool,
) -> Option<CoveringReadPlan> {
    // Phase 1: reject shapes that are intentionally outside the first covering
    // read route contract before touching projection details.
    if plan.grouped_plan().is_some() || !plan.scalar_plan().mode.is_load() {
        return None;
    }
    if plan.has_residual_predicate() && !strict_predicate_compatible {
        return None;
    }

    // Phase 2: project one immutable covering-access contract shared by every
    // field in the scalar covering-read output.
    let metadata = covering_access_metadata(&plan.access)?;
    let order_contract = covering_projection_order_contract(
        plan.scalar_plan().order.as_ref(),
        metadata.index_fields,
        metadata.prefix_len,
        primary_key_name,
        metadata.path_kind_is_range,
    )?;

    // Phase 3: derive one source contract per output field in canonical
    // projection order. Any unsupported field shape falls back to the current
    // row-materialized path.
    let projection = plan.projection_spec(model);
    let fields = covering_read_fields_from_projection(
        model,
        &projection,
        metadata.index_fields,
        primary_key_name,
        &plan.access,
    )?;
    if fields.is_empty() {
        return None;
    }

    Some(CoveringReadPlan {
        fields,
        prefix_len: metadata.prefix_len,
        order_contract,
    })
}

/// Derive one execution-grade scalar covering-read plan from the existing
/// planner-owned projection contract plus explicit row-presence semantics.
#[must_use]
pub(in crate::db) fn covering_read_execution_plan(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
    strict_predicate_compatible: bool,
) -> Option<CoveringReadExecutionPlan> {
    // Phase 1: keep current secondary/index-backed shapes explicit but
    // conservative until `0.69` ships a truly row-free runtime for them.
    if let Some(covering) =
        covering_read_plan(model, plan, primary_key_name, strict_predicate_compatible)
    {
        return Some(CoveringReadExecutionPlan {
            fields: covering.fields,
            prefix_len: covering.prefix_len,
            order_contract: covering.order_contract,
            existing_row_mode: CoveringExistingRowMode::RequiresRowPresenceCheck,
        });
    }

    // Phase 2: admit only authoritative primary-store traversal shapes as the
    // first planner-proven existing-row cohort. These keys come from the row
    // store itself, so they do not inherit secondary-index stale-entry risk.
    primary_store_covering_execution_plan(model, plan, primary_key_name)
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
    let component_index = metadata
        .index_fields
        .iter()
        .position(|field| *field == target_field)?;
    let order_contract = covering_projection_order_contract(
        order,
        metadata.index_fields,
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
        metadata.index_fields,
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
    index_fields: &[&'static str],
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

    let direction = match order.deterministic_secondary_order_direction(primary_key_name)? {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    };
    if order.matches_index_suffix_plus_primary_key(index_fields, prefix_len, primary_key_name) {
        return Some(CoveringProjectionOrder::IndexOrder(direction));
    }

    if path_kind_is_range {
        return None;
    }

    order
        .matches_index_full_plus_primary_key(index_fields, primary_key_name)
        .then_some(CoveringProjectionOrder::IndexOrder(direction))
}

// Resolve one planner-owned covering execution plan for primary-key-only
// projection over primary-store access shapes.
//
// This helper now admits two explicit cohorts:
// - authoritative primary-store traversal (`FullScan` / `KeyRange`), which is
//   planner-proven because emitted keys come from the row store itself
// - exact primary-key lookup (`ByKey` / `ByKeys`), which still requires row
//   presence checks because the access payload names keys rather than proving
//   their existence
fn primary_store_covering_execution_plan(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    primary_key_name: &'static str,
) -> Option<CoveringReadExecutionPlan> {
    // Phase 1: keep primary-store covering admission narrow and explicit.
    if plan.grouped_plan().is_some()
        || !plan.scalar_plan().mode.is_load()
        || plan.scalar_plan().distinct
        || plan.has_residual_predicate()
    {
        return None;
    }
    let existing_row_mode = primary_store_covering_existing_row_mode(&plan.access)?;

    // Phase 2: require a direct-field projection that can be satisfied by the
    // authoritative primary key alone under one PK-order contract.
    let order_contract = covering_projection_order_contract(
        plan.scalar_plan().order.as_ref(),
        &[],
        0,
        primary_key_name,
        false,
    )?;
    let fields = covering_read_fields_from_projection(
        model,
        &plan.projection_spec(model),
        &[],
        primary_key_name,
        &plan.access,
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
    if !primary_store_covering_order_supported(&plan.access, order_contract) {
        return None;
    }

    Some(CoveringReadExecutionPlan {
        fields,
        prefix_len: 0,
        order_contract,
        existing_row_mode,
    })
}

// Resolve one constant projection value from index-prefix component bindings.
fn constant_covering_projection_value_from_prefix(
    index_fields: &[&'static str],
    prefix_values: &[Value],
    target_field: &str,
) -> Option<Value> {
    index_fields
        .iter()
        .zip(prefix_values.iter())
        .find_map(|(field, value)| (*field == target_field).then(|| value.clone()))
}

///
/// CoveringAccessMetadata
///
/// Shared planner covering-access metadata for index-backed access shapes.
/// This keeps prefix/range covering bookkeeping under one authority instead of
/// re-deriving the same index-field and prefix metadata in each helper.
///

struct CoveringAccessMetadata<'a> {
    index_fields: &'a [&'static str],
    prefix_values: &'a [Value],
    prefix_len: usize,
    path_kind_is_range: bool,
}

// Project one immutable covering-access metadata bundle from one access shape.
fn covering_access_metadata<K>(access: &AccessPlan<K>) -> Option<CoveringAccessMetadata<'_>> {
    if let Some((index, values)) = access.as_index_prefix_path() {
        return Some(CoveringAccessMetadata {
            index_fields: index.fields(),
            prefix_values: values,
            prefix_len: values.len(),
            path_kind_is_range: false,
        });
    }
    if let Some((index, prefix_values, _, _)) = access.as_index_range_path() {
        return Some(CoveringAccessMetadata {
            index_fields: index.fields(),
            prefix_values,
            prefix_len: prefix_values.len(),
            path_kind_is_range: true,
        });
    }

    None
}

// Classify the planner-owned existing-row mode for one primary-store covering
// access shape.
fn primary_store_covering_existing_row_mode<K>(
    access: &AccessPlan<K>,
) -> Option<CoveringExistingRowMode> {
    let path = access.as_path()?;
    if path.is_primary_store_authoritative_scan() {
        return Some(CoveringExistingRowMode::ProvenByPlanner);
    }

    path.is_primary_key_lookup()
        .then_some(CoveringExistingRowMode::RequiresRowPresenceCheck)
}

// Return whether the current runtime can preserve one primary-key-only output
// order contract for this primary-store covering access shape.
fn primary_store_covering_order_supported<K>(
    access: &AccessPlan<K>,
    order_contract: CoveringProjectionOrder,
) -> bool {
    let Some(path) = access.as_path() else {
        return false;
    };

    // Authoritative scans already preserve the planner-owned PK order
    // contract through their route direction + runtime reorder behavior.
    if path.is_primary_store_authoritative_scan() {
        return true;
    }

    // Exact key lookups are singleton-safe regardless of requested PK
    // direction because there can be at most one emitted row.
    if path.is_by_key() {
        return matches!(order_contract, CoveringProjectionOrder::PrimaryKeyOrder(_));
    }

    // Multi-key lookup currently resolves keys in canonical ascending PK
    // order, so phase 1 stays fail-closed on descending PK order here.
    path.as_by_keys().is_some_and(|_| {
        matches!(
            order_contract,
            CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc)
        )
    })
}

// Derive one canonical covering-read field list from one direct-field
// projection under one immutable covering access shape.
#[cfg_attr(not(test), allow(dead_code))]
fn covering_read_fields_from_projection(
    model: &EntityModel,
    projection: &ProjectionSpec,
    index_fields: &[&'static str],
    primary_key_name: &'static str,
    access: &AccessPlan<Value>,
) -> Option<Vec<CoveringReadField>> {
    let mut fields = Vec::with_capacity(projection.len());

    for projection_field in projection.fields() {
        let field_name = projection_field_direct_field_name(projection_field)?;
        let field_slot = FieldSlot::resolve(model, field_name)?;
        let source =
            covering_read_field_source(field_name, index_fields, primary_key_name, access)?;

        fields.push(CoveringReadField { field_slot, source });
    }

    Some(fields)
}

// Resolve one covering-read field source for one direct projected field.
#[cfg_attr(not(test), allow(dead_code))]
fn covering_read_field_source(
    field_name: &str,
    index_fields: &[&'static str],
    primary_key_name: &'static str,
    access: &AccessPlan<Value>,
) -> Option<CoveringReadFieldSource> {
    if field_name == primary_key_name {
        return Some(CoveringReadFieldSource::PrimaryKey);
    }
    if let Some(value) = constant_covering_projection_value_from_access(access, field_name) {
        return Some(CoveringReadFieldSource::Constant(value));
    }

    index_fields
        .iter()
        .position(|field| *field == field_name)
        .map(|component_index| CoveringReadFieldSource::IndexComponent { component_index })
}
