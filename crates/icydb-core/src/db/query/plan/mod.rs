//! Query plan contracts, planning, and validation wiring.

mod access_projection;
pub(crate) mod planner;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

use crate::{
    db::{
        access::{
            AccessPlan, PushdownApplicability, SecondaryOrderPushdownEligibility,
            assess_secondary_order_pushdown_from_parts,
            assess_secondary_order_pushdown_if_applicable_validated_from_parts,
        },
        contracts::ReadConsistency,
        direction::Direction,
        query::predicate::Predicate,
    },
    model::entity::EntityModel,
};
use std::ops::{Deref, DerefMut};

pub(in crate::db) use crate::db::query::fingerprint::canonical;
pub(crate) use access_projection::{
    AccessPlanProjection, project_access_plan, project_explain_access_path,
};

pub(crate) use validate::OrderPlanError;
///
/// Re-Exports
///
pub use validate::PlanError;

///
/// QueryMode
///
/// Discriminates load vs delete intent at planning time.
/// Encodes mode-specific fields so invalid states are unrepresentable.
/// Mode checks are explicit and stable at execution time.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryMode {
    Load(LoadSpec),
    Delete(DeleteSpec),
}

impl QueryMode {
    /// True if this mode represents a load intent.
    #[must_use]
    pub const fn is_load(&self) -> bool {
        match self {
            Self::Load(_) => true,
            Self::Delete(_) => false,
        }
    }

    /// True if this mode represents a delete intent.
    #[must_use]
    pub const fn is_delete(&self) -> bool {
        match self {
            Self::Delete(_) => true,
            Self::Load(_) => false,
        }
    }
}

///
/// LoadSpec
///
/// Mode-specific fields for load intents.
/// Encodes pagination without leaking into delete intents.
///
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoadSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}

impl LoadSpec {
    /// Create an empty load spec.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            limit: None,
            offset: 0,
        }
    }
}

///
/// DeleteSpec
///
/// Mode-specific fields for delete intents.
/// Encodes delete limits without leaking into load intents.
///
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeleteSpec {
    pub limit: Option<u32>,
}

impl DeleteSpec {
    /// Create an empty delete spec.
    #[must_use]
    pub const fn new() -> Self {
        Self { limit: None }
    }
}

///
/// OrderDirection
/// Executor-facing ordering direction (applied after filtering).
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

///
/// OrderSpec
/// Executor-facing ordering specification.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderSpec {
    pub(crate) fields: Vec<(String, OrderDirection)>,
}

///
/// DeleteLimitSpec
/// Executor-facing delete bound with no offsets.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteLimitSpec {
    pub max_rows: u32,
}

///
/// PageSpec
/// Executor-facing pagination specification.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PageSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}

///
/// LogicalPlan
///
/// Pure logical query intent produced by the planner.
///
/// A `LogicalPlan` represents the access-independent query semantics:
/// predicate/filter, ordering, distinct behavior, pagination/delete windows,
/// and read-consistency mode.
///
/// Design notes:
/// - Predicates are applied *after* data access
/// - Ordering is applied after filtering
/// - Pagination is applied after ordering (load only)
/// - Delete limits are applied after ordering (delete only)
/// - Missing-row policy is explicit and must not depend on access strategy
///
/// This struct is the logical compiler stage output and intentionally excludes
/// access-path details.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalPlan {
    /// Load vs delete intent.
    pub(crate) mode: QueryMode,

    /// Optional residual predicate applied after access.
    pub(crate) predicate: Option<Predicate>,

    /// Optional ordering specification.
    pub(crate) order: Option<OrderSpec>,

    /// Optional distinct semantics over ordered rows.
    pub(crate) distinct: bool,

    /// Optional delete bound (delete intents only).
    pub(crate) delete_limit: Option<DeleteLimitSpec>,

    /// Optional pagination specification.
    pub(crate) page: Option<PageSpec>,

    /// Missing-row policy for execution.
    pub(crate) consistency: ReadConsistency,
}

///
/// AccessPlannedQuery
///
/// Access-planned query produced after access-path selection.
/// Binds one pure `LogicalPlan` to one chosen `AccessPlan`.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccessPlannedQuery<K> {
    pub(crate) logical: LogicalPlan,
    pub(crate) access: AccessPlan<K>,
}

impl<K> AccessPlannedQuery<K> {
    /// Construct an access-planned query from logical + access stages.
    #[must_use]
    pub(crate) const fn from_parts(logical: LogicalPlan, access: AccessPlan<K>) -> Self {
        Self { logical, access }
    }

    /// Decompose into logical + access stages.
    #[must_use]
    pub(crate) fn into_parts(self) -> (LogicalPlan, AccessPlan<K>) {
        (self.logical, self.access)
    }

    /// Construct a minimal access-planned query with only an access path.
    ///
    /// Predicates, ordering, and pagination may be attached later.
    #[cfg(test)]
    pub(crate) fn new(
        access: crate::db::access::AccessPath<K>,
        consistency: ReadConsistency,
    ) -> Self {
        Self {
            logical: LogicalPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: None,
                distinct: false,
                delete_limit: None,
                page: None,
                consistency,
            },
            access: AccessPlan::path(access),
        }
    }
}

impl<K> Deref for AccessPlannedQuery<K> {
    type Target = LogicalPlan;

    fn deref(&self) -> &Self::Target {
        &self.logical
    }
}

impl<K> DerefMut for AccessPlannedQuery<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.logical
    }
}

fn direction_from_order(direction: OrderDirection) -> Direction {
    if direction == OrderDirection::Desc {
        Direction::Desc
    } else {
        Direction::Asc
    }
}

fn order_fields_as_direction_refs(
    order_fields: &[(String, OrderDirection)],
) -> Vec<(&str, Direction)> {
    order_fields
        .iter()
        .map(|(field, direction)| (field.as_str(), direction_from_order(*direction)))
        .collect()
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan.
pub(crate) fn assess_secondary_order_pushdown<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> SecondaryOrderPushdownEligibility {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_from_parts(model, order_fields.as_deref(), &plan.access)
}

#[cfg(test)]
/// Evaluate pushdown eligibility only when secondary-index ORDER BY is applicable.
pub(crate) fn assess_secondary_order_pushdown_if_applicable<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    crate::db::access::assess_secondary_order_pushdown_if_applicable_from_parts(
        model,
        order_fields.as_deref(),
        &plan.access,
    )
}

/// Evaluate pushdown applicability for plans that have already passed full
/// logical/executor validation.
pub(crate) fn assess_secondary_order_pushdown_if_applicable_validated<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_if_applicable_validated_from_parts(
        model,
        order_fields.as_deref(),
        &plan.access,
    )
}
