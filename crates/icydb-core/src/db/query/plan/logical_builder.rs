//! Module: query::plan::logical_builder
//! Responsibility: construct logical planning inputs and logical plan contracts from query intent.
//! Does not own: access-path planning heuristics or runtime executor routing.
//! Boundary: emits planner-domain logical plan structures prior to access planning.

use crate::{
    db::{
        predicate::{MissingRowPolicy, Predicate},
        query::plan::{
            DeleteLimitSpec, GroupHavingSpec, GroupPlan, GroupSpec, LogicalPlan, OrderDirection,
            OrderSpec, PageSpec, QueryMode, ScalarPlan,
        },
    },
    model::entity::EntityModel,
};

///
/// LogicalPlanningInputs
///
/// Logical-planning input contract projected from query intent.
/// Carries mode and shape declarations independent of access-path selection.
/// Logical planning consumes this contract together with normalized predicates.
///

#[derive(Debug)]
pub(in crate::db::query) struct LogicalPlanningInputs {
    mode: QueryMode,
    order: Option<OrderSpec>,
    distinct: bool,
    group: Option<GroupSpec>,
    having: Option<GroupHavingSpec>,
}

impl LogicalPlanningInputs {
    /// Build logical-planning inputs from intent-projected shape values.
    #[must_use]
    pub(in crate::db::query) const fn new(
        mode: QueryMode,
        order: Option<OrderSpec>,
        distinct: bool,
        group: Option<GroupSpec>,
        having: Option<GroupHavingSpec>,
    ) -> Self {
        Self {
            mode,
            order,
            distinct,
            group,
            having,
        }
    }
}

///
/// LogicalQuery
///
/// Plan-owned normalized logical query contract assembled from query intent.
/// This DTO captures logical query semantics before access-path selection is
/// coupled into one `AccessPlannedQuery`.
///

#[derive(Clone, Debug)]
pub(in crate::db::query) struct LogicalQuery {
    pub(in crate::db::query) mode: QueryMode,
    pub(in crate::db::query) normalized_predicate: Option<Predicate>,
    pub(in crate::db::query) order: Option<OrderSpec>,
    pub(in crate::db::query) distinct: bool,
    pub(in crate::db::query) group: Option<GroupSpec>,
    pub(in crate::db::query) having: Option<GroupHavingSpec>,
    pub(in crate::db::query) consistency: MissingRowPolicy,
}

/// Project one plan-owned `LogicalQuery` DTO from logical-planning inputs.
#[must_use]
pub(in crate::db::query) fn logical_query_from_logical_inputs(
    inputs: LogicalPlanningInputs,
    normalized_predicate: Option<Predicate>,
    consistency: MissingRowPolicy,
) -> LogicalQuery {
    let LogicalPlanningInputs {
        mode,
        order,
        distinct,
        group,
        having,
    } = inputs;

    LogicalQuery {
        mode,
        normalized_predicate,
        order,
        distinct,
        group,
        having,
        consistency,
    }
}

/// Build a logical plan from intent-owned scalar and grouped plan inputs.
#[must_use]
pub(in crate::db::query) fn build_logical_plan(
    model: &EntityModel,
    query: LogicalQuery,
) -> LogicalPlan {
    let LogicalQuery {
        mode,
        normalized_predicate,
        order,
        distinct,
        group,
        having,
        consistency,
    } = query;

    // Build scalar shape first so grouped/non-grouped plans share one scalar contract.
    let scalar = ScalarPlan {
        mode,
        predicate: normalized_predicate,
        order: canonicalize_order_spec(model, order),
        distinct,
        delete_limit: match mode {
            QueryMode::Delete(spec) => spec.limit.map(|max_rows| DeleteLimitSpec { max_rows }),
            QueryMode::Load(_) => None,
        },
        page: match mode {
            QueryMode::Load(spec) => {
                if spec.limit.is_some() || spec.offset > 0 {
                    Some(PageSpec {
                        limit: spec.limit,
                        offset: spec.offset,
                    })
                } else {
                    None
                }
            }
            QueryMode::Delete(_) => None,
        },
        consistency,
    };

    // Grouped shape wraps scalar shape; HAVING without GROUP BY is invalid and
    // should be rejected by intent validation before reaching this boundary.
    if let Some(group) = group {
        LogicalPlan::Grouped(GroupPlan {
            scalar,
            group,
            having,
        })
    } else {
        debug_assert!(
            having.is_none(),
            "HAVING clauses require grouped shape before logical plan assembly"
        );

        LogicalPlan::Scalar(scalar)
    }
}

/// Normalize one ORDER BY shape into the planner's canonical deterministic form.
///
/// This helper is shared across access planning and logical-plan assembly so
/// both boundaries agree on the exact `..., primary_key` ordering contract.
#[must_use]
pub(in crate::db::query) fn canonicalize_order_spec(
    model: &EntityModel,
    order: Option<OrderSpec>,
) -> Option<OrderSpec> {
    let mut order = order?;
    let pk = model.primary_key.name;

    let mut pk_direction = None;

    order.fields.retain(|(field, dir)| {
        if field == pk {
            pk_direction.get_or_insert(*dir);
            false
        } else {
            true
        }
    });

    order
        .fields
        .push((pk.to_string(), pk_direction.unwrap_or(OrderDirection::Asc)));

    Some(order)
}
