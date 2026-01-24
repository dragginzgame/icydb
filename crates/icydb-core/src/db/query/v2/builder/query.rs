use crate::{
    db::query::v2::{
        plan::{
            AccessPlan, LogicalPlan, OrderDirection, OrderSpec, PageSpec, plan_access,
            validate_plan,
        },
        predicate::{Predicate, SchemaInfo},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
};
use icydb_schema::node::{Entity, Schema};
use std::marker::PhantomData;

///
/// QueryBuilder
///
/// Typed, declarative query builder for v2 queries.
///
/// This builder:
/// - Collects predicate, ordering, and pagination into a `QuerySpec`
/// - Is purely declarative (no schema access, planning, or execution)
/// - Is parameterized by the entity type `E`
///
/// Important design notes:
/// - No validation occurs here beyond structural composition
/// - Field names are accepted as strings; validity is checked later
/// - Schema is consulted only during `QuerySpec::plan`
///
/// This separation allows query construction to remain lightweight,
/// testable, and independent of runtime context.
///

pub struct QueryBuilder<E: EntityKind> {
    predicate: Option<Predicate>,
    order: Option<OrderSpec>,
    page: Option<PageSpec>,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> Default for QueryBuilder<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: EntityKind> QueryBuilder<E> {
    /// Create a new empty query builder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            predicate: None,
            order: None,
            page: None,
            _marker: PhantomData,
        }
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.predicate = match self.predicate.take() {
            Some(existing) => Some(Predicate::And(vec![existing, predicate])),
            None => Some(predicate),
        };
        self
    }

    /// Explicit AND combinator for predicates.
    #[must_use]
    pub fn and(mut self, predicate: Predicate) -> Self {
        self.predicate = match self.predicate.take() {
            Some(existing) => Some(Predicate::And(vec![existing, predicate])),
            None => Some(predicate),
        };
        self
    }

    /// Explicit OR combinator for predicates.
    #[must_use]
    pub fn or(mut self, predicate: Predicate) -> Self {
        self.predicate = match self.predicate.take() {
            Some(existing) => Some(Predicate::Or(vec![existing, predicate])),
            None => Some(predicate),
        };
        self
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: &'static str) -> Self {
        self.order = Some(push_order(self.order, field, OrderDirection::Asc));
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: &'static str) -> Self {
        self.order = Some(push_order(self.order, field, OrderDirection::Desc));
        self
    }

    /// Set or replace the result limit.
    #[must_use]
    pub const fn limit(mut self, n: u32) -> Self {
        self.page = Some(match self.page {
            Some(mut page) => {
                page.limit = Some(n);
                page
            }
            None => PageSpec {
                limit: Some(n),
                offset: 0,
            },
        });
        self
    }

    /// Set or replace the result offset.
    #[must_use]
    pub const fn offset(mut self, n: u32) -> Self {
        self.page = Some(match self.page {
            Some(mut page) => {
                page.offset = n;
                page
            }
            None => PageSpec {
                limit: None,
                offset: n,
            },
        });
        self
    }

    /// Finalize the builder into an immutable `QuerySpec`.
    #[must_use]
    pub fn build(self) -> QuerySpec {
        QuerySpec {
            predicate: self.predicate,
            order: self.order,
            page: self.page,
        }
    }
}

///
/// QuerySpec
///
/// Immutable specification produced by `QueryBuilder`.
///
/// `QuerySpec` represents a fully constructed query intent, but
/// not yet a concrete execution plan. It is the handoff point
/// between the builder layer and the planner/executor pipeline.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySpec {
    pub predicate: Option<Predicate>,
    pub order: Option<OrderSpec>,
    pub page: Option<PageSpec>,
}

impl QuerySpec {
    /// Convert this query specification into a validated `LogicalPlan`.
    ///
    /// This is the boundary where:
    /// - Schema is consulted (via macro-generated metadata)
    /// - Predicate validation and coercion checks occur
    /// - Index eligibility is determined by the planner
    ///
    /// Composite access plans (union/intersection) are explicitly rejected
    /// here, as executors currently require a single concrete access path.
    pub fn plan<E: EntityKind>(&self, schema: &Schema) -> Result<LogicalPlan, InternalError> {
        let entity = schema.cast_node::<Entity>(E::PATH).map_err(|err| {
            InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Query,
                format!("schema unavailable: {err}"),
            )
        })?;
        let schema_info = SchemaInfo::from_entity_schema(entity, schema);

        let access_plan = plan_access::<E>(self.predicate.as_ref()).map_err(|err| {
            InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
        })?;
        access_plan.validate_invariants::<E>(&schema_info, self.predicate.as_ref());

        let access = match access_plan {
            AccessPlan::Path(path) => path,
            AccessPlan::Union(_) | AccessPlan::Intersection(_) => {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Query,
                    "planner produced composite access plan; executor requires a single access path"
                        .to_string(),
                ));
            }
        };

        let plan = LogicalPlan {
            access,
            predicate: self.predicate.clone(),
            order: self.order.clone(),
            page: self.page.clone(),
        };
        validate_plan::<E>(&plan).map_err(|err| {
            InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
        })?;

        Ok(plan)
    }
}

/// Helper to append an ordering field while preserving existing order spec.
fn push_order(
    order: Option<OrderSpec>,
    field: &'static str,
    direction: OrderDirection,
) -> OrderSpec {
    match order {
        Some(mut spec) => {
            spec.fields.push((field.to_string(), direction));
            spec
        }
        None => OrderSpec {
            fields: vec![(field.to_string(), direction)],
        },
    }
}
