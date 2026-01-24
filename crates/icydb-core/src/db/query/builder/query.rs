use crate::{
    db::query::{
        plan::{
            AccessPlan, ExplainPlan, LogicalPlan, OrderDirection, OrderSpec, PageSpec, PlanError,
            PlanFingerprint, cache, planner::plan_access, validate::validate_plan_with_schema_info,
            validate_plan_invariants,
        },
        predicate::{Predicate, SchemaInfo, ValidateError, normalize, validate_model},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
};
use icydb_schema::node::Schema;
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error as ThisError;

///
/// QueryBuilder
///
/// Typed, declarative query builder for queries.
///
/// This builder:
/// - Collects predicate, ordering, and pagination into a `QuerySpec`
/// - Is purely declarative (no schema access, planning, or execution)
/// - Is parameterized by the entity type `E`
///
/// Important design notes:
/// - No validation occurs here beyond structural composition
/// - Field names are accepted as strings; validity is checked later
/// - Runtime schema (EntityModel) is consulted only during `QuerySpec::plan`
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

///
/// QueryExplain
///
/// Read-only explanation and fingerprint for a query.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryExplain {
    pub explain: ExplainPlan,
    pub fingerprint: PlanFingerprint,
}

///
/// QueryError
///

#[derive(Debug, ThisError)]
pub enum QueryError {
    #[error("{0}")]
    Validate(#[from] ValidateError),
    #[error("{0}")]
    Plan(#[from] PlanError),
    #[error("planner produced composite access plan; executor requires a single access path")]
    CompositeAccessPlan,
    #[error("{0}")]
    Execute(#[from] InternalError),
}

impl QuerySpec {
    /// Convert this query specification into a validated `LogicalPlan`.
    ///
    /// This is the boundary where:
    /// - EntityModel metadata is consulted (macro-generated runtime schema)
    /// - Predicate validation and coercion checks occur
    /// - Index eligibility is determined by the planner
    ///
    /// Composite access plans (union/intersection) are explicitly rejected
    /// here, as executors currently require a single concrete access path.
    ///
    /// The `schema` parameter is retained for legacy callers and is not consulted.
    pub fn plan<E: EntityKind>(&self, _schema: &Schema) -> Result<LogicalPlan, InternalError> {
        self.build_plan::<E>().map_err(|err| match err {
            QueryError::Validate(err) => {
                InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
            }
            QueryError::Plan(err) => {
                InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
            }
            QueryError::CompositeAccessPlan => InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Query,
                "planner produced composite access plan; executor requires a single access path"
                    .to_string(),
            ),
            QueryError::Execute(err) => err,
        })
    }

    /// Explain this query without executing it.
    ///
    /// The `schema` parameter is retained for legacy callers and is not consulted.
    pub fn explain<E: EntityKind>(&self, _schema: &Schema) -> Result<QueryExplain, QueryError> {
        let plan = self.build_plan::<E>()?;
        Ok(QueryExplain {
            explain: plan.explain(),
            fingerprint: plan.fingerprint(),
        })
    }

    pub(crate) fn build_plan<E: EntityKind>(&self) -> Result<LogicalPlan, QueryError> {
        // `schema` is retained for legacy callers; planning uses `EntityModel`.
        let model = E::MODEL;
        if let Some(predicate) = &self.predicate {
            validate_model(model, predicate)?;
        }

        let schema_info = SchemaInfo::from_entity_model(model)?;

        let normalized_predicate = self.predicate.as_ref().map(normalize);
        let access_plan = plan_access::<E>(&schema_info, normalized_predicate.as_ref())?;
        validate_plan_invariants::<E>(&access_plan, &schema_info, normalized_predicate.as_ref());

        let access = match access_plan {
            AccessPlan::Path(path) => path,
            AccessPlan::Union(_) | AccessPlan::Intersection(_) => {
                return Err(QueryError::CompositeAccessPlan);
            }
        };

        let plan = LogicalPlan {
            access,
            predicate: normalized_predicate,
            order: self.order.clone(),
            page: self.page.clone(),
        };
        validate_plan_with_schema_info(&schema_info, model, &plan)?;

        let fingerprint = plan.fingerprint();
        if let Some(cached) = cache::get(&fingerprint) {
            cache::record_hit();
            return Ok((*cached).clone());
        }
        cache::record_miss();
        cache::insert(fingerprint, Arc::new(plan.clone()));
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
