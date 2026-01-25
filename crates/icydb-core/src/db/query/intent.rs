use crate::{
    db::query::{
        ReadConsistency,
        plan::{
            ExecutablePlan, ExplainPlan, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
            PlanError, ProjectionSpec, cache, planner::plan_access, validate::validate_access_plan,
            validate::validate_order,
        },
        predicate::{Predicate, SchemaInfo, ValidateError, normalize},
    },
    error::InternalError,
    traits::EntityKind,
};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error as ThisError;

///
/// Query
///
/// Typed, declarative query intent for a specific entity type.
///
/// This intent is:
/// - schema-agnostic at construction
/// - normalized and validated only during planning
/// - free of access-path decisions
///
#[derive(Debug)]
pub struct Query<E: EntityKind> {
    predicate: Option<Predicate>,
    order: Option<OrderSpec>,
    page: Option<PageSpec>,
    projection: ProjectionSpec,
    consistency: ReadConsistency,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> Query<E> {
    /// Create a new intent with an explicit missing-row policy.
    #[must_use]
    pub const fn new(consistency: ReadConsistency) -> Self {
        Self {
            predicate: None,
            order: None,
            page: None,
            projection: ProjectionSpec::All,
            consistency,
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

    /// Replace the current pagination settings.
    #[must_use]
    pub const fn page(mut self, page: PageSpec) -> Self {
        self.page = Some(page);
        self
    }

    /// Explain this intent without executing it.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        let plan = self.build_plan::<E>()?;
        Ok(plan.explain())
    }

    /// Plan this intent into an executor-ready plan.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        let plan = self.build_plan::<E>()?;
        Ok(ExecutablePlan::new(plan))
    }

    fn build_plan<T: EntityKind>(&self) -> Result<LogicalPlan, QueryError> {
        let model = T::MODEL;
        let schema_info = SchemaInfo::from_entity_model(model)?;

        if let Some(order) = &self.order {
            validate_order(&schema_info, order)?;
        }

        let normalized_predicate = self.predicate.as_ref().map(normalize);
        let access_plan = plan_access::<T>(&schema_info, normalized_predicate.as_ref())?;
        crate::db::query::plan::validate_plan_invariants::<T>(
            &access_plan,
            &schema_info,
            normalized_predicate.as_ref(),
        );

        validate_access_plan(&schema_info, model, &access_plan)?;

        let plan = LogicalPlan {
            access: access_plan,
            predicate: normalized_predicate,
            order: self.order.clone(),
            page: self.page.clone(),
            projection: self.projection.clone(),
            consistency: self.consistency,
        };

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

///
/// QueryError
///
#[derive(Debug, ThisError)]
pub enum QueryError {
    #[error("{0}")]
    Validate(#[from] ValidateError),
    #[error("{0}")]
    Plan(#[from] PlanError),
    #[error("{0}")]
    Execute(#[from] InternalError),
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

#[cfg(test)]
mod tests;
