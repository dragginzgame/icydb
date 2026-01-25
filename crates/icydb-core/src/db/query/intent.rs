use crate::{
    db::query::{
        ReadConsistency,
        plan::{
            DeleteLimitSpec, ExecutablePlan, ExplainPlan, LogicalPlan, OrderDirection, OrderSpec,
            PageSpec, PlanError, ProjectionSpec, planner::plan_access,
            validate::validate_access_plan, validate::validate_order,
        },
        predicate::{Predicate, SchemaInfo, ValidateError, normalize},
    },
    error::InternalError,
    traits::EntityKind,
};
use std::marker::PhantomData;
use thiserror::Error as ThisError;

///
/// QueryMode
/// Discriminates load vs delete intent at planning time.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryMode {
    Load,
    Delete,
}

///
/// DeleteLimit
/// Declarative deletion bound for a query window.
/// Expressed as a max row count; no offsets.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteLimit {
    pub max_rows: u32,
}

impl DeleteLimit {
    /// Create a new delete limit bound.
    #[must_use]
    pub const fn new(max_rows: u32) -> Self {
        Self { max_rows }
    }

    pub(crate) const fn to_spec(self) -> DeleteLimitSpec {
        DeleteLimitSpec {
            max_rows: self.max_rows,
        }
    }
}

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
    mode: QueryMode,
    predicate: Option<Predicate>,
    order: Option<OrderSpec>,
    delete_limit: Option<DeleteLimit>,
    page: Option<Page>,
    projection: ProjectionSpec,
    consistency: ReadConsistency,
    _marker: PhantomData<E>,
}

///
/// Page
/// Declarative pagination intent for a query window.
/// Expressed as limit/offset only; no response semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Page {
    pub limit: u32,
    pub offset: u64,
}

impl Page {
    /// Create a new pagination intent with a limit and offset.
    #[must_use]
    pub const fn new(limit: u32, offset: u64) -> Self {
        Self { limit, offset }
    }

    pub(crate) const fn to_spec(self) -> PageSpec {
        PageSpec {
            limit: Some(self.limit),
            offset: self.offset,
        }
    }
}

impl<E: EntityKind> Query<E> {
    /// Create a new intent with an explicit missing-row policy.
    #[must_use]
    pub const fn new(consistency: ReadConsistency) -> Self {
        Self {
            mode: QueryMode::Load,
            predicate: None,
            order: None,
            delete_limit: None,
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

    /// Mark this intent as a delete query.
    #[must_use]
    pub const fn delete(mut self) -> Self {
        self.mode = QueryMode::Delete;
        self
    }

    /// Bound a delete query to at most `max_rows` rows.
    #[must_use]
    pub const fn delete_limit(mut self, max_rows: u32) -> Self {
        self.delete_limit = Some(DeleteLimit::new(max_rows));
        self
    }

    /// Replace the current pagination settings with an explicit limit/offset window.
    /// Pagination is part of intent and is enforced during planning.
    #[must_use]
    pub const fn page(mut self, limit: u32, offset: u64) -> Self {
        self.page = Some(Page::new(limit, offset));
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
        // Phase 1: schema surface and intent validation.
        let model = T::MODEL;
        let schema_info = SchemaInfo::from_entity_model(model)?;
        self.validate_intent()?;

        if let Some(order) = &self.order {
            validate_order(&schema_info, order)?;
        }

        // Phase 2: predicate normalization and access planning.
        let normalized_predicate = self.predicate.as_ref().map(normalize);
        let access_plan = plan_access::<T>(&schema_info, normalized_predicate.as_ref())?;
        crate::db::query::plan::validate_plan_invariants::<T>(
            &access_plan,
            &schema_info,
            normalized_predicate.as_ref(),
        );

        validate_access_plan(&schema_info, model, &access_plan)?;

        // Phase 3: assemble the executor-ready plan.
        let plan = LogicalPlan {
            mode: self.mode,
            access: access_plan,
            predicate: normalized_predicate,
            order: self.order.clone(),
            delete_limit: self.delete_limit.map(DeleteLimit::to_spec),
            page: self.page.map(Page::to_spec),
            projection: self.projection.clone(),
            consistency: self.consistency,
        };

        Ok(plan)
    }

    // Validate delete-specific intent rules before planning.
    const fn validate_intent(&self) -> Result<(), IntentError> {
        match self.mode {
            QueryMode::Load => {
                if self.delete_limit.is_some() {
                    return Err(IntentError::DeleteLimitOnLoad);
                }
            }
            QueryMode::Delete => {
                if self.page.is_some() && self.delete_limit.is_some() {
                    return Err(IntentError::DeleteLimitWithPagination);
                }
                if self.page.is_some() {
                    return Err(IntentError::DeletePaginationNotSupported);
                }
                if self.delete_limit.is_some() && self.order.is_none() {
                    return Err(IntentError::DeleteLimitRequiresOrder);
                }
            }
        }

        Ok(())
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
    Intent(#[from] IntentError),
    #[error("{0}")]
    Execute(#[from] InternalError),
}

///
/// IntentError
///
#[derive(Debug, ThisError)]
pub enum IntentError {
    #[error("delete limit is only valid for delete intents")]
    DeleteLimitOnLoad,
    #[error("delete queries do not support pagination offsets")]
    DeletePaginationNotSupported,
    #[error("delete limit cannot be combined with pagination")]
    DeleteLimitWithPagination,
    #[error("delete limit requires an explicit ordering")]
    DeleteLimitRequiresOrder,
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
