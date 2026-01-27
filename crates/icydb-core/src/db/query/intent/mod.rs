use crate::{
    db::query::{
        ReadConsistency,
        plan::{
            DeleteLimitSpec, ExecutablePlan, ExplainPlan, LogicalPlan, OrderDirection, OrderSpec,
            PageSpec, PlanError, ProjectionSpec,
            planner::{PlannerError, plan_access},
            validate::validate_access_plan,
            validate::validate_order,
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
/// Mode-specific fields for load intents.
/// Encodes pagination without leaking into delete intents.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoadSpec {
    pub limit: Option<u32>,
    pub offset: u64,
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
    projection: ProjectionSpec,
    consistency: ReadConsistency,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> Query<E> {
    /// Create a new intent with an explicit missing-row policy.
    /// MissingOk favors idempotency and may mask index/data divergence on deletes.
    /// Use Strict to surface missing rows during scan/delete execution.
    #[must_use]
    pub const fn new(consistency: ReadConsistency) -> Self {
        Self {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            projection: ProjectionSpec::All,
            consistency,
            _marker: PhantomData,
        }
    }

    /// Return the intent mode (load vs delete).
    #[must_use]
    pub const fn mode(&self) -> QueryMode {
        self.mode
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
        if self.mode.is_load() {
            self.mode = QueryMode::Delete(DeleteSpec::new());
        }
        self
    }

    /// Apply a limit to the current mode.
    ///
    /// Load limits bound result size; delete limits bound mutation size.
    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        match self.mode {
            QueryMode::Load(mut spec) => {
                spec.limit = Some(limit);
                self.mode = QueryMode::Load(spec);
            }
            QueryMode::Delete(mut spec) => {
                spec.limit = Some(limit);
                self.mode = QueryMode::Delete(spec);
            }
        }
        self
    }

    /// Apply an offset to a load intent.
    ///
    /// Note: session-bound queries reject offsets on delete intents.
    #[must_use]
    pub const fn offset(mut self, offset: u64) -> Self {
        if let QueryMode::Load(mut spec) = self.mode {
            spec.offset = offset;
            self.mode = QueryMode::Load(spec);
        }
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

        validate_access_plan(&schema_info, model, &access_plan)?;

        // Phase 3: assemble the executor-ready plan.
        let plan = LogicalPlan {
            mode: self.mode,
            access: access_plan,
            predicate: normalized_predicate,
            order: self.order.clone(),
            delete_limit: match self.mode {
                QueryMode::Delete(spec) => spec.limit.map(|max_rows| DeleteLimitSpec { max_rows }),
                QueryMode::Load(_) => None,
            },
            page: match self.mode {
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
            projection: self.projection.clone(),
            consistency: self.consistency,
        };

        Ok(plan)
    }

    // Validate delete-specific intent rules before planning.
    const fn validate_intent(&self) -> Result<(), IntentError> {
        match self.mode {
            QueryMode::Load(_) => {}
            QueryMode::Delete(spec) => {
                if spec.limit.is_some() && self.order.is_none() {
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

impl From<PlannerError> for QueryError {
    fn from(err: PlannerError) -> Self {
        match err {
            PlannerError::Plan(err) => Self::Plan(err),
            PlannerError::Internal(err) => Self::Execute(err),
        }
    }
}

///
/// IntentError
///

#[derive(Clone, Copy, Debug, ThisError)]
pub enum IntentError {
    #[error("delete limit requires an explicit ordering")]
    DeleteLimitRequiresOrder,

    #[error("offsets are only valid for load intents")]
    OffsetOnDelete,

    #[error("load execution requested for a delete intent")]
    ExecuteLoadOnDelete,

    #[error("delete execution requested for a load intent")]
    ExecuteDeleteOnLoad,
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
