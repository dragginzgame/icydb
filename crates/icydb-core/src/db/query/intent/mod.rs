#![allow(clippy::used_underscore_binding)]
mod key_access;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        query::{
            ReadConsistency,
            expr::{FilterExpr, SortExpr, SortLowerError},
            plan::{
                DeleteLimitSpec, ExecutablePlan, ExplainPlan, LogicalPlan, OrderDirection,
                OrderSpec, PageSpec, PlanError,
                planner::{PlannerError, plan_access},
                validate::validate_logical_plan_model,
            },
            predicate::{Predicate, SchemaInfo, ValidateError, normalize},
        },
        response::ResponseError,
    },
    error::InternalError,
    traits::{EntityKind, FieldValue, SingletonEntity},
    value::Value,
};
use std::marker::PhantomData;
use thiserror::Error as ThisError;

pub use key_access::*;

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
/// QueryModel
///
/// Model-level query intent and planning context.
/// `EntityModel` is the source of truth; schema surfaces are derived on demand.
///

#[derive(Debug)]
pub(crate) struct QueryModel<'m, K> {
    model: &'m crate::model::entity::EntityModel,
    mode: QueryMode,
    predicate: Option<Predicate>,
    key_access: Option<KeyAccessState<K>>,
    key_access_conflict: bool,
    order: Option<OrderSpec>,
    consistency: ReadConsistency,
}

impl<'m, K: FieldValue> QueryModel<'m, K> {
    #[must_use]
    pub const fn new(
        model: &'m crate::model::entity::EntityModel,
        consistency: ReadConsistency,
    ) -> Self {
        Self {
            model,
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            key_access: None,
            key_access_conflict: false,
            order: None,
            consistency,
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

    /// Apply a dynamic filter expression using the model schema.
    pub(crate) fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        let schema = SchemaInfo::from_entity_model(self.model)?;
        let predicate = expr.lower_with(&schema).map_err(QueryError::Validate)?;

        Ok(self.filter(predicate))
    }

    /// Apply a dynamic sort expression using the model schema.
    pub(crate) fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        let schema = SchemaInfo::from_entity_model(self.model)?;
        let order = match expr.lower_with(&schema) {
            Ok(order) => order,
            Err(SortLowerError::Validate(err)) => return Err(QueryError::Validate(err)),
            Err(SortLowerError::Plan(err)) => return Err(QueryError::Plan(err)),
        };

        if order.fields.is_empty() {
            return Err(QueryError::Intent(IntentError::EmptyOrderSpec));
        }

        Ok(self.order_spec(order))
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.order = Some(push_order(self.order, field.as_ref(), OrderDirection::Asc));
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.order = Some(push_order(self.order, field.as_ref(), OrderDirection::Desc));
        self
    }

    /// Set a fully-specified order spec (validated before reaching this boundary).
    pub(crate) fn order_spec(mut self, order: OrderSpec) -> Self {
        self.order = Some(order);
        self
    }

    /// Track key-only access paths and detect conflicting key intents.
    fn set_key_access(mut self, kind: KeyAccessKind, access: KeyAccess<K>) -> Self {
        if let Some(existing) = &self.key_access
            && existing.kind != kind
        {
            self.key_access_conflict = true;
        }

        self.key_access = Some(KeyAccessState { kind, access });

        self
    }

    /// Set the access path to a single primary key lookup.
    pub(crate) fn by_key(self, key: K) -> Self {
        self.set_key_access(KeyAccessKind::Single, KeyAccess::Single(key))
    }

    /// Set the access path to a primary key batch lookup.
    pub(crate) fn by_keys<I>(self, keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
    {
        self.set_key_access(
            KeyAccessKind::Many,
            KeyAccess::Many(keys.into_iter().collect()),
        )
    }

    /// Set the access path to the singleton primary key.
    pub(crate) fn only(self, id: K) -> Self {
        self.set_key_access(KeyAccessKind::Only, KeyAccess::Single(id))
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
    #[must_use]
    pub const fn offset(mut self, offset: u32) -> Self {
        if let QueryMode::Load(mut spec) = self.mode {
            spec.offset = offset;
            self.mode = QueryMode::Load(spec);
        }
        self
    }

    /// Build a model-level logical plan using Value-based access keys.
    fn build_plan_model(&self) -> Result<LogicalPlan<Value>, QueryError> {
        // Phase 1: schema surface and intent validation.
        let schema_info = SchemaInfo::from_entity_model(self.model)?;
        self.validate_intent()?;

        // Phase 2: predicate normalization and access planning.
        let normalized_predicate = self.predicate.as_ref().map(normalize);
        let access_plan_value = match &self.key_access {
            Some(state) => access_plan_from_keys_value(&state.access),
            None => plan_access(self.model, &schema_info, normalized_predicate.as_ref())?,
        };

        // Phase 3: assemble the executor-ready plan.
        let plan = LogicalPlan {
            mode: self.mode,
            access: access_plan_value,
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
            consistency: self.consistency,
        };

        validate_logical_plan_model(&schema_info, self.model, &plan)?;

        Ok(plan)
    }

    // Validate delete-specific intent rules before planning.
    const fn validate_intent(&self) -> Result<(), IntentError> {
        if self.key_access_conflict {
            return Err(IntentError::KeyAccessConflict);
        }

        if let Some(order) = &self.order
            && order.fields.is_empty()
        {
            return Err(IntentError::EmptyOrderSpec);
        }

        if let Some(state) = &self.key_access {
            match state.kind {
                KeyAccessKind::Many if self.predicate.is_some() => {
                    return Err(IntentError::ManyWithPredicate);
                }
                KeyAccessKind::Only if self.predicate.is_some() => {
                    return Err(IntentError::OnlyWithPredicate);
                }
                _ => {}
            }
        }

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
    intent: QueryModel<'static, E::Id>,
    #[allow(clippy::struct_field_names)]
    _marker: PhantomData<E>,
}

impl<E: EntityKind> Query<E> {
    /// Create a new intent with an explicit missing-row policy.
    /// MissingOk favors idempotency and may mask index/data divergence on deletes.
    /// Use Strict to surface missing rows during scan/delete execution.
    #[must_use]
    pub const fn new(consistency: ReadConsistency) -> Self {
        Self {
            intent: QueryModel::new(E::MODEL, consistency),
            _marker: PhantomData,
        }
    }

    /// Return the intent mode (load vs delete).
    #[must_use]
    pub const fn mode(&self) -> QueryMode {
        self.intent.mode()
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.intent = self.intent.filter(predicate);
        self
    }

    /// Apply a dynamic filter expression.
    pub fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        let Self { intent, _marker } = self;
        let intent = intent.filter_expr(expr)?;

        Ok(Self { intent, _marker })
    }

    /// Apply a dynamic sort expression.
    pub fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        let Self { intent, _marker } = self;
        let intent = intent.sort_expr(expr)?;

        Ok(Self { intent, _marker })
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.intent = self.intent.order_by(field);
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.intent = self.intent.order_by_desc(field);
        self
    }

    /// Set the access path to a single primary key lookup.
    pub(crate) fn by_key(self, key: E::Id) -> Self {
        let Self { intent, _marker } = self;
        Self {
            intent: intent.by_key(key),
            _marker,
        }
    }

    /// Set the access path to a primary key batch lookup.
    pub(crate) fn by_keys<I>(self, keys: I) -> Self
    where
        I: IntoIterator<Item = E::Id>,
    {
        let Self { intent, _marker } = self;
        Self {
            intent: intent.by_keys(keys),
            _marker,
        }
    }

    /// Mark this intent as a delete query.
    #[must_use]
    pub fn delete(mut self) -> Self {
        self.intent = self.intent.delete();
        self
    }

    /// Apply a limit to the current mode.
    ///
    /// Load limits bound result size; delete limits bound mutation size.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.intent = self.intent.limit(limit);
        self
    }

    /// Apply an offset to a load intent.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.intent = self.intent.offset(offset);
        self
    }

    /// Explain this intent without executing it.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        let plan = self.build_plan()?;

        Ok(plan.explain())
    }

    /// Plan this intent into an executor-ready plan.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        let plan = self.build_plan()?;

        Ok(ExecutablePlan::new(plan))
    }

    // Build a logical plan for the current intent.
    fn build_plan(&self) -> Result<LogicalPlan<E::Id>, QueryError> {
        let plan_value = self.intent.build_plan_model()?;
        let LogicalPlan {
            mode,
            access,
            predicate,
            order,
            delete_limit,
            page,
            consistency,
        } = plan_value;

        let access = access_plan_to_entity_keys::<E>(E::MODEL, access)?;
        let plan = LogicalPlan {
            mode,
            access,
            predicate,
            order,
            delete_limit,
            page,
            consistency,
        };

        Ok(plan)
    }
}

impl<E> Query<E>
where
    E: EntityKind + SingletonEntity,
{
    /// Set the access path to the singleton primary key.
    pub(crate) fn only(self, id: E::Id) -> Self {
        let Self { intent, _marker } = self;

        Self {
            intent: intent.only(id),
            _marker,
        }
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
    Response(#[from] ResponseError),

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

    #[error("order specification must include at least one field")]
    EmptyOrderSpec,

    #[error("many() cannot be combined with predicates")]
    ManyWithPredicate,

    #[error("only() cannot be combined with predicates")]
    OnlyWithPredicate,

    #[error("multiple key access methods were used on the same query")]
    KeyAccessConflict,
}

/// Helper to append an ordering field while preserving existing order spec.
fn push_order(order: Option<OrderSpec>, field: &str, direction: OrderDirection) -> OrderSpec {
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
