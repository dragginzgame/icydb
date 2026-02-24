#![expect(clippy::used_underscore_binding)]
#[cfg(test)]
mod tests;

// Key-only access intent and helpers (split out for readability).
mod key_access;
pub(crate) use key_access::*;

use crate::{
    db::{
        query::{
            ReadConsistency,
            explain::ExplainPlan,
            expr::{FilterExpr, SortExpr, SortLowerError},
            plan::{
                DeleteLimitSpec, ExecutablePlan, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
                PlanError,
                planner::{PlannerError, plan_access},
                validate::validate_logical_plan_model,
            },
            policy,
            predicate::{
                Predicate, PredicateFieldSlots, SchemaInfo, ValidateError, normalize,
                normalize_enum_literals, validate::reject_unsupported_query_features,
            },
        },
        response::ResponseError,
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, FieldValue, SingletonEntity},
    value::Value,
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
/// Consumes an `EntityModel` derived from typed entity definitions.
///

#[derive(Debug)]
pub(crate) struct QueryModel<'m, K> {
    model: &'m EntityModel,
    mode: QueryMode,
    predicate: Option<Predicate>,
    key_access: Option<KeyAccessState<K>>,
    key_access_conflict: bool,
    order: Option<OrderSpec>,
    distinct: bool,
    consistency: ReadConsistency,
}

impl<'m, K: FieldValue> QueryModel<'m, K> {
    #[must_use]
    pub(crate) const fn new(model: &'m EntityModel, consistency: ReadConsistency) -> Self {
        Self {
            model,
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            key_access: None,
            key_access_conflict: false,
            order: None,
            distinct: false,
            consistency,
        }
    }

    /// Return the intent mode (load vs delete).
    #[must_use]
    pub(crate) const fn mode(&self) -> QueryMode {
        self.mode
    }

    #[must_use]
    fn has_explicit_order(&self) -> bool {
        policy::has_explicit_order(self.order.as_ref())
    }

    #[must_use]
    const fn load_spec(&self) -> Option<LoadSpec> {
        match self.mode {
            QueryMode::Load(spec) => Some(spec),
            QueryMode::Delete(_) => None,
        }
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub(crate) fn filter(mut self, predicate: Predicate) -> Self {
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
            Err(SortLowerError::Plan(err)) => return Err(QueryError::from(*err)),
        };

        policy::validate_order_shape(Some(&order))
            .map_err(IntentError::from)
            .map_err(QueryError::from)?;

        Ok(self.order_spec(order))
    }

    /// Append an ascending sort key.
    #[must_use]
    pub(crate) fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.order = Some(push_order(self.order, field.as_ref(), OrderDirection::Asc));
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub(crate) fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.order = Some(push_order(self.order, field.as_ref(), OrderDirection::Desc));
        self
    }

    /// Set a fully-specified order spec (validated before reaching this boundary).
    pub(crate) fn order_spec(mut self, order: OrderSpec) -> Self {
        self.order = Some(order);
        self
    }

    /// Enable DISTINCT semantics for this query intent.
    #[must_use]
    pub(crate) const fn distinct(mut self) -> Self {
        self.distinct = true;
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
    pub(crate) fn by_id(self, id: K) -> Self {
        self.set_key_access(KeyAccessKind::Single, KeyAccess::Single(id))
    }

    /// Set the access path to a primary key batch lookup.
    pub(crate) fn by_ids<I>(self, ids: I) -> Self
    where
        I: IntoIterator<Item = K>,
    {
        self.set_key_access(
            KeyAccessKind::Many,
            KeyAccess::Many(ids.into_iter().collect()),
        )
    }

    /// Set the access path to the singleton primary key.
    pub(crate) fn only(self, id: K) -> Self {
        self.set_key_access(KeyAccessKind::Only, KeyAccess::Single(id))
    }

    /// Mark this intent as a delete query.
    #[must_use]
    pub(crate) const fn delete(mut self) -> Self {
        if self.mode.is_load() {
            self.mode = QueryMode::Delete(DeleteSpec::new());
        }
        self
    }

    /// Apply a limit to the current mode.
    ///
    /// Load limits bound result size; delete limits bound mutation size.
    #[must_use]
    pub(crate) const fn limit(mut self, limit: u32) -> Self {
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
    pub(crate) const fn offset(mut self, offset: u32) -> Self {
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
        let normalized_predicate = self
            .predicate
            .as_ref()
            .map(|predicate| {
                reject_unsupported_query_features(predicate).map_err(ValidateError::from)?;
                let predicate = normalize_enum_literals(&schema_info, predicate)?;
                Ok::<Predicate, ValidateError>(normalize(&predicate))
            })
            .transpose()?;
        let access_plan_value = match &self.key_access {
            Some(state) => access_plan_from_keys_value(&state.access),
            None => plan_access(self.model, &schema_info, normalized_predicate.as_ref())?,
        };

        // Phase 3: assemble the executor-ready plan.
        let plan = LogicalPlan {
            mode: self.mode,
            access: access_plan_value,
            predicate: normalized_predicate,
            // Canonicalize ORDER BY to include an explicit primary-key tie-break.
            // This ensures explain/fingerprint/execution share one deterministic order shape.
            order: canonicalize_order_spec(self.model, self.order.clone()),
            distinct: self.distinct,
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

    // Validate pre-plan policy invariants and key-access rules before planning.
    fn validate_intent(&self) -> Result<(), IntentError> {
        if self.key_access_conflict {
            return Err(IntentError::KeyAccessConflict);
        }

        policy::validate_intent_plan_shape(self.mode, self.order.as_ref())
            .map_err(IntentError::from)?;

        if let Some(state) = &self.key_access {
            match state.kind {
                KeyAccessKind::Many if self.predicate.is_some() => {
                    return Err(IntentError::ByIdsWithPredicate);
                }
                KeyAccessKind::Only if self.predicate.is_some() => {
                    return Err(IntentError::OnlyWithPredicate);
                }
                _ => {
                    // NOTE: Single/Many without predicates impose no additional constraints.
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
    intent: QueryModel<'static, E::Key>,
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

    #[must_use]
    pub(crate) fn has_explicit_order(&self) -> bool {
        self.intent.has_explicit_order()
    }

    #[must_use]
    pub(crate) const fn load_spec(&self) -> Option<LoadSpec> {
        self.intent.load_spec()
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

    /// Enable DISTINCT semantics for this query.
    #[must_use]
    pub fn distinct(mut self) -> Self {
        self.intent = self.intent.distinct();
        self
    }

    /// Set the access path to a single primary key lookup.
    pub(crate) fn by_id(self, id: E::Key) -> Self {
        let Self { intent, _marker } = self;
        Self {
            intent: intent.by_id(id),
            _marker,
        }
    }

    /// Set the access path to a primary key batch lookup.
    pub(crate) fn by_ids<I>(self, ids: I) -> Self
    where
        I: IntoIterator<Item = E::Key>,
    {
        let Self { intent, _marker } = self;
        Self {
            intent: intent.by_ids(ids),
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
    /// For load queries, any use of `limit` or `offset` requires an explicit
    /// `order_by(...)` so pagination is deterministic.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.intent = self.intent.limit(limit);
        self
    }

    /// Apply an offset to a load intent.
    ///
    /// Any use of `offset` or `limit` requires an explicit `order_by(...)`.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.intent = self.intent.offset(offset);
        self
    }

    /// Explain this intent without executing it.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        let plan = self.build_plan()?;

        Ok(plan.explain_with_model(E::MODEL))
    }

    /// Plan this intent into an executor-ready plan.
    pub fn plan(&self) -> Result<ExecutablePlan<E>, QueryError> {
        let plan = self.build_plan()?;
        let predicate_slots = plan
            .predicate
            .as_ref()
            .map(PredicateFieldSlots::resolve::<E>);

        Ok(ExecutablePlan::new_with_compiled_predicate_slots(
            plan,
            predicate_slots,
        ))
    }

    // Build a logical plan for the current intent.
    fn build_plan(&self) -> Result<LogicalPlan<E::Key>, QueryError> {
        let plan_value = self.intent.build_plan_model()?;
        let LogicalPlan {
            mode,
            access,
            predicate,
            order,
            distinct,
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
            distinct,
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
    E::Key: Default,
{
    /// Set the access path to the singleton primary key.
    pub(crate) fn only(self) -> Self {
        let Self { intent, _marker } = self;

        Self {
            intent: intent.only(E::Key::default()),
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
    Plan(Box<PlanError>),

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
            PlannerError::Plan(err) => Self::from(*err),
            PlannerError::Internal(err) => Self::Execute(*err),
        }
    }
}

impl From<PlanError> for QueryError {
    fn from(err: PlanError) -> Self {
        Self::Plan(Box::new(err))
    }
}

///
/// IntentError
///

#[derive(Clone, Copy, Debug, ThisError)]
pub enum IntentError {
    #[error("{0}")]
    PlanShape(#[from] policy::PlanPolicyError),

    #[error("by_ids() cannot be combined with predicates")]
    ByIdsWithPredicate,

    #[error("only() cannot be combined with predicates")]
    OnlyWithPredicate,

    #[error("multiple key access methods were used on the same query")]
    KeyAccessConflict,

    #[error("cursor pagination requires an explicit ordering")]
    CursorRequiresOrder,

    #[error("cursor pagination requires an explicit limit")]
    CursorRequiresLimit,

    #[error("cursor tokens can only be used with .page().execute()")]
    CursorRequiresPagedExecution,
}

impl From<policy::CursorPagingPolicyError> for IntentError {
    fn from(err: policy::CursorPagingPolicyError) -> Self {
        match err {
            policy::CursorPagingPolicyError::CursorRequiresOrder => Self::CursorRequiresOrder,
            policy::CursorPagingPolicyError::CursorRequiresLimit => Self::CursorRequiresLimit,
        }
    }
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

// Normalize ORDER BY into a canonical, deterministic shape:
// - preserve user field order
// - remove explicit primary-key references from the user segment
// - append exactly one primary-key field as the terminal tie-break
fn canonicalize_order_spec(model: &EntityModel, order: Option<OrderSpec>) -> Option<OrderSpec> {
    let mut order = order?;
    if order.fields.is_empty() {
        return Some(order);
    }

    let pk_field = model.primary_key.name;
    let mut pk_direction = None;
    order.fields.retain(|(field, direction)| {
        if field == pk_field {
            if pk_direction.is_none() {
                pk_direction = Some(*direction);
            }
            false
        } else {
            true
        }
    });

    let pk_direction = pk_direction.unwrap_or(OrderDirection::Asc);
    order.fields.push((pk_field.to_string(), pk_direction));

    Some(order)
}
