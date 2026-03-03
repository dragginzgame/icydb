use crate::{
    db::{
        predicate::{
            CompareOp, MissingRowPolicy, Predicate, SchemaInfo, ValidateError, normalize,
            normalize_enum_literals, reject_unsupported_query_features,
        },
        query::{
            builder::aggregate::AggregateExpr,
            expr::{FilterExpr, SortExpr, SortLowerError},
            intent::{
                DeleteSpec, IntentError, KeyAccess, KeyAccessKind, KeyAccessState, LoadSpec,
                QueryError, QueryMode, access_plan_from_keys_value,
                order::{canonicalize_order_spec, push_order},
            },
            plan::{
                AccessPlannedQuery, DeleteLimitSpec, GroupAggregateSpec, GroupHavingClause,
                GroupHavingSpec, GroupHavingSymbol, GroupSpec, GroupedExecutionConfig,
                IntentKeyAccessKind as IntentValidationKeyAccessKind, LogicalPlan, OrderDirection,
                OrderSpec, PageSpec, ScalarPlan, has_explicit_order, plan_access,
                resolve_group_field_slot, validate_group_query_semantics,
                validate_intent_key_access_policy, validate_intent_plan_shape,
                validate_order_shape, validate_query_semantics,
            },
        },
    },
    model::entity::EntityModel,
    traits::FieldValue,
    value::Value,
};

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
    group: Option<crate::db::query::plan::GroupSpec>,
    having: Option<GroupHavingSpec>,
    order: Option<OrderSpec>,
    distinct: bool,
    consistency: MissingRowPolicy,
}

impl<'m, K: FieldValue> QueryModel<'m, K> {
    #[must_use]
    pub(crate) const fn new(model: &'m EntityModel, consistency: MissingRowPolicy) -> Self {
        Self {
            model,
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            key_access: None,
            key_access_conflict: false,
            group: None,
            having: None,
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
    pub(in crate::db::query::intent) fn has_explicit_order(&self) -> bool {
        has_explicit_order(self.order.as_ref())
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn has_grouping(&self) -> bool {
        self.group.is_some()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn load_spec(&self) -> Option<LoadSpec> {
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

        validate_order_shape(Some(&order))
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

    // Resolve one grouped field into one stable field slot and append it to the
    // grouped spec in declaration order.
    pub(in crate::db::query::intent) fn push_group_field(
        mut self,
        field: &str,
    ) -> Result<Self, QueryError> {
        let field_slot = resolve_group_field_slot(self.model, field).map_err(QueryError::from)?;
        let group = self.group.get_or_insert(GroupSpec {
            group_fields: Vec::new(),
            aggregates: Vec::new(),
            execution: GroupedExecutionConfig::unbounded(),
        });
        if !group
            .group_fields
            .iter()
            .any(|existing| existing.index() == field_slot.index())
        {
            group.group_fields.push(field_slot);
        }

        Ok(self)
    }

    // Append one grouped aggregate terminal to the grouped declarative spec.
    pub(in crate::db::query::intent) fn push_group_aggregate(
        mut self,
        aggregate: AggregateExpr,
    ) -> Self {
        let group = self.group.get_or_insert(GroupSpec {
            group_fields: Vec::new(),
            aggregates: Vec::new(),
            execution: GroupedExecutionConfig::unbounded(),
        });
        group
            .aggregates
            .push(GroupAggregateSpec::from_aggregate_expr(&aggregate));

        self
    }

    // Override grouped hard limits for this grouped query.
    pub(in crate::db::query::intent) fn grouped_limits(
        mut self,
        max_groups: u64,
        max_group_bytes: u64,
    ) -> Self {
        let group = self.group.get_or_insert(GroupSpec {
            group_fields: Vec::new(),
            aggregates: Vec::new(),
            execution: GroupedExecutionConfig::unbounded(),
        });
        group.execution = GroupedExecutionConfig::with_hard_limits(max_groups, max_group_bytes);

        self
    }

    // Append one grouped HAVING compare clause after GROUP BY terminal declaration.
    fn push_having_clause(mut self, clause: GroupHavingClause) -> Result<Self, QueryError> {
        if self.group.is_none() {
            return Err(QueryError::Intent(IntentError::HavingRequiresGroupBy));
        }

        let having = self.having.get_or_insert(GroupHavingSpec {
            clauses: Vec::new(),
        });
        having.clauses.push(clause);

        Ok(self)
    }

    // Append one grouped HAVING clause that references one grouped key field.
    pub(in crate::db::query::intent) fn push_having_group_clause(
        self,
        field: &str,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        let field_slot = resolve_group_field_slot(self.model, field).map_err(QueryError::from)?;

        self.push_having_clause(GroupHavingClause {
            symbol: GroupHavingSymbol::GroupField(field_slot),
            op,
            value,
        })
    }

    // Append one grouped HAVING clause that references one grouped aggregate output.
    pub(in crate::db::query::intent) fn push_having_aggregate_clause(
        self,
        aggregate_index: usize,
        op: CompareOp,
        value: Value,
    ) -> Result<Self, QueryError> {
        self.push_having_clause(GroupHavingClause {
            symbol: GroupHavingSymbol::AggregateIndex(aggregate_index),
            op,
            value,
        })
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
    pub(in crate::db::query::intent) fn build_plan_model(
        &self,
    ) -> Result<AccessPlannedQuery<Value>, QueryError> {
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
        let scalar = ScalarPlan {
            mode: self.mode,
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
        let mut plan =
            AccessPlannedQuery::from_parts(LogicalPlan::Scalar(scalar), access_plan_value);
        if let Some(group) = self.group.clone() {
            plan = match self.having.clone() {
                Some(having) => plan.into_grouped_with_having(group, Some(having)),
                None => plan.into_grouped(group),
            };
        }

        if plan.grouped_plan().is_some() {
            validate_group_query_semantics(&schema_info, self.model, &plan)?;
        } else {
            validate_query_semantics(&schema_info, self.model, &plan)?;
        }

        Ok(plan)
    }

    // Validate pre-plan policy invariants and key-access rules before planning.
    pub(in crate::db::query::intent) fn validate_intent(&self) -> Result<(), IntentError> {
        validate_intent_plan_shape(self.mode, self.order.as_ref()).map_err(IntentError::from)?;

        let key_access_kind = self.key_access.as_ref().map(|state| match state.kind {
            KeyAccessKind::Single => IntentValidationKeyAccessKind::Single,
            KeyAccessKind::Many => IntentValidationKeyAccessKind::Many,
            KeyAccessKind::Only => IntentValidationKeyAccessKind::Only,
        });
        validate_intent_key_access_policy(
            self.key_access_conflict,
            key_access_kind,
            self.predicate.is_some(),
        )
        .map_err(IntentError::from)?;
        if self.having.is_some() && self.group.is_none() {
            return Err(IntentError::HavingRequiresGroupBy);
        }

        Ok(())
    }
}
