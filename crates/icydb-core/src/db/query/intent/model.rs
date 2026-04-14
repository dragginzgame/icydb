//! Module: query::intent::model
//! Responsibility: query-intent model normalization and planner handoff construction.
//! Does not own: executor runtime behavior or post-plan execution routing.
//! Boundary: turns fluent/query intent state into validated logical/planned contracts.

use crate::db::query::intent::state::GroupedIntent;
#[cfg(feature = "sql")]
use crate::db::query::plan::expr::FieldId;
use crate::{
    db::{
        access::{AccessPlan, canonical::canonicalize_value_set},
        codec::{finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_u64},
        predicate::{
            CompareOp, MissingRowPolicy, Predicate, hash_predicate as hash_model_predicate,
        },
        query::{
            builder::aggregate::AggregateExpr,
            expr::{FilterExpr, SortExpr},
            fingerprint::{
                aggregate_hash::{AggregateHashShape, hash_group_aggregate_structural_fingerprint},
                hash_parts::{hash_access_plan, write_str, write_tag, write_u32, write_value},
                projection_hash::hash_projection_field_selection_fingerprint,
            },
            intent::{IntentError, QueryError, QueryIntent, build_access_plan_from_keys},
            plan::{
                AccessPlannedQuery, GroupAggregateSpec, GroupHavingClause, GroupHavingSymbol,
                LogicalPlan, OrderDirection, OrderSpec, QueryMode, VisibleIndexes,
                build_logical_plan, expr::ProjectionSelection, fold_constant_predicate,
                is_limit_zero_load_window, logical_query_from_logical_inputs,
                normalize_query_predicate, plan_query_access, predicate_is_constant_false,
                project_access_choice_explain_snapshot_with_indexes, resolve_group_field_slot,
                validate_group_query_semantics, validate_order_shape, validate_query_semantics,
            },
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, field::FieldKind},
    traits::FieldValue,
    value::Value,
};

const QUERY_PLAN_CACHE_FINGERPRINT_PROFILE_TAG: &[u8] = b"querycache";
const QUERY_CACHE_SECTION_MODE_TAG: u8 = 0x01;
const QUERY_CACHE_SECTION_PREDICATE_TAG: u8 = 0x02;
const QUERY_CACHE_SECTION_KEY_ACCESS_TAG: u8 = 0x03;
const QUERY_CACHE_SECTION_ORDER_TAG: u8 = 0x04;
const QUERY_CACHE_SECTION_DISTINCT_TAG: u8 = 0x05;
const QUERY_CACHE_SECTION_PROJECTION_TAG: u8 = 0x06;
const QUERY_CACHE_SECTION_GROUP_TAG: u8 = 0x07;
const QUERY_CACHE_SECTION_CONSISTENCY_TAG: u8 = 0x08;

const QUERY_MODE_LOAD_TAG: u8 = 0x60;
const QUERY_MODE_DELETE_TAG: u8 = 0x61;

const PREDICATE_ABSENT_TAG: u8 = 0x20;
const PREDICATE_PRESENT_TAG: u8 = 0x21;
const KEY_ACCESS_ABSENT_TAG: u8 = 0x22;
const KEY_ACCESS_PRESENT_TAG: u8 = 0x23;
const ORDER_NONE_TAG: u8 = 0x30;
const ORDER_FIELDS_TAG: u8 = 0x31;
const DISTINCT_DISABLED_TAG: u8 = 0x45;
const DISTINCT_ENABLED_TAG: u8 = 0x44;
const PROJECTION_ALL_TAG: u8 = 0x80;
const PROJECTION_FIELDS_TAG: u8 = 0x81;
const PROJECTION_EXPRS_TAG: u8 = 0x82;
const GROUP_NONE_TAG: u8 = 0x70;
const GROUP_PRESENT_TAG: u8 = 0x71;
const GROUP_HAVING_ABSENT_TAG: u8 = 0x74;
const GROUP_HAVING_PRESENT_TAG: u8 = 0x75;
const GROUP_HAVING_GROUP_FIELD_TAG: u8 = 0x76;
const GROUP_HAVING_AGGREGATE_INDEX_TAG: u8 = 0x77;
const CONSISTENCY_IGNORE_TAG: u8 = 0x50;
const CONSISTENCY_ERROR_TAG: u8 = 0x51;
const ORDER_DIRECTION_ASC_TAG: u8 = 0x01;
const ORDER_DIRECTION_DESC_TAG: u8 = 0x02;

///
/// QueryModel
///
/// Model-level query intent and planning context.
/// Consumes an `EntityModel` derived from typed entity definitions.
///

#[derive(Clone, Debug)]
pub(crate) struct QueryModel<'m, K> {
    model: &'m EntityModel,
    intent: QueryIntent<K>,
    consistency: MissingRowPolicy,
}

impl<'m, K: FieldValue> QueryModel<'m, K> {
    #[must_use]
    pub(crate) const fn new(model: &'m EntityModel, consistency: MissingRowPolicy) -> Self {
        Self {
            model,
            intent: QueryIntent::new(),
            consistency,
        }
    }

    // Fingerprint one generic-free query intent at the pre-plan boundary so
    // repeated session-local planning can reuse one cached `AccessPlannedQuery`
    // without depending on SQL strings or typed fluent wrappers.
    #[must_use]
    pub(in crate::db) fn cache_fingerprint(&self) -> [u8; 32] {
        let scalar = self.intent.scalar();
        let key_access_override = scalar
            .key_access
            .as_ref()
            .map(|state| build_access_plan_from_keys(&state.access));
        let mut hasher = new_hash_sha256_prefixed(QUERY_PLAN_CACHE_FINGERPRINT_PROFILE_TAG);

        write_tag(&mut hasher, QUERY_CACHE_SECTION_MODE_TAG);
        hash_query_mode(&mut hasher, self.intent.mode());

        write_tag(&mut hasher, QUERY_CACHE_SECTION_PREDICATE_TAG);
        hash_query_predicate(&mut hasher, scalar.predicate.as_ref());

        write_tag(&mut hasher, QUERY_CACHE_SECTION_KEY_ACCESS_TAG);
        hash_query_key_access(&mut hasher, key_access_override.as_ref());

        write_tag(&mut hasher, QUERY_CACHE_SECTION_ORDER_TAG);
        hash_query_order(&mut hasher, scalar.order.as_ref());

        write_tag(&mut hasher, QUERY_CACHE_SECTION_DISTINCT_TAG);
        hash_query_distinct(&mut hasher, scalar.distinct);

        write_tag(&mut hasher, QUERY_CACHE_SECTION_PROJECTION_TAG);
        hash_query_projection_selection(&mut hasher, &scalar.projection_selection);

        write_tag(&mut hasher, QUERY_CACHE_SECTION_GROUP_TAG);
        hash_query_grouping(&mut hasher, self.intent.grouped());

        write_tag(&mut hasher, QUERY_CACHE_SECTION_CONSISTENCY_TAG);
        hash_query_consistency(&mut hasher, self.consistency);

        finalize_hash_sha256(hasher)
    }

    /// Return the intent mode (load vs delete).
    #[must_use]
    pub(crate) const fn mode(&self) -> QueryMode {
        self.intent.mode()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn model(&self) -> &'m EntityModel {
        self.model
    }

    #[must_use]
    pub(in crate::db::query::intent) fn has_explicit_order(&self) -> bool {
        self.intent.has_explicit_order()
    }

    #[must_use]
    pub(in crate::db::query::intent) const fn has_grouping(&self) -> bool {
        self.intent.is_grouped()
    }

    #[must_use]
    pub(crate) fn filter(mut self, predicate: Predicate) -> Self {
        self.intent.append_predicate(predicate);
        self
    }

    /// Apply a dynamic filter expression using the model schema.
    pub(crate) fn filter_expr(self, expr: FilterExpr) -> Result<Self, QueryError> {
        let schema = SchemaInfo::cached_for_entity_model(self.model);
        let predicate = expr.lower_with(schema).map_err(QueryError::validate)?;

        Ok(self.filter(predicate))
    }

    /// Apply a dynamic sort expression using the model schema.
    pub(crate) fn sort_expr(self, expr: SortExpr) -> Result<Self, QueryError> {
        let schema = SchemaInfo::cached_for_entity_model(self.model);
        let order = expr.lower_with(schema).map_err(QueryError::from)?;

        validate_order_shape(Some(&order))
            .map_err(IntentError::from)
            .map_err(QueryError::from)?;

        Ok(self.order_spec(order))
    }

    /// Append an ascending sort key.
    #[must_use]
    pub(crate) fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.intent.push_order_ascending(field.as_ref());
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub(crate) fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.intent.push_order_descending(field.as_ref());
        self
    }

    /// Set a fully-specified order spec (validated before reaching this boundary).
    pub(crate) fn order_spec(mut self, order: OrderSpec) -> Self {
        self.intent.set_order_spec(order);
        self
    }

    /// Enable DISTINCT semantics for this query intent.
    #[must_use]
    pub(crate) const fn distinct(mut self) -> Self {
        self.intent.set_distinct();
        self
    }

    /// Select one explicit scalar field projection list.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(crate) fn select_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let fields = fields
            .into_iter()
            .map(|field| FieldId::new(field.into()))
            .collect::<Vec<_>>();
        self.intent
            .set_projection_selection(ProjectionSelection::Fields(fields));

        self
    }

    /// Override scalar projection selection with one already-lowered planner contract.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db::query::intent) fn projection_selection(
        mut self,
        selection: ProjectionSelection,
    ) -> Self {
        self.intent.set_projection_selection(selection);
        self
    }

    // Resolve one grouped field into one stable field slot and append it to the
    // grouped spec in declaration order.
    pub(in crate::db::query::intent) fn push_group_field(
        mut self,
        field: &str,
    ) -> Result<Self, QueryError> {
        let field_slot = resolve_group_field_slot(self.model, field).map_err(QueryError::from)?;
        self.intent.push_group_field_slot(field_slot);

        Ok(self)
    }

    // Append one grouped aggregate terminal to the grouped declarative spec.
    pub(in crate::db::query::intent) fn push_group_aggregate(
        mut self,
        aggregate: AggregateExpr,
    ) -> Self {
        self.intent
            .push_group_aggregate(GroupAggregateSpec::from_aggregate_expr(&aggregate));

        self
    }

    // Override grouped hard limits for this grouped query.
    pub(in crate::db::query::intent) fn grouped_limits(
        mut self,
        max_groups: u64,
        max_group_bytes: u64,
    ) -> Self {
        self.intent.set_grouped_limits(max_groups, max_group_bytes);

        self
    }

    // Append one grouped HAVING compare clause after GROUP BY terminal declaration.
    fn push_having_clause(mut self, clause: GroupHavingClause) -> Result<Self, QueryError> {
        self.intent
            .push_having_clause(clause)
            .map_err(QueryError::intent)?;

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
        let value = canonicalize_group_field_numeric_value_for_kind(self.model, field, &value)
            .unwrap_or(value);

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

    /// Set the access path to a single primary key lookup.
    pub(crate) fn by_id(mut self, id: K) -> Self {
        self.intent.set_by_id(id);
        self
    }

    /// Set the access path to a primary key batch lookup.
    pub(crate) fn by_ids<I>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = K>,
    {
        self.intent.set_by_ids(ids);
        self
    }

    /// Set the access path to the singleton primary key.
    pub(crate) fn only(mut self, id: K) -> Self {
        self.intent.set_only(id);
        self
    }

    /// Mark this intent as a delete query.
    #[must_use]
    pub(crate) fn delete(mut self) -> Self {
        self.intent = self.intent.set_delete_mode();
        self
    }

    /// Apply a limit to the current mode.
    ///
    /// Load limits bound result size; delete limits bound mutation size.
    #[must_use]
    pub(crate) fn limit(mut self, limit: u32) -> Self {
        self.intent = self.intent.apply_limit(limit);
        self
    }

    /// Apply an offset to the current mode.
    ///
    /// Load mode uses this as a pagination offset. Delete mode uses this as an
    /// ordered delete window offset.
    #[must_use]
    pub(crate) fn offset(mut self, offset: u32) -> Self {
        self.intent = self.intent.apply_offset(offset);
        self
    }

    /// Build a model-level logical plan using Value-based access keys.
    #[inline(never)]
    pub(in crate::db::query::intent) fn build_plan_model(
        &self,
    ) -> Result<AccessPlannedQuery, QueryError> {
        self.build_plan_model_with_indexes(&VisibleIndexes::schema_owned(self.model.indexes()))
    }

    /// Build a model-level logical plan using one explicit planner-visible
    /// secondary-index set.
    #[inline(never)]
    pub(in crate::db::query::intent) fn build_plan_model_with_indexes(
        &self,
        visible_indexes: &VisibleIndexes<'_>,
    ) -> Result<AccessPlannedQuery, QueryError> {
        // Phase 1: schema surface and intent validation.
        let schema_info = SchemaInfo::cached_for_entity_model(self.model);
        self.intent.validate_policy_shape()?;

        // Phase 2: normalize scalar predicate and fold constant predicates
        // before access planning.
        let access_inputs = self.intent.planning_access_inputs();
        let normalized_predicate = fold_constant_predicate(normalize_query_predicate(
            schema_info,
            access_inputs.predicate(),
        )?);
        let plan_mode = self.intent.mode();
        let limit_zero_window = is_limit_zero_load_window(plan_mode);
        let constant_false_predicate = predicate_is_constant_false(normalized_predicate.as_ref());
        let access_plan_value = if limit_zero_window || constant_false_predicate {
            AccessPlan::by_keys(Vec::new())
        } else {
            plan_query_access(
                self.model,
                visible_indexes.as_slice(),
                schema_info,
                normalized_predicate.as_ref(),
                access_inputs.order(),
                access_inputs.into_key_access_override(),
            )?
        };
        let normalized_predicate = strip_redundant_primary_key_predicate_for_exact_access(
            self.model,
            &access_plan_value,
            normalized_predicate,
        );

        // Phase 3: assemble logical plan from normalized scalar/grouped intent.
        let logical_inputs = self.intent.planning_logical_inputs();
        let logical_query = logical_query_from_logical_inputs(
            logical_inputs,
            normalized_predicate,
            self.consistency,
        );
        let logical = build_logical_plan(self.model, logical_query);
        let mut plan = AccessPlannedQuery::from_parts_with_projection(
            logical,
            access_plan_value,
            self.intent.scalar().projection_selection.clone(),
        );
        simplify_limit_one_page_for_by_key_access(&mut plan);

        // Phase 4: freeze the planner-owned route profile before validation so
        // policy gates that depend on finalized access/order contracts, such as
        // expression ORDER BY support, see the accepted route semantics.
        plan.finalize_planner_route_profile_for_model(self.model);

        // Phase 5: validate the assembled plan against schema, access-shape,
        // and planner-policy contracts before projecting explain metadata.
        if plan.grouped_plan().is_some() {
            validate_group_query_semantics(schema_info, self.model, &plan)?;
        } else {
            validate_query_semantics(schema_info, self.model, &plan)?;
        }

        // Phase 6: freeze planner-owned execution metadata only after semantic
        // validation succeeds so user-facing projection/order errors remain
        // planner-domain failures instead of executor invariant violations.
        plan.finalize_static_planning_shape_for_model(self.model)
            .map_err(QueryError::execute)?;

        // Phase 7: freeze the access-choice explain snapshot after validation
        // so downstream execution and explain surfaces reuse the exact planner
        // winner metadata for the accepted plan.
        let access_choice = project_access_choice_explain_snapshot_with_indexes(
            self.model,
            visible_indexes.as_slice(),
            &plan,
        );
        plan.set_access_choice(access_choice);

        Ok(plan)
    }
}

fn hash_query_mode(hasher: &mut sha2::Sha256, mode: QueryMode) {
    match mode {
        QueryMode::Load(spec) => {
            write_tag(hasher, QUERY_MODE_LOAD_TAG);
            hash_query_optional_u32(hasher, spec.limit());
            write_u32(hasher, spec.offset());
        }
        QueryMode::Delete(spec) => {
            write_tag(hasher, QUERY_MODE_DELETE_TAG);
            hash_query_optional_u32(hasher, spec.limit());
            write_u32(hasher, spec.offset());
        }
    }
}

fn hash_query_predicate(hasher: &mut sha2::Sha256, predicate: Option<&Predicate>) {
    let Some(predicate) = predicate else {
        write_tag(hasher, PREDICATE_ABSENT_TAG);
        return;
    };

    write_tag(hasher, PREDICATE_PRESENT_TAG);
    hash_model_predicate(hasher, predicate);
}

fn hash_query_key_access(
    hasher: &mut sha2::Sha256,
    key_access_override: Option<&AccessPlan<Value>>,
) {
    let Some(key_access_override) = key_access_override else {
        write_tag(hasher, KEY_ACCESS_ABSENT_TAG);
        return;
    };

    write_tag(hasher, KEY_ACCESS_PRESENT_TAG);
    hash_access_plan(hasher, key_access_override);
}

fn hash_query_order(hasher: &mut sha2::Sha256, order: Option<&OrderSpec>) {
    let Some(order) = order else {
        write_tag(hasher, ORDER_NONE_TAG);
        return;
    };
    if order.fields.is_empty() {
        write_tag(hasher, ORDER_NONE_TAG);
        return;
    }

    write_tag(hasher, ORDER_FIELDS_TAG);
    write_u32(
        hasher,
        u32::try_from(order.fields.len()).unwrap_or(u32::MAX),
    );
    for (field, direction) in &order.fields {
        write_str(hasher, field);
        write_tag(hasher, order_direction_tag(*direction));
    }
}

fn hash_query_distinct(hasher: &mut sha2::Sha256, distinct: bool) {
    if distinct {
        write_tag(hasher, DISTINCT_ENABLED_TAG);
    } else {
        write_tag(hasher, DISTINCT_DISABLED_TAG);
    }
}

fn hash_query_projection_selection(hasher: &mut sha2::Sha256, projection: &ProjectionSelection) {
    match projection {
        ProjectionSelection::All => write_tag(hasher, PROJECTION_ALL_TAG),
        ProjectionSelection::Fields(fields) => {
            write_tag(hasher, PROJECTION_FIELDS_TAG);
            write_u32(hasher, u32::try_from(fields.len()).unwrap_or(u32::MAX));
            for field in fields {
                write_str(hasher, field.as_str());
            }
        }
        ProjectionSelection::Exprs(fields) => {
            write_tag(hasher, PROJECTION_EXPRS_TAG);
            hash_projection_field_selection_fingerprint(hasher, fields);
        }
    }
}

fn hash_query_grouping<K>(hasher: &mut sha2::Sha256, grouped: Option<&GroupedIntent<K>>) {
    let Some(grouped) = grouped else {
        write_tag(hasher, GROUP_NONE_TAG);
        return;
    };

    write_tag(hasher, GROUP_PRESENT_TAG);
    write_u32(
        hasher,
        u32::try_from(grouped.group.group_fields.len()).unwrap_or(u32::MAX),
    );
    for field in &grouped.group.group_fields {
        write_u32(hasher, u32::try_from(field.index).unwrap_or(u32::MAX));
        write_str(hasher, field.field.as_str());
    }

    write_u32(
        hasher,
        u32::try_from(grouped.group.aggregates.len()).unwrap_or(u32::MAX),
    );
    for aggregate in &grouped.group.aggregates {
        hash_group_aggregate_structural_fingerprint(
            hasher,
            &AggregateHashShape::semantic(
                aggregate.kind,
                aggregate.target_field.as_deref(),
                aggregate.distinct,
            ),
        );
    }

    hash_query_group_having(hasher, grouped.having.as_ref());
    write_hash_u64(hasher, grouped.group.execution.max_groups);
    write_hash_u64(hasher, grouped.group.execution.max_group_bytes);
}

fn hash_query_group_having(
    hasher: &mut sha2::Sha256,
    having: Option<&crate::db::query::plan::GroupHavingSpec>,
) {
    let Some(having) = having else {
        write_tag(hasher, GROUP_HAVING_ABSENT_TAG);
        return;
    };

    write_tag(hasher, GROUP_HAVING_PRESENT_TAG);
    write_u32(
        hasher,
        u32::try_from(having.clauses.len()).unwrap_or(u32::MAX),
    );
    for clause in &having.clauses {
        match &clause.symbol {
            GroupHavingSymbol::GroupField(field) => {
                write_tag(hasher, GROUP_HAVING_GROUP_FIELD_TAG);
                write_u32(hasher, u32::try_from(field.index).unwrap_or(u32::MAX));
                write_str(hasher, field.field.as_str());
            }
            GroupHavingSymbol::AggregateIndex(index) => {
                write_tag(hasher, GROUP_HAVING_AGGREGATE_INDEX_TAG);
                write_u32(hasher, u32::try_from(*index).unwrap_or(u32::MAX));
            }
        }
        write_tag(hasher, clause.op.tag());
        write_value(hasher, &clause.value);
    }
}

fn hash_query_consistency(hasher: &mut sha2::Sha256, consistency: MissingRowPolicy) {
    match consistency {
        MissingRowPolicy::Ignore => write_tag(hasher, CONSISTENCY_IGNORE_TAG),
        MissingRowPolicy::Error => write_tag(hasher, CONSISTENCY_ERROR_TAG),
    }
}

fn hash_query_optional_u32(hasher: &mut sha2::Sha256, value: Option<u32>) {
    match value {
        Some(value) => {
            write_tag(hasher, 1);
            write_u32(hasher, value);
        }
        None => write_tag(hasher, 0),
    }
}

const fn order_direction_tag(direction: OrderDirection) -> u8 {
    match direction {
        OrderDirection::Asc => ORDER_DIRECTION_ASC_TAG,
        OrderDirection::Desc => ORDER_DIRECTION_DESC_TAG,
    }
}

// Keep grouped fluent HAVING literals aligned with the model-owned field kind
// when the conversion is lossless and unambiguous. This preserves plan parity
// with the SQL lowering boundary for grouped key clauses without widening the
// rule to unrelated aggregate symbols or non-numeric values.
fn canonicalize_group_field_numeric_value_for_kind(
    model: &EntityModel,
    field: &str,
    value: &Value,
) -> Option<Value> {
    let field_kind = model
        .fields()
        .iter()
        .find(|candidate| candidate.name() == field)
        .map(crate::model::field::FieldModel::kind)?;

    canonicalize_group_field_numeric_value(field_kind, value)
}

// Only the narrow Int<->Uint lossless cases are canonicalized here. Grouped
// fluent clauses already carry runtime `Value`s, so this helper only removes
// the remaining representation drift for numeric key fields that SQL literals
// already normalize before grouped HAVING binding.
fn canonicalize_group_field_numeric_value(field_kind: FieldKind, value: &Value) -> Option<Value> {
    match field_kind {
        FieldKind::Relation { key_kind, .. } => {
            canonicalize_group_field_numeric_value(*key_kind, value)
        }
        FieldKind::Int => match value {
            Value::Int(inner) => Some(Value::Int(*inner)),
            Value::Uint(inner) => i64::try_from(*inner).ok().map(Value::Int),
            _ => None,
        },
        FieldKind::Uint => match value {
            Value::Int(inner) => u64::try_from(*inner).ok().map(Value::Uint),
            Value::Uint(inner) => Some(Value::Uint(*inner)),
            _ => None,
        },
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Enum { .. }
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Principal
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit => None,
    }
}

// Drop one normalized primary-key predicate when access planning already
// resolved the exact same authoritative PK access path. This prevents duplicate
// predicate evaluation and unlocks downstream PK fast paths.
fn strip_redundant_primary_key_predicate_for_exact_access(
    model: &EntityModel,
    access: &AccessPlan<Value>,
    normalized_predicate: Option<Predicate>,
) -> Option<Predicate> {
    let predicate = normalized_predicate?;

    if let Some(access_keys) = access.as_path().and_then(|path| path.as_by_keys())
        && !access_keys.is_empty()
        && predicate_matches_primary_key_in_set(&predicate, model.primary_key.name, access_keys)
    {
        return None;
    }

    if let Some(access_key) = access.as_path().and_then(|path| path.as_by_key()) {
        let Predicate::Compare(cmp) = &predicate else {
            return Some(predicate);
        };
        if cmp.field != model.primary_key.name || cmp.op != CompareOp::Eq {
            return Some(predicate);
        }
        if cmp.value != *access_key {
            return Some(predicate);
        }

        return None;
    }

    if let Some((start, end)) = access.as_primary_key_range_path()
        && predicate_matches_primary_key_half_open_range(
            &predicate,
            model.primary_key.name,
            start,
            end,
        )
    {
        return None;
    }

    Some(predicate)
}

// Return whether one normalized predicate is exactly the same primary-key IN
// set already guaranteed by one canonical `ByKeys` access path.
fn predicate_matches_primary_key_in_set(
    predicate: &Predicate,
    primary_key_name: &str,
    access_keys: &[Value],
) -> bool {
    let Predicate::Compare(cmp) = predicate else {
        return false;
    };
    if cmp.field != primary_key_name || cmp.op != CompareOp::In {
        return false;
    }

    let Value::List(predicate_keys) = &cmp.value else {
        return false;
    };

    let mut canonical_predicate_keys = predicate_keys.clone();
    canonicalize_value_set(&mut canonical_predicate_keys);

    canonical_predicate_keys == access_keys
}

// Return whether one normalized predicate is exactly the same half-open
// primary-key range already guaranteed by one `KeyRange` access path.
fn predicate_matches_primary_key_half_open_range(
    predicate: &Predicate,
    primary_key_name: &str,
    start: &Value,
    end: &Value,
) -> bool {
    let Predicate::And(children) = predicate else {
        return false;
    };
    if children.len() != 2 {
        return false;
    }

    let mut lower_matches = false;
    let mut upper_matches = false;
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return false;
        };
        if cmp.field != primary_key_name {
            return false;
        }

        match cmp.op {
            CompareOp::Gte if cmp.value == *start => lower_matches = true,
            CompareOp::Lt if cmp.value == *end => upper_matches = true,
            _ => return false,
        }
    }

    lower_matches && upper_matches
}

// Collapse `LIMIT 1` pagination overhead when access is already one exact
// primary-key lookup and no offset is requested.
fn simplify_limit_one_page_for_by_key_access(plan: &mut AccessPlannedQuery) {
    if !plan
        .access
        .as_path()
        .is_some_and(|path: &crate::db::access::AccessPath<Value>| path.is_by_key())
    {
        return;
    }

    let scalar = match &mut plan.logical {
        LogicalPlan::Scalar(scalar) => scalar,
        LogicalPlan::Grouped(grouped) => &mut grouped.scalar,
    };
    let Some(page) = scalar.page.as_ref() else {
        return;
    };
    if page.offset != 0 || page.limit != Some(1) {
        return;
    }

    scalar.page = None;
}
