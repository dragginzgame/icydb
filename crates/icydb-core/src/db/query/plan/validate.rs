//! Query-plan validation at logical and executor boundaries.
//!
//! Validation ownership contract:
//! - `validate_logical_plan_model` owns user-facing query semantics and emits `PlanError`.
//! - `validate_executor_plan` is defensive: it re-checks owned semantics/invariants before
//!   execution and must not introduce new user-visible semantics.
//!
//! Future rule changes must declare a semantic owner. Defensive re-check layers may mirror
//! rules, but must not reinterpret semantics or error class intent.
use super::{AccessPath, AccessPlan, LogicalPlan, OrderSpec};
use crate::{
    db::query::predicate::{self, SchemaInfo, coercion::canonical_cmp},
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::entity::EntityModel,
    model::index::IndexModel,
    traits::{EntityKind, FieldValue},
    value::Value,
};
use thiserror::Error as ThisError;

///
/// PlanError
///
/// Executor-visible validation failures for logical plans.
///
/// These errors indicate that a plan cannot be safely executed against the
/// current schema or entity definition. They are *not* planner bugs.
///

#[derive(Debug, ThisError)]
pub enum PlanError {
    #[error("predicate validation failed: {0}")]
    PredicateInvalid(#[from] predicate::ValidateError),

    /// ORDER BY references an unknown field.
    #[error("unknown order field '{field}'")]
    UnknownOrderField { field: String },

    /// ORDER BY references a field that cannot be ordered.
    #[error("order field '{field}' is not orderable")]
    UnorderableField { field: String },

    /// Access plan references an index not declared on the entity.
    #[error("index '{index}' not found on entity")]
    IndexNotFound { index: IndexModel },

    /// Index prefix exceeds the number of indexed fields.
    #[error("index prefix length {prefix_len} exceeds index field count {field_len}")]
    IndexPrefixTooLong { prefix_len: usize, field_len: usize },

    /// Index prefix must include at least one value.
    #[error("index prefix must include at least one value")]
    IndexPrefixEmpty,

    /// Index prefix literal does not match indexed field type.
    #[error("index prefix value for field '{field}' is incompatible")]
    IndexPrefixValueMismatch { field: String },

    /// Primary key field exists but is not key-compatible.
    #[error("primary key field '{field}' is not key-compatible")]
    PrimaryKeyNotKeyable { field: String },

    /// Supplied key does not match the primary key type.
    #[error("key '{key:?}' is incompatible with primary key '{field}'")]
    PrimaryKeyMismatch { field: String, key: Value },

    /// Key range has invalid ordering.
    #[error("key range start is greater than end")]
    InvalidKeyRange,

    /// ORDER BY must specify at least one field.
    #[error("order specification must include at least one field")]
    EmptyOrderSpec,

    /// Ordered plans must terminate with the primary-key tie-break.
    #[error("order specification must end with primary key '{field}' as deterministic tie-break")]
    MissingPrimaryKeyTieBreak { field: String },

    /// Delete plans must not carry pagination.
    #[error("delete plans must not include pagination")]
    DeletePlanWithPagination,

    /// Load plans must not carry delete limits.
    #[error("load plans must not include delete limits")]
    LoadPlanWithDeleteLimit,

    /// Delete limits require an explicit ordering.
    #[error("delete limit requires an explicit ordering")]
    DeleteLimitRequiresOrder,

    /// Pagination requires an explicit ordering.
    #[error(
        "Unordered pagination is not allowed.\nThis query uses LIMIT or OFFSET without an ORDER BY clause.\nPagination without a total ordering is non-deterministic.\nAdd an explicit order_by(...) to make the query stable."
    )]
    UnorderedPagination,

    /// Cursor continuation requires an explicit ordering.
    #[error("cursor pagination requires an explicit ordering")]
    CursorRequiresOrder,

    /// Cursor token could not be decoded.
    #[error("invalid continuation cursor: {reason}")]
    InvalidContinuationCursor { reason: String },

    /// Cursor token version is unsupported.
    #[error("unsupported continuation cursor version: {version}")]
    ContinuationCursorVersionMismatch { version: u8 },

    /// Cursor token does not belong to this canonical query shape.
    #[error(
        "continuation cursor does not match query plan signature for '{entity_path}': expected={expected}, actual={actual}"
    )]
    ContinuationCursorSignatureMismatch {
        entity_path: &'static str,
        expected: String,
        actual: String,
    },

    /// Cursor boundary width does not match canonical order width.
    #[error("continuation cursor boundary arity mismatch: expected {expected}, found {found}")]
    ContinuationCursorBoundaryArityMismatch { expected: usize, found: usize },

    /// Cursor boundary value type mismatch for a non-primary-key ordered field.
    #[error(
        "continuation cursor boundary type mismatch for field '{field}': expected {expected}, found {value:?}"
    )]
    ContinuationCursorBoundaryTypeMismatch {
        field: String,
        expected: String,
        value: Value,
    },

    /// Cursor primary-key boundary does not match the entity key type.
    #[error(
        "continuation cursor primary key type mismatch for '{field}': expected {expected}, found {value:?}"
    )]
    ContinuationCursorPrimaryKeyTypeMismatch {
        field: String,
        expected: String,
        value: Option<Value>,
    },
}

/// Validate a logical plan with model-level key values.
///
/// Ownership:
/// - semantic owner for user-facing query validity at planning boundaries
/// - failures here are user-visible planning failures (`PlanError`)
///
/// New user-facing validation rules must be introduced here first, then mirrored
/// defensively in downstream layers without changing semantics.
pub(crate) fn validate_logical_plan_model(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &LogicalPlan<Value>,
) -> Result<(), PlanError> {
    if let Some(predicate) = &plan.predicate {
        predicate::validate(schema, predicate)?;
    }

    if let Some(order) = &plan.order {
        validate_order(schema, order)?;
        validate_primary_key_tie_break(model, order)?;
    }

    validate_access_plan_model(schema, model, &plan.access)?;
    validate_plan_semantics(plan)?;

    Ok(())
}

/// Validate plan-level invariants not covered by schema checks.
fn validate_plan_semantics<K>(plan: &LogicalPlan<K>) -> Result<(), PlanError> {
    if let Some(order) = &plan.order
        && order.fields.is_empty()
    {
        return Err(PlanError::EmptyOrderSpec);
    }

    if plan.mode.is_delete() {
        if plan.page.is_some() {
            return Err(PlanError::DeletePlanWithPagination);
        }

        if plan.delete_limit.is_some()
            && plan
                .order
                .as_ref()
                .is_none_or(|order| order.fields.is_empty())
        {
            return Err(PlanError::DeleteLimitRequiresOrder);
        }
    }

    if plan.mode.is_load() {
        if plan.delete_limit.is_some() {
            return Err(PlanError::LoadPlanWithDeleteLimit);
        }

        if plan.page.is_some()
            && plan
                .order
                .as_ref()
                .is_none_or(|order| order.fields.is_empty())
        {
            return Err(PlanError::UnorderedPagination);
        }
    }

    Ok(())
}

/// Validate plans at executor boundaries and surface invariant violations.
///
/// Ownership:
/// - defensive execution-boundary guardrail, not a semantic owner
/// - must map violations to internal invariant failures, never new user semantics
///
/// Any disagreement with logical validation indicates an internal bug and is not
/// a recoverable user-input condition.
pub(crate) fn validate_executor_plan<E: EntityKind>(
    plan: &LogicalPlan<E::Key>,
) -> Result<(), InternalError> {
    let schema = SchemaInfo::from_entity_model(E::MODEL).map_err(|err| {
        InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            format!("entity schema invalid for {}: {err}", E::PATH),
        )
    })?;

    if let Some(predicate) = &plan.predicate {
        predicate::validate(&schema, predicate).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                err.to_string(),
            )
        })?;
    }

    if let Some(order) = &plan.order {
        validate_executor_order(&schema, order).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                err.to_string(),
            )
        })?;
        validate_primary_key_tie_break(E::MODEL, order).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                err.to_string(),
            )
        })?;
    }

    validate_access_plan(&schema, E::MODEL, &plan.access).map_err(|err| {
        InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            err.to_string(),
        )
    })?;

    validate_plan_semantics(plan).map_err(|err| {
        InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            err.to_string(),
        )
    })?;

    Ok(())
}

/// Validate ORDER BY fields against the schema.
pub(crate) fn validate_order(schema: &SchemaInfo, order: &OrderSpec) -> Result<(), PlanError> {
    for (field, _) in &order.fields {
        let field_type = schema
            .field(field)
            .ok_or_else(|| PlanError::UnknownOrderField {
                field: field.clone(),
            })?;

        if !field_type.is_orderable() {
            // CONTRACT: ORDER BY rejects non-queryable or unordered fields.
            return Err(PlanError::UnorderableField {
                field: field.clone(),
            });
        }
    }

    Ok(())
}

/// Validate ORDER BY fields for executor-only plans.
///
/// CONTRACT: executor ordering validation matches planner rules.
fn validate_executor_order(schema: &SchemaInfo, order: &OrderSpec) -> Result<(), PlanError> {
    validate_order(schema, order)
}

// Ordered plans must include exactly one terminal primary-key field so ordering is total and
// deterministic across explain, fingerprint, and executor comparison paths.
fn validate_primary_key_tie_break(model: &EntityModel, order: &OrderSpec) -> Result<(), PlanError> {
    if order.fields.is_empty() {
        return Ok(());
    }

    let pk_field = model.primary_key.name;
    let pk_count = order
        .fields
        .iter()
        .filter(|(field, _)| field == pk_field)
        .count();
    let trailing_pk = order
        .fields
        .last()
        .is_some_and(|(field, _)| field == pk_field);

    if pk_count == 1 && trailing_pk {
        Ok(())
    } else {
        Err(PlanError::MissingPrimaryKeyTieBreak {
            field: pk_field.to_string(),
        })
    }
}

///
/// AccessPlanKeyAdapter
/// Adapter for key validation and ordering across access-plan representations.
///

trait AccessPlanKeyAdapter<K> {
    /// Validate a key against the entity's primary key type.
    fn validate_pk_key(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        key: &K,
    ) -> Result<(), PlanError>;

    /// Validate a key range and enforce representation-specific ordering rules.
    fn validate_key_range(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        start: &K,
        end: &K,
    ) -> Result<(), PlanError>;
}

///
/// GenericKeyAdapater
/// Adapter for typed key plans (FieldValue + Ord)
///

struct GenericKeyAdapter;

impl<K> AccessPlanKeyAdapter<K> for GenericKeyAdapter
where
    K: FieldValue + Ord,
{
    fn validate_pk_key(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        key: &K,
    ) -> Result<(), PlanError> {
        validate_pk_key(schema, model, key)
    }

    fn validate_key_range(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        start: &K,
        end: &K,
    ) -> Result<(), PlanError> {
        validate_pk_key(schema, model, start)?;
        validate_pk_key(schema, model, end)?;
        if start > end {
            return Err(PlanError::InvalidKeyRange);
        }

        Ok(())
    }
}

///
/// ValueKeyAdapter
/// Adapter for model-level Value plans (partial ordering).
///

struct ValueKeyAdapter;

impl AccessPlanKeyAdapter<Value> for ValueKeyAdapter {
    fn validate_pk_key(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        key: &Value,
    ) -> Result<(), PlanError> {
        validate_pk_value(schema, model, key)
    }

    fn validate_key_range(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        start: &Value,
        end: &Value,
    ) -> Result<(), PlanError> {
        validate_pk_value(schema, model, start)?;
        validate_pk_value(schema, model, end)?;
        let ordering = canonical_cmp(start, end);
        if ordering == std::cmp::Ordering::Greater {
            return Err(PlanError::InvalidKeyRange);
        }

        Ok(())
    }
}

// Validate access plans by delegating key checks to the adapter.
fn validate_access_plan_with<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<K>,
    adapter: &impl AccessPlanKeyAdapter<K>,
) -> Result<(), PlanError> {
    match access {
        AccessPlan::Path(path) => validate_access_path_with(schema, model, path, adapter),
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            for child in children {
                validate_access_plan_with(schema, model, child, adapter)?;
            }

            Ok(())
        }
    }
}

// Validate access paths using representation-specific key semantics.
fn validate_access_path_with<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPath<K>,
    adapter: &impl AccessPlanKeyAdapter<K>,
) -> Result<(), PlanError> {
    match access {
        AccessPath::ByKey(key) => adapter.validate_pk_key(schema, model, key),
        AccessPath::ByKeys(keys) => {
            // Empty key lists are a valid no-op.
            if keys.is_empty() {
                return Ok(());
            }
            for key in keys {
                adapter.validate_pk_key(schema, model, key)?;
            }

            Ok(())
        }
        AccessPath::KeyRange { start, end } => {
            adapter.validate_key_range(schema, model, start, end)
        }
        AccessPath::IndexPrefix { index, values } => {
            validate_index_prefix(schema, model, index, values)
        }
        AccessPath::FullScan => Ok(()),
    }
}

/// Validate executor-visible access paths.
///
/// This ensures keys, ranges, and index prefixes are schema-compatible.
pub(crate) fn validate_access_plan<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<K>,
) -> Result<(), PlanError>
where
    K: FieldValue + Ord,
{
    validate_access_plan_with(schema, model, access, &GenericKeyAdapter)
}

/// Validate access paths that carry model-level key values.
pub(crate) fn validate_access_plan_model(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<Value>,
) -> Result<(), PlanError> {
    validate_access_plan_with(schema, model, access, &ValueKeyAdapter)
}

/// Validate that a key matches the entity's primary key type.
fn validate_pk_key<K>(schema: &SchemaInfo, model: &EntityModel, key: &K) -> Result<(), PlanError>
where
    K: FieldValue,
{
    let field = model.primary_key.name;

    let field_type = schema
        .field(field)
        .ok_or_else(|| PlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        })?;

    if !field_type.is_keyable() {
        return Err(PlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        });
    }

    let value = key.to_value();
    if !predicate::validate::literal_matches_type(&value, field_type) {
        return Err(PlanError::PrimaryKeyMismatch {
            field: field.to_string(),
            key: value,
        });
    }

    Ok(())
}

// Validate that a model-level key value matches the entity's primary key type.
fn validate_pk_value(
    schema: &SchemaInfo,
    model: &EntityModel,
    key: &Value,
) -> Result<(), PlanError> {
    let field = model.primary_key.name;

    let field_type = schema
        .field(field)
        .ok_or_else(|| PlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        })?;

    if !field_type.is_keyable() {
        return Err(PlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        });
    }

    if !predicate::validate::literal_matches_type(key, field_type) {
        return Err(PlanError::PrimaryKeyMismatch {
            field: field.to_string(),
            key: key.clone(),
        });
    }

    Ok(())
}

/// Validate that an index prefix is valid for execution.
fn validate_index_prefix(
    schema: &SchemaInfo,
    model: &EntityModel,
    index: &IndexModel,
    values: &[Value],
) -> Result<(), PlanError> {
    if !model.indexes.contains(&index) {
        return Err(PlanError::IndexNotFound { index: *index });
    }

    if values.is_empty() {
        return Err(PlanError::IndexPrefixEmpty);
    }

    if values.len() > index.fields.len() {
        return Err(PlanError::IndexPrefixTooLong {
            prefix_len: values.len(),
            field_len: index.fields.len(),
        });
    }

    for (field, value) in index.fields.iter().zip(values.iter()) {
        let field_type =
            schema
                .field(field)
                .ok_or_else(|| PlanError::IndexPrefixValueMismatch {
                    field: field.to_string(),
                })?;

        if !predicate::validate::literal_matches_type(value, field_type) {
            return Err(PlanError::IndexPrefixValueMismatch {
                field: field.to_string(),
            });
        }
    }

    Ok(())
}

/// Map scalar field types to compatible key variants.
///
/// Non-scalar and non-queryable field types are intentionally excluded.
///
/// TESTS
///

#[cfg(test)]
mod tests {
    // NOTE: Invalid helpers remain only for intentionally invalid schemas.
    use super::{PlanError, validate_logical_plan_model};
    use crate::{
        db::query::{
            plan::{AccessPath, AccessPlan, LogicalPlan, OrderDirection, OrderSpec, PageSpec},
            predicate::{SchemaInfo, ValidateError},
        },
        model::{
            entity::EntityModel,
            field::{EntityFieldKind, EntityFieldModel},
            index::IndexModel,
        },
        test_fixtures::InvalidEntityModelBuilder,
        traits::EntitySchema,
        types::Ulid,
        value::Value,
    };

    fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
        EntityFieldModel { name, kind }
    }

    const INDEX_FIELDS: [&str; 1] = ["tag"];
    const INDEX_MODEL: IndexModel =
        IndexModel::new("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);

    crate::test_entity_schema! {
        PlanValidateIndexedEntity,
        id = Ulid,
        path = "plan_validate::IndexedEntity",
        entity_name = "IndexedEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", EntityFieldKind::Ulid),
            ("tag", EntityFieldKind::Text),
        ],
        indexes = [&INDEX_MODEL],
    }

    crate::test_entity_schema! {
        PlanValidateListEntity,
        id = Ulid,
        path = "plan_validate::ListEntity",
        entity_name = "ListEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", EntityFieldKind::Ulid),
            ("tags", EntityFieldKind::List(&EntityFieldKind::Text)),
        ],
        indexes = [],
    }

    // Helper for tests that need the indexed model derived from a typed schema.
    fn model_with_index() -> &'static EntityModel {
        <PlanValidateIndexedEntity as EntitySchema>::MODEL
    }

    #[test]
    fn model_rejects_missing_primary_key() {
        // Invalid test scaffolding: models are hand-built to exercise
        // validation failures that helpers intentionally prevent.
        let fields: &'static [EntityFieldModel] =
            Box::leak(vec![field("id", EntityFieldKind::Ulid)].into_boxed_slice());
        let missing_pk = Box::leak(Box::new(field("missing", EntityFieldKind::Ulid)));

        let model = InvalidEntityModelBuilder::from_static(
            "test::Entity",
            "TestEntity",
            missing_pk,
            fields,
            &[],
        );

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::InvalidPrimaryKey { .. })
        ));
    }

    #[test]
    fn model_rejects_duplicate_fields() {
        let model = InvalidEntityModelBuilder::from_fields(
            vec![
                field("dup", EntityFieldKind::Text),
                field("dup", EntityFieldKind::Text),
            ],
            0,
        );

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::DuplicateField { .. })
        ));
    }

    #[test]
    fn model_rejects_invalid_primary_key_type() {
        let model = InvalidEntityModelBuilder::from_fields(
            vec![field("pk", EntityFieldKind::List(&EntityFieldKind::Text))],
            0,
        );

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::InvalidPrimaryKeyType { .. })
        ));
    }

    #[test]
    fn model_rejects_index_unknown_field() {
        const INDEX_FIELDS: [&str; 1] = ["missing"];
        const INDEX_MODEL: IndexModel = IndexModel::new(
            "test::idx_missing",
            "test::IndexStore",
            &INDEX_FIELDS,
            false,
        );
        const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

        let fields: &'static [EntityFieldModel] =
            Box::leak(vec![field("id", EntityFieldKind::Ulid)].into_boxed_slice());
        let model = InvalidEntityModelBuilder::from_static(
            "test::Entity",
            "TestEntity",
            &fields[0],
            fields,
            &INDEXES,
        );

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::IndexFieldUnknown { .. })
        ));
    }

    #[test]
    fn model_rejects_index_non_queryable_field() {
        const INDEX_FIELDS: [&str; 1] = ["broken"];
        const INDEX_MODEL: IndexModel =
            IndexModel::new("test::idx_broken", "test::IndexStore", &INDEX_FIELDS, false);
        const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

        let fields: &'static [EntityFieldModel] = Box::leak(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("broken", EntityFieldKind::Structured { queryable: false }),
            ]
            .into_boxed_slice(),
        );
        let model = InvalidEntityModelBuilder::from_static(
            "test::Entity",
            "TestEntity",
            &fields[0],
            fields,
            &INDEXES,
        );

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::IndexFieldNotQueryable { .. })
        ));
    }

    #[test]
    fn model_rejects_index_map_field_in_0_7_x() {
        const INDEX_FIELDS: [&str; 1] = ["attributes"];
        const INDEX_MODEL: IndexModel = IndexModel::new(
            "test::idx_attributes",
            "test::IndexStore",
            &INDEX_FIELDS,
            false,
        );
        const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

        let fields: &'static [EntityFieldModel] = Box::leak(
            vec![
                field("id", EntityFieldKind::Ulid),
                field(
                    "attributes",
                    EntityFieldKind::Map {
                        key: &EntityFieldKind::Text,
                        value: &EntityFieldKind::Uint,
                    },
                ),
            ]
            .into_boxed_slice(),
        );
        let model = InvalidEntityModelBuilder::from_static(
            "test::Entity",
            "TestEntity",
            &fields[0],
            fields,
            &INDEXES,
        );

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::IndexFieldMapNotQueryable { .. })
        ));
    }

    #[test]
    fn model_rejects_duplicate_index_names() {
        const INDEX_FIELDS_A: [&str; 1] = ["id"];
        const INDEX_FIELDS_B: [&str; 1] = ["other"];
        const INDEX_A: IndexModel = IndexModel::new(
            "test::dup_index",
            "test::IndexStore",
            &INDEX_FIELDS_A,
            false,
        );
        const INDEX_B: IndexModel = IndexModel::new(
            "test::dup_index",
            "test::IndexStore",
            &INDEX_FIELDS_B,
            false,
        );
        const INDEXES: [&IndexModel; 2] = [&INDEX_A, &INDEX_B];

        let fields: &'static [EntityFieldModel] = Box::leak(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("other", EntityFieldKind::Text),
            ]
            .into_boxed_slice(),
        );
        let model = InvalidEntityModelBuilder::from_static(
            "test::Entity",
            "TestEntity",
            &fields[0],
            fields,
            &INDEXES,
        );

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::DuplicateIndexName { .. })
        ));
    }

    #[test]
    fn plan_rejects_unorderable_field() {
        let model = <PlanValidateListEntity as EntitySchema>::MODEL;

        let schema = SchemaInfo::from_entity_model(model).expect("valid model");
        let plan: LogicalPlan<Value> = LogicalPlan {
            mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: AccessPlan::Path(AccessPath::FullScan),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("tags".to_string(), OrderDirection::Asc)],
            }),
            delete_limit: None,
            page: None,
            consistency: crate::db::query::ReadConsistency::MissingOk,
        };

        let err =
            validate_logical_plan_model(&schema, model, &plan).expect_err("unorderable field");
        assert!(matches!(err, PlanError::UnorderableField { .. }));
    }

    #[test]
    fn plan_rejects_index_prefix_too_long() {
        let model = model_with_index();
        let schema = SchemaInfo::from_entity_model(model).expect("valid model");
        let plan: LogicalPlan<Value> = LogicalPlan {
            mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: AccessPlan::Path(AccessPath::IndexPrefix {
                index: INDEX_MODEL,
                values: vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
            }),
            predicate: None,
            order: None,
            delete_limit: None,
            page: None,
            consistency: crate::db::query::ReadConsistency::MissingOk,
        };

        let err =
            validate_logical_plan_model(&schema, model, &plan).expect_err("index prefix too long");
        assert!(matches!(err, PlanError::IndexPrefixTooLong { .. }));
    }

    #[test]
    fn plan_rejects_empty_index_prefix() {
        let model = model_with_index();
        let schema = SchemaInfo::from_entity_model(model).expect("valid model");
        let plan: LogicalPlan<Value> = LogicalPlan {
            mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: AccessPlan::Path(AccessPath::IndexPrefix {
                index: INDEX_MODEL,
                values: vec![],
            }),
            predicate: None,
            order: None,
            delete_limit: None,
            page: None,
            consistency: crate::db::query::ReadConsistency::MissingOk,
        };

        let err =
            validate_logical_plan_model(&schema, model, &plan).expect_err("index prefix empty");
        assert!(matches!(err, PlanError::IndexPrefixEmpty));
    }

    #[test]
    fn plan_accepts_model_based_validation() {
        let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
        let schema = SchemaInfo::from_entity_model(model).expect("valid model");
        let plan: LogicalPlan<Value> = LogicalPlan {
            mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: AccessPlan::Path(AccessPath::ByKey(Value::Ulid(Ulid::nil()))),
            predicate: None,
            order: None,
            delete_limit: None,
            page: None,
            consistency: crate::db::query::ReadConsistency::MissingOk,
        };

        validate_logical_plan_model(&schema, model, &plan).expect("valid plan");
    }

    #[test]
    fn plan_rejects_unordered_pagination() {
        let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
        let schema = SchemaInfo::from_entity_model(model).expect("valid model");
        let plan: LogicalPlan<Value> = LogicalPlan {
            mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: AccessPlan::Path(AccessPath::FullScan),
            predicate: None,
            order: None,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(10),
                offset: 2,
            }),
            consistency: crate::db::query::ReadConsistency::MissingOk,
        };

        let err = validate_logical_plan_model(&schema, model, &plan)
            .expect_err("pagination without ordering must be rejected");
        assert!(matches!(err, PlanError::UnorderedPagination));
    }

    #[test]
    fn plan_accepts_ordered_pagination() {
        let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
        let schema = SchemaInfo::from_entity_model(model).expect("valid model");
        let plan: LogicalPlan<Value> = LogicalPlan {
            mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: AccessPlan::Path(AccessPath::FullScan),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(10),
                offset: 2,
            }),
            consistency: crate::db::query::ReadConsistency::MissingOk,
        };

        validate_logical_plan_model(&schema, model, &plan).expect("ordered pagination is valid");
    }

    #[test]
    fn plan_rejects_order_without_terminal_primary_key_tie_break() {
        let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
        let schema = SchemaInfo::from_entity_model(model).expect("valid model");
        let plan: LogicalPlan<Value> = LogicalPlan {
            mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: AccessPlan::Path(AccessPath::FullScan),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("tag".to_string(), OrderDirection::Asc)],
            }),
            delete_limit: None,
            page: None,
            consistency: crate::db::query::ReadConsistency::MissingOk,
        };

        let err =
            validate_logical_plan_model(&schema, model, &plan).expect_err("missing PK tie-break");
        assert!(matches!(err, PlanError::MissingPrimaryKeyTieBreak { .. }));
    }
}
