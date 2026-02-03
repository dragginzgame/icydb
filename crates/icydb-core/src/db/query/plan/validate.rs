//! Executor-ready plan validation against a concrete entity schema.
use super::{AccessPath, AccessPlan, LogicalPlan, OrderSpec};
use crate::{
    db::query::predicate::{self, SchemaInfo},
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
    PrimaryKeyUnsupported { field: String },

    /// Supplied key does not match the primary key type.
    #[error("key '{key:?}' is incompatible with primary key '{field}'")]
    PrimaryKeyMismatch { field: String, key: Value },

    /// Key range has invalid ordering.
    #[error("key range start is greater than end")]
    InvalidKeyRange,

    /// ORDER BY must specify at least one field.
    #[error("order specification must include at least one field")]
    EmptyOrderSpec,

    /// Delete plans must not carry pagination.
    #[error("delete plans must not include pagination")]
    DeletePlanWithPagination,

    /// Load plans must not carry delete limits.
    #[error("load plans must not include delete limits")]
    LoadPlanWithDeleteLimit,

    /// Delete limits require an explicit ordering.
    #[error("delete limit requires an explicit ordering")]
    DeleteLimitRequiresOrder,
}

/// Validate a logical plan using a prebuilt schema surface.
#[cfg(test)]
pub(crate) fn validate_plan_with_schema_info<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &LogicalPlan<K>,
) -> Result<(), PlanError>
where
    K: FieldValue + Ord,
{
    validate_logical_plan(schema, model, plan)
}

/// Validate a logical plan against the runtime entity model.
///
/// This is the executor-safe entrypoint and must not consult global schema.
#[cfg(test)]
#[expect(dead_code)]
pub(crate) fn validate_plan_with_model<K>(
    plan: &LogicalPlan<K>,
    model: &EntityModel,
) -> Result<(), PlanError>
where
    K: FieldValue + Ord,
{
    let schema = SchemaInfo::from_entity_model(model)?;
    validate_plan_with_schema_info(&schema, model, plan)
}

/// Validate a logical plan against schema and plan-level invariants.
#[cfg(test)]
pub(crate) fn validate_logical_plan<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    plan: &LogicalPlan<K>,
) -> Result<(), PlanError>
where
    K: FieldValue + Ord,
{
    if let Some(predicate) = &plan.predicate {
        predicate::validate(schema, predicate)?;
    }

    if let Some(order) = &plan.order {
        validate_order(schema, order)?;
    }

    validate_access_plan(schema, model, &plan.access)?;
    validate_plan_semantics(plan)?;

    Ok(())
}

/// Validate a logical plan with model-level key values.
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

    if plan.mode.is_load() && plan.delete_limit.is_some() {
        return Err(PlanError::LoadPlanWithDeleteLimit);
    }

    Ok(())
}

/// Validate plans at executor boundaries and surface invariant violations.
pub(crate) fn validate_executor_plan<E: EntityKind>(
    plan: &LogicalPlan<E::Id>,
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
            // CONTRACT: ORDER BY rejects unsupported or unordered fields.
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
    match access {
        AccessPlan::Path(path) => validate_access_path(schema, model, path),
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            for child in children {
                validate_access_plan(schema, model, child)?;
            }
            Ok(())
        }
    }
}

/// Validate access paths that carry model-level key values.
pub(crate) fn validate_access_plan_model(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<Value>,
) -> Result<(), PlanError> {
    match access {
        AccessPlan::Path(path) => validate_access_path_model(schema, model, path),
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            for child in children {
                validate_access_plan_model(schema, model, child)?;
            }
            Ok(())
        }
    }
}

fn validate_access_path<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPath<K>,
) -> Result<(), PlanError>
where
    K: FieldValue + Ord,
{
    match access {
        AccessPath::ByKey(key) => validate_pk_key(schema, model, key),
        AccessPath::ByKeys(keys) => {
            // Empty key lists are a valid no-op.
            if keys.is_empty() {
                return Ok(());
            }
            for key in keys {
                validate_pk_key(schema, model, key)?;
            }
            Ok(())
        }
        AccessPath::KeyRange { start, end } => {
            validate_pk_key(schema, model, start)?;
            validate_pk_key(schema, model, end)?;
            if start > end {
                return Err(PlanError::InvalidKeyRange);
            }
            Ok(())
        }
        AccessPath::IndexPrefix { index, values } => {
            validate_index_prefix(schema, model, index, values)
        }
        AccessPath::FullScan => Ok(()),
    }
}

// Validate executor-visible access paths that carry model-level key values.
fn validate_access_path_model(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPath<Value>,
) -> Result<(), PlanError> {
    match access {
        AccessPath::ByKey(key) => validate_pk_value(schema, model, key),
        AccessPath::ByKeys(keys) => {
            if keys.is_empty() {
                return Ok(());
            }
            for key in keys {
                validate_pk_value(schema, model, key)?;
            }
            Ok(())
        }
        AccessPath::KeyRange { start, end } => {
            validate_pk_value(schema, model, start)?;
            validate_pk_value(schema, model, end)?;
            let Some(ordering) = start.partial_cmp(end) else {
                return Err(PlanError::InvalidKeyRange);
            };
            if ordering == std::cmp::Ordering::Greater {
                return Err(PlanError::InvalidKeyRange);
            }
            Ok(())
        }
        AccessPath::IndexPrefix { index, values } => {
            validate_index_prefix(schema, model, index, values)
        }
        AccessPath::FullScan => Ok(()),
    }
}

/// Validate that a key matches the entity's primary key type.
fn validate_pk_key<K>(schema: &SchemaInfo, model: &EntityModel, key: &K) -> Result<(), PlanError>
where
    K: FieldValue,
{
    let field = model.primary_key.name;

    let field_type = schema
        .field(field)
        .ok_or_else(|| PlanError::PrimaryKeyUnsupported {
            field: field.to_string(),
        })?;

    if !field_type.is_keyable() {
        return Err(PlanError::PrimaryKeyUnsupported {
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
        .ok_or_else(|| PlanError::PrimaryKeyUnsupported {
            field: field.to_string(),
        })?;

    if !field_type.is_keyable() {
        return Err(PlanError::PrimaryKeyUnsupported {
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
/// Non-scalar and unsupported field types are intentionally excluded.
///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{PlanError, validate_logical_plan_model};
    use crate::{
        db::query::{
            plan::{AccessPath, AccessPlan, LogicalPlan, OrderDirection, OrderSpec},
            predicate::{SchemaInfo, ValidateError},
        },
        model::{
            entity::EntityModel,
            field::{EntityFieldKind, EntityFieldModel},
            index::IndexModel,
        },
        types::Ulid,
        value::Value,
    };

    fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
        EntityFieldModel { name, kind }
    }

    fn model_with_fields(fields: Vec<EntityFieldModel>, pk_index: usize) -> EntityModel {
        let fields: &'static [EntityFieldModel] = Box::leak(fields.into_boxed_slice());
        let primary_key = &fields[pk_index];
        let indexes: &'static [&'static IndexModel] = &[];

        EntityModel {
            path: "test::Entity",
            entity_name: "TestEntity",
            primary_key,
            fields,
            indexes,
        }
    }

    const INDEX_FIELDS: [&str; 1] = ["tag"];
    const INDEX_MODEL: IndexModel =
        IndexModel::new("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    fn model_with_index() -> EntityModel {
        let fields: &'static [EntityFieldModel] = Box::leak(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("tag", EntityFieldKind::Text),
            ]
            .into_boxed_slice(),
        );

        EntityModel {
            path: "test::Entity",
            entity_name: "TestEntity",
            primary_key: &fields[0],
            fields,
            indexes: &INDEXES,
        }
    }

    #[test]
    fn model_rejects_missing_primary_key() {
        let fields: &'static [EntityFieldModel] =
            Box::leak(vec![field("id", EntityFieldKind::Ulid)].into_boxed_slice());
        let missing_pk = Box::leak(Box::new(field("missing", EntityFieldKind::Ulid)));

        let model = EntityModel {
            path: "test::Entity",
            entity_name: "TestEntity",
            primary_key: missing_pk,
            fields,
            indexes: &[],
        };

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::InvalidPrimaryKey { .. })
        ));
    }

    #[test]
    fn model_rejects_duplicate_fields() {
        let model = model_with_fields(
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
        let model = model_with_fields(
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
        let model = EntityModel {
            path: "test::Entity",
            entity_name: "TestEntity",
            primary_key: &fields[0],
            fields,
            indexes: &INDEXES,
        };

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::IndexFieldUnknown { .. })
        ));
    }

    #[test]
    fn model_rejects_index_unsupported_field() {
        const INDEX_FIELDS: [&str; 1] = ["broken"];
        const INDEX_MODEL: IndexModel =
            IndexModel::new("test::idx_broken", "test::IndexStore", &INDEX_FIELDS, false);
        const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

        let fields: &'static [EntityFieldModel] = Box::leak(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("broken", EntityFieldKind::Unsupported),
            ]
            .into_boxed_slice(),
        );
        let model = EntityModel {
            path: "test::Entity",
            entity_name: "TestEntity",
            primary_key: &fields[0],
            fields,
            indexes: &INDEXES,
        };

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::IndexFieldUnsupported { .. })
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
        let model = EntityModel {
            path: "test::Entity",
            entity_name: "TestEntity",
            primary_key: &fields[0],
            fields,
            indexes: &INDEXES,
        };

        assert!(matches!(
            SchemaInfo::from_entity_model(&model),
            Err(ValidateError::DuplicateIndexName { .. })
        ));
    }

    #[test]
    fn plan_rejects_unorderable_field() {
        let model = model_with_fields(
            vec![
                field("id", EntityFieldKind::Ulid),
                field("tags", EntityFieldKind::List(&EntityFieldKind::Text)),
            ],
            0,
        );

        let schema = SchemaInfo::from_entity_model(&model).expect("valid model");
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
            validate_logical_plan_model(&schema, &model, &plan).expect_err("unorderable field");
        assert!(matches!(err, PlanError::UnorderableField { .. }));
    }

    #[test]
    fn plan_rejects_index_prefix_too_long() {
        let model = model_with_index();
        let schema = SchemaInfo::from_entity_model(&model).expect("valid model");
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
            validate_logical_plan_model(&schema, &model, &plan).expect_err("index prefix too long");
        assert!(matches!(err, PlanError::IndexPrefixTooLong { .. }));
    }

    #[test]
    fn plan_rejects_empty_index_prefix() {
        let model = model_with_index();
        let schema = SchemaInfo::from_entity_model(&model).expect("valid model");
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
            validate_logical_plan_model(&schema, &model, &plan).expect_err("index prefix empty");
        assert!(matches!(err, PlanError::IndexPrefixEmpty));
    }

    #[test]
    fn plan_accepts_model_based_validation() {
        let model = model_with_fields(vec![field("id", EntityFieldKind::Ulid)], 0);
        let schema = SchemaInfo::from_entity_model(&model).expect("valid model");
        let plan: LogicalPlan<Value> = LogicalPlan {
            mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: AccessPlan::Path(AccessPath::ByKey(Value::Ulid(Ulid::nil()))),
            predicate: None,
            order: None,
            delete_limit: None,
            page: None,
            consistency: crate::db::query::ReadConsistency::MissingOk,
        };

        validate_logical_plan_model(&schema, &model, &plan).expect("valid plan");
    }
}
