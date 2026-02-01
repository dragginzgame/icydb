//! Planner invariants and assertions; must not surface user-facing errors.

use crate::{
    db::query::predicate::{
        CoercionId, CompareOp, Predicate, SchemaInfo,
        validate::{FieldType, ScalarType, literal_matches_type},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, FieldValue},
    types::Ref,
    value::Value,
};

use super::{
    canonical,
    types::{AccessPath, AccessPlan},
};

pub fn validate_plan_invariants<E: EntityKind<PrimaryKey = Ref<E>>>(
    plan: &AccessPlan<E::PrimaryKey>,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<(), InternalError> {
    let Some(predicate) = predicate else {
        return Ok(());
    };

    let info = StrictPredicateInfo::from_predicate::<E>(schema, Some(predicate));
    validate_access_plan::<E>(plan, schema, &info)
}

#[derive(Default)]
struct StrictPredicateInfo {
    pk_keys: Vec<Value>,
    field_values: std::collections::BTreeMap<String, Vec<Value>>,
}

impl StrictPredicateInfo {
    fn from_predicate<E: EntityKind<PrimaryKey = Ref<E>>>(
        schema: &SchemaInfo,
        predicate: Option<&Predicate>,
    ) -> Self {
        let mut info = Self::default();
        if let Some(predicate) = predicate {
            collect_strict_predicate_info::<E>(schema, predicate, false, &mut info);
        }
        info
    }

    fn contains_field_value(&self, field: &str, value: &Value) -> bool {
        self.field_values
            .get(field)
            .is_some_and(|values| values.contains(value))
    }
}

fn collect_strict_predicate_info<E: EntityKind<PrimaryKey = Ref<E>>>(
    schema: &SchemaInfo,
    predicate: &Predicate,
    negated: bool,
    info: &mut StrictPredicateInfo,
) {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                collect_strict_predicate_info::<E>(schema, child, negated, info);
            }
        }
        Predicate::Not(inner) => {
            collect_strict_predicate_info::<E>(schema, inner, !negated, info);
        }
        Predicate::Compare(cmp) => {
            if negated || cmp.coercion.id != CoercionId::Strict {
                return;
            }

            let mut push_value = |value: &Value| {
                if is_primary_key::<E>(schema, &cmp.field) {
                    if value_matches_pk::<E>(schema, value) && !info.pk_keys.contains(value) {
                        info.pk_keys.push(value.clone());
                    }
                    return;
                }

                let Some(field_type) = schema.field(&cmp.field) else {
                    return;
                };
                if !literal_matches_type(value, field_type) {
                    return;
                }

                let values = info.field_values.entry(cmp.field.clone()).or_default();
                values.retain(|existing| existing != value);
                values.push(value.clone());
            };

            match cmp.op {
                CompareOp::Eq => {
                    push_value(&cmp.value);
                }
                CompareOp::In => {
                    if let Value::List(items) = &cmp.value {
                        for item in items {
                            push_value(item);
                        }
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
}

fn validate_access_plan<E: EntityKind<PrimaryKey = Ref<E>>>(
    plan: &AccessPlan<E::PrimaryKey>,
    schema: &SchemaInfo,
    info: &StrictPredicateInfo,
) -> Result<(), InternalError> {
    match plan {
        AccessPlan::Path(path) => validate_access_path::<E>(path, schema, info),
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            // Internal invariant: composite plans must retain at least one child.
            ensure_invariant(
                !children.is_empty(),
                "planner invariant violated: composite plan must have children",
            )?;
            // Internal invariant: FullScan cannot appear inside composite access plans.
            ensure_invariant(
                !children.iter().any(is_full_scan),
                "planner invariant violated: FullScan cannot appear inside composite plans",
            )?;
            ensure_invariant(
                canonical::is_canonical_sorted(children),
                "planner invariant violated: composite plan children must be canonicalized",
            )?;
            for child in children {
                validate_access_plan::<E>(child, schema, info)?;
            }
            Ok(())
        }
    }
}

fn validate_access_path<E: EntityKind<PrimaryKey = Ref<E>>>(
    path: &AccessPath<E::PrimaryKey>,
    schema: &SchemaInfo,
    info: &StrictPredicateInfo,
) -> Result<(), InternalError> {
    match path {
        AccessPath::FullScan => {}
        AccessPath::ByKey(key) => {
            let key_value = key.to_value();
            // Internal invariant: ByKey only targets the primary key.
            ensure_invariant(
                value_matches_pk::<E>(schema, &key_value),
                "planner invariant violated: ByKey must target the primary key",
            )?;
            // Internal invariant: ByKey only arises from strict primary key predicates.
            ensure_invariant(
                info.pk_keys.contains(&key_value),
                "planner invariant violated: ByKey requires strict primary key predicate",
            )?;
        }
        AccessPath::ByKeys(keys) => {
            // Empty ByKeys is a valid no-op for key-only intents.
            for key in keys {
                let key_value = key.to_value();
                // Internal invariant: ByKeys only targets the primary key.
                ensure_invariant(
                    value_matches_pk::<E>(schema, &key_value),
                    "planner invariant violated: ByKeys must target the primary key",
                )?;
                // Internal invariant: ByKeys only arises from strict primary key predicates.
                ensure_invariant(
                    info.pk_keys.contains(&key_value),
                    "planner invariant violated: ByKeys requires strict primary key predicate",
                )?;
            }
        }
        AccessPath::KeyRange { start, end } => {
            // Internal invariant: KeyRange only targets the primary key.
            ensure_invariant(
                value_matches_pk::<E>(schema, &start.to_value())
                    && value_matches_pk::<E>(schema, &end.to_value()),
                "planner invariant violated: KeyRange must target the primary key",
            )?;
            // Internal invariant: KeyRange ordering must be normalized.
            ensure_invariant(
                start <= end,
                "planner invariant violated: KeyRange start must be <= end",
            )?;
        }
        AccessPath::IndexPrefix { index, values } => {
            ensure_invariant(
                !values.is_empty(),
                "planner invariant violated: IndexPrefix must be non-empty",
            )?;
            // Internal invariant: index prefix cannot exceed indexed field count.
            ensure_invariant(
                values.len() <= index.fields.len(),
                "planner invariant violated: index prefix exceeds field count",
            )?;
            for (field, value) in index.fields.iter().zip(values.iter()) {
                // Internal invariant: IndexPrefix requires strict predicate values.
                ensure_invariant(
                    info.contains_field_value(field, value),
                    "planner invariant violated: IndexPrefix requires strict predicate value",
                )?;
            }
        }
    }
    Ok(())
}

const fn is_full_scan<K>(plan: &AccessPlan<K>) -> bool {
    matches!(plan, AccessPlan::Path(AccessPath::FullScan))
}

fn is_primary_key<E: EntityKind<PrimaryKey = Ref<E>>>(schema: &SchemaInfo, field: &str) -> bool {
    field == E::PRIMARY_KEY && schema.field(field).is_some()
}

fn ensure_invariant(condition: bool, message: &str) -> Result<(), InternalError> {
    if condition {
        Ok(())
    } else {
        debug_assert!(condition, "{}", message);
        Err(InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            message,
        ))
    }
}

fn value_matches_pk<E: EntityKind<PrimaryKey = Ref<E>>>(
    schema: &SchemaInfo,
    value: &Value,
) -> bool {
    let field = E::PRIMARY_KEY;
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    if !is_key_compatible(field_type) {
        return false;
    }

    literal_matches_type(value, field_type)
}

const fn is_key_compatible(field_type: &FieldType) -> bool {
    matches!(
        field_type,
        FieldType::Scalar(
            ScalarType::Account
                | ScalarType::Int
                | ScalarType::Principal
                | ScalarType::Subaccount
                | ScalarType::Timestamp
                | ScalarType::Uint
                | ScalarType::Ulid
                | ScalarType::Unit
        )
    )
}
