//! Planner invariants and assertions; must not surface user-facing errors.

use super::{
    canonical,
    types::{AccessPath, AccessPlan},
};
use crate::{
    db::query::predicate::{
        CoercionId, CompareOp, Predicate, SchemaInfo, coercion::canonical_cmp,
        validate::literal_matches_type,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::entity::EntityModel,
    value::Value,
};

#[cfg(test)]
use crate::traits::{EntityKind, FieldValue};

#[cfg(test)]
#[allow(dead_code)]
pub fn validate_plan_invariants<E: EntityKind>(
    plan: &AccessPlan<E::Key>,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> Result<(), InternalError> {
    let Some(predicate) = predicate else {
        return Ok(());
    };

    let info = StrictPredicateInfo::from_predicate::<E>(schema, Some(predicate));
    validate_access_plan::<E>(plan, schema, &info)
}

pub fn validate_plan_invariants_model(
    plan: &AccessPlan<Value>,
    schema: &SchemaInfo,
    model: &EntityModel,
    predicate: Option<&Predicate>,
) -> Result<(), InternalError> {
    let Some(predicate) = predicate else {
        return Ok(());
    };

    let info = StrictPredicateInfo::from_predicate_model(schema, model, Some(predicate));
    validate_access_plan_model(plan, schema, model, &info)
}

#[derive(Default)]
struct StrictPredicateInfo {
    pk_keys: Vec<Value>,
    field_values: std::collections::BTreeMap<String, Vec<Value>>,
}

impl StrictPredicateInfo {
    #[cfg(test)]
    #[allow(dead_code)]
    fn from_predicate<E: EntityKind>(schema: &SchemaInfo, predicate: Option<&Predicate>) -> Self {
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

    fn from_predicate_model(
        schema: &SchemaInfo,
        model: &EntityModel,
        predicate: Option<&Predicate>,
    ) -> Self {
        let mut info = Self::default();
        if let Some(predicate) = predicate {
            collect_strict_predicate_info_model(schema, model, predicate, false, &mut info);
        }
        info
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn collect_strict_predicate_info<E: EntityKind>(
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
                _ => {
                    // NOTE: Only Eq/In comparisons contribute to strict predicate info.
                }
            }
        }
        _ => {
            // NOTE: Non-comparison predicates do not contribute to strict predicate info.
        }
    }
}

fn collect_strict_predicate_info_model(
    schema: &SchemaInfo,
    model: &EntityModel,
    predicate: &Predicate,
    negated: bool,
    info: &mut StrictPredicateInfo,
) {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                collect_strict_predicate_info_model(schema, model, child, negated, info);
            }
        }
        Predicate::Not(inner) => {
            collect_strict_predicate_info_model(schema, model, inner, !negated, info);
        }
        Predicate::Compare(cmp) => {
            if negated || cmp.coercion.id != CoercionId::Strict {
                return;
            }

            let mut push_value = |value: &Value| {
                if is_primary_key_model(schema, model, &cmp.field) {
                    if value_matches_pk_model(schema, model, value) && !info.pk_keys.contains(value)
                    {
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
                _ => {
                    // NOTE: Only Eq/In comparisons contribute to strict predicate info.
                }
            }
        }
        _ => {
            // NOTE: Non-comparison predicates do not contribute to strict predicate info.
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn validate_access_plan<E: EntityKind>(
    plan: &AccessPlan<E::Key>,
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

fn validate_access_plan_model(
    plan: &AccessPlan<Value>,
    schema: &SchemaInfo,
    model: &EntityModel,
    info: &StrictPredicateInfo,
) -> Result<(), InternalError> {
    match plan {
        AccessPlan::Path(path) => validate_access_path_model(path, schema, model, info),
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            ensure_invariant(
                !children.is_empty(),
                "planner invariant violated: composite plan must have children",
            )?;
            ensure_invariant(
                !children.iter().any(is_full_scan),
                "planner invariant violated: FullScan cannot appear inside composite plans",
            )?;
            ensure_invariant(
                canonical::is_canonical_sorted_value(children),
                "planner invariant violated: composite plan children must be canonicalized",
            )?;
            for child in children {
                validate_access_plan_model(child, schema, model, info)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn validate_access_path<E: EntityKind>(
    path: &AccessPath<E::Key>,
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

fn validate_access_path_model(
    path: &AccessPath<Value>,
    schema: &SchemaInfo,
    model: &EntityModel,
    info: &StrictPredicateInfo,
) -> Result<(), InternalError> {
    match path {
        AccessPath::FullScan => {}
        AccessPath::ByKey(key) => {
            ensure_invariant(
                value_matches_pk_model(schema, model, key),
                "planner invariant violated: ByKey must target the primary key",
            )?;
            ensure_invariant(
                info.pk_keys.contains(key),
                "planner invariant violated: ByKey requires strict primary key predicate",
            )?;
        }
        AccessPath::ByKeys(keys) => {
            for key in keys {
                ensure_invariant(
                    value_matches_pk_model(schema, model, key),
                    "planner invariant violated: ByKeys must target the primary key",
                )?;
                ensure_invariant(
                    info.pk_keys.contains(key),
                    "planner invariant violated: ByKeys requires strict primary key predicate",
                )?;
            }
        }
        AccessPath::KeyRange { start, end } => {
            ensure_invariant(
                value_matches_pk_model(schema, model, start)
                    && value_matches_pk_model(schema, model, end),
                "planner invariant violated: KeyRange must target the primary key",
            )?;
            let ordering = canonical_cmp(start, end);
            ensure_invariant(
                ordering != std::cmp::Ordering::Greater,
                "planner invariant violated: KeyRange start must be <= end",
            )?;
        }
        AccessPath::IndexPrefix { index, values } => {
            ensure_invariant(
                !values.is_empty(),
                "planner invariant violated: IndexPrefix must be non-empty",
            )?;
            ensure_invariant(
                values.len() <= index.fields.len(),
                "planner invariant violated: index prefix exceeds field count",
            )?;
            for (field, value) in index.fields.iter().zip(values.iter()) {
                let Some(field_type) = schema.field(field) else {
                    // NOTE: Missing fields imply upstream validation failure.
                    continue;
                };
                if !literal_matches_type(value, field_type) {
                    // NOTE: Literal mismatches imply upstream validation failure.
                    continue;
                }
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

#[cfg(test)]
#[allow(dead_code)]
fn is_primary_key<E: EntityKind>(schema: &SchemaInfo, field: &str) -> bool {
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

#[cfg(test)]
#[allow(dead_code)]
fn value_matches_pk<E: EntityKind>(schema: &SchemaInfo, value: &Value) -> bool {
    let field = E::PRIMARY_KEY;
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    if !field_type.is_keyable() {
        return false;
    }

    literal_matches_type(value, field_type)
}

fn is_primary_key_model(schema: &SchemaInfo, model: &EntityModel, field: &str) -> bool {
    field == model.primary_key.name && schema.field(field).is_some()
}

fn value_matches_pk_model(schema: &SchemaInfo, model: &EntityModel, value: &Value) -> bool {
    let field = model.primary_key.name;
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    if !field_type.is_keyable() {
        return false;
    }

    literal_matches_type(value, field_type)
}
