//! Planner invariants and assertions; must not surface user-facing errors.

use crate::{
    db::query::predicate::{
        CoercionId, CompareOp, Predicate, SchemaInfo,
        validate::{FieldType, ScalarType, literal_matches_type},
    },
    key::Key,
    traits::EntityKind,
    value::Value,
};

use super::{
    canonical,
    types::{AccessPath, AccessPlan},
};

pub fn validate_plan_invariants<E: EntityKind>(
    plan: &AccessPlan,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) {
    let Some(predicate) = predicate else {
        return;
    };

    let info = StrictPredicateInfo::from_predicate::<E>(schema, Some(predicate));
    validate_access_plan::<E>(plan, schema, &info);
}

#[derive(Default)]
struct StrictPredicateInfo {
    pk_keys: std::collections::BTreeSet<Key>,
    field_values: std::collections::BTreeMap<String, Vec<Value>>,
}

impl StrictPredicateInfo {
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
}

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
                    if let Some(key) = value.as_key()
                        && key_matches_pk::<E>(schema, &key)
                    {
                        info.pk_keys.insert(key);
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

fn validate_access_plan<E: EntityKind>(
    plan: &AccessPlan,
    schema: &SchemaInfo,
    info: &StrictPredicateInfo,
) {
    match plan {
        AccessPlan::Path(path) => validate_access_path::<E>(path, schema, info),
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            // Internal invariant: composite plans must retain at least one child.
            assert!(
                !children.is_empty(),
                "planner invariant violated: composite plan must have children"
            );
            // Internal invariant: FullScan cannot appear inside composite access plans.
            assert!(
                !children.iter().any(is_full_scan),
                "planner invariant violated: FullScan cannot appear inside composite plans"
            );
            debug_assert!(
                canonical::is_canonical_sorted(children),
                "planner invariant violated: composite plan children must be canonicalized"
            );
            for child in children {
                validate_access_plan::<E>(child, schema, info);
            }
        }
    }
}

fn validate_access_path<E: EntityKind>(
    path: &AccessPath,
    schema: &SchemaInfo,
    info: &StrictPredicateInfo,
) {
    match path {
        AccessPath::FullScan => {}
        AccessPath::ByKey(key) => {
            // Internal invariant: ByKey only targets the primary key.
            assert!(
                key_matches_pk::<E>(schema, key),
                "planner invariant violated: ByKey must target the primary key"
            );
            // Internal invariant: ByKey only arises from strict primary key predicates.
            assert!(
                info.pk_keys.contains(key),
                "planner invariant violated: ByKey requires strict primary key predicate"
            );
        }
        AccessPath::ByKeys(keys) => {
            // Internal invariant: ByKeys must contain at least one key.
            assert!(
                !keys.is_empty(),
                "planner invariant violated: ByKeys must be non-empty"
            );
            for key in keys {
                // Internal invariant: ByKeys only targets the primary key.
                assert!(
                    key_matches_pk::<E>(schema, key),
                    "planner invariant violated: ByKeys must target the primary key"
                );
                // Internal invariant: ByKeys only arises from strict primary key predicates.
                assert!(
                    info.pk_keys.contains(key),
                    "planner invariant violated: ByKeys requires strict primary key predicate"
                );
            }
        }
        AccessPath::KeyRange { start, end } => {
            // Internal invariant: KeyRange only targets the primary key.
            assert!(
                key_matches_pk::<E>(schema, start) && key_matches_pk::<E>(schema, end),
                "planner invariant violated: KeyRange must target the primary key"
            );
            // Internal invariant: KeyRange ordering must be normalized.
            assert!(
                start <= end,
                "planner invariant violated: KeyRange start must be <= end"
            );
        }
        AccessPath::IndexPrefix { index, values } => {
            debug_assert!(
                !values.is_empty(),
                "planner invariant violated: IndexPrefix must be non-empty"
            );
            // Internal invariant: index prefix cannot exceed indexed field count.
            assert!(
                values.len() <= index.fields.len(),
                "planner invariant violated: index prefix exceeds field count"
            );
            for (field, value) in index.fields.iter().zip(values.iter()) {
                // Internal invariant: IndexPrefix requires strict predicate values.
                assert!(
                    info.contains_field_value(field, value),
                    "planner invariant violated: IndexPrefix requires strict predicate value"
                );
            }
        }
    }
}

const fn is_full_scan(plan: &AccessPlan) -> bool {
    matches!(plan, AccessPlan::Path(AccessPath::FullScan))
}

fn is_primary_key<E: EntityKind>(schema: &SchemaInfo, field: &str) -> bool {
    field == E::PRIMARY_KEY && schema.field(field).is_some()
}

fn key_matches_pk<E: EntityKind>(schema: &SchemaInfo, key: &Key) -> bool {
    let field = E::PRIMARY_KEY;
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    let Some(expected) = key_type_for_field(field_type) else {
        return false;
    };

    key_variant(key) == expected
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyVariant {
    Account,
    Int,
    Principal,
    Subaccount,
    Timestamp,
    Uint,
    Ulid,
    Unit,
}

const fn key_variant(key: &Key) -> KeyVariant {
    match key {
        Key::Account(_) => KeyVariant::Account,
        Key::Int(_) => KeyVariant::Int,
        Key::Principal(_) => KeyVariant::Principal,
        Key::Subaccount(_) => KeyVariant::Subaccount,
        Key::Timestamp(_) => KeyVariant::Timestamp,
        Key::Uint(_) => KeyVariant::Uint,
        Key::Ulid(_) => KeyVariant::Ulid,
        Key::Unit => KeyVariant::Unit,
    }
}

const fn key_type_for_field(field_type: &FieldType) -> Option<KeyVariant> {
    match field_type {
        FieldType::Scalar(ScalarType::Account) => Some(KeyVariant::Account),
        FieldType::Scalar(ScalarType::Int) => Some(KeyVariant::Int),
        FieldType::Scalar(ScalarType::Principal) => Some(KeyVariant::Principal),
        FieldType::Scalar(ScalarType::Subaccount) => Some(KeyVariant::Subaccount),
        FieldType::Scalar(ScalarType::Timestamp) => Some(KeyVariant::Timestamp),
        FieldType::Scalar(ScalarType::Uint) => Some(KeyVariant::Uint),
        FieldType::Scalar(ScalarType::Ulid) => Some(KeyVariant::Ulid),
        FieldType::Scalar(ScalarType::Unit) => Some(KeyVariant::Unit),
        _ => None,
    }
}
