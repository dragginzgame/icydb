use crate::{
    db::{
        index::fingerprint,
        query::v2::predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo,
            validate::{FieldType, ScalarType, literal_matches_type},
        },
    },
    key::Key,
    traits::EntityKind,
    value::Value,
};

use super::{AccessPath, PlanError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessPlan {
    Path(AccessPath),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

impl AccessPlan {
    #[must_use]
    pub const fn full_scan() -> Self {
        Self::Path(AccessPath::FullScan)
    }

    fn normalize(self) -> Self {
        match self {
            Self::Path(_) => self,
            Self::Union(children) => normalize_union(children),
            Self::Intersection(children) => normalize_intersection(children),
        }
    }
}

#[must_use]
pub fn plan_access<E: EntityKind>(predicate: Option<&Predicate>) -> Result<AccessPlan, PlanError> {
    let Some(predicate) = predicate else {
        return Ok(AccessPlan::full_scan());
    };

    let schema = SchemaInfo::from_entity::<E>()?;
    crate::db::query::v2::predicate::validate(&schema, predicate)?;

    Ok(plan_predicate::<E>(&schema, predicate).normalize())
}

fn plan_predicate<E: EntityKind>(schema: &SchemaInfo, predicate: &Predicate) -> AccessPlan {
    match predicate {
        Predicate::True | Predicate::False => AccessPlan::full_scan(),
        Predicate::Not(_) => AccessPlan::full_scan(),
        Predicate::And(children) => {
            let mut plans = children
                .iter()
                .map(|child| plan_predicate::<E>(schema, child))
                .collect::<Vec<_>>();

            if let Some(prefix) = index_prefix_from_and::<E>(schema, children) {
                plans.push(AccessPlan::Path(prefix));
            }

            AccessPlan::Intersection(plans)
        }
        Predicate::Or(children) => AccessPlan::Union(
            children
                .iter()
                .map(|child| plan_predicate::<E>(schema, child))
                .collect::<Vec<_>>(),
        ),
        Predicate::Compare(cmp) => plan_compare::<E>(schema, cmp),
        Predicate::IsNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::MapContainsKey { .. }
        | Predicate::MapContainsValue { .. }
        | Predicate::MapContainsEntry { .. } => AccessPlan::full_scan(),
    }
}

fn plan_compare<E: EntityKind>(schema: &SchemaInfo, cmp: &ComparePredicate) -> AccessPlan {
    if cmp.coercion.id != CoercionId::Strict {
        return AccessPlan::full_scan();
    }

    if is_primary_key::<E>(schema, &cmp.field) {
        if let Some(path) = plan_pk_compare::<E>(schema, cmp) {
            return AccessPlan::Path(path);
        }
    }

    match cmp.op {
        CompareOp::Eq => {
            if let Some(paths) = index_prefix_for_eq::<E>(schema, &cmp.field, &cmp.value) {
                return AccessPlan::Union(paths);
            }
        }
        CompareOp::In => {
            if let Value::List(items) = &cmp.value {
                let mut plans = Vec::new();
                for item in items {
                    if let Some(paths) = index_prefix_for_eq::<E>(schema, &cmp.field, item) {
                        plans.extend(paths);
                    }
                }
                if !plans.is_empty() {
                    return AccessPlan::Union(plans);
                }
            }
        }
        _ => {}
    }

    AccessPlan::full_scan()
}

fn plan_pk_compare<E: EntityKind>(
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Option<AccessPath> {
    match cmp.op {
        CompareOp::Eq => {
            let key = cmp.value.as_key()?;
            key_matches_pk::<E>(schema, &key).then_some(AccessPath::ByKey(key))
        }
        CompareOp::In => {
            let Value::List(items) = &cmp.value else {
                return None;
            };
            let mut keys = Vec::with_capacity(items.len());
            for item in items {
                let key = item.as_key()?;
                if !key_matches_pk::<E>(schema, &key) {
                    return None;
                }
                keys.push(key);
            }
            Some(AccessPath::ByKeys(keys))
        }
        _ => None,
    }
}

fn index_prefix_for_eq<E: EntityKind>(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> Option<Vec<AccessPlan>> {
    let field_type = schema.field(field)?;

    if !literal_matches_type(value, field_type) {
        return None;
    }

    if fingerprint::to_index_fingerprint(value).is_none() {
        return None;
    }

    let mut out = Vec::new();
    for index in E::INDEXES {
        if index.fields.first() != Some(&field) {
            continue;
        }
        out.push(AccessPlan::Path(AccessPath::IndexPrefix {
            index: **index,
            values: vec![value.clone()],
        }));
    }

    if out.is_empty() { None } else { Some(out) }
}

fn index_prefix_from_and<E: EntityKind>(
    schema: &SchemaInfo,
    children: &[Predicate],
) -> Option<AccessPath> {
    let mut field_values = Vec::new();

    for child in children {
        let Predicate::Compare(cmp) = child else {
            continue;
        };
        if cmp.op != CompareOp::Eq {
            continue;
        }
        if cmp.coercion.id != CoercionId::Strict {
            continue;
        }
        field_values.push((cmp.field.as_str(), &cmp.value));
    }

    for index in E::INDEXES {
        let mut prefix = Vec::new();
        for field in index.fields {
            let Some((_, value)) = field_values.iter().find(|(name, _)| *name == *field) else {
                break;
            };
            let field_type = schema.field(field)?;
            if !literal_matches_type(value, field_type) {
                prefix.clear();
                break;
            }
            if fingerprint::to_index_fingerprint(value).is_none() {
                prefix.clear();
                break;
            }
            prefix.push((*value).clone());
        }

        if !prefix.is_empty() {
            return Some(AccessPath::IndexPrefix {
                index: **index,
                values: prefix,
            });
        }
    }

    None
}

fn normalize_union(children: Vec<AccessPlan>) -> AccessPlan {
    let mut out = Vec::new();

    for child in children {
        let child = child.normalize();
        if is_full_scan(&child) {
            return AccessPlan::full_scan();
        }

        match child {
            AccessPlan::Union(grand) => out.extend(grand),
            _ => out.push(child),
        }
    }

    if out.is_empty() {
        return AccessPlan::full_scan();
    }
    if out.len() == 1 {
        return out.pop().expect("single union child");
    }

    sort_access_plans(&mut out);
    AccessPlan::Union(out)
}

fn normalize_intersection(children: Vec<AccessPlan>) -> AccessPlan {
    let mut out = Vec::new();

    for child in children {
        let child = child.normalize();
        if is_full_scan(&child) {
            continue;
        }

        match child {
            AccessPlan::Intersection(grand) => out.extend(grand),
            _ => out.push(child),
        }
    }

    if out.is_empty() {
        return AccessPlan::full_scan();
    }
    if out.len() == 1 {
        return out.pop().expect("single intersection child");
    }

    sort_access_plans(&mut out);
    AccessPlan::Intersection(out)
}

fn sort_access_plans(plans: &mut [AccessPlan]) {
    plans.sort_by_key(plan_sort_key);
}

fn plan_sort_key(plan: &AccessPlan) -> String {
    match plan {
        AccessPlan::Path(path) => access_path_sort_key(path),
        AccessPlan::Union(children) => format!(
            "U:{}",
            children
                .iter()
                .map(plan_sort_key)
                .collect::<Vec<_>>()
                .join("|")
        ),
        AccessPlan::Intersection(children) => format!(
            "I:{}",
            children
                .iter()
                .map(plan_sort_key)
                .collect::<Vec<_>>()
                .join("|")
        ),
    }
}

fn access_path_sort_key(path: &AccessPath) -> String {
    match path {
        AccessPath::ByKey(key) => format!("K:{key:?}"),
        AccessPath::ByKeys(keys) => format!("Ks:{keys:?}"),
        AccessPath::KeyRange { start, end } => format!("R:{start:?}-{end:?}"),
        AccessPath::IndexPrefix { index, values } => {
            format!("I:{}:{}:{values:?}", index.store, index.fields.join(","))
        }
        AccessPath::FullScan => "F".to_string(),
    }
}

fn is_full_scan(plan: &AccessPlan) -> bool {
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
