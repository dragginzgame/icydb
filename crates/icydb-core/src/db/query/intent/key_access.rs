use crate::{
    db::query::plan::{AccessPath, AccessPlan, PlanError, canonical},
    traits::{EntityKind, FieldValue},
    value::Value,
};

///
/// KeyAccess
/// Primary-key-only access hints for query planning.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum KeyAccess<K> {
    Single(K),
    Many(Vec<K>),
}

///
/// KeyAccessKind
/// Identifies which key-only builder set the access path.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyAccessKind {
    Single,
    Many,
    Only,
}

///
/// KeyAccessState
/// Tracks key-only access plus its origin for intent validation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KeyAccessState<K> {
    pub kind: KeyAccessKind,
    pub access: KeyAccess<K>,
}

// Build a model-level access plan for key-only intents.
pub(crate) fn access_plan_from_keys_value<K>(access: &KeyAccess<K>) -> AccessPlan<Value>
where
    K: FieldValue,
{
    match access {
        KeyAccess::Single(key) => AccessPlan::path(AccessPath::ByKey(key.to_value())),
        KeyAccess::Many(keys) => {
            let mut values: Vec<Value> = keys.iter().map(FieldValue::to_value).collect();
            canonical::canonicalize_key_values(&mut values);
            if let Some(first) = values.first()
                && values.len() == 1
            {
                return AccessPlan::path(AccessPath::ByKey(first.clone()));
            }

            AccessPlan::path(AccessPath::ByKeys(values))
        }
    }
}

// Convert model-level access plans into entity-keyed access plans.
pub(crate) fn access_plan_to_entity_keys<E: EntityKind>(
    model: &crate::model::entity::EntityModel,
    access: AccessPlan<Value>,
) -> Result<AccessPlan<E::Key>, PlanError> {
    let plan = match access {
        AccessPlan::Path(path) => AccessPlan::path(access_path_to_entity_keys::<E>(model, *path)?),
        AccessPlan::Union(children) => {
            let mut out = Vec::with_capacity(children.len());
            for child in children {
                out.push(access_plan_to_entity_keys::<E>(model, child)?);
            }
            AccessPlan::Union(out)
        }
        AccessPlan::Intersection(children) => {
            let mut out = Vec::with_capacity(children.len());
            for child in children {
                out.push(access_plan_to_entity_keys::<E>(model, child)?);
            }
            AccessPlan::Intersection(out)
        }
    };

    Ok(plan)
}

// Convert model-level access paths into entity-keyed access paths.
pub(crate) fn access_path_to_entity_keys<E: EntityKind>(
    model: &crate::model::entity::EntityModel,
    path: AccessPath<Value>,
) -> Result<AccessPath<E::Key>, PlanError> {
    let path = match path {
        AccessPath::ByKey(key) => AccessPath::ByKey(coerce_entity_key::<E>(model, &key)?),
        AccessPath::ByKeys(keys) => {
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                out.push(coerce_entity_key::<E>(model, &key)?);
            }
            AccessPath::ByKeys(out)
        }
        AccessPath::KeyRange { start, end } => AccessPath::KeyRange {
            start: coerce_entity_key::<E>(model, &start)?,
            end: coerce_entity_key::<E>(model, &end)?,
        },
        AccessPath::IndexPrefix { index, values } => AccessPath::IndexPrefix { index, values },
        AccessPath::IndexRange {
            index,
            prefix,
            lower,
            upper,
        } => AccessPath::IndexRange {
            index,
            prefix,
            lower,
            upper,
        },
        AccessPath::FullScan => AccessPath::FullScan,
    };

    Ok(path)
}

// Convert model-level key values into typed entity keys.
pub(crate) fn coerce_entity_key<E: EntityKind>(
    model: &crate::model::entity::EntityModel,
    key: &Value,
) -> Result<E::Key, PlanError> {
    E::Key::from_value(key).ok_or_else(|| PlanError::PrimaryKeyMismatch {
        field: model.primary_key.name.to_string(),
        key: key.clone(),
    })
}
