use crate::{
    db::query::plan::{AccessPath, AccessPlan, AccessPlanError, PlanError, canonical},
    model::entity::EntityModel,
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
    model: &EntityModel,
    access: AccessPlan<Value>,
) -> Result<AccessPlan<E::Key>, PlanError> {
    access.into_executable::<E>(model)
}

// Convert model-level key values into typed entity keys.
pub(crate) fn coerce_entity_key<E: EntityKind>(
    model: &EntityModel,
    key: &Value,
) -> Result<E::Key, PlanError> {
    E::Key::from_value(key).ok_or_else(|| {
        PlanError::from(AccessPlanError::PrimaryKeyMismatch {
            field: model.primary_key.name.to_string(),
            key: key.clone(),
        })
    })
}

impl AccessPlan<Value> {
    /// Convert model-level access plans into typed executable access plans.
    pub(crate) fn into_executable<E: EntityKind>(
        self,
        model: &EntityModel,
    ) -> Result<AccessPlan<E::Key>, PlanError> {
        match self {
            Self::Path(path) => Ok(AccessPlan::path(path.into_executable::<E>(model)?)),
            Self::Union(children) => {
                let mut out = Vec::with_capacity(children.len());
                for child in children {
                    out.push(child.into_executable::<E>(model)?);
                }

                Ok(AccessPlan::Union(out))
            }
            Self::Intersection(children) => {
                let mut out = Vec::with_capacity(children.len());
                for child in children {
                    out.push(child.into_executable::<E>(model)?);
                }

                Ok(AccessPlan::Intersection(out))
            }
        }
    }
}

impl AccessPath<Value> {
    /// Convert one model-level access path into a typed executable access path.
    pub(crate) fn into_executable<E: EntityKind>(
        self,
        model: &EntityModel,
    ) -> Result<AccessPath<E::Key>, PlanError> {
        match self {
            Self::ByKey(key) => Ok(AccessPath::ByKey(coerce_entity_key::<E>(model, &key)?)),
            Self::ByKeys(keys) => {
                let mut out = Vec::with_capacity(keys.len());
                for key in keys {
                    out.push(coerce_entity_key::<E>(model, &key)?);
                }

                Ok(AccessPath::ByKeys(out))
            }
            Self::KeyRange { start, end } => Ok(AccessPath::KeyRange {
                start: coerce_entity_key::<E>(model, &start)?,
                end: coerce_entity_key::<E>(model, &end)?,
            }),
            Self::IndexPrefix { index, values } => Ok(AccessPath::IndexPrefix { index, values }),
            Self::IndexRange { spec } => Ok(AccessPath::IndexRange { spec }),
            Self::FullScan => Ok(AccessPath::FullScan),
        }
    }
}
