use crate::{
    db::{
        access::{AccessPlan, AccessPlanError, normalize_access_plan_value},
        query::plan::PlanError,
    },
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
pub(crate) fn build_access_plan_from_keys<K>(access: &KeyAccess<K>) -> AccessPlan<Value>
where
    K: FieldValue,
{
    // Phase 1: map typed keys into model-level Value access paths.
    let plan = match access {
        KeyAccess::Single(key) => AccessPlan::by_key(key.to_value()),
        KeyAccess::Many(keys) => {
            let mut values = Vec::with_capacity(keys.len());
            values.extend(keys.iter().map(FieldValue::to_value));

            AccessPlan::by_keys(values)
        }
    };

    // Phase 2: canonicalize the access shape via the shared access boundary.
    normalize_access_plan_value(plan)
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
        self.map_keys(|key| coerce_entity_key::<E>(model, &key))
    }
}
