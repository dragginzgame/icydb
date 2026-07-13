//! Module: db::query::intent::key_access
//! Responsibility: typed primary-key access hints owned by query intent.
//! Does not own: full logical-plan validation or access-path execution.
//! Boundary: lowers key-only builder state into planner-owned access plans.

use crate::{
    db::KeyValueCodec,
    db::access::{AccessPlan, normalize_access_plan_value},
    db::query::plan::{PrimaryKeyInputResourceSummary, primary_key_input_resource_from_value_list},
    value::Value,
};

///
/// KeyAccess
///
/// Primary-key-only access hints for query planning.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::query) enum KeyAccess<K> {
    Single(K),
    Many(Vec<K>),
}

///
/// KeyAccessKind
///
/// Identifies which key-only builder set the access path.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query) enum KeyAccessKind {
    Single,
    Many,
    Only,
}

///
/// KeyAccessState
///
/// Tracks key-only access plus its origin for intent validation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::query) struct KeyAccessState<K> {
    pub(in crate::db::query::intent) kind: KeyAccessKind,
    pub(in crate::db::query::intent) access: KeyAccess<K>,
}

///
/// KeyAccessPlanningProjection
///
/// Planner-facing key-access projection that keeps the normalized access path
/// and pre-canonicalization input resource facts from drifting apart.
///

pub(in crate::db::query) struct KeyAccessPlanningProjection {
    access_plan: AccessPlan<Value>,
    input_resource: Option<PrimaryKeyInputResourceSummary>,
}

impl KeyAccessPlanningProjection {
    #[must_use]
    pub(in crate::db::query) fn into_parts(
        self,
    ) -> (AccessPlan<Value>, Option<PrimaryKeyInputResourceSummary>) {
        (self.access_plan, self.input_resource)
    }
}

/// Build a model-level access plan for key-only intents.
pub(in crate::db::query) fn build_access_plan_from_keys<K>(
    access: &KeyAccess<K>,
) -> AccessPlan<Value>
where
    K: KeyValueCodec,
{
    // Phase 1: map typed keys into model-level Value access paths.
    let plan = match access {
        KeyAccess::Single(key) => AccessPlan::by_key(key.to_key_value()),
        KeyAccess::Many(keys) => {
            let mut values = Vec::with_capacity(keys.len());
            values.extend(keys.iter().map(KeyValueCodec::to_key_value));

            AccessPlan::by_keys(values)
        }
    };

    // Phase 2: canonicalize the access shape via the shared access boundary.
    normalize_access_plan_value(plan)
}

/// Build planner access plus raw input-work facts in one typed key pass.
pub(in crate::db::query) fn project_key_access_for_planning<K>(
    access: &KeyAccess<K>,
) -> KeyAccessPlanningProjection
where
    K: KeyValueCodec,
{
    let (plan, input_resource) = match access {
        KeyAccess::Single(key) => (AccessPlan::by_key(key.to_key_value()), None),
        KeyAccess::Many(keys) => {
            let mut values = Vec::with_capacity(keys.len());
            values.extend(keys.iter().map(KeyValueCodec::to_key_value));
            let input_resource = primary_key_input_resource_from_value_list(&values);

            (AccessPlan::by_keys(values), input_resource)
        }
    };

    KeyAccessPlanningProjection {
        access_plan: normalize_access_plan_value(plan),
        input_resource,
    }
}
