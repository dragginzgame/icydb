//! Module: db::query::intent::key_access
//! Responsibility: typed primary-key access hints owned by query intent.
//! Does not own: full logical-plan validation or access-path execution.
//! Boundary: lowers key-only builder state into planner-owned access plans.

use crate::{
    db::access::{AccessPlan, normalize_access_plan_value},
    traits::KeyValueCodec,
    value::Value,
};

///
/// KeyAccess
///
/// Primary-key-only access hints for query planning.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum KeyAccess<K> {
    Single(K),
    Many(Vec<K>),
}

///
/// KeyAccessKind
///
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
///
/// Tracks key-only access plus its origin for intent validation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KeyAccessState<K> {
    pub(in crate::db::query::intent) kind: KeyAccessKind,
    pub(in crate::db::query::intent) access: KeyAccess<K>,
}

/// Build a model-level access plan for key-only intents.
pub(crate) fn build_access_plan_from_keys<K>(access: &KeyAccess<K>) -> AccessPlan<Value>
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
