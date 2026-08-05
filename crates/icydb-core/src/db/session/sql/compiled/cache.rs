//! Compiled SQL schema fingerprints and prepared-plan cache entries.
//! Does not own: compiled command variants or execution context handoff.

use crate::db::{
    commit::CommitSchemaFingerprint,
    executor::SharedPreparedExecutionPlan,
    index::UserIndexPrefixCardinalityKey,
    session::{AcceptedSchemaCatalogContext, query::StructuralProjectionContract},
};
use std::rc::Rc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct SqlCompiledSchemaFingerprint {
    method_version: u8,
    fingerprint: CommitSchemaFingerprint,
}

impl SqlCompiledSchemaFingerprint {
    #[must_use]
    pub(in crate::db) const fn new(
        method_version: u8,
        fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            method_version,
            fingerprint,
        }
    }

    #[must_use]
    pub(in crate::db) fn from_catalog(catalog: &AcceptedSchemaCatalogContext) -> Self {
        Self::new(catalog.fingerprint_method_version(), catalog.fingerprint())
    }

    #[must_use]
    pub(in crate::db) fn matches(self, other: Self) -> bool {
        self.method_version == other.method_version && self.fingerprint == other.fingerprint
    }
}

#[derive(Debug)]
pub(in crate::db) struct SqlSelectPlanCacheEntry {
    pub(super) schema_fingerprint: SqlCompiledSchemaFingerprint,
    prepared_plan: SharedPreparedExecutionPlan,
    projection: StructuralProjectionContract,
}

impl SqlSelectPlanCacheEntry {
    #[must_use]
    pub(in crate::db) const fn new(
        schema_fingerprint: SqlCompiledSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
    ) -> Self {
        Self {
            schema_fingerprint,
            prepared_plan,
            projection,
        }
    }

    #[must_use]
    pub(in crate::db) fn prepared_plan(&self) -> SharedPreparedExecutionPlan {
        self.prepared_plan.clone()
    }

    #[must_use]
    pub(in crate::db) fn projection(&self) -> StructuralProjectionContract {
        self.projection.clone()
    }
}

#[derive(Debug)]
pub(in crate::db) struct SqlGlobalAggregatePlanCacheEntry {
    pub(super) schema_fingerprint: SqlCompiledSchemaFingerprint,
    prepared_plan: SharedPreparedExecutionPlan,
}

impl SqlGlobalAggregatePlanCacheEntry {
    #[must_use]
    pub(in crate::db) const fn new(
        schema_fingerprint: SqlCompiledSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
    ) -> Self {
        Self {
            schema_fingerprint,
            prepared_plan,
        }
    }

    #[must_use]
    pub(in crate::db) fn prepared_plan(&self) -> SharedPreparedExecutionPlan {
        self.prepared_plan.clone()
    }
}

#[derive(Debug)]
pub(in crate::db) struct SqlGlobalAggregateCountPlanCacheEntry {
    pub(super) schema_fingerprint: SqlCompiledSchemaFingerprint,
    prefix_keys: Rc<[UserIndexPrefixCardinalityKey]>,
}

impl SqlGlobalAggregateCountPlanCacheEntry {
    #[must_use]
    pub(in crate::db) const fn new(
        schema_fingerprint: SqlCompiledSchemaFingerprint,
        prefix_keys: Rc<[UserIndexPrefixCardinalityKey]>,
    ) -> Self {
        Self {
            schema_fingerprint,
            prefix_keys,
        }
    }

    #[must_use]
    pub(in crate::db) fn prefix_keys(&self) -> &[UserIndexPrefixCardinalityKey] {
        self.prefix_keys.as_ref()
    }
}
