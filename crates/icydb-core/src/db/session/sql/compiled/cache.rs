//! Compiled SQL schema fingerprints and prepared-plan cache entries.
//! Does not own: compiled command variants or execution context handoff.

use crate::db::{
    commit::CommitSchemaFingerprint,
    executor::{ExactCardinalityTarget, SharedPreparedExecutionPlan},
    index::{IndexId, UserIndexPrefixCardinalityKey},
    session::{AcceptedSchemaCatalogContext, query::StructuralProjectionContract},
};
use std::{ops::Bound, rc::Rc};

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
    plan: SqlGlobalAggregateCachedPlan,
}

impl SqlGlobalAggregatePlanCacheEntry {
    #[must_use]
    pub(in crate::db) const fn new(
        schema_fingerprint: SqlCompiledSchemaFingerprint,
        plan: SqlGlobalAggregateCachedPlan,
    ) -> Self {
        Self {
            schema_fingerprint,
            plan,
        }
    }

    #[must_use]
    pub(in crate::db) fn exact_cardinality_target(&self) -> Option<ExactCardinalityTarget<'_>> {
        self.plan.exact_cardinality_target()
    }

    #[must_use]
    pub(in crate::db) fn prepared_plan(&self) -> Option<SharedPreparedExecutionPlan> {
        self.plan.prepared_plan()
    }
}

#[derive(Clone, Debug)]
pub(in crate::db) enum SqlGlobalAggregateCachedPlan {
    ExactEntityCardinality,
    ExactUserIndexFirstComponentDistinct(IndexId),
    ExactUserIndexFirstComponentRange {
        index_id: IndexId,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    },
    ExactUserIndexPrefixes(Rc<[UserIndexPrefixCardinalityKey]>),
    Prepared(SharedPreparedExecutionPlan),
}

impl SqlGlobalAggregateCachedPlan {
    #[must_use]
    pub(in crate::db) const fn exact_entity_cardinality() -> Self {
        Self::ExactEntityCardinality
    }

    #[must_use]
    pub(in crate::db) const fn exact_user_index_prefixes(
        prefix_keys: Rc<[UserIndexPrefixCardinalityKey]>,
    ) -> Self {
        Self::ExactUserIndexPrefixes(prefix_keys)
    }

    #[must_use]
    pub(in crate::db) const fn exact_user_index_first_component_distinct(
        index_id: IndexId,
    ) -> Self {
        Self::ExactUserIndexFirstComponentDistinct(index_id)
    }

    #[must_use]
    pub(in crate::db) const fn prepared(prepared_plan: SharedPreparedExecutionPlan) -> Self {
        Self::Prepared(prepared_plan)
    }

    #[must_use]
    pub(in crate::db) fn exact_cardinality_target(&self) -> Option<ExactCardinalityTarget<'_>> {
        match self {
            Self::ExactEntityCardinality => Some(ExactCardinalityTarget::Entity),
            Self::ExactUserIndexFirstComponentDistinct(index_id) => Some(
                ExactCardinalityTarget::UserIndexFirstComponentDistinct(*index_id),
            ),
            Self::ExactUserIndexFirstComponentRange {
                index_id,
                lower,
                upper,
            } => Some(ExactCardinalityTarget::UserIndexFirstComponentRange {
                index_id: *index_id,
                lower,
                upper,
            }),
            Self::ExactUserIndexPrefixes(prefix_keys) => Some(
                ExactCardinalityTarget::UserIndexPrefixes(prefix_keys.as_ref()),
            ),
            Self::Prepared(_) => None,
        }
    }

    #[must_use]
    pub(in crate::db) fn prepared_plan(&self) -> Option<SharedPreparedExecutionPlan> {
        match self {
            Self::Prepared(prepared_plan) => Some(prepared_plan.clone()),
            Self::ExactEntityCardinality
            | Self::ExactUserIndexFirstComponentDistinct(_)
            | Self::ExactUserIndexFirstComponentRange { .. }
            | Self::ExactUserIndexPrefixes(_) => None,
        }
    }
}
