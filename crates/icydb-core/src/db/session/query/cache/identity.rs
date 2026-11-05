//! Module: db::session::query::cache::identity
//! Responsibility: shared query-plan cache identity DTOs.
//! Does not own: cache storage, plan lookup/insert behavior, or query planning.
//! Boundary: defines stable in-heap cache key dimensions consumed by the
//! session query cache owner.

use crate::db::{
    QueryError,
    commit::CommitSchemaFingerprint,
    executor::{AcceptedSchemaCatalogContext, EntityAuthority},
    query::{
        intent::{StructuralQuery, StructuralQueryCacheKey},
        plan::PreparedQueryParameterContract,
        preparation::PreparationWork,
    },
    schema::{
        AcceptedSchemaRevision, AcceptedSchemaRuntimeRootIdentity, AcceptedSchemaSnapshot,
        SchemaVersion,
    },
};
use std::rc::Rc;

///
/// QueryPlanVisibility
///
/// QueryPlanVisibility records whether a store's recovered index state can
/// participate in planning-visible secondary index selection.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) enum QueryPlanVisibility {
    StoreNotReady,
    StoreReady,
    /// Recovered store authority with secondary indexes deliberately excluded.
    #[cfg(feature = "sql")]
    PrimaryOnly,
}

///
/// QueryPlanCacheKey
///
/// QueryPlanCacheKey is the session-level identity for one shared prepared
/// query plan. It includes store visibility and schema identity so cached
/// plans cannot cross lifecycle or schema boundaries.
///

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::db) struct QueryPlanCacheKey {
    entity_path: Rc<str>,
    schema_identity: SchemaCacheIdentity,
    visibility: QueryPlanVisibility,
    structural_query: StructuralQueryCacheKey,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct SchemaCacheIdentity {
    runtime_root: AcceptedSchemaRuntimeRootIdentity,
    revision: AcceptedSchemaRevision,
    version: SchemaVersion,
    fingerprint_method_version: u8,
    fingerprint: CommitSchemaFingerprint,
}

impl SchemaCacheIdentity {
    pub(super) const fn new(
        runtime_root: AcceptedSchemaRuntimeRootIdentity,
        revision: AcceptedSchemaRevision,
        version: SchemaVersion,
        fingerprint_method_version: u8,
        fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            runtime_root,
            revision,
            version,
            fingerprint_method_version,
            fingerprint,
        }
    }

    pub(super) fn from_accepted_authority_with_fingerprint(
        accepted_schema: &AcceptedSchemaSnapshot,
        fingerprint: CommitSchemaFingerprint,
        authority: &EntityAuthority,
    ) -> Self {
        Self::new(
            authority.accepted_runtime_root_identity(),
            authority.accepted_schema_authority().revision(),
            accepted_schema.persisted_snapshot().version(),
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            fingerprint,
        )
    }

    pub(super) fn from_catalog(catalog: &AcceptedSchemaCatalogContext) -> Self {
        Self::new(
            catalog.runtime_root_identity(),
            catalog.revision(),
            catalog.schema_version(),
            catalog.fingerprint_method_version(),
            catalog.fingerprint(),
        )
    }

    pub(super) const fn fingerprint(self) -> CommitSchemaFingerprint {
        self.fingerprint
    }
}

impl QueryPlanCacheKey {
    // Assemble the authority shell once; callers only choose structural identity.
    fn from_authority_cache_inputs(
        authority: &EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        structural_query: StructuralQueryCacheKey,
    ) -> Self {
        Self {
            entity_path: authority.entity_path_handle(),
            schema_identity,
            visibility,
            structural_query,
        }
    }

    pub(super) fn for_authority_with_normalized_predicate_fingerprint(
        authority: &EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        normalized_predicate_fingerprint: Option<[u8; 32]>,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Ok(Self::from_authority_cache_inputs(
            authority,
            schema_identity,
            visibility,
            query.structural_cache_key_with_normalized_predicate_fingerprint(
                normalized_predicate_fingerprint,
                work,
            )?,
        ))
    }

    pub(super) fn for_authority_with_parameter_contract(
        authority: &EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        parameter_contract: PreparedQueryParameterContract,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Ok(Self::from_authority_cache_inputs(
            authority,
            schema_identity,
            visibility,
            query.structural_cache_key_with_parameter_contract(parameter_contract, work)?,
        ))
    }
}

#[cfg(test)]
mod tests;

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(QueryPlanCacheKey {
Self{entity_path,schema_identity,visibility,structural_query} => [entity_path,schema_identity,visibility,structural_query],
});
crate::retained::retained_copy!(QueryPlanVisibility);
crate::retained::retained_copy!(SchemaCacheIdentity);
