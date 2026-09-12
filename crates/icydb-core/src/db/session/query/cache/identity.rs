//! Module: db::session::query::cache::identity
//! Responsibility: shared query-plan cache identity DTOs.
//! Does not own: cache storage, plan lookup/insert behavior, or query planning.
//! Boundary: defines stable in-heap cache key dimensions consumed by the
//! session query cache owner.

use crate::db::{
    QueryError,
    commit::CommitSchemaFingerprint,
    executor::EntityAuthority,
    query::{
        intent::{StructuralQuery, StructuralQueryCacheKey},
        plan::PreparedQueryParameterContract,
        preparation::PreparationWork,
    },
    schema::{
        AcceptedSchemaRevision, AcceptedSchemaRuntimeRootIdentity, AcceptedSchemaSnapshot,
        SchemaVersion,
    },
    session::AcceptedSchemaCatalogContext,
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

    pub(super) const fn from_accepted_schema_with_fingerprint(
        accepted_schema: &AcceptedSchemaSnapshot,
        fingerprint: CommitSchemaFingerprint,
        runtime_root: AcceptedSchemaRuntimeRootIdentity,
        revision: AcceptedSchemaRevision,
    ) -> Self {
        Self::new(
            runtime_root,
            revision,
            accepted_schema.persisted_snapshot().version(),
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            fingerprint,
        )
    }

    fn from_catalog(catalog: &AcceptedSchemaCatalogContext) -> Self {
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

#[derive(Clone, Copy)]
pub(super) struct QueryPlanAcceptedSchema<'schema> {
    accepted_schema: &'schema AcceptedSchemaSnapshot,
    identity: SchemaCacheIdentity,
}

impl<'schema> QueryPlanAcceptedSchema<'schema> {
    pub(super) const fn from_accepted_schema_with_fingerprint(
        accepted_schema: &'schema AcceptedSchemaSnapshot,
        fingerprint: CommitSchemaFingerprint,
        runtime_root: AcceptedSchemaRuntimeRootIdentity,
        revision: AcceptedSchemaRevision,
    ) -> Self {
        Self {
            accepted_schema,
            identity: SchemaCacheIdentity::from_accepted_schema_with_fingerprint(
                accepted_schema,
                fingerprint,
                runtime_root,
                revision,
            ),
        }
    }

    pub(super) fn from_catalog(catalog: &'schema AcceptedSchemaCatalogContext) -> Self {
        Self {
            accepted_schema: catalog.snapshot(),
            identity: SchemaCacheIdentity::from_catalog(catalog),
        }
    }

    pub(super) const fn accepted_schema(self) -> &'schema AcceptedSchemaSnapshot {
        self.accepted_schema
    }

    pub(super) const fn identity(self) -> SchemaCacheIdentity {
        self.identity
    }

    pub(super) const fn fingerprint(self) -> CommitSchemaFingerprint {
        self.identity.fingerprint
    }
}

impl QueryPlanCacheKey {
    // Assemble the authority shell once; callers only choose structural identity.
    fn from_authority_cache_inputs(
        authority: EntityAuthority,
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
        authority: EntityAuthority,
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
        authority: EntityAuthority,
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
mod tests {
    use super::SchemaCacheIdentity;
    use crate::db::{
        integrity::DatabaseIncarnationId,
        schema::{
            AcceptedSchemaRevision, AcceptedSchemaRuntimeRootIdentity,
            AcceptedSchemaRuntimeStoreRoot, SchemaVersion,
            empty_accepted_schema_candidate_for_tests,
        },
    };

    fn runtime_root(fill: u8) -> AcceptedSchemaRuntimeRootIdentity {
        let candidate = empty_accepted_schema_candidate_for_tests(
            "test::CacheIdentity",
            AcceptedSchemaRevision::INITIAL,
        );
        AcceptedSchemaRuntimeRootIdentity::from_store_roots(
            DatabaseIncarnationId::for_tests(fill),
            &[AcceptedSchemaRuntimeStoreRoot::new(
                "test::CacheIdentity",
                Some(candidate.root()),
            )],
        )
        .expect("cache identity root should admit")
    }

    #[test]
    fn schema_cache_identity_rejects_mixed_runtime_roots() {
        let first = SchemaCacheIdentity::new(
            runtime_root(1),
            AcceptedSchemaRevision::INITIAL,
            SchemaVersion::initial(),
            1,
            [7; 16],
        );
        let second = SchemaCacheIdentity::new(
            runtime_root(2),
            AcceptedSchemaRevision::INITIAL,
            SchemaVersion::initial(),
            1,
            [7; 16],
        );

        assert_ne!(first, second);
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(QueryPlanCacheKey {
Self{entity_path,schema_identity,visibility,structural_query} => [entity_path,schema_identity,visibility,structural_query],
});
crate::retained::retained_copy!(QueryPlanVisibility);
crate::retained::retained_copy!(SchemaCacheIdentity);
