//! Module: db::session::query::cache::identity
//! Responsibility: shared query-plan cache identity and compile attribution
//! DTOs.
//! Does not own: cache storage, plan lookup/insert behavior, or query planning.
//! Boundary: defines stable in-heap cache key dimensions and measurement
//! buckets consumed by the session query cache owner.

#[cfg(any(feature = "diagnostics", feature = "sql"))]
use crate::db::diagnostics::measure_local_instruction_delta as measure_query_plan_compile_stage;
use crate::db::{
    commit::CommitSchemaFingerprint,
    executor::EntityAuthority,
    query::intent::{StructuralQuery, StructuralQueryCacheKey},
    schema::{
        AcceptedCatalogIdentity, AcceptedSchemaRevision, AcceptedSchemaSnapshot, SchemaVersion,
    },
    session::AcceptedSchemaCatalogContext,
};

#[cfg(not(any(feature = "diagnostics", feature = "sql")))]
fn measure_query_plan_compile_stage<T>(run: impl FnOnce() -> T) -> (u64, T) {
    (0, run())
}

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
    entity_path: &'static str,
    schema_identity: SchemaCacheIdentity,
    visibility: QueryPlanVisibility,
    structural_query: StructuralQueryCacheKey,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct SchemaCacheIdentity {
    revision: AcceptedSchemaRevision,
    version: SchemaVersion,
    fingerprint_method_version: u8,
    fingerprint: CommitSchemaFingerprint,
}

impl SchemaCacheIdentity {
    pub(super) const fn new(
        revision: AcceptedSchemaRevision,
        version: SchemaVersion,
        fingerprint_method_version: u8,
        fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            revision,
            version,
            fingerprint_method_version,
            fingerprint,
        }
    }

    #[cfg(feature = "sql")]
    pub(super) const fn from_accepted_schema_with_fingerprint(
        accepted_schema: &AcceptedSchemaSnapshot,
        fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self::new(
            AcceptedSchemaRevision::NONE,
            accepted_schema.persisted_snapshot().version(),
            crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
            fingerprint,
        )
    }

    pub(super) const fn from_accepted_catalog_identity(identity: AcceptedCatalogIdentity) -> Self {
        Self::new(
            identity.accepted_schema_revision(),
            identity.accepted_schema_version(),
            identity.fingerprint_method_version(),
            identity.accepted_schema_fingerprint(),
        )
    }

    const fn from_catalog(catalog: &AcceptedSchemaCatalogContext) -> Self {
        Self::new(
            catalog.revision(),
            catalog.schema_version(),
            catalog.fingerprint_method_version(),
            catalog.fingerprint(),
        )
    }

    pub(super) const fn fingerprint(self) -> CommitSchemaFingerprint {
        self.fingerprint
    }

    pub(super) fn same_version(self, other: Self) -> bool {
        self.revision == other.revision && self.version == other.version
    }

    pub(super) fn same_fingerprint(self, other: Self) -> bool {
        self.fingerprint_method_version == other.fingerprint_method_version
            && self.fingerprint == other.fingerprint
    }
}

#[derive(Clone, Copy)]
pub(super) struct QueryPlanAcceptedSchema<'schema> {
    accepted_schema: &'schema AcceptedSchemaSnapshot,
    identity: SchemaCacheIdentity,
}

impl<'schema> QueryPlanAcceptedSchema<'schema> {
    #[cfg(feature = "sql")]
    pub(super) const fn from_accepted_schema_with_fingerprint(
        accepted_schema: &'schema AcceptedSchemaSnapshot,
        fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            accepted_schema,
            identity: SchemaCacheIdentity::from_accepted_schema_with_fingerprint(
                accepted_schema,
                fingerprint,
            ),
        }
    }

    pub(super) const fn from_catalog(catalog: &'schema AcceptedSchemaCatalogContext) -> Self {
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

///
/// QueryPlanCacheAttribution
///
/// QueryPlanCacheAttribution reports whether one shared query-plan lookup hit
/// or missed without exposing the cache map itself to diagnostics callers.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct QueryPlanCacheAttribution {
    pub hits: u64,
    pub misses: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct QueryPlanCompilePhaseAttribution {
    pub schema_catalog: u64,
    pub schema_info: u64,
    pub prepare: u64,
    pub cache_key: u64,
    pub cache_lookup: u64,
    pub plan_build: u64,
    pub cache_insert: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum QueryPlanCompilePhase {
    SchemaCatalog,
    SchemaInfo,
    Prepare,
    CacheKey,
    CacheLookup,
    PlanBuild,
    CacheInsert,
}

pub(super) struct QueryPlanCompilePhaseRecorder<'a> {
    attribution: Option<&'a mut QueryPlanCompilePhaseAttribution>,
}

impl QueryPlanCacheAttribution {
    #[must_use]
    pub(super) const fn hit() -> Self {
        Self { hits: 1, misses: 0 }
    }

    #[must_use]
    pub(super) const fn miss() -> Self {
        Self { hits: 0, misses: 1 }
    }
}

impl QueryPlanCompilePhaseAttribution {
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    pub(in crate::db) const fn planner_local_instructions(self) -> u64 {
        self.schema_info
            .saturating_add(self.prepare)
            .saturating_add(self.cache_key)
            .saturating_add(self.cache_lookup)
            .saturating_add(self.plan_build)
            .saturating_add(self.cache_insert)
    }

    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    pub(in crate::db) const fn merge(&mut self, other: Self) {
        self.schema_catalog = self.schema_catalog.saturating_add(other.schema_catalog);
        self.schema_info = self.schema_info.saturating_add(other.schema_info);
        self.prepare = self.prepare.saturating_add(other.prepare);
        self.cache_key = self.cache_key.saturating_add(other.cache_key);
        self.cache_lookup = self.cache_lookup.saturating_add(other.cache_lookup);
        self.plan_build = self.plan_build.saturating_add(other.plan_build);
        self.cache_insert = self.cache_insert.saturating_add(other.cache_insert);
    }

    const fn record(&mut self, phase: QueryPlanCompilePhase, local_instructions: u64) {
        match phase {
            QueryPlanCompilePhase::SchemaCatalog => {
                self.schema_catalog = self.schema_catalog.saturating_add(local_instructions);
            }
            QueryPlanCompilePhase::SchemaInfo => {
                self.schema_info = self.schema_info.saturating_add(local_instructions);
            }
            QueryPlanCompilePhase::Prepare => {
                self.prepare = self.prepare.saturating_add(local_instructions);
            }
            QueryPlanCompilePhase::CacheKey => {
                self.cache_key = self.cache_key.saturating_add(local_instructions);
            }
            QueryPlanCompilePhase::CacheLookup => {
                self.cache_lookup = self.cache_lookup.saturating_add(local_instructions);
            }
            QueryPlanCompilePhase::PlanBuild => {
                self.plan_build = self.plan_build.saturating_add(local_instructions);
            }
            QueryPlanCompilePhase::CacheInsert => {
                self.cache_insert = self.cache_insert.saturating_add(local_instructions);
            }
        }
    }
}

impl QueryPlanCompilePhaseRecorder<'_> {
    pub(super) const fn none() -> Self {
        Self { attribution: None }
    }

    #[cfg(feature = "diagnostics")]
    pub(super) const fn new(
        attribution: &mut QueryPlanCompilePhaseAttribution,
    ) -> QueryPlanCompilePhaseRecorder<'_> {
        QueryPlanCompilePhaseRecorder {
            attribution: Some(attribution),
        }
    }

    pub(super) fn measure<T>(
        &mut self,
        phase: QueryPlanCompilePhase,
        run: impl FnOnce() -> T,
    ) -> T {
        if let Some(attribution) = &mut self.attribution {
            let (local_instructions, output) = measure_query_plan_compile_stage(run);
            attribution.record(phase, local_instructions);

            output
        } else {
            run()
        }
    }
}

impl QueryPlanCacheKey {
    pub(super) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    pub(super) const fn visibility(&self) -> QueryPlanVisibility {
        self.visibility
    }

    pub(super) const fn schema_identity(&self) -> SchemaCacheIdentity {
        self.schema_identity
    }

    pub(super) const fn structural_query(&self) -> &StructuralQueryCacheKey {
        &self.structural_query
    }

    // Assemble the canonical cache-key shell once so the test and
    // normalized-predicate constructors only decide which structural query key
    // they feed into the shared session cache identity.
    fn from_authority_cache_inputs(
        authority: EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        structural_query: StructuralQueryCacheKey,
    ) -> Self {
        Self::from_entity_path_cache_inputs(
            authority.entity_path(),
            schema_identity,
            visibility,
            structural_query,
        )
    }

    const fn from_entity_path_cache_inputs(
        entity_path: &'static str,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        structural_query: StructuralQueryCacheKey,
    ) -> Self {
        Self {
            entity_path,
            schema_identity,
            visibility,
            structural_query,
        }
    }

    #[cfg(test)]
    pub(super) fn for_authority(
        authority: EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
    ) -> Self {
        Self::from_authority_cache_inputs(
            authority,
            schema_identity,
            visibility,
            query.structural_cache_key(),
        )
    }

    pub(super) fn for_authority_with_normalized_predicate_fingerprint(
        authority: EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        normalized_predicate_fingerprint: Option<[u8; 32]>,
    ) -> Self {
        Self::from_authority_cache_inputs(
            authority,
            schema_identity,
            visibility,
            query.structural_cache_key_with_normalized_predicate_fingerprint(
                normalized_predicate_fingerprint,
            ),
        )
    }

    pub(super) fn for_entity_path_with_normalized_predicate_fingerprint(
        entity_path: &'static str,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        normalized_predicate_fingerprint: Option<[u8; 32]>,
    ) -> Self {
        Self::from_entity_path_cache_inputs(
            entity_path,
            schema_identity,
            visibility,
            query.structural_cache_key_with_normalized_predicate_fingerprint(
                normalized_predicate_fingerprint,
            ),
        )
    }
}
