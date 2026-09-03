//! Module: db::session::query::cache::identity
//! Responsibility: shared query-plan cache identity and compile attribution
//! DTOs.
//! Does not own: cache storage, plan lookup/insert behavior, or query planning.
//! Boundary: defines stable in-heap cache key dimensions and measurement
//! buckets consumed by the session query cache owner.

use crate::db::diagnostics::measure_local_instruction_delta as measure_query_plan_compile_stage;
use crate::db::{
    commit::CommitSchemaFingerprint,
    executor::EntityAuthority,
    query::{
        intent::{StructuralQuery, StructuralQueryCacheKey},
        plan::PreparedQueryParameterContract,
    },
    schema::{
        AcceptedSchemaRevision, AcceptedSchemaRuntimeRootIdentity, AcceptedSchemaSnapshot,
        SchemaVersion,
    },
    session::AcceptedSchemaCatalogContext,
};
use std::rc::Rc;

// Charge one conservative shell allowance for a structural cache key. Query
// topology is separately bounded by planner admission, while this fixed charge
// keeps retained-byte accounting cheap on the miss path.
const QUERY_PLAN_CACHE_KEY_RETAINED_BYTES_ESTIMATE: usize = 8 * 1024;

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
    pub insertions: u64,
    pub evictions: u64,
    pub rejected_oversize: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct QueryPlanCompilePhaseAttribution {
    pub schema_info: u64,
    pub prepare: u64,
    pub cache_key: u64,
    pub cache_lookup: u64,
    pub plan_build: u64,
    pub cache_insert: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum QueryPlanCompilePhase {
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
        Self {
            hits: 1,
            misses: 0,
            insertions: 0,
            evictions: 0,
            rejected_oversize: 0,
        }
    }

    #[must_use]
    pub(super) const fn miss() -> Self {
        Self {
            hits: 0,
            misses: 1,
            insertions: 1,
            evictions: 0,
            rejected_oversize: 0,
        }
    }

    #[must_use]
    pub(super) fn with_template_insert(
        mut self,
        evictions: usize,
        rejected_oversize: bool,
    ) -> Self {
        self.insertions = u64::from(!rejected_oversize);
        self.evictions = u64::try_from(evictions).unwrap_or(u64::MAX);
        self.rejected_oversize = u64::from(rejected_oversize);
        self
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
        self.schema_info = self.schema_info.saturating_add(other.schema_info);
        self.prepare = self.prepare.saturating_add(other.prepare);
        self.cache_key = self.cache_key.saturating_add(other.cache_key);
        self.cache_lookup = self.cache_lookup.saturating_add(other.cache_lookup);
        self.plan_build = self.plan_build.saturating_add(other.plan_build);
        self.cache_insert = self.cache_insert.saturating_add(other.cache_insert);
    }

    const fn record(&mut self, phase: QueryPlanCompilePhase, local_instructions: u64) {
        match phase {
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
    pub(super) fn estimated_retained_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.entity_path.len())
            .saturating_add(QUERY_PLAN_CACHE_KEY_RETAINED_BYTES_ESTIMATE)
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
            authority.entity_path_handle(),
            schema_identity,
            visibility,
            structural_query,
        )
    }

    fn from_entity_path_cache_inputs(
        entity_path: impl Into<Rc<str>>,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        structural_query: StructuralQueryCacheKey,
    ) -> Self {
        Self {
            entity_path: entity_path.into(),
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

    pub(super) fn for_authority_with_parameter_contract(
        authority: EntityAuthority,
        schema_identity: SchemaCacheIdentity,
        visibility: QueryPlanVisibility,
        query: &StructuralQuery,
        parameter_contract: PreparedQueryParameterContract,
    ) -> Self {
        Self::from_authority_cache_inputs(
            authority,
            schema_identity,
            visibility,
            query.structural_cache_key_with_parameter_contract(parameter_contract),
        )
    }

    pub(super) fn for_entity_path_with_normalized_predicate_fingerprint(
        entity_path: &str,
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
