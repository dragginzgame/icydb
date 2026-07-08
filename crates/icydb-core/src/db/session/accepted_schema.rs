//! Module: db::session::accepted_schema
//! Responsibility: accepted-schema runtime authority, query cache, and
//! save-contract projection for session execution paths.
//! Does not own: schema reconciliation policy, query planning, or mutation
//! staging.
//! Boundary: loads accepted schema snapshots from store authority and exposes
//! typed session helpers for query, SQL, catalog, and write adapters.

use super::DbSession;
use crate::{
    db::{
        EntityRuntimeHooks,
        commit::CommitSchemaFingerprint,
        executor::EntityAuthority,
        schema::{
            AcceptedCatalogIdentity, AcceptedCatalogSnapshotSelection, AcceptedRowDecodeContract,
            AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot, SchemaInfo, SchemaVersion,
            accepted_commit_schema_fingerprint, accepted_schema_cache_fingerprint,
            ensure_accepted_schema_snapshot,
        },
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, Path},
};
use std::{
    cell::{OnceCell, RefCell},
    collections::HashMap,
};

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct AcceptedCatalogRuntimeCounterSnapshot {
    pub schema_info_projections: u64,
    pub persisted_schema_decodes: u64,
    pub generated_compatible_row_layout_proofs: u64,
    pub latest_by_entity_calls: u64,
    pub visible_index_projections: u64,
}

#[derive(Clone, Debug)]
struct AcceptedSchemaQueryCacheEntry {
    snapshot: AcceptedSchemaSnapshot,
    identity: AcceptedCatalogIdentity,
}

type AcceptedSchemaQueryCacheKey = (usize, &'static str);

pub(in crate::db) type AcceptedSaveContract = (
    AcceptedRowDecodeContract,
    AcceptedRowDecodeContract,
    SchemaInfo,
    CommitSchemaFingerprint,
);

#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedSchemaCatalogContext {
    snapshot: AcceptedSchemaSnapshot,
    identity: AcceptedCatalogIdentity,
    schema_info: OnceCell<SchemaInfo>,
}

impl AcceptedSchemaCatalogContext {
    #[must_use]
    pub(in crate::db) const fn new(
        snapshot: AcceptedSchemaSnapshot,
        identity: AcceptedCatalogIdentity,
    ) -> Self {
        Self {
            snapshot,
            identity,
            schema_info: OnceCell::new(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn snapshot(&self) -> &AcceptedSchemaSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub(in crate::db) const fn schema_version(&self) -> SchemaVersion {
        self.identity.accepted_schema_version()
    }

    #[must_use]
    pub(in crate::db) const fn fingerprint(&self) -> CommitSchemaFingerprint {
        self.identity.accepted_schema_fingerprint()
    }

    #[must_use]
    pub(in crate::db) const fn fingerprint_method_version(&self) -> u8 {
        self.identity.fingerprint_method_version()
    }

    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) const fn identity(&self) -> AcceptedCatalogIdentity {
        self.identity
    }

    fn debug_assert_matches_entity<E>(&self)
    where
        E: EntityKind,
    {
        debug_assert_eq!(self.identity.entity_tag(), E::ENTITY_TAG);
        debug_assert_eq!(self.identity.entity_path(), E::PATH);
        debug_assert_eq!(self.identity.store_path(), E::Store::PATH);
    }

    pub(in crate::db) fn accepted_entity_authority_for<E>(
        &self,
    ) -> Result<EntityAuthority, InternalError>
    where
        E: EntityKind,
    {
        let schema_info = self.accepted_schema_info_for::<E>();

        self.accepted_entity_authority_for_schema_info::<E>(schema_info)
    }

    fn accepted_entity_authority_for_schema_info<E>(
        &self,
        schema_info: SchemaInfo,
    ) -> Result<EntityAuthority, InternalError>
    where
        E: EntityKind,
    {
        self.debug_assert_matches_entity::<E>();
        let authority = EntityAuthority::new(E::MODEL, E::ENTITY_TAG, E::Store::PATH);
        let (accepted_row_layout, row_proof) =
            AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
                &self.snapshot,
                authority.model(),
            )?;
        let row_decode_contract = accepted_row_layout.row_decode_contract();

        Ok(
            authority.with_accepted_row_decode_contract(
                row_proof,
                row_decode_contract,
                schema_info,
            ),
        )
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn accepted_entity_authority_and_schema_info_for<E>(
        &self,
    ) -> Result<(EntityAuthority, SchemaInfo), InternalError>
    where
        E: EntityKind,
    {
        let schema_info = self.accepted_schema_info_for::<E>();
        let authority = self.accepted_entity_authority_for_schema_info::<E>(schema_info.clone())?;

        Ok((authority, schema_info))
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn accepted_or_provided_entity_authority_for<E>(
        &self,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<EntityAuthority, InternalError>
    where
        E: EntityKind,
    {
        match accepted_authority {
            Some(authority) => Ok(authority.clone()),
            None => self.accepted_entity_authority_for::<E>(),
        }
    }

    #[cfg(feature = "sql-explain")]
    pub(in crate::db) fn accepted_or_provided_entity_authority_and_schema_info_for<E>(
        &self,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<(EntityAuthority, SchemaInfo), InternalError>
    where
        E: EntityKind,
    {
        let schema_info = self.accepted_schema_info_for::<E>();
        let authority = match accepted_authority {
            Some(authority) => authority.clone(),
            None => self.accepted_entity_authority_for_schema_info::<E>(schema_info.clone())?,
        };

        Ok((authority, schema_info))
    }

    #[must_use]
    pub(in crate::db) fn accepted_schema_info_for<E>(&self) -> SchemaInfo
    where
        E: EntityKind,
    {
        self.debug_assert_matches_entity::<E>();
        self.schema_info
            .get_or_init(|| {
                SchemaInfo::from_accepted_snapshot_for_model_with_expression_indexes(
                    E::MODEL,
                    &self.snapshot,
                    true,
                )
            })
            .clone()
    }
}

pub(in crate::db) fn accepted_save_contract_for_descriptor<E>(
    accepted_schema: &AcceptedSchemaSnapshot,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<AcceptedSaveContract, InternalError>
where
    E: EntityKind,
{
    let row_decode_contract = descriptor.row_decode_contract();
    let mutation_row_decode_contract = row_decode_contract.clone();
    let schema_info = SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, accepted_schema);
    let schema_fingerprint = accepted_commit_schema_fingerprint(accepted_schema)?;

    Ok((
        row_decode_contract,
        mutation_row_decode_contract,
        schema_info,
        schema_fingerprint,
    ))
}

thread_local! {
    // Query-side SQL/fluent cache setup needs accepted runtime schema authority,
    // but repeated read calls should not reload the stable schema snapshot just
    // to prove an already-warmed cache key. SQL DDL publication invalidates this
    // heap cache before the next query observes the new accepted schema.
    static ACCEPTED_SCHEMA_QUERY_CACHES: RefCell<HashMap<(usize, &'static str), AcceptedSchemaQueryCacheEntry>> =
        RefCell::new(HashMap::default());
}

impl<C: CanisterKind> DbSession<C> {
    #[cfg(test)]
    pub(in crate::db) fn reset_accepted_catalog_runtime_counters_for_tests() {
        crate::db::schema::reset_accepted_schema_info_projection_count_for_tests();
        crate::db::schema::reset_persisted_schema_snapshot_decode_count_for_tests();
        crate::db::schema::reset_generated_compatible_row_layout_proof_count_for_tests();
        crate::db::schema::reset_latest_raw_snapshots_by_entity_call_count_for_tests();
        super::query::reset_visible_index_projection_count_for_tests();
    }

    #[cfg(test)]
    pub(in crate::db) fn accepted_catalog_runtime_counter_snapshot_for_tests()
    -> AcceptedCatalogRuntimeCounterSnapshot {
        AcceptedCatalogRuntimeCounterSnapshot {
            schema_info_projections:
                crate::db::schema::accepted_schema_info_projection_count_for_tests(),
            persisted_schema_decodes:
                crate::db::schema::persisted_schema_snapshot_decode_count_for_tests(),
            generated_compatible_row_layout_proofs:
                crate::db::schema::generated_compatible_row_layout_proof_count_for_tests(),
            latest_by_entity_calls:
                crate::db::schema::latest_raw_snapshots_by_entity_call_count_for_tests(),
            visible_index_projections: super::query::visible_index_projection_count_for_tests(),
        }
    }

    pub(in crate::db::session) fn accepted_schema_catalog_context_for_runtime_hook(
        &self,
        hooks: &EntityRuntimeHooks<C>,
        store: crate::db::registry::StoreHandle,
    ) -> Result<AcceptedSchemaCatalogContext, InternalError> {
        let cache_key = self.accepted_schema_query_cache_key(hooks.entity_path);
        if let Some(context) =
            Self::accepted_schema_catalog_context_from_runtime_hook_cache(cache_key, hooks, store)?
        {
            return Ok(context);
        }

        let snapshot = Self::load_accepted_schema_snapshot_for_runtime_hook(hooks, store)?;
        let identity = AcceptedCatalogIdentity::new(
            hooks.entity_tag,
            hooks.entity_path,
            hooks.store_path,
            snapshot.persisted_snapshot().version(),
            accepted_schema_cache_fingerprint(&snapshot)?,
        );
        let context = AcceptedSchemaCatalogContext::new(snapshot.clone(), identity);
        Self::insert_accepted_schema_query_cache(cache_key, snapshot, identity);

        Ok(context)
    }

    fn accepted_schema_catalog_context_from_runtime_hook_cache(
        cache_key: AcceptedSchemaQueryCacheKey,
        hooks: &EntityRuntimeHooks<C>,
        store: crate::db::registry::StoreHandle,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError> {
        let selection = store.with_schema_mut(|schema_store| {
            schema_store.latest_catalog_identity(
                hooks.entity_tag,
                hooks.entity_path,
                hooks.store_path,
            )
        })?;
        if let Some(selection) = selection
            && let Some(context) = Self::accepted_schema_catalog_context_from_query_cache(
                cache_key,
                selection.identity(),
            )
        {
            return Ok(Some(context));
        }

        Ok(None)
    }

    fn load_accepted_schema_snapshot_for_runtime_hook(
        hooks: &EntityRuntimeHooks<C>,
        store: crate::db::registry::StoreHandle,
    ) -> Result<AcceptedSchemaSnapshot, InternalError> {
        store.with_schema_mut(|schema_store| {
            if let Some(snapshot) = schema_store.latest_persisted_snapshot(hooks.entity_tag)? {
                let accepted = AcceptedSchemaSnapshot::try_new(snapshot)?;
                if accepted.entity_path() == hooks.entity_path {
                    return Ok(accepted);
                }
            }

            ensure_accepted_schema_snapshot(
                schema_store,
                hooks.entity_tag,
                hooks.entity_path,
                hooks.model,
            )
        })
    }

    // Ensure and return the accepted schema snapshot for one generated entity.
    // This may write the first snapshot for an empty store; otherwise it loads
    // the latest stored snapshot and applies the current exact-match policy.
    pub(in crate::db::session) fn ensure_accepted_schema_snapshot<E>(
        &self,
    ) -> Result<AcceptedSchemaSnapshot, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let store = self.db.recovered_store(E::Store::PATH)?;

        store.with_schema_mut(|schema_store| {
            ensure_accepted_schema_snapshot(schema_store, E::ENTITY_TAG, E::PATH, E::MODEL)
        })
    }

    fn accepted_schema_query_cache_key(
        &self,
        entity_path: &'static str,
    ) -> AcceptedSchemaQueryCacheKey {
        (self.db.cache_scope_id(), entity_path)
    }

    fn accepted_schema_catalog_context_from_query_cache(
        cache_key: AcceptedSchemaQueryCacheKey,
        identity: AcceptedCatalogIdentity,
    ) -> Option<AcceptedSchemaCatalogContext> {
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache.borrow().get(&cache_key).and_then(|entry| {
                (entry.identity == identity)
                    .then(|| AcceptedSchemaCatalogContext::new(entry.snapshot.clone(), identity))
            })
        })
    }

    fn insert_accepted_schema_query_cache(
        cache_key: AcceptedSchemaQueryCacheKey,
        snapshot: AcceptedSchemaSnapshot,
        identity: AcceptedCatalogIdentity,
    ) {
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache.borrow_mut().insert(
                cache_key,
                AcceptedSchemaQueryCacheEntry { snapshot, identity },
            );
        });
    }

    #[cfg(test)]
    pub(in crate::db) fn clear_accepted_schema_query_cache_for_tests() {
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache.borrow_mut().clear();
        });
    }

    // Load the current accepted schema snapshot for read/query paths without
    // rerunning generated proposal reconciliation on every cold query call.
    // Startup and write paths still own reconciliation; the fallback only keeps
    // first-use test stores and freshly initialized local stores functional.
    pub(in crate::db::session) fn accepted_schema_catalog_context_for_query<E>(
        &self,
    ) -> Result<AcceptedSchemaCatalogContext, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let cache_key = self.accepted_schema_query_cache_key(E::PATH);
        if let Some(entry) =
            ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| cache.borrow().get(&cache_key).cloned())
        {
            return Ok(AcceptedSchemaCatalogContext::new(
                entry.snapshot,
                entry.identity,
            ));
        }

        let snapshot = self.load_accepted_schema_snapshot_for_query::<E>()?;
        let fingerprint = accepted_schema_cache_fingerprint(&snapshot)?;
        let identity = AcceptedCatalogIdentity::new(
            E::ENTITY_TAG,
            E::PATH,
            E::Store::PATH,
            snapshot.persisted_snapshot().version(),
            fingerprint,
        );
        Self::insert_accepted_schema_query_cache(cache_key, snapshot.clone(), identity);

        Ok(AcceptedSchemaCatalogContext::new(snapshot, identity))
    }

    pub(in crate::db::session) fn accepted_catalog_snapshot_selection_for_query<E>(
        &self,
    ) -> Result<Option<AcceptedCatalogSnapshotSelection>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let store = self.db.recovered_store(E::Store::PATH)?;

        store.with_schema_mut(|schema_store| {
            schema_store.latest_catalog_identity(E::ENTITY_TAG, E::PATH, E::Store::PATH)
        })
    }

    pub(in crate::db::session) fn accepted_schema_catalog_context_from_selection<E>(
        &self,
        selection: &AcceptedCatalogSnapshotSelection,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let cache_key = self.accepted_schema_query_cache_key(E::PATH);
        if let Some(context) =
            Self::accepted_schema_catalog_context_from_query_cache(cache_key, selection.identity())
        {
            return Ok(Some(context));
        }

        let snapshot = selection.decode_verified()?;
        if snapshot.persisted_snapshot().fields().len() != E::MODEL.fields().len() {
            return Ok(None);
        }
        let context = AcceptedSchemaCatalogContext::new(snapshot.clone(), selection.identity());

        Self::insert_accepted_schema_query_cache(cache_key, snapshot, selection.identity());

        Ok(Some(context))
    }

    #[cfg(feature = "sql")]
    pub(in crate::db::session) fn invalidate_accepted_schema_query_cache_for_entity<E>(&self)
    where
        E: EntityKind<Canister = C>,
    {
        let cache_key = self.accepted_schema_query_cache_key(E::PATH);
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache.borrow_mut().remove(&cache_key);
        });
    }

    fn load_accepted_schema_snapshot_for_query<E>(
        &self,
    ) -> Result<AcceptedSchemaSnapshot, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let store = self.db.recovered_store(E::Store::PATH)?;

        store.with_schema_mut(|schema_store| {
            if let Some(snapshot) = schema_store.latest_persisted_snapshot(E::ENTITY_TAG)? {
                let accepted = AcceptedSchemaSnapshot::try_new(snapshot)?;
                if AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
                    &accepted,
                    E::MODEL,
                )
                .is_ok()
                {
                    return Ok(accepted);
                }
            }

            ensure_accepted_schema_snapshot(schema_store, E::ENTITY_TAG, E::PATH, E::MODEL)
        })
    }

    // Build the accepted schema-info projection for one typed entity. Fluent
    // terminal adapters use this before constructing slot-bound descriptors so
    // field slot authority comes from the accepted schema snapshot.
    pub(in crate::db) fn accepted_schema_info_for_entity<E>(
        &self,
    ) -> Result<SchemaInfo, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let catalog = self.accepted_schema_catalog_context_for_query::<E>()?;

        Ok(catalog.accepted_schema_info_for::<E>())
    }

    // Derive typed executor authority from an accepted snapshot the caller
    // already loaded, avoiding a second schema-store pass in SQL write/select
    // adapters that need both write descriptors and read selector authority.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn accepted_entity_authority_for_schema<E>(
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<EntityAuthority, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        EntityAuthority::from_accepted_schema_for_type::<E>(accepted_schema)
    }

    // Ensure accepted schema metadata is safe for write paths that still encode
    // rows through generated field contracts. Returning only the snapshot keeps
    // SQL write type checks unchanged while the schema-runtime contract guard
    // rejects unsupported layout or payload drift before mutation staging.
    pub(in crate::db::session) fn ensure_generated_compatible_accepted_save_schema<E>(
        &self,
    ) -> Result<
        (
            AcceptedRowDecodeContract,
            SchemaInfo,
            CommitSchemaFingerprint,
        ),
        InternalError,
    >
    where
        E: EntityKind<Canister = C>,
    {
        let accepted_schema = self.ensure_accepted_schema_snapshot::<E>()?;
        let (accepted_row_layout, _) =
            AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
                &accepted_schema,
                E::MODEL,
            )?;
        let (row_decode_contract, _, schema_info, schema_fingerprint) =
            accepted_save_contract_for_descriptor::<E>(&accepted_schema, &accepted_row_layout)?;

        Ok((row_decode_contract, schema_info, schema_fingerprint))
    }
}
