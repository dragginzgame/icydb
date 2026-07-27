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
        commit::CommitSchemaFingerprint,
        entity_registration::EntityRuntimeRegistration,
        executor::EntityAuthority,
        schema::{
            AcceptedCatalogIdentity, AcceptedEnumCatalog, AcceptedInspectionPlan,
            AcceptedRowLayoutRuntimeContract, AcceptedSchemaAuthority, AcceptedSchemaRevision,
            AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, CompiledAcceptedRowConstraints,
            SchemaInfo, SchemaStore, SchemaVersion,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};
use std::{
    cell::{OnceCell, RefCell},
    collections::HashMap,
};

#[derive(Clone, Debug)]
struct AcceptedSchemaQueryCacheEntry {
    inspection_plan: AcceptedInspectionPlan,
}

type AcceptedSchemaQueryCacheKey = (usize, &'static str);

#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedSchemaCatalogContext {
    inspection_plan: AcceptedInspectionPlan,
    schema_info: OnceCell<SchemaInfo>,
}

pub(in crate::db::session) enum AcceptedInspectionPlanLoadError {
    Unselected(InternalError),
    Selected {
        identity: AcceptedCatalogIdentity,
        error: InternalError,
    },
}

impl AcceptedInspectionPlanLoadError {
    pub(in crate::db::session) fn into_internal(self) -> InternalError {
        match self {
            Self::Unselected(error) | Self::Selected { error, .. } => error,
        }
    }
}

impl AcceptedSchemaCatalogContext {
    const fn new(inspection_plan: AcceptedInspectionPlan) -> Self {
        Self {
            inspection_plan,
            schema_info: OnceCell::new(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn snapshot(&self) -> &AcceptedSchemaSnapshot {
        self.inspection_plan.snapshot()
    }

    #[must_use]
    pub(in crate::db) fn enum_catalog(&self) -> &AcceptedEnumCatalog {
        self.inspection_plan.value_catalog().enum_catalog()
    }

    #[must_use]
    pub(in crate::db) const fn value_catalog_handle(&self) -> &AcceptedValueCatalogHandle {
        self.inspection_plan.value_catalog()
    }

    #[must_use]
    pub(in crate::db) const fn schema_version(&self) -> SchemaVersion {
        self.inspection_plan.identity().accepted_schema_version()
    }

    #[must_use]
    pub(in crate::db) const fn revision(&self) -> AcceptedSchemaRevision {
        self.inspection_plan.identity().accepted_schema_revision()
    }

    #[must_use]
    pub(in crate::db) const fn fingerprint(&self) -> CommitSchemaFingerprint {
        self.inspection_plan
            .identity()
            .accepted_schema_fingerprint()
    }

    /// Borrow the accepted check program compiled for this exact fingerprint.
    #[must_use]
    pub(in crate::db) const fn accepted_row_constraints(&self) -> &CompiledAcceptedRowConstraints {
        self.inspection_plan.write_constraints()
    }

    /// Borrow the canonical accepted inspection projection.
    #[must_use]
    pub(in crate::db) const fn inspection_plan(&self) -> &AcceptedInspectionPlan {
        &self.inspection_plan
    }

    #[must_use]
    pub(in crate::db) const fn fingerprint_method_version(&self) -> u8 {
        self.inspection_plan.identity().fingerprint_method_version()
    }

    #[must_use]
    pub(in crate::db) const fn identity(&self) -> AcceptedCatalogIdentity {
        self.inspection_plan.identity()
    }

    /// Build executor authority directly from accepted catalog state.
    pub(in crate::db) fn accepted_entity_authority(
        &self,
    ) -> Result<EntityAuthority, InternalError> {
        let accepted_row_layout =
            AcceptedRowLayoutRuntimeContract::from_accepted_schema(self.snapshot())?;
        let row_decode_contract =
            accepted_row_layout.row_decode_contract(self.inspection_plan.value_catalog().clone());
        debug_assert_eq!(
            row_decode_contract.accepted_schema_revision(),
            self.revision()
        );
        debug_assert!(std::ptr::eq(
            row_decode_contract.enum_catalog(),
            self.enum_catalog()
        ));

        Ok(EntityAuthority::from_accepted_row_decode_contract(
            self.inspection_plan.identity().entity_path(),
            self.inspection_plan.identity().entity_tag(),
            self.inspection_plan.identity().store_path(),
            row_decode_contract,
            self.accepted_schema_info(),
        ))
    }

    #[cfg(feature = "sql")]
    pub(in crate::db) fn accepted_or_provided_entity_authority(
        &self,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<EntityAuthority, InternalError> {
        match accepted_authority {
            Some(authority) => Ok(authority.clone()),
            None => self.accepted_entity_authority(),
        }
    }

    /// Project schema metadata from the accepted snapshot only.
    #[must_use]
    pub(in crate::db) fn accepted_schema_info(&self) -> SchemaInfo {
        self.schema_info
            .get_or_init(|| {
                let schema_info = SchemaInfo::from_accepted_snapshot_and_catalog(
                    self.inspection_plan.snapshot(),
                    self.inspection_plan.value_catalog().clone(),
                    true,
                );
                debug_assert!(
                    schema_info
                        .enum_catalog()
                        .is_some_and(|catalog| std::ptr::eq(catalog, self.enum_catalog()))
                );
                schema_info
            })
            .clone()
    }
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
    pub(in crate::db::session) fn accepted_schema_catalog_context_for_runtime_registration(
        &self,
        registration: EntityRuntimeRegistration<C>,
        store: crate::db::registry::StoreHandle,
    ) -> Result<AcceptedSchemaCatalogContext, InternalError> {
        self.accepted_inspection_plan_for_runtime_registration(registration, store)
            .map(AcceptedSchemaCatalogContext::new)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)
    }

    /// Resolve one accepted catalog by its editable SQL/display entity name.
    pub(in crate::db::session) fn accepted_schema_catalog_context_for_entity_name(
        &self,
        entity_name: Option<&str>,
    ) -> Result<AcceptedSchemaCatalogContext, InternalError> {
        if let Some(entity_name) = entity_name
            && let Some(context) =
                self.accepted_schema_catalog_context_from_cached_entity_name(entity_name)?
        {
            return Ok(context);
        }

        let mut matched = None;

        for entity_registration in self.db.entity_registrations {
            let registration = entity_registration.runtime().resolve(&self.db)?;
            let store = self.db.recovered_store(registration.store_path)?;
            let context =
                self.accepted_schema_catalog_context_for_runtime_registration(registration, store)?;
            if entity_name.is_some_and(|name| context.snapshot().entity_name() != name) {
                continue;
            }
            if matched.is_some() {
                return Err(InternalError::store_corruption());
            }
            matched = Some(context);
            if entity_name.is_none() {
                break;
            }
        }

        matched.ok_or_else(|| InternalError::unsupported_entity_path(entity_name))
    }

    fn accepted_schema_catalog_context_from_cached_entity_name(
        &self,
        entity_name: &str,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError> {
        let scope_id = self.db.cache_scope_id();
        let candidates = ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache
                .borrow()
                .iter()
                .filter_map(|(cache_key, entry)| {
                    (cache_key.0 == scope_id
                        && entry.inspection_plan.snapshot().entity_name() == entity_name)
                        .then_some((*cache_key, entry.inspection_plan.identity().store_path()))
                })
                .collect::<Vec<_>>()
        });
        let mut matched = None;

        for (cache_key, store_path) in candidates {
            let store = self.db.recovered_store(store_path)?;
            let Some(context) = Self::accepted_schema_catalog_context_from_current_authority_cache(
                cache_key, store,
            )?
            else {
                continue;
            };
            if matched.is_some() {
                return Err(InternalError::store_corruption());
            }
            matched = Some(context);
        }

        Ok(matched)
    }

    pub(in crate::db::session) fn accepted_inspection_plan_for_runtime_registration(
        &self,
        registration: EntityRuntimeRegistration<C>,
        store: crate::db::registry::StoreHandle,
    ) -> Result<AcceptedInspectionPlan, AcceptedInspectionPlanLoadError> {
        let cache_key = self.accepted_schema_query_cache_key(registration.entity_path);
        if let Some(context) =
            Self::accepted_schema_catalog_context_from_runtime_registration_cache(
                cache_key,
                registration,
                store,
            )
            .map_err(AcceptedInspectionPlanLoadError::Unselected)?
        {
            return Ok(context.inspection_plan);
        }

        let selection = store
            .with_schema(|schema_store| {
                schema_store.current_accepted_catalog_selection(
                    registration.entity_tag,
                    registration.entity_path,
                    registration.store_path,
                )
            })
            .map_err(AcceptedInspectionPlanLoadError::Unselected)?
            .ok_or_else(|| {
                AcceptedInspectionPlanLoadError::Unselected(InternalError::store_corruption())
            })?;
        let identity = selection.identity();
        let snapshot = selection
            .decode_verified()
            .map_err(|error| AcceptedInspectionPlanLoadError::Selected { identity, error })?;
        let inspection_plan = AcceptedInspectionPlan::compile(
            &self.db,
            identity,
            snapshot,
            selection.value_catalog_handle().clone(),
        )
        .map_err(|error| AcceptedInspectionPlanLoadError::Selected { identity, error })?;
        Self::insert_accepted_schema_query_cache(cache_key, inspection_plan.clone());

        Ok(inspection_plan)
    }

    fn accepted_schema_catalog_context_from_runtime_registration_cache(
        cache_key: AcceptedSchemaQueryCacheKey,
        registration: EntityRuntimeRegistration<C>,
        store: crate::db::registry::StoreHandle,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError> {
        let context =
            Self::accepted_schema_catalog_context_from_current_authority_cache(cache_key, store)?;
        if let Some(context) = &context {
            debug_assert_eq!(
                context.inspection_plan.identity().entity_tag(),
                registration.entity_tag
            );
            debug_assert_eq!(
                context.inspection_plan.identity().entity_path(),
                registration.entity_path
            );
            debug_assert_eq!(
                context.inspection_plan.identity().store_path(),
                registration.store_path
            );
        }
        Ok(context)
    }

    fn accepted_schema_query_cache_key(
        &self,
        entity_path: &'static str,
    ) -> AcceptedSchemaQueryCacheKey {
        (self.db.cache_scope_id(), entity_path)
    }

    fn accepted_schema_catalog_context_from_current_authority_cache(
        cache_key: AcceptedSchemaQueryCacheKey,
        store: crate::db::registry::StoreHandle,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError> {
        let entry =
            ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| cache.borrow().get(&cache_key).cloned());
        let Some(entry) = entry else {
            return Ok(None);
        };
        if !store.with_schema(|schema_store| {
            schema_store.current_accepted_schema_authority_matches(
                entry.inspection_plan.value_catalog().authority(),
            )
        })? {
            return Ok(None);
        }

        Ok(Some(AcceptedSchemaCatalogContext::new(
            entry.inspection_plan,
        )))
    }

    fn insert_accepted_schema_query_cache(
        cache_key: AcceptedSchemaQueryCacheKey,
        inspection_plan: AcceptedInspectionPlan,
    ) {
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache
                .borrow_mut()
                .insert(cache_key, AcceptedSchemaQueryCacheEntry { inspection_plan });
        });
    }

    /// Verify accepted authority for a schema-resolved structural operation.
    pub(in crate::db::session) fn ensure_accepted_schema_authority_is_current_for_store_path(
        &self,
        store_path: &'static str,
        expected: &AcceptedSchemaAuthority,
    ) -> Result<(), InternalError> {
        let store = self.db.recovered_store(store_path)?;
        if store.with_schema(|schema_store| {
            schema_store.current_accepted_schema_authority_matches(expected)
        })? {
            return Ok(());
        }

        let current_revision = store.with_schema(SchemaStore::current_accepted_schema_revision)?;

        Err(InternalError::query_stale_accepted_schema_revision(
            expected.revision().get(),
            current_revision.map(AcceptedSchemaRevision::get),
        ))
    }

    #[cfg(feature = "sql")]
    pub(in crate::db::session) fn invalidate_accepted_schema_query_cache(
        &self,
        entity_path: &'static str,
    ) {
        let cache_key = self.accepted_schema_query_cache_key(entity_path);
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache.borrow_mut().remove(&cache_key);
        });
    }
}
