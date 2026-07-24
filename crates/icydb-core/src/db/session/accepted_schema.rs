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
            AcceptedCatalogIdentity, AcceptedCatalogSnapshotSelection, AcceptedEnumCatalog,
            AcceptedInspectionPlan, AcceptedRowDecodeContract, AcceptedRowLayoutRuntimeContract,
            AcceptedSchemaAuthority, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
            AcceptedValueCatalogHandle, CompiledAcceptedRowConstraints, SchemaInfo, SchemaStore,
            SchemaVersion, authored_projection::AcceptedAuthoredFieldProjection,
            enum_catalog::ValueAdmissionBudget, output_value_from_runtime,
        },
    },
    entity::EntityKind,
    error::InternalError,
    traits::{AuthoredFieldProjection, CanisterKind, Path},
    value::OutputValue,
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
    inspection_plan: AcceptedInspectionPlan,
}

type AcceptedSchemaQueryCacheKey = (usize, &'static str);

pub(in crate::db) type AcceptedSaveContract = (
    AcceptedRowDecodeContract,
    AcceptedRowDecodeContract,
    SchemaInfo,
    CommitSchemaFingerprint,
    CompiledAcceptedRowConstraints,
);

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
    pub(in crate::db) fn composite_catalog(&self) -> &crate::db::schema::AcceptedCompositeCatalog {
        self.inspection_plan.value_catalog().composite_catalog()
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
    #[cfg(feature = "sql")]
    pub(in crate::db) const fn identity(&self) -> AcceptedCatalogIdentity {
        self.inspection_plan.identity()
    }

    fn debug_assert_matches_entity<E>(&self)
    where
        E: EntityKind,
    {
        debug_assert_eq!(self.inspection_plan.identity().entity_tag(), E::ENTITY_TAG);
        debug_assert_eq!(self.inspection_plan.identity().entity_path(), E::PATH);
        debug_assert_eq!(self.inspection_plan.identity().store_path(), E::Store::PATH);
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
        let (accepted_row_layout, row_proof) =
            AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
                self.inspection_plan.snapshot(),
                E::MODEL,
                self.enum_catalog(),
                self.composite_catalog(),
            )?;
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
            E::MODEL,
            E::ENTITY_TAG,
            E::Store::PATH,
            row_proof,
            row_decode_contract,
            schema_info,
        ))
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
                let schema_info = SchemaInfo::from_accepted_snapshot_and_catalog_for_model(
                    E::MODEL,
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

/// Build one save contract pinned to the selected catalog context.
pub(in crate::db) fn accepted_save_contract_for_catalog_context<E>(
    context: &AcceptedSchemaCatalogContext,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> AcceptedSaveContract
where
    E: EntityKind,
{
    let inspection_plan = context.inspection_plan();
    let row_decode_contract =
        descriptor.row_decode_contract(inspection_plan.value_catalog().clone());
    (
        row_decode_contract.clone(),
        row_decode_contract,
        context.accepted_schema_info_for::<E>(),
        context.fingerprint(),
        inspection_plan.write_constraints().clone(),
    )
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
    /// Project selected generated entity fields through the current accepted
    /// catalog into public output values.
    #[doc(hidden)]
    pub fn project_entity_output_values<E>(
        &self,
        entity: &E,
        slots: &[usize],
    ) -> Result<Vec<OutputValue>, InternalError>
    where
        E: EntityKind<Canister = C> + AuthoredFieldProjection,
    {
        let (row_contract, _, _, _) =
            self.ensure_generated_compatible_accepted_save_schema::<E>()?;
        let projection = AcceptedAuthoredFieldProjection::new(&row_contract);
        let catalog = row_contract.value_catalog_handle();
        let mut values = Vec::with_capacity(slots.len());
        let mut budget = ValueAdmissionBudget::standard();
        for slot in slots {
            let admitted = projection
                .admit_field(entity, *slot, &mut budget)
                .map_err(|_| InternalError::persisted_row_encode_internal())?;
            values.push(
                output_value_from_runtime(catalog.enum_catalog(), admitted.value())
                    .map_err(|_| InternalError::store_invariant())?,
            );
        }
        Ok(values)
    }

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
        self.accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map(AcceptedSchemaCatalogContext::new)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)
    }

    pub(in crate::db::session) fn accepted_inspection_plan_for_runtime_hook(
        &self,
        hooks: &EntityRuntimeHooks<C>,
        store: crate::db::registry::StoreHandle,
    ) -> Result<AcceptedInspectionPlan, AcceptedInspectionPlanLoadError> {
        let cache_key = self.accepted_schema_query_cache_key(hooks.entity_path);
        if let Some(context) =
            Self::accepted_schema_catalog_context_from_runtime_hook_cache(cache_key, hooks, store)
                .map_err(AcceptedInspectionPlanLoadError::Unselected)?
        {
            return Ok(context.inspection_plan);
        }

        let selection = store
            .with_schema(|schema_store| {
                schema_store.current_accepted_catalog_selection(
                    hooks.entity_tag,
                    hooks.entity_path,
                    hooks.store_path,
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

    fn accepted_schema_catalog_context_from_runtime_hook_cache(
        cache_key: AcceptedSchemaQueryCacheKey,
        hooks: &EntityRuntimeHooks<C>,
        store: crate::db::registry::StoreHandle,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError> {
        let context =
            Self::accepted_schema_catalog_context_from_current_authority_cache(cache_key, store)?;
        if let Some(context) = &context {
            debug_assert_eq!(
                context.inspection_plan.identity().entity_tag(),
                hooks.entity_tag
            );
            debug_assert_eq!(
                context.inspection_plan.identity().entity_path(),
                hooks.entity_path
            );
            debug_assert_eq!(
                context.inspection_plan.identity().store_path(),
                hooks.store_path
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

    fn accepted_schema_catalog_context_from_query_cache(
        cache_key: AcceptedSchemaQueryCacheKey,
        identity: AcceptedCatalogIdentity,
        authority: &AcceptedSchemaAuthority,
    ) -> Option<AcceptedSchemaCatalogContext> {
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache
                .borrow()
                .get(&cache_key)
                .filter(|entry| entry.inspection_plan.matches_selection(identity, authority))
                .map(|entry| AcceptedSchemaCatalogContext::new(entry.inspection_plan.clone()))
        })
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

    #[cfg(test)]
    pub(in crate::db) fn clear_accepted_schema_query_cache_for_tests() {
        ACCEPTED_SCHEMA_QUERY_CACHES.with(|cache| {
            cache.borrow_mut().clear();
        });
    }

    // Load the current accepted schema snapshot for read/query paths from the
    // immutable root and validate the cache entry against that authority.
    pub(in crate::db::session) fn accepted_schema_catalog_context_for_query<E>(
        &self,
    ) -> Result<AcceptedSchemaCatalogContext, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let cache_key = self.accepted_schema_query_cache_key(E::PATH);
        let store = self.db.recovered_store(E::Store::PATH)?;
        if let Some(context) =
            Self::accepted_schema_catalog_context_from_current_authority_cache(cache_key, store)?
        {
            return Ok(context);
        }
        let selection = self
            .accepted_catalog_snapshot_selection_for_query::<E>()?
            .ok_or_else(InternalError::store_corruption)?;
        if let Some(context) = Self::accepted_schema_catalog_context_from_query_cache(
            cache_key,
            selection.identity(),
            selection.value_catalog_handle().authority(),
        ) {
            return Ok(context);
        }

        let snapshot = selection.decode_verified()?;
        let _runtime_contract = AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
            &snapshot,
            E::MODEL,
            selection.value_catalog_handle().enum_catalog(),
            selection.value_catalog_handle().composite_catalog(),
        )
        .map_err(|_error| InternalError::store_unsupported())?;
        let inspection_plan = AcceptedInspectionPlan::compile(
            &self.db,
            selection.identity(),
            snapshot,
            selection.value_catalog_handle().clone(),
        )?;
        Self::insert_accepted_schema_query_cache(cache_key, inspection_plan.clone());

        Ok(AcceptedSchemaCatalogContext::new(inspection_plan))
    }

    pub(in crate::db::session) fn ensure_accepted_schema_authority_is_current<E>(
        &self,
        expected: &AcceptedSchemaAuthority,
    ) -> Result<(), InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let store = self.db.recovered_store(E::Store::PATH)?;
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

    pub(in crate::db::session) fn accepted_catalog_snapshot_selection_for_query<E>(
        &self,
    ) -> Result<Option<AcceptedCatalogSnapshotSelection>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let store = self.db.recovered_store(E::Store::PATH)?;
        let cache_key = self.accepted_schema_query_cache_key(E::PATH);
        if let Some(context) =
            Self::accepted_schema_catalog_context_from_current_authority_cache(cache_key, store)?
        {
            return AcceptedCatalogSnapshotSelection::from_accepted_snapshot(
                context.inspection_plan.identity(),
                context.inspection_plan.value_catalog().clone(),
                context.inspection_plan.snapshot(),
            )
            .map(Some);
        }

        let selection = store.with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(E::ENTITY_TAG, E::PATH, E::Store::PATH)
        })?;

        #[cfg(test)]
        let selection = if selection.is_none() {
            store.with_schema_mut(|schema_store| {
                crate::db::schema::bootstrap_test_accepted_schema_snapshot(
                    schema_store,
                    E::ENTITY_TAG,
                    E::PATH,
                    E::Store::PATH,
                    E::MODEL,
                )
            })?;
            store.with_schema(|schema_store| {
                schema_store.current_accepted_catalog_selection(
                    E::ENTITY_TAG,
                    E::PATH,
                    E::Store::PATH,
                )
            })?
        } else {
            selection
        };

        Ok(selection)
    }

    pub(in crate::db::session) fn accepted_schema_catalog_context_from_selection<E>(
        &self,
        selection: &AcceptedCatalogSnapshotSelection,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        let cache_key = self.accepted_schema_query_cache_key(E::PATH);
        if let Some(context) = Self::accepted_schema_catalog_context_from_query_cache(
            cache_key,
            selection.identity(),
            selection.value_catalog_handle().authority(),
        ) {
            return Ok(Some(context));
        }

        let snapshot = selection.decode_verified()?;
        if snapshot.persisted_snapshot().fields().len() != E::MODEL.fields().len() {
            return Ok(None);
        }
        let inspection_plan = AcceptedInspectionPlan::compile(
            &self.db,
            selection.identity(),
            snapshot,
            selection.value_catalog_handle().clone(),
        )?;
        let context = AcceptedSchemaCatalogContext::new(inspection_plan.clone());

        Self::insert_accepted_schema_query_cache(cache_key, inspection_plan);

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

    // Ensure accepted schema metadata is safe for write paths that still encode
    // rows through generated field contracts. The save contract retains the
    // same immutable catalog and revision selected for schema validation.
    pub(in crate::db::session) fn ensure_generated_compatible_accepted_save_schema<E>(
        &self,
    ) -> Result<
        (
            AcceptedRowDecodeContract,
            SchemaInfo,
            CommitSchemaFingerprint,
            CompiledAcceptedRowConstraints,
        ),
        InternalError,
    >
    where
        E: EntityKind<Canister = C>,
    {
        let context = self.accepted_schema_catalog_context_for_query::<E>()?;
        let (descriptor, _) = AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
            context.snapshot(),
            E::MODEL,
            context.enum_catalog(),
            context.composite_catalog(),
        )?;
        let row_decode_contract =
            descriptor.row_decode_contract(context.value_catalog_handle().clone());

        Ok((
            row_decode_contract,
            context.accepted_schema_info_for::<E>(),
            context.fingerprint(),
            context.accepted_row_constraints().clone(),
        ))
    }
}
