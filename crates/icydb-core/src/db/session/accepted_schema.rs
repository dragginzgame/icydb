//! Module: db::session::accepted_schema
//! Responsibility: accepted-schema runtime-root publication and session lookup.
//! Does not own: schema reconciliation policy, query planning, or mutation staging.
//! Boundary: captures every registered accepted store root and publishes one
//! immutable database-wide runtime authority for query, SQL, and write adapters.

use crate::{
    db::{
        DbSession,
        commit::database_incarnation_id,
        executor::{
            AcceptedInspectionPlanLoadError, AcceptedSchemaCatalogContext,
            AcceptedSchemaRuntimeRoot,
        },
        registry::StoreHandle,
        runtime_entity_catalog::AcceptedRuntimeEntity,
        schema::{
            AcceptedInspectionPlan, AcceptedSchemaAuthority, AcceptedSchemaRevision,
            AcceptedSchemaRuntimeRootIdentity, AcceptedSchemaRuntimeStoreRoot, SchemaStore,
            enum_catalog::AcceptedSchemaRootSelection,
        },
    },
    error::InternalError,
    metrics::{ExecutionMetricsPhase, ExecutionMetricsSpan},
    traits::CanisterKind,
};
use std::{cell::RefCell, collections::HashMap, rc::Rc};

thread_local! {
    // Each registry owns one database-wide accepted runtime root. A cache hit
    // revalidates only the compact store-root records and never serializes or
    // hashes an accepted entity snapshot. The root binds the incarnation when
    // built; upgrade/reinstall clears this heap cache, while schema publication
    // explicitly invalidates it before another ordinary operation.
    static ACCEPTED_SCHEMA_RUNTIME_ROOTS: RefCell<HashMap<usize, Rc<AcceptedSchemaRuntimeRoot>>> =
        RefCell::new(HashMap::default());
}

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db) fn current_accepted_runtime_root_identity(
        &self,
    ) -> Result<AcceptedSchemaRuntimeRootIdentity, InternalError> {
        self.accepted_schema_runtime_root()
            .map(|root| root.identity())
            .map_err(AcceptedInspectionPlanLoadError::into_internal)
    }

    fn capture_accepted_runtime_store_roots(
        &self,
    ) -> Result<Vec<AcceptedSchemaRuntimeStoreRoot>, InternalError> {
        let mut stores = self
            .db
            .with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
        icydb_schema::compact_sort_unstable_by(&mut stores, |left, right| left.0.cmp(right.0));
        stores
            .into_iter()
            .map(|(store_path, store)| {
                let root = store
                    .with_schema(SchemaStore::current_accepted_schema_root)?
                    .map(AcceptedSchemaRootSelection::root);
                Ok(AcceptedSchemaRuntimeStoreRoot::new(store_path, root))
            })
            .collect()
    }

    fn accepted_schema_runtime_root(
        &self,
    ) -> Result<Rc<AcceptedSchemaRuntimeRoot>, AcceptedInspectionPlanLoadError> {
        self.db
            .ensure_recovered_state()
            .map_err(AcceptedInspectionPlanLoadError::Unselected)?;
        let store_roots = self
            .capture_accepted_runtime_store_roots()
            .map_err(AcceptedInspectionPlanLoadError::Unselected)?;
        let scope_id = self.db.cache_scope_id();
        let cached = ACCEPTED_SCHEMA_RUNTIME_ROOTS.with(|roots| {
            roots
                .borrow()
                .get(&scope_id)
                .filter(|root| root.matches_store_roots(store_roots.as_slice()))
                .cloned()
        });
        if let Some(root) = cached {
            return Ok(root);
        }

        let database_incarnation =
            database_incarnation_id().map_err(AcceptedInspectionPlanLoadError::Unselected)?;
        let identity = AcceptedSchemaRuntimeRootIdentity::from_store_roots(
            database_incarnation,
            store_roots.as_slice(),
        )
        .map_err(AcceptedInspectionPlanLoadError::Unselected)?;

        let root = {
            // Cache-hit validation stays outside this cold compilation interval.
            let _span = ExecutionMetricsSpan::new(ExecutionMetricsPhase::RuntimeCompilation);
            Rc::new(AcceptedSchemaRuntimeRoot::compile(
                &self.db,
                identity,
                store_roots.clone(),
            )?)
        };
        let current_incarnation =
            database_incarnation_id().map_err(AcceptedInspectionPlanLoadError::Unselected)?;
        let current_store_roots = self
            .capture_accepted_runtime_store_roots()
            .map_err(AcceptedInspectionPlanLoadError::Unselected)?;
        if current_incarnation != database_incarnation || current_store_roots != store_roots {
            return Err(AcceptedInspectionPlanLoadError::Unselected(
                InternalError::store_invariant(),
            ));
        }

        ACCEPTED_SCHEMA_RUNTIME_ROOTS.with(|roots| {
            roots.borrow_mut().insert(scope_id, root.clone());
        });

        Ok(root)
    }

    pub(in crate::db::session) fn accepted_schema_catalog_context_for_runtime_entity(
        &self,
        runtime_entity: AcceptedRuntimeEntity,
        store: StoreHandle,
    ) -> Result<AcceptedSchemaCatalogContext, InternalError> {
        self.accepted_integrity_catalog_context_for_runtime_entity(runtime_entity, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)
    }

    pub(in crate::db::session) fn accepted_integrity_catalog_context_for_runtime_entity(
        &self,
        runtime_entity: AcceptedRuntimeEntity,
        store: StoreHandle,
    ) -> Result<AcceptedSchemaCatalogContext, AcceptedInspectionPlanLoadError> {
        let expected_store = runtime_entity
            .store(&self.db)
            .map_err(AcceptedInspectionPlanLoadError::Unselected)?;
        if !std::ptr::eq(store.schema_store(), expected_store.schema_store()) {
            return Err(AcceptedInspectionPlanLoadError::Unselected(
                InternalError::store_invariant(),
            ));
        }
        let root = self.accepted_schema_runtime_root()?;
        let entity = root
            .entity_for_runtime_entity(&runtime_entity)
            .map_err(AcceptedInspectionPlanLoadError::Unselected)?;

        Ok(AcceptedSchemaCatalogContext::new(root, entity))
    }

    /// Resolve one accepted catalog through its immutable authored source key.
    pub(in crate::db::session) fn accepted_schema_catalog_context_for_entity_source_key(
        &self,
        entity_source: &str,
    ) -> Result<AcceptedSchemaCatalogContext, InternalError> {
        self.find_accepted_schema_catalog_context_for_entity_source_key(entity_source)?
            .ok_or_else(|| InternalError::unsupported_entity_path(entity_source))
    }

    /// Find one accepted catalog through immutable source identity.
    pub(in crate::db::session) fn find_accepted_schema_catalog_context_for_entity_source_key(
        &self,
        entity_source: &str,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError> {
        let root = self
            .accepted_schema_runtime_root()
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;

        Ok(root
            .entity_for_path(entity_source)
            .map(|entity| AcceptedSchemaCatalogContext::new(root, entity)))
    }

    /// Resolve one accepted catalog by its editable SQL/display entity name.
    pub(in crate::db::session) fn accepted_schema_catalog_context_for_entity_name(
        &self,
        entity_name: Option<&str>,
    ) -> Result<AcceptedSchemaCatalogContext, InternalError> {
        let root = self
            .accepted_schema_runtime_root()
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;
        let entity = match entity_name {
            Some(entity_name) => root.entity_for_name(entity_name),
            None => root.first_entity(),
        }
        .ok_or_else(|| InternalError::unsupported_entity_path(entity_name))?;

        Ok(AcceptedSchemaCatalogContext::new(root, entity))
    }

    /// Resolve a case-insensitive accepted SQL/display entity name from one root.
    pub(in crate::db::session) fn find_accepted_schema_catalog_context_for_entity_name(
        &self,
        entity_name: &str,
    ) -> Result<Option<AcceptedSchemaCatalogContext>, InternalError> {
        let root = self
            .accepted_schema_runtime_root()
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;

        Ok(root
            .entity_for_name(entity_name)
            .map(|entity| AcceptedSchemaCatalogContext::new(root, entity)))
    }

    pub(in crate::db::session) fn accepted_inspection_plan_for_runtime_entity(
        &self,
        runtime_entity: AcceptedRuntimeEntity,
        store: StoreHandle,
    ) -> Result<AcceptedInspectionPlan, AcceptedInspectionPlanLoadError> {
        self.accepted_integrity_catalog_context_for_runtime_entity(runtime_entity, store)
            .map(|catalog| catalog.inspection_plan().clone())
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

    /// Drop the complete cached root after schema publication by this session.
    pub(in crate::db::session) fn invalidate_accepted_schema_runtime_root(&self) {
        let scope_id = self.db.cache_scope_id();
        ACCEPTED_SCHEMA_RUNTIME_ROOTS.with(|roots| {
            roots.borrow_mut().remove(&scope_id);
        });
    }
}
