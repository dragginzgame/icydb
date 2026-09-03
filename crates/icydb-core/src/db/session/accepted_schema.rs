//! Module: db::session::accepted_schema
//! Responsibility: accepted-schema runtime-root publication and session lookup.
//! Does not own: schema reconciliation policy, query planning, or mutation staging.
//! Boundary: captures every registered accepted store root and publishes one
//! immutable database-wide runtime authority for query, SQL, and write adapters.

use crate::{
    db::{
        DbSession,
        commit::{CommitSchemaFingerprint, database_incarnation_id},
        executor::EntityAuthority,
        identity::EntityName,
        registry::StoreHandle,
        runtime_entity_catalog::AcceptedRuntimeEntity,
        schema::{
            AcceptedCatalogIdentity, AcceptedEnumCatalog, AcceptedInspectionPlan,
            AcceptedSchemaAuthority, AcceptedSchemaRevision, AcceptedSchemaRuntimeRootIdentity,
            AcceptedSchemaRuntimeStoreRoot, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
            CompiledAcceptedRowConstraints, SchemaInfo, SchemaStore, SchemaVersion,
            enum_catalog::AcceptedSchemaRootSelection,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};
use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

///
/// AcceptedSchemaEntityRuntime
///
/// Immutable per-entity runtime state compiled exactly once for one accepted
/// database-wide root. It owns all derived query and row-decode authority used
/// by session execution.
///

#[derive(Debug)]
struct AcceptedSchemaEntityRuntime {
    inspection_plan: AcceptedInspectionPlan,
    schema_info: Arc<SchemaInfo>,
    authority: EntityAuthority,
}

impl AcceptedSchemaEntityRuntime {
    fn compile<C: CanisterKind>(
        db: &crate::db::Db<C>,
        root_identity: AcceptedSchemaRuntimeRootIdentity,
        runtime_entity: AcceptedRuntimeEntity,
        store: StoreHandle,
    ) -> Result<Self, AcceptedInspectionPlanLoadError> {
        let selection = store
            .with_schema(|schema_store| {
                schema_store.current_accepted_catalog_selection(
                    runtime_entity.entity_tag(),
                    runtime_entity.entity_path(),
                    runtime_entity.store_path(),
                )
            })
            .map_err(AcceptedInspectionPlanLoadError::Unselected)?
            .ok_or_else(|| {
                AcceptedInspectionPlanLoadError::Unselected(InternalError::store_corruption())
            })?;
        let identity = selection.identity();
        let snapshot = selection.decode_verified().map_err(|error| {
            AcceptedInspectionPlanLoadError::Selected {
                identity: identity.clone(),
                error,
            }
        })?;
        let inspection_plan = AcceptedInspectionPlan::compile(
            db,
            identity.clone(),
            snapshot,
            selection.value_catalog_handle().clone(),
        )
        .map_err(|error| AcceptedInspectionPlanLoadError::Selected {
            identity: identity.clone(),
            error,
        })?;
        let schema_info = Arc::new(SchemaInfo::from_accepted_snapshot_and_catalog(
            inspection_plan.snapshot(),
            inspection_plan.value_catalog().clone(),
            true,
        ));
        debug_assert!(std::ptr::eq(
            schema_info.enum_catalog(),
            inspection_plan.value_catalog().enum_catalog(),
        ));
        let authority = EntityAuthority::from_accepted_runtime_contracts(
            identity.entity_path_handle(),
            identity.entity_tag(),
            identity.store_path(),
            inspection_plan.row_contract().clone(),
            schema_info.clone(),
            identity.accepted_schema_fingerprint(),
            root_identity,
        );

        let runtime = Self {
            inspection_plan,
            schema_info,
            authority,
        };

        Ok(runtime)
    }
}

///
/// AcceptedSchemaRuntimeRoot
///
/// One atomically published runtime view of every accepted entity authority in
/// a database incarnation. Store-root facts are retained to revalidate a warm
/// root without serializing or hashing accepted entity snapshots.
///

#[derive(Debug)]
struct AcceptedSchemaRuntimeRoot {
    identity: AcceptedSchemaRuntimeRootIdentity,
    store_roots: Vec<AcceptedSchemaRuntimeStoreRoot>,
    entities: Vec<Rc<AcceptedSchemaEntityRuntime>>,
    entities_by_path: HashMap<Rc<str>, Rc<AcceptedSchemaEntityRuntime>>,
    entities_by_canonical_name: HashMap<EntityName, Rc<AcceptedSchemaEntityRuntime>>,
}

impl AcceptedSchemaRuntimeRoot {
    fn compile<C: CanisterKind>(
        db: &crate::db::Db<C>,
        identity: AcceptedSchemaRuntimeRootIdentity,
        store_roots: Vec<AcceptedSchemaRuntimeStoreRoot>,
    ) -> Result<Self, AcceptedInspectionPlanLoadError> {
        let runtime_entities = db
            .accepted_runtime_entities()
            .map_err(AcceptedInspectionPlanLoadError::Unselected)?;
        let mut entities = Vec::with_capacity(runtime_entities.len());
        let mut entities_by_path = HashMap::with_capacity(runtime_entities.len());
        let mut entities_by_canonical_name = HashMap::with_capacity(runtime_entities.len());

        for runtime_entity in runtime_entities {
            let store = runtime_entity
                .store(db)
                .map_err(AcceptedInspectionPlanLoadError::Unselected)?;
            let entity = Rc::new(AcceptedSchemaEntityRuntime::compile(
                db,
                identity,
                runtime_entity,
                store,
            )?);
            let entity_path = entity.inspection_plan.identity().entity_path_handle();
            let canonical_entity_name =
                EntityName::try_from_str(entity.inspection_plan.snapshot().entity_name())
                    .map(EntityName::ascii_case_fold)
                    .map_err(|_| {
                        AcceptedInspectionPlanLoadError::Unselected(
                            InternalError::store_corruption(),
                        )
                    })?;
            if entities_by_path
                .insert(entity_path, entity.clone())
                .is_some()
                || entities_by_canonical_name
                    .insert(canonical_entity_name, entity.clone())
                    .is_some()
            {
                return Err(AcceptedInspectionPlanLoadError::Unselected(
                    InternalError::store_corruption(),
                ));
            }
            entities.push(entity);
        }

        let root = Self {
            identity,
            store_roots,
            entities,
            entities_by_path,
            entities_by_canonical_name,
        };

        Ok(root)
    }

    #[must_use]
    const fn identity(&self) -> AcceptedSchemaRuntimeRootIdentity {
        self.identity
    }

    #[must_use]
    fn matches_store_roots(&self, store_roots: &[AcceptedSchemaRuntimeStoreRoot]) -> bool {
        self.store_roots == store_roots
    }

    fn entity_for_runtime_entity(
        &self,
        runtime_entity: &AcceptedRuntimeEntity,
    ) -> Result<Rc<AcceptedSchemaEntityRuntime>, InternalError> {
        let entity = self
            .entities_by_path
            .get(runtime_entity.entity_path())
            .cloned()
            .ok_or_else(InternalError::store_corruption)?;
        let identity = entity.inspection_plan.identity_ref();
        if identity.entity_tag() != runtime_entity.entity_tag()
            || identity.store_path() != runtime_entity.store_path()
        {
            return Err(InternalError::store_corruption());
        }

        Ok(entity)
    }

    #[must_use]
    fn entity_for_path(&self, entity_path: &str) -> Option<Rc<AcceptedSchemaEntityRuntime>> {
        self.entities_by_path.get(entity_path).cloned()
    }

    #[must_use]
    fn entity_for_name(&self, entity_name: &str) -> Option<Rc<AcceptedSchemaEntityRuntime>> {
        let entity_name = entity_name.rsplit('.').next()?;
        let canonical_entity_name = EntityName::try_from_str(entity_name)
            .ok()?
            .ascii_case_fold();
        self.entities_by_canonical_name
            .get(&canonical_entity_name)
            .cloned()
    }

    #[must_use]
    fn first_entity(&self) -> Option<Rc<AcceptedSchemaEntityRuntime>> {
        self.entities.first().cloned()
    }
}

///
/// AcceptedSchemaCatalogContext
///
/// One entity projection borrowed from a captured database-wide accepted
/// runtime root. Cloning the context retains that exact root publication.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedSchemaCatalogContext {
    root: Rc<AcceptedSchemaRuntimeRoot>,
    entity: Rc<AcceptedSchemaEntityRuntime>,
}

impl AcceptedSchemaCatalogContext {
    const fn new(
        root: Rc<AcceptedSchemaRuntimeRoot>,
        entity: Rc<AcceptedSchemaEntityRuntime>,
    ) -> Self {
        Self { root, entity }
    }

    /// Resolve another accepted entity while retaining this exact captured
    /// database-wide root publication.
    pub(in crate::db) fn for_entity_name(&self, entity_name: &str) -> Option<Self> {
        self.root
            .entity_for_name(entity_name)
            .map(|entity| Self::new(self.root.clone(), entity))
    }

    /// Resolve another accepted entity path while retaining this exact
    /// captured database-wide root publication.
    pub(in crate::db) fn for_entity_path(&self, entity_path: &str) -> Option<Self> {
        self.root
            .entity_for_path(entity_path)
            .map(|entity| Self::new(self.root.clone(), entity))
    }

    #[must_use]
    pub(in crate::db) fn snapshot(&self) -> &AcceptedSchemaSnapshot {
        self.entity.inspection_plan.snapshot()
    }

    /// Resolve one relation target from the same captured database-wide root.
    pub(in crate::db) fn relation_target_description(
        &self,
        target_path: &str,
    ) -> Result<(String, String), InternalError> {
        let target = self
            .root
            .entity_for_path(target_path)
            .ok_or_else(InternalError::store_invariant)?;
        Ok((
            target.inspection_plan.snapshot().entity_name().to_string(),
            target
                .inspection_plan
                .identity_ref()
                .store_path()
                .to_string(),
        ))
    }

    #[must_use]
    pub(in crate::db) fn enum_catalog(&self) -> &AcceptedEnumCatalog {
        self.entity.inspection_plan.value_catalog().enum_catalog()
    }

    #[must_use]
    pub(in crate::db) fn value_catalog_handle(&self) -> &AcceptedValueCatalogHandle {
        self.entity.inspection_plan.value_catalog()
    }

    #[must_use]
    pub(in crate::db) fn schema_version(&self) -> SchemaVersion {
        self.entity
            .inspection_plan
            .identity_ref()
            .accepted_schema_version()
    }

    #[must_use]
    pub(in crate::db) fn revision(&self) -> AcceptedSchemaRevision {
        self.entity
            .inspection_plan
            .identity_ref()
            .accepted_schema_revision()
    }

    #[must_use]
    pub(in crate::db) fn fingerprint(&self) -> CommitSchemaFingerprint {
        self.entity
            .inspection_plan
            .identity_ref()
            .accepted_schema_fingerprint()
    }

    /// Return the database-wide root identity captured by this context.
    #[must_use]
    pub(in crate::db) fn runtime_root_identity(&self) -> AcceptedSchemaRuntimeRootIdentity {
        self.root.identity()
    }

    /// Borrow the accepted row-constraint program compiled for this fingerprint.
    #[must_use]
    pub(in crate::db) fn accepted_row_constraints(&self) -> &CompiledAcceptedRowConstraints {
        self.entity.inspection_plan.write_constraints()
    }

    /// Borrow the canonical accepted inspection projection.
    #[must_use]
    pub(in crate::db) fn inspection_plan(&self) -> &AcceptedInspectionPlan {
        &self.entity.inspection_plan
    }

    #[must_use]
    pub(in crate::db) fn fingerprint_method_version(&self) -> u8 {
        self.entity
            .inspection_plan
            .identity_ref()
            .fingerprint_method_version()
    }

    #[must_use]
    pub(in crate::db) fn identity(&self) -> AcceptedCatalogIdentity {
        self.entity.inspection_plan.identity()
    }

    /// Clone executor authority from this immutable accepted entity runtime.
    #[must_use]
    pub(in crate::db) fn accepted_entity_authority(&self) -> EntityAuthority {
        self.entity.authority.clone()
    }

    #[must_use]
    pub(in crate::db) fn accepted_or_provided_entity_authority(
        &self,
        accepted_authority: Option<&EntityAuthority>,
    ) -> EntityAuthority {
        match accepted_authority {
            Some(authority) => authority.clone(),
            None => self.accepted_entity_authority(),
        }
    }

    /// Borrow schema metadata compiled once with the accepted runtime root.
    #[must_use]
    pub(in crate::db) fn accepted_schema_info(&self) -> &SchemaInfo {
        self.entity.schema_info.as_ref()
    }
}

///
/// AcceptedInspectionPlanLoadError
///
/// Distinguishes failure before entity selection from failure compiling one
/// selected accepted entity so integrity callers can retain entity identity.
///

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

        let root = Rc::new(AcceptedSchemaRuntimeRoot::compile(
            &self.db,
            identity,
            store_roots.clone(),
        )?);
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
