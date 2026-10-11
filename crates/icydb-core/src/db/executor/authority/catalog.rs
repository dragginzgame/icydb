//! Compiled accepted runtime authority shared by read and mutation executors.
//! Session lookup retains the root cache; schema remains the semantic owner.

use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        executor::EntityAuthority,
        identity::EntityName,
        registry::StoreHandle,
        runtime_entity_catalog::AcceptedRuntimeEntity,
        schema::{
            AcceptedCatalogIdentity, AcceptedEnumCatalog, AcceptedInspectionPlan,
            AcceptedSchemaRevision, AcceptedSchemaRuntimeRootIdentity,
            AcceptedSchemaRuntimeStoreRoot, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
            CompiledAcceptedRowConstraints, SchemaInfo, SchemaVersion,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};
use std::{collections::HashMap, rc::Rc};

///
/// AcceptedSchemaEntityRuntime
///
/// Immutable per-entity runtime state compiled exactly once for one accepted
/// database-wide root. It owns all derived query and row-decode authority used
/// by session execution.
///

#[derive(Debug)]
pub(in crate::db) struct AcceptedSchemaEntityRuntime {
    inspection_plan: AcceptedInspectionPlan,
    schema_info: Rc<SchemaInfo>,
    authority: EntityAuthority,
}

impl AcceptedSchemaEntityRuntime {
    pub(in crate::db) fn compile<C: CanisterKind>(
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
        let snapshot = selection.snapshot();
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
        let schema_info = Rc::new(SchemaInfo::from_accepted_snapshot_and_catalog(
            inspection_plan.snapshot(),
            inspection_plan.value_catalog().clone(),
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
pub(in crate::db) struct AcceptedSchemaRuntimeRoot {
    identity: AcceptedSchemaRuntimeRootIdentity,
    store_roots: Vec<AcceptedSchemaRuntimeStoreRoot>,
    entities: Vec<Rc<AcceptedSchemaEntityRuntime>>,
    entities_by_path: HashMap<Rc<str>, Rc<AcceptedSchemaEntityRuntime>>,
    entities_by_canonical_name: HashMap<EntityName, Rc<AcceptedSchemaEntityRuntime>>,
}

impl AcceptedSchemaRuntimeRoot {
    pub(in crate::db) fn compile<C: CanisterKind>(
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
    pub(in crate::db) const fn identity(&self) -> AcceptedSchemaRuntimeRootIdentity {
        self.identity
    }

    #[must_use]
    pub(in crate::db) fn matches_store_roots(
        &self,
        store_roots: &[AcceptedSchemaRuntimeStoreRoot],
    ) -> bool {
        self.store_roots == store_roots
    }

    pub(in crate::db) fn entity_for_runtime_entity(
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
    pub(in crate::db) fn entity_for_path(
        &self,
        entity_path: &str,
    ) -> Option<Rc<AcceptedSchemaEntityRuntime>> {
        self.entities_by_path.get(entity_path).cloned()
    }

    #[must_use]
    pub(in crate::db) fn entity_for_name(
        &self,
        entity_name: &str,
    ) -> Option<Rc<AcceptedSchemaEntityRuntime>> {
        let entity_name = entity_name.rsplit('.').next()?;
        let canonical_entity_name = EntityName::try_from_str(entity_name)
            .ok()?
            .ascii_case_fold();
        self.entities_by_canonical_name
            .get(&canonical_entity_name)
            .cloned()
    }

    #[must_use]
    pub(in crate::db) fn first_entity(&self) -> Option<Rc<AcceptedSchemaEntityRuntime>> {
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
    pub(in crate::db) const fn new(
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

pub(in crate::db) enum AcceptedInspectionPlanLoadError {
    Unselected(InternalError),
    Selected {
        identity: AcceptedCatalogIdentity,
        error: InternalError,
    },
}

impl AcceptedInspectionPlanLoadError {
    pub(in crate::db) fn into_internal(self) -> InternalError {
        match self {
            Self::Unselected(error) | Self::Selected { error, .. } => error,
        }
    }
}
